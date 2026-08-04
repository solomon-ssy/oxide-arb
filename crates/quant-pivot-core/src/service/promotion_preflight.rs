//! Side-effect-free, fail-closed promotion preflight orchestration.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::quant::{
        CandidateExplanationValidation, ModelCandidateManifestInfo, ModelVersionInfo,
        PromotionPermitInfo, PromotionPermitScope, PromotionPermitScopeInput,
        PromotionPermitStatus, PromotionPolicyProjection, PromotionPreflight,
        PromotionPreflightInput, PromotionServingConstraints, PromotionServingConstraintsInput,
    },
    enums::{common::MarketCategory, model::ModelFamily},
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{FeedbackCycleId, PromotionPermitId},
};
use quant_pivot_repository::traits::{
    FeedbackCycleRepository, ModelCandidateManifestRepository, PromotionPermitRepository,
};

use crate::{
    observability::metrics_hub::MetricsHub,
    service::{
        feedback_decision_stage::{FeedbackDecisionStageAdapter, PromotionDecisionEvidence},
        model_route_evidence::{ModelRouteEvidenceService, ModelRouteParityProof},
    },
};

/// Operator-selected authority limits for server-derived permit issuance.
///
/// Candidate, champion, profile, category, policy, and artifact identities are
/// deliberately absent and can only come from the verified feedback cycle.
#[derive(Debug, Clone)]
pub struct PromotionPreflightDraft {
    pub feedback_cycle_id: FeedbackCycleId,
    pub ttl_secs: u32,
}

/// Exact side-effect-free route projection and its content-addressed preflight.
#[derive(Debug, Clone)]
pub struct PromotionPreflightPlan {
    preflight: PromotionPreflight,
    projection: PromotionPolicyProjection,
}

impl PromotionPreflightPlan {
    #[must_use]
    pub const fn preflight(&self) -> &PromotionPreflight {
        &self.preflight
    }

    #[must_use]
    pub const fn projection(&self) -> &PromotionPolicyProjection {
        &self.projection
    }
}

/// Active persisted permit paired with a freshly recomputed exact preflight.
#[derive(Debug, Clone)]
pub struct VerifiedPromotionPreflight {
    permit: PromotionPermitInfo,
    plan: PromotionPreflightPlan,
}

impl VerifiedPromotionPreflight {
    #[must_use]
    pub const fn permit(&self) -> &PromotionPermitInfo {
        &self.permit
    }

    #[must_use]
    pub const fn preflight(&self) -> &PromotionPreflight {
        self.plan.preflight()
    }

    #[must_use]
    pub const fn projection(&self) -> &PromotionPolicyProjection {
        self.plan.projection()
    }
}

/// Persistence and runtime snapshots required for one promotion preflight.
pub struct PromotionPreflightServiceDeps {
    pub permits: Arc<dyn PromotionPermitRepository>,
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub decisions: Arc<FeedbackDecisionStageAdapter>,
    pub manifests: Arc<dyn ModelCandidateManifestRepository>,
    pub route_evidence: Arc<ModelRouteEvidenceService>,
    pub metrics: Arc<MetricsHub>,
}

/// Canonical server-side owner of permit drafting and promotion preflight.
pub struct PromotionPreflightService {
    deps: PromotionPreflightServiceDeps,
}

impl PromotionPreflightService {
    #[must_use]
    pub const fn new(deps: PromotionPreflightServiceDeps) -> Self {
        Self { deps }
    }

    /// Derive the exact permit scope and preflight from durable server truth.
    ///
    /// This operation is read-only. The governed P02 service remains the sole
    /// issuer and must persist this exact scope and preflight hash.
    pub async fn prepare_issue(
        &self,
        draft: PromotionPreflightDraft,
    ) -> QuantResult<PromotionPreflightPlan> {
        let database_now = self.deps.cycles.database_time().await?;
        if !(300..=3_600).contains(&draft.ttl_secs) {
            return Err(Self::invalid(
                "promotion permit TTL must be between 300 and 3600 seconds",
            ));
        }
        let expires_at = database_now
            .checked_add_signed(Duration::seconds(i64::from(draft.ttl_secs)))
            .ok_or_else(|| Self::invalid("promotion permit expiry overflowed"))?;
        Box::pin(self.resolve(draft.feedback_cycle_id, expires_at, database_now)).await
    }

    /// Load an active permit and recompute every preflight field from current
    /// durable/runtime truth before any promotion transaction may begin.
    pub async fn verify_permit(
        &self,
        permit_id: &PromotionPermitId,
        cycle_id: FeedbackCycleId,
    ) -> QuantResult<VerifiedPromotionPreflight> {
        let permit = self.deps.permits.load(permit_id).await?;
        let database_now = self.deps.cycles.database_time().await?;
        if permit.status_at(database_now)? != PromotionPermitStatus::Active {
            self.deps.metrics.record_feedback_permit_expiry();
            return Err(Self::invalid(
                "promotion permit is expired or revoked at the PostgreSQL clock",
            ));
        }
        let stored_scope = permit.scope()?;
        let plan =
            Box::pin(self.resolve(cycle_id, stored_scope.expires_at(), database_now)).await?;
        if plan.preflight.scope() != &stored_scope
            || plan.preflight.preflight_hash() != permit.preflight_hash
        {
            return Err(Self::invalid(
                "promotion permit scope or preflight hash differs from current server truth",
            ));
        }
        Ok(VerifiedPromotionPreflight { permit, plan })
    }

    async fn resolve(
        &self,
        cycle_id: FeedbackCycleId,
        expires_at: DateTime<Utc>,
        database_now: DateTime<Utc>,
    ) -> QuantResult<PromotionPreflightPlan> {
        if expires_at <= database_now {
            return Err(Self::invalid(
                "promotion authority expired while preflight was being resolved",
            ));
        }
        let evidence = self.deps.decisions.promotion_evidence(&cycle_id).await?;
        let bundle = self
            .deps
            .route_evidence
            .current_bundle()
            .await
            .map_err(Self::route_error)?;
        let category = evidence
            .shadow_contract
            .category_scope()
            .ok_or_else(|| Self::invalid("promotion candidate cannot use the pooled route"))?;
        if !matches!(category, MarketCategory::Crypto | MarketCategory::Weather) {
            return Err(Self::invalid(
                "promotion candidate must own an exact Crypto or Weather route",
            ));
        }
        let candidate_id = evidence.shadow_contract.candidate_model_version_id();
        let projection = PromotionPolicyProjection::try_new(&bundle, category, candidate_id)?;
        Self::verify_evidence(&evidence, &bundle, &projection)?;
        self.verify_generation(&evidence, category, projection.shadow_binding_generation())?;

        let champion = self
            .deps
            .route_evidence
            .load_model(evidence.shadow_contract.champion_model_version_id())
            .await?;
        let candidate = self
            .deps
            .route_evidence
            .load_model(candidate_id)
            .await
            .map_err(Self::route_error)?;
        self.verify_models(&evidence, category, &champion, &candidate)
            .await?;
        let parity = self
            .deps
            .route_evidence
            .parity_proof(&candidate)
            .await
            .map_err(Self::route_error)?;
        let manifest = self
            .require_manifest(&evidence, &candidate, category)
            .await?;
        let constraints = Self::serving_constraints(&candidate, &manifest, category, &parity)?;
        let constraints_hash = constraints.constraints_hash()?;
        let runtime = self
            .deps
            .route_evidence
            .current_runtime()
            .await
            .map_err(Self::route_error)?;
        let scope = PromotionPermitScope::try_new(PromotionPermitScopeInput {
            feedback_cycle_id: evidence.cycle.feedback_cycle_id,
            profile_ref: evidence.shadow_contract.profile_ref().clone(),
            category,
            expected_policy_generation: bundle.generation,
            expected_runtime_control_revision: runtime.revision,
            expected_decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            expected_snapshot_hash: bundle.snapshot_hash,
            expected_route_generation: projection.shadow_binding_generation(),
            champion_model_version_id: champion.model_version_id,
            champion_serving_contract_hash: champion.serving_contract_hash,
            candidate_model_version_id: constraints.candidate_model_version_id(),
            candidate_manifest_id: constraints.candidate_manifest_id(),
            candidate_manifest_hash: constraints.candidate_manifest_hash(),
            promotion_gate_hash: constraints.promotion_gate_hash(),
            allowed_runtime_modes: vec![runtime.quant_runtime_mode],
            non_route_policy_hash: projection.non_route_policy_hash(),
            serving_constraints_hash: constraints_hash,
            expires_at,
        })?;
        let preflight = PromotionPreflight::try_seal(PromotionPreflightInput {
            scope,
            feedback_cycle_id: evidence.cycle.feedback_cycle_id,
            cycle_idempotency_hash: evidence.cycle.idempotency_hash,
            decision_artifact_id: evidence.decision_artifact_id,
            decision_artifact_hash: evidence.decision_artifact_hash,
            decision_object_hash: evidence.decision_object_hash,
            decision_job_input_hash: evidence.decision_job_input_hash,
            shadow_artifact_id: evidence.shadow_artifact_id,
            shadow_artifact_hash: evidence.shadow_artifact_hash,
            shadow_object_hash: evidence.shadow_object_hash,
            shadow_contract_hash: evidence.shadow_contract.contract_hash(),
            candidate_recipe_hash: evidence.candidate_recipe_hash,
            serving_constraints: constraints,
            current_runtime_mode: runtime.quant_runtime_mode,
            runtime_control_revision: runtime.revision,
        })?;
        Ok(PromotionPreflightPlan {
            preflight,
            projection,
        })
    }

    fn verify_evidence(
        evidence: &PromotionDecisionEvidence,
        bundle: &ActivePolicyBundle,
        projection: &PromotionPolicyProjection,
    ) -> QuantResult<()> {
        let contract = &evidence.shadow_contract;
        if evidence.cycle.profile_ref != *contract.profile_ref()
            || evidence.cycle.feedback_policy_hash != contract.feedback_policy_hash()
            || evidence.cycle.champion_model_version_id != contract.champion_model_version_id()
            || evidence.cycle.champion_serving_contract_hash
                != contract.champion_serving_contract_hash()
            || contract.policy_bundle_generation() != bundle.generation
            || contract.decision_policy_snapshot_id() != bundle.decision_policy_snapshot_id
            || contract.decision_policy_snapshot_hash() != bundle.snapshot_hash
            || projection.champion_model_version_id() != contract.champion_model_version_id()
            || projection.candidate_model_version_id() != contract.candidate_model_version_id()
        {
            return Err(Self::invalid(
                "cycle, F10 contract, committed policy, champion, or candidate identity differs",
            ));
        }
        Ok(())
    }

    fn verify_generation(
        &self,
        evidence: &PromotionDecisionEvidence,
        category: MarketCategory,
        expected_route_generation: u64,
    ) -> QuantResult<()> {
        let route = BuyModelRoute::try_from(Some(category))
            .map_err(|error| Self::invalid(error.to_string()))?;
        let identity = self
            .deps
            .route_evidence
            .current_route(route)
            .ok_or_else(|| {
                Self::invalid(format!(
                    "current serving generation has no active route {route:?}"
                ))
            })?
            .published_shadow_identity()?;
        let contract = &evidence.shadow_contract;
        let binding = &evidence.shadow_binding;
        if binding.route != route
            || binding.committed_policy_generation != contract.policy_bundle_generation()
            || binding.committed_snapshot_id != contract.decision_policy_snapshot_id()
            || binding.committed_snapshot_hash != contract.decision_policy_snapshot_hash()
            || binding.champion_model_version_id != contract.champion_model_version_id()
            || binding.champion_serving_contract_hash != contract.champion_serving_contract_hash()
            || binding.candidate_model_version_id != contract.candidate_model_version_id()
            || binding.candidate_serving_contract_hash != contract.candidate_serving_contract_hash()
            || identity.route != route
            || identity.category_scope != Some(category)
            || identity.research_profile_artifact_id != contract.profile_ref().artifact_id()
            || identity.decision_policy_snapshot_id != contract.decision_policy_snapshot_id()
            || identity.decision_policy_snapshot_hash != contract.decision_policy_snapshot_hash()
            || identity.policy_bundle_generation != contract.policy_bundle_generation()
            || identity.champion_model_version_id != contract.champion_model_version_id()
            || identity.champion_serving_contract_hash != contract.champion_serving_contract_hash()
            || identity.candidate_model_version_id != contract.candidate_model_version_id()
            || identity.candidate_serving_contract_hash
                != contract.candidate_serving_contract_hash()
            || identity.route_generation != expected_route_generation
            || identity.route_generation != binding.binding_generation
            || identity.shadow_bound_at != binding.bound_at
            || identity.minimum_topn_decision_overlap != contract.minimum_topn_decision_overlap()
            || identity.required_shadow_window_secs != contract.required_window_secs()
        {
            return Err(Self::invalid(
                "ShadowBind/F10 contract differs from the atomically published serving generation",
            ));
        }
        Ok(())
    }

    async fn verify_models(
        &self,
        evidence: &PromotionDecisionEvidence,
        category: MarketCategory,
        champion: &ModelVersionInfo,
        candidate: &ModelVersionInfo,
    ) -> QuantResult<()> {
        champion
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        candidate
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let contract = &evidence.shadow_contract;
        if champion.model_version_id != contract.champion_model_version_id()
            || champion.serving_contract_hash != contract.champion_serving_contract_hash()
            || champion.profile_ref != *contract.profile_ref()
            || champion.category_scope != Some(category)
            || candidate.model_version_id != contract.candidate_model_version_id()
            || candidate.serving_contract_hash != contract.candidate_serving_contract_hash()
            || candidate.profile_ref != *contract.profile_ref()
            || candidate.category_scope != Some(category)
            || !matches!(
                candidate.model_family,
                ModelFamily::WeightedFactor | ModelFamily::ClassicalGradientBoostedTrees
            )
        {
            return Err(Self::invalid(
                "champion or candidate model projection differs from F10 serving evidence",
            ));
        }
        self.deps
            .route_evidence
            .load_runtime(champion)
            .await
            .map_err(Self::route_error)?;
        self.deps
            .route_evidence
            .load_runtime(candidate)
            .await
            .map_err(Self::route_error)?;
        Ok(())
    }

    fn serving_constraints(
        candidate: &ModelVersionInfo,
        manifest: &ModelCandidateManifestInfo,
        category: MarketCategory,
        parity: &ModelRouteParityProof,
    ) -> QuantResult<PromotionServingConstraints> {
        let training_dataset_id = candidate.training_dataset_id.ok_or_else(|| {
            Self::invalid("promotion candidate has no exact training dataset binding")
        })?;
        PromotionServingConstraints::try_new(PromotionServingConstraintsInput {
            candidate_model_version_id: candidate.model_version_id,
            candidate_manifest_id: manifest.manifest_id,
            candidate_manifest_hash: manifest.manifest_hash,
            promotion_gate_hash: manifest.promotion_gate_hash,
            candidate_model_spec_id: candidate.model_spec_id,
            candidate_model_family: candidate.model_family,
            candidate_artifact_hash: candidate.artifact_hash,
            candidate_serving_contract_hash: candidate.serving_contract_hash,
            candidate_model_spec_definition_hash: candidate.model_spec_definition_hash,
            candidate_training_dataset_id: training_dataset_id,
            feature_parity_run_id: parity.run_id,
            feature_parity_state_id: parity.state_id,
            feature_parity_evidence_hash: parity.evidence_hash,
            profile_ref: candidate.profile_ref.clone(),
            category,
        })
        .map_err(Into::into)
    }

    async fn require_manifest(
        &self,
        evidence: &PromotionDecisionEvidence,
        candidate: &ModelVersionInfo,
        category: MarketCategory,
    ) -> QuantResult<ModelCandidateManifestInfo> {
        let contract = candidate
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let bindings = contract.bindings();
        let explanation = CandidateExplanationValidation::try_from(bindings)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let manifest = self
            .deps
            .manifests
            .find_candidate(
                evidence.cycle.feedback_cycle_id,
                evidence.candidate_recipe_hash,
            )
            .await?
            .ok_or_else(|| {
                Self::invalid("CandidateReady cycle has no pre-shadow candidate readiness manifest")
            })?;
        manifest
            .validate()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let document = &manifest.document;
        let gate = &document.promotion_gate;
        let mismatches = [
            (
                document.feedback_cycle_id != evidence.cycle.feedback_cycle_id,
                "feedback_cycle_id",
            ),
            (
                document.candidate_recipe_hash != evidence.candidate_recipe_hash,
                "candidate_recipe_hash",
            ),
            (
                document.model_version_id != candidate.model_version_id,
                "model_version_id",
            ),
            (
                document.model_spec_id != candidate.model_spec_id,
                "model_spec_id",
            ),
            (
                document.model_family != candidate.model_family,
                "model_family",
            ),
            (
                document.model_artifact_hash != candidate.artifact_hash,
                "model_artifact_hash",
            ),
            (
                document.serving_contract_hash != candidate.serving_contract_hash,
                "serving_contract_hash",
            ),
            (
                document.training_dataset_id != bindings.dataset.manifest.training_dataset_id,
                "training_dataset_id",
            ),
            (document.profile_ref != candidate.profile_ref, "profile_ref"),
            (document.category != category, "category"),
            (
                document.feedback_policy_hash != evidence.cycle.feedback_policy_hash,
                "feedback_policy_hash",
            ),
            (
                document.decision_policy_snapshot_hash
                    != evidence.cycle.decision_policy_snapshot_hash,
                "decision_policy_snapshot_hash",
            ),
            (
                document.explanation_validation != explanation,
                "explanation_validation",
            ),
            (
                gate.truth_freeze_hash != evidence.dag.truth_freeze_hash,
                "truth_freeze_hash",
            ),
            (
                gate.attribution_manifest_hash != evidence.dag.attribution_manifest_hash,
                "attribution_manifest_hash",
            ),
            (
                gate.validation_artifact_hash != evidence.dag.validation_artifact_hash,
                "validation_artifact_hash",
            ),
            (
                gate.quality_gate_report_hash != evidence.dag.quality_gate_report_hash,
                "quality_gate_report_hash",
            ),
            (
                gate.comparison_artifact_hash != evidence.dag.comparison_artifact_hash,
                "comparison_artifact_hash",
            ),
            (
                gate.cpcv_path_set_id != evidence.dag.cpcv_path_set_id,
                "cpcv_path_set_id",
            ),
            (
                gate.cpcv_path_set_hash != evidence.dag.cpcv_path_set_hash,
                "cpcv_path_set_hash",
            ),
        ]
        .into_iter()
        .filter_map(|(mismatch, field)| mismatch.then_some(field))
        .collect::<Vec<_>>();
        if !mismatches.is_empty() {
            return Err(Self::invalid(format!(
                "pre-shadow candidate manifest differs from terminal CandidateReady evidence: {}",
                mismatches.join(", ")
            )));
        }
        Ok(manifest)
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidPromotionPreflight {
            detail: detail.into(),
        }
        .into()
    }

    fn route_error(error: QuantError) -> QuantError {
        match error {
            QuantError::Feedback(FeedbackError::InvalidModelRouteEvidence { detail }) => {
                Self::invalid(detail)
            }
            other => other,
        }
    }
}
