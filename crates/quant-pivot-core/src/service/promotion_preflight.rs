//! Side-effect-free, fail-closed promotion preflight orchestration.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        governance::RuntimeControlInfo,
        quant::{
            FrozenFeatureParitySubjectId, ModelVersionInfo, PromotionPermitInfo,
            PromotionPermitScope, PromotionPermitScopeInput, PromotionPermitStatus,
            PromotionPolicyProjection, PromotionPreflight, PromotionPreflightInput,
            PromotionServingConstraints, PromotionServingConstraintsInput,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
            PublicationStatus, QuantRuntimeMode,
        },
    },
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{
        ContentHash, FeatureParityRunId, FeatureParityStateId, FeedbackCycleId, ModelVersionId,
        PromotionPermitId,
    },
};
use quant_pivot_repository::traits::{
    FeatureParityRepository, FeedbackCycleRepository, ModelRegistryRepository, PolicyRepository,
    PromotionPermitRepository, RuntimeControlRepository,
};

use crate::{
    governance::RuntimeControlsHandle,
    runtime_config::DecisionPolicyStore,
    service::{
        feedback_decision_stage::{FeedbackDecisionStageAdapter, PromotionDecisionEvidence},
        model_serving_generation::ModelServingGenerationStore,
        model_serving_registry::ModelServingRuntimeRegistry,
    },
};

/// Operator-selected authority limits for server-derived permit issuance.
///
/// Candidate, champion, profile, category, policy, and artifact identities are
/// deliberately absent and can only come from the verified feedback cycle.
#[derive(Debug, Clone)]
pub struct PromotionPreflightDraft {
    pub feedback_cycle_id: FeedbackCycleId,
    pub allowed_runtime_modes: Vec<QuantRuntimeMode>,
    pub expires_at: DateTime<Utc>,
}

/// Exact side-effect-free route projection and its content-addressed preflight.
#[derive(Debug, Clone)]
pub struct PromotionPreflightPlan {
    preflight: PromotionPreflight,
    projection: PromotionPolicyProjection,
}

struct PromotionParityProof {
    run_id: FeatureParityRunId,
    state_id: FeatureParityStateId,
    evidence_hash: ContentHash,
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
    pub policies: Arc<dyn PolicyRepository>,
    pub durable_runtime: Arc<dyn RuntimeControlRepository>,
    pub runtime_controls: RuntimeControlsHandle,
    pub policy_store: Arc<DecisionPolicyStore>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub feature_parity: Arc<dyn FeatureParityRepository>,
    pub runtime_registry: Arc<ModelServingRuntimeRegistry>,
    pub serving_generations: Arc<ModelServingGenerationStore>,
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
        if draft.expires_at <= database_now {
            return Err(Self::invalid(
                "promotion permit expiry must be later than the PostgreSQL clock",
            ));
        }
        Box::pin(self.resolve(
            draft.feedback_cycle_id,
            draft.allowed_runtime_modes,
            draft.expires_at,
            database_now,
        ))
        .await
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
            return Err(Self::invalid(
                "promotion permit is expired or revoked at the PostgreSQL clock",
            ));
        }
        let stored_scope = permit.scope()?;
        let plan = Box::pin(self.resolve(
            cycle_id,
            stored_scope.allowed_runtime_modes().to_vec(),
            stored_scope.expires_at(),
            database_now,
        ))
        .await?;
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
        allowed_runtime_modes: Vec<QuantRuntimeMode>,
        expires_at: DateTime<Utc>,
        database_now: DateTime<Utc>,
    ) -> QuantResult<PromotionPreflightPlan> {
        if expires_at <= database_now {
            return Err(Self::invalid(
                "promotion authority expired while preflight was being resolved",
            ));
        }
        let evidence = self.deps.decisions.promotion_evidence(&cycle_id).await?;
        let bundle = self.current_bundle().await?;
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
        self.verify_generation(&evidence, category)?;

        let champion = self
            .load_model(evidence.shadow_contract.champion_model_version_id())
            .await?;
        let candidate = self.load_model(candidate_id).await?;
        self.verify_models(&evidence, category, &champion, &candidate)
            .await?;
        let parity = self.parity_proof(&candidate).await?;
        let constraints = Self::serving_constraints(&candidate, category, &parity)?;
        let constraints_hash = constraints.constraints_hash()?;
        let scope = PromotionPermitScope::try_new(PromotionPermitScopeInput {
            profile_ref: evidence.shadow_contract.profile_ref().clone(),
            category,
            expected_policy_generation: bundle.generation,
            expected_decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            expected_snapshot_hash: bundle.snapshot_hash,
            champion_model_version_id: champion.model_version_id,
            champion_serving_contract_hash: champion.serving_contract_hash,
            allowed_runtime_modes,
            non_route_policy_hash: projection.non_route_policy_hash(),
            serving_constraints_hash: constraints_hash,
            expires_at,
        })?;
        let runtime = self.current_runtime().await?;
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

    async fn current_bundle(&self) -> QuantResult<ActivePolicyBundle> {
        let durable = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("no committed policy bundle exists"))?;
        let local = self
            .deps
            .policy_store
            .active_bundle()
            .ok_or_else(|| Self::invalid("local committed policy bundle is unavailable"))?;
        let actual_hash = durable.snapshot.persistence_hash().map_err(|error| {
            Self::invalid(format!("committed policy hash projection failed: {error}"))
        })?;
        let validation = durable.snapshot.validate_runtime_config();
        if durable != local
            || actual_hash != durable.snapshot_hash
            || durable.revision_vector != durable.snapshot.revisions
            || validation.has_errors()
        {
            return Err(Self::invalid(format!(
                "durable/local policy bundle, hash, revisions, or validation differ: {validation}"
            )));
        }
        Ok(durable)
    }

    async fn current_runtime(&self) -> QuantResult<RuntimeControlInfo> {
        let durable = self.deps.durable_runtime.load().await?;
        let local = self.deps.runtime_controls.snapshot();
        if durable.quant_runtime_mode != local.quant_runtime_mode
            || durable.settlement_write_policy != local.settlement_write_policy
            || durable.kill_switch_state != local.kill_switch_state
            || durable.kill_switch_requires_ack != local.kill_switch_requires_ack
            || durable.revision != local.revision
            || durable.changed_by != local.changed_by
            || durable.reason != local.reason
            || durable.changed_at != local.changed_at
        {
            return Err(Self::invalid(
                "durable runtime control differs from the atomically applied local revision",
            ));
        }
        Ok(durable)
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
    ) -> QuantResult<()> {
        let route = BuyModelRoute::try_from(Some(category))
            .map_err(|error| Self::invalid(error.to_string()))?;
        let identity = self
            .deps
            .serving_generations
            .current_route(route)
            .ok_or_else(|| {
                Self::invalid(format!(
                    "current serving generation has no active route {route:?}"
                ))
            })?
            .published_shadow_identity()?;
        let contract = &evidence.shadow_contract;
        if identity.route != route
            || identity.category_scope != Some(category)
            || identity.research_profile_artifact_id != contract.profile_ref().artifact_id()
            || identity.decision_policy_snapshot_id != contract.decision_policy_snapshot_id()
            || identity.decision_policy_snapshot_hash != contract.decision_policy_snapshot_hash()
            || identity.policy_bundle_generation != contract.policy_bundle_generation()
            || identity.active_model_version_id != contract.champion_model_version_id()
            || identity.active_serving_contract_hash != contract.champion_serving_contract_hash()
            || identity.shadow_model_version_id != contract.candidate_model_version_id()
            || identity.shadow_serving_contract_hash != contract.candidate_serving_contract_hash()
            || identity.minimum_topn_overlap != contract.minimum_topn_overlap()
            || identity.required_shadow_window_secs != contract.required_window_secs()
        {
            return Err(Self::invalid(
                "F10 contract differs from the atomically published serving generation",
            ));
        }
        Ok(())
    }

    async fn load_model(&self, model_id: ModelVersionId) -> QuantResult<ModelVersionInfo> {
        self.deps
            .models
            .find_model_version(&model_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_model_version", model_id).into())
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
            || champion.publication_status != PublicationStatus::Published
            || candidate.model_version_id != contract.candidate_model_version_id()
            || candidate.serving_contract_hash != contract.candidate_serving_contract_hash()
            || candidate.profile_ref != *contract.profile_ref()
            || candidate.category_scope != Some(category)
            || candidate.model_family != ModelFamily::WeightedFactor
            || candidate.publication_status != PublicationStatus::Shadow
        {
            return Err(Self::invalid(
                "champion or candidate model projection differs from F10 serving evidence",
            ));
        }
        self.deps.runtime_registry.load(champion).await?;
        self.deps.runtime_registry.load(candidate).await?;
        Ok(())
    }

    fn serving_constraints(
        candidate: &ModelVersionInfo,
        category: MarketCategory,
        parity: &PromotionParityProof,
    ) -> QuantResult<PromotionServingConstraints> {
        let training_dataset_id = candidate.training_dataset_id.ok_or_else(|| {
            Self::invalid("promotion candidate has no exact training dataset binding")
        })?;
        PromotionServingConstraints::try_new(PromotionServingConstraintsInput {
            candidate_model_version_id: candidate.model_version_id,
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
            expected_publication_status: candidate.publication_status,
        })
        .map_err(Into::into)
    }

    async fn parity_proof(
        &self,
        candidate: &ModelVersionInfo,
    ) -> QuantResult<PromotionParityProof> {
        let training_dataset_id = candidate.training_dataset_id.ok_or_else(|| {
            Self::invalid("promotion candidate has no exact training dataset binding")
        })?;
        let contract = candidate
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let run = self
            .deps
            .feature_parity
            .latest_full_for_model(&candidate.model_version_id, &training_dataset_id)
            .await?
            .ok_or_else(|| Self::invalid("promotion candidate has no full parity proof"))?;
        let run_exact = run.kind == FeatureParityRunKind::Full
            && run.status == FeatureParityRunStatus::Passed
            && run.report_id.is_none()
            && run.model_version_id == Some(candidate.model_version_id)
            && run.training_dataset_id == Some(training_dataset_id)
            && run.total_count > 0
            && run.compared_count == run.total_count
            && run.matched_count == run.total_count
            && run.mismatched_count == 0
            && run.pending_materialization_count == 0
            && run.feature_contract_hash == Some(contract.bindings().schemas.feature_schema_hash)
            && run.transform_hash == Some(contract.bindings().transform.input_transform_hash)
            && run
                .finished_at
                .is_some_and(|finished_at| finished_at >= candidate.created_at)
            && run.failure_code.is_none()
            && run.failure_detail.is_none();
        if !run_exact {
            return Err(Self::invalid(
                "latest candidate full parity proof is not an exact passed publication proof",
            ));
        }
        let mut subjects = self
            .deps
            .feature_parity
            .load_frozen_subjects(&run.run_id)
            .await?;
        if subjects.len() != 1 {
            return Err(Self::invalid(
                "candidate full parity proof must have exactly one frozen subject",
            ));
        }
        let subject = subjects
            .pop()
            .ok_or_else(|| Self::invalid("candidate full parity subject disappeared"))?;
        let subject_exact = matches!(
            subject.subject_id,
            FrozenFeatureParitySubjectId::ModelVersion {
                model_version_id,
                training_dataset_id: subject_dataset_id,
            } if model_version_id == candidate.model_version_id
                && subject_dataset_id == training_dataset_id
        ) && subject.subject_generation == candidate.artifact_hash
            && subject.market_selection_id.is_none()
            && subject.decision_at.is_none()
            && subject.selection_hash.is_none()
            && subject.candidates.is_empty();
        if !subject_exact {
            return Err(Self::invalid(
                "candidate full parity subject differs from the immutable model artifact",
            ));
        }
        let state = self
            .deps
            .feature_parity
            .current_state()
            .await?
            .ok_or_else(|| Self::invalid("feature parity latch is uninitialized"))?;
        if state.state != FeatureParityLatchState::Clear {
            return Err(Self::invalid(
                "feature parity latch is not clear for promotion",
            ));
        }
        Ok(PromotionParityProof {
            run_id: run.run_id,
            state_id: state.state_id,
            evidence_hash: subject.evidence_hash,
        })
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidPromotionPreflight {
            detail: detail.into(),
        }
        .into()
    }
}
