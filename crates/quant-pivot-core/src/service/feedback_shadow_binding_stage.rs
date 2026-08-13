//! Coordinator-owned Comparison-to-ShadowBind boundary.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            CandidateRecipePlanOutcome, FeedbackComparisonArtifactRef,
            FeedbackLearningStageArtifactRef, FeedbackValidationArtifact, ShadowBindingArtifact,
            ShadowBindingJobInput, ShadowBindingJobParams,
        },
        quant::{
            BacktestPathSetInfo, CalibrationArtifactInfo, CandidateExplanationValidation,
            FeedbackCycleInfo, FeedbackStageJobIdentity, ModelCandidateManifestDocument,
            ModelCandidateManifestInfo, ModelCandidateManifestInput, ModelVersionInfo,
            NewModelCandidateManifest, NewResearchJob, PortfolioScenarioModelArtifact,
            PortfolioScenarioRouteModelLineage, PortfolioScenarioVisibility, PromotionGateArtifact,
            PromotionGateArtifactInput, RepresentedRouteSet, ResearchJobArtifactRef,
            ResearchJobInfo, ResearchJobResultRef, RouteCompatibilityDigests, RouteContractHash,
            scenario_model_bindings_hash,
        },
    },
    enums::quant::{
        FeedbackStage, FeedbackStageEventKind, ResearchJobKind, ResearchJobResultKind,
        ResearchJobStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{ActivePolicyBundle, BuyModelRoute, PortfolioScenarioModelArtifactBinding},
    types::{
        BacktestPathSetId, ContentHash, ModelVersionId, ResearchJobId, ResearchJobParams, RoleCode,
        model_lineage::ModelVersionDerivation,
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, CalibrationArtifactRepository, FeedbackCycleLeaseGuard,
    FeedbackCycleRepository, ModelCandidateManifestRepository, ModelCandidateManifestWriteOutcome,
    ModelRegistryRepository, PolicyRepository, ResearchJobRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_comparison::{FeedbackComparisonArtifact, FeedbackComparisonCodec},
    feedback_governance::FeedbackGovernanceCodec,
    feedback_learning::{
        FeedbackCpcvStageResult, FeedbackLearningStageArtifact, FeedbackLearningStageCodec,
        FeedbackLearningStageResults,
    },
    feedback_shadow_binding::ShadowBindingCodec,
    portfolio::{
        PortfolioScenarioGenerator, PortfolioScenarioMethodology, PortfolioScenarioModelFitInput,
        PortfolioScenarioModelFitter, PortfolioScenarioRouteFitInput,
    },
};
use uuid::Uuid;

use crate::service::{
    feedback_coordinator::FeedbackStageSuccess, feedback_recipe_stage::FeedbackRecipeStageAdapter,
    portfolio_context::PromotedRouteContract,
};

struct VerifiedComparison {
    reference: FeedbackComparisonArtifactRef,
    artifact: FeedbackComparisonArtifact,
}

struct VerifiedValidation {
    reference: ResearchJobArtifactRef,
    artifact: FeedbackValidationArtifact,
    cpcv: FeedbackLearningStageArtifactRef,
}

struct CandidateManifestEvidence {
    candidate_recipe_hash: ContentHash,
    cpcv_path_set_id: BacktestPathSetId,
    cpcv_path_set_hash: ContentHash,
    truth_freeze_hash: ContentHash,
    attribution_manifest_hash: ContentHash,
    validation_artifact_hash: ContentHash,
    quality_gate_report_hash: ContentHash,
    comparison_artifact_hash: ContentHash,
    portfolio_scenario_model_bindings: Vec<PortfolioScenarioModelArtifactBinding>,
    scenario_model_bindings_hash: ContentHash,
}

struct ScenarioContractSet {
    current: Vec<PromotedRouteContract>,
    prospective: Vec<PromotedRouteContract>,
    prospective_models: Vec<ModelVersionInfo>,
}

struct ScenarioEvidence {
    path_sets: Vec<BacktestPathSetInfo>,
    calibrations: Vec<CalibrationArtifactInfo>,
    model_lineages: Vec<PortfolioScenarioRouteModelLineage>,
}

#[derive(Clone, Copy)]
struct ScenarioRefitInput<'a> {
    cycle: &'a FeedbackCycleInfo,
    bundle: &'a ActivePolicyBundle,
    candidate: &'a ModelVersionInfo,
    candidate_path_set: &'a BacktestPathSetInfo,
    active_binding: &'a PortfolioScenarioModelArtifactBinding,
    bound_at: DateTime<Utc>,
}

pub(crate) struct VerifiedShadowBinding {
    pub job_id: ResearchJobId,
    pub reference: ResearchJobArtifactRef,
    pub params: ShadowBindingJobParams,
    pub artifact: ShadowBindingArtifact,
}

pub struct FeedbackShadowBindingStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub path_sets: Arc<dyn BacktestPathSetRepository>,
    pub calibrations: Arc<dyn CalibrationArtifactRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub manifests: Arc<dyn ModelCandidateManifestRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub recipes: Arc<FeedbackRecipeStageAdapter>,
    pub total_shadow_model_budget_bytes: u64,
    pub max_recovery_attempts: i32,
}

pub struct FeedbackShadowBindingStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    path_sets: Arc<dyn BacktestPathSetRepository>,
    calibrations: Arc<dyn CalibrationArtifactRepository>,
    policies: Arc<dyn PolicyRepository>,
    manifests: Arc<dyn ModelCandidateManifestRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    recipes: Arc<FeedbackRecipeStageAdapter>,
    total_shadow_model_budget_bytes: u64,
    max_recovery_attempts: i32,
}

impl FeedbackShadowBindingStageAdapter {
    pub fn try_new(deps: FeedbackShadowBindingStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 || deps.total_shadow_model_budget_bytes == 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "ShadowBind recovery and total memory budgets must be valid".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            models: deps.models,
            path_sets: deps.path_sets,
            calibrations: deps.calibrations,
            policies: deps.policies,
            manifests: deps.manifests,
            artifacts: deps.artifacts,
            recipes: deps.recipes,
            total_shadow_model_budget_bytes: deps.total_shadow_model_budget_bytes,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    pub async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, lease, identity)?;
        cycle.validate()?;
        let plan = self.recipes.load_plan(cycle).await?;
        let CandidateRecipePlanOutcome::Ready {
            candidate_family,
            selections,
        } = &plan.artifact.outcome
        else {
            return Err(Self::invalid(
                "ShadowBind cannot follow a RecipePlan NoAction",
            ));
        };
        let comparison = self.load_comparison(cycle).await?;
        if comparison.reference.candidate_family_hash != candidate_family.candidate_family_hash() {
            return Err(Self::invalid(
                "Comparison candidate family differs from RecipePlan",
            ));
        }
        let Some((selected_result, selected_replay)) = comparison.artifact.selected_candidate()
        else {
            return Err(Self::invalid(
                "ShadowBind cannot follow Comparison without an eligible challenger",
            ));
        };
        let selection = selections
            .iter()
            .find(|selection| {
                selection.candidate_recipe_hash == selected_result.candidate_recipe_hash
            })
            .ok_or_else(|| {
                Self::invalid("selected Comparison challenger is absent from RecipePlan")
            })?;
        let validation = self
            .load_validation(cycle, selected_result.candidate_recipe_hash)
            .await?;
        let candidate_gate = validation
            .artifact
            .candidates
            .iter()
            .find(|candidate| {
                candidate.candidate_recipe_hash == selected_result.candidate_recipe_hash
            })
            .ok_or_else(|| Self::invalid("selected challenger is absent from Validation"))?;
        if !candidate_gate.is_comparison_eligible()
            || candidate_gate.model_version_id != selected_replay.model_version_id
        {
            return Err(Self::invalid(
                "selected challenger did not pass the exact Validation gate",
            ));
        }
        let (path_set_id, path_set_hash) = self
            .load_cpcv(
                cycle,
                &validation.cpcv,
                selected_result.candidate_recipe_hash,
                selected_replay.model_version_id,
            )
            .await?;
        let candidate = self
            .models
            .find_model_version(&selected_replay.model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_model_version", selected_replay.model_version_id)
            })?;
        let truth_hash = self
            .load_governance_hash(cycle, FeedbackStage::TruthFreeze)
            .await?;
        let attribution_hash = self
            .load_governance_hash(cycle, FeedbackStage::Attribution)
            .await?;
        let bundle = self
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("ShadowBind has no active policy bundle"))?;
        let expected_route_generation = Self::expected_route_generation(cycle, &bundle)?;
        let prepared_at = self.cycles.database_time().await?;
        let portfolio_scenario_model_bindings = self
            .fit_scenario_models(
                cycle,
                &bundle,
                &candidate,
                path_set_id,
                path_set_hash,
                prepared_at,
            )
            .await?;
        let scenario_model_bindings_hash =
            scenario_model_bindings_hash(&portfolio_scenario_model_bindings)
                .map_err(|error| Self::invalid(error.to_string()))?;
        let manifest = self
            .ensure_manifest(
                cycle,
                &candidate,
                CandidateManifestEvidence {
                    candidate_recipe_hash: selected_result.candidate_recipe_hash,
                    cpcv_path_set_id: path_set_id,
                    cpcv_path_set_hash: path_set_hash,
                    truth_freeze_hash: truth_hash,
                    attribution_manifest_hash: attribution_hash,
                    validation_artifact_hash: validation.reference.content_hash,
                    quality_gate_report_hash: candidate_gate.quality_gate_report.report_hash,
                    comparison_artifact_hash: comparison.artifact.artifact_hash(),
                    portfolio_scenario_model_bindings,
                    scenario_model_bindings_hash,
                },
            )
            .await?;
        let model_routing_revision_id = bundle
            .revision_vector
            .model_routing
            .ok_or_else(|| Self::invalid("active bundle has no ModelRouting revision"))?;
        let training_dataset_id = candidate
            .training_dataset_id
            .ok_or_else(|| Self::invalid("shadow candidate has no training dataset"))?;
        let params = ShadowBindingJobParams::try_new(ShadowBindingJobInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            prepared_at,
            profile_ref: cycle.profile_ref.clone(),
            route: cycle.route,
            comparison: comparison.reference,
            candidate_recipe_hash: selected_result.candidate_recipe_hash,
            champion_model_version_id: cycle.champion_model_version_id,
            champion_serving_contract_hash: cycle.champion_serving_contract_hash,
            candidate_model_version_id: candidate.model_version_id,
            candidate_artifact_hash: candidate.artifact_hash,
            candidate_serving_contract_hash: candidate.serving_contract_hash,
            candidate_manifest_id: manifest.manifest_id,
            candidate_manifest_hash: manifest.manifest_hash,
            candidate_training_dataset_id: training_dataset_id,
            expected_policy_generation: bundle.generation,
            expected_snapshot_id: bundle.decision_policy_snapshot_id,
            expected_snapshot_hash: bundle.snapshot_hash,
            expected_model_routing_revision_id: model_routing_revision_id,
            expected_route_generation,
            reserved_model_bytes: selection.template.resource_budget.max_resident_model_bytes,
            total_shadow_model_budget_bytes: self.total_shadow_model_budget_bytes,
        })?;
        self.bind_job(cycle, identity, params)
    }

    fn expected_route_generation(
        cycle: &FeedbackCycleInfo,
        bundle: &ActivePolicyBundle,
    ) -> QuantResult<u64> {
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(cycle.route)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let expected = u64::try_from(cycle.route_generation)
            .map_err(|error| Self::invalid(format!("route generation overflow: {error}")))?;
        let active_policy_generation = bundle.generation;
        let expected_policy_generation = cycle.policy_bundle_generation;
        let active_snapshot_id = bundle.decision_policy_snapshot_id;
        let expected_snapshot_id = cycle.decision_policy_snapshot_id;
        let active_snapshot_hash = bundle.snapshot_hash;
        let expected_snapshot_hash = cycle.decision_policy_snapshot_hash;
        let active_champion_id = route.champion.model_version_id;
        let expected_champion_id = cycle.champion_model_version_id;
        if active_policy_generation != expected_policy_generation
            || active_snapshot_id != expected_snapshot_id
            || active_snapshot_hash != expected_snapshot_hash
            || active_champion_id != expected_champion_id
            || route.champion.generation != expected
            || route.shadow.is_some()
        {
            return Err(Self::invalid(
                "ShadowBind policy, champion, generation, or route slot is stale",
            ));
        }
        Ok(expected)
    }

    pub async fn succeeded(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let verified = self.verify_job(cycle, job, None).await?;
        Ok(FeedbackStageSuccess::advance(
            verified.reference.uri,
            verified.reference.content_hash,
        ))
    }

    pub(crate) async fn load_binding(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<VerifiedShadowBinding> {
        let (job, reference) = self
            .load_stage_job(
                cycle,
                FeedbackStage::ShadowBind,
                ResearchJobKind::FeedbackShadowBind,
                ResearchJobResultKind::ShadowBindingArtifact,
            )
            .await?;
        self.verify_job(cycle, &job, Some(reference)).await
    }

    async fn verify_job(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        known_reference: Option<ResearchJobArtifactRef>,
    ) -> QuantResult<VerifiedShadowBinding> {
        let ResearchJobParams::FeedbackShadowBind(params) = &job.params_json else {
            return Err(Self::invalid("ShadowBind job lost its typed parameters"));
        };
        params.validate()?;
        let reference = match known_reference {
            Some(reference) => reference,
            None => Self::require_job_result(
                cycle,
                job,
                FeedbackStage::ShadowBind,
                ResearchJobKind::FeedbackShadowBind,
                ResearchJobResultKind::ShadowBindingArtifact,
                params.artifact_id.as_uuid(),
            )?,
        };
        let bytes = self.artifacts.get(&reference.uri).await?;
        if CanonicalDigest::content_hash_bytes(&bytes) != reference.content_hash {
            return Err(Self::invalid(
                "ShadowBind bytes differ from their terminal hash",
            ));
        }
        let artifact = ShadowBindingCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        Ok(VerifiedShadowBinding {
            job_id: job.job_id,
            reference,
            params: params.as_ref().clone(),
            artifact,
        })
    }

    async fn load_comparison(&self, cycle: &FeedbackCycleInfo) -> QuantResult<VerifiedComparison> {
        let (job, artifact_ref) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Comparison,
                ResearchJobKind::FeedbackComparison,
                ResearchJobResultKind::FeedbackComparisonArtifact,
            )
            .await?;
        let ResearchJobParams::FeedbackComparison(params) = &job.params_json else {
            return Err(Self::invalid(
                "Comparison predecessor lost its typed parameters",
            ));
        };
        params.validate()?;
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackComparisonCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Comparison predecessor bytes differ from their terminal hash",
            ));
        }
        let artifact = FeedbackComparisonCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        Ok(VerifiedComparison {
            reference: FeedbackComparisonArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: job.job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash()?,
                candidate_family_hash: params.candidate_family_hash,
                decision_policy_snapshot_id: params.decision_policy_snapshot_id,
                artifact: artifact_ref,
            },
            artifact,
        })
    }

    async fn load_validation(
        &self,
        cycle: &FeedbackCycleInfo,
        candidate_recipe_hash: ContentHash,
    ) -> QuantResult<VerifiedValidation> {
        let (job, reference) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Validation,
                ResearchJobKind::FeedbackValidation,
                ResearchJobResultKind::FeedbackValidationArtifact,
            )
            .await?;
        let ResearchJobParams::FeedbackValidation(params) = &job.params_json else {
            return Err(Self::invalid(
                "Validation predecessor lost its typed parameters",
            ));
        };
        params.validate()?;
        let bytes = self.artifacts.get(&reference.uri).await?;
        if FeedbackGovernanceCodec::bytes_hash(&bytes) != reference.content_hash {
            return Err(Self::invalid(
                "Validation predecessor bytes differ from their terminal hash",
            ));
        }
        let artifact = FeedbackGovernanceCodec::decode_validation(&bytes)?;
        let cycle_identity_exact = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_identity_exact
            || artifact.input_hash != params.input_hash()?
            || !artifact
                .candidates
                .iter()
                .any(|candidate| candidate.candidate_recipe_hash == candidate_recipe_hash)
        {
            return Err(Self::invalid(
                "Validation artifact differs from cycle, job, or selected challenger",
            ));
        }
        Ok(VerifiedValidation {
            reference,
            artifact,
            cpcv: params.cpcv.clone(),
        })
    }

    async fn load_cpcv(
        &self,
        cycle: &FeedbackCycleInfo,
        reference: &FeedbackLearningStageArtifactRef,
        candidate_recipe_hash: ContentHash,
        model_version_id: ModelVersionId,
    ) -> QuantResult<(BacktestPathSetId, ContentHash)> {
        reference.validate_for(cycle.feedback_cycle_id, FeedbackStage::Cpcv)?;
        let job = self
            .jobs
            .find_by_id(&reference.job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", reference.job_id))?;
        if job.status != ResearchJobStatus::Succeeded
            || job.kind != ResearchJobKind::FeedbackCpcv
            || job.result_artifact().as_ref() != Some(&reference.artifact)
        {
            return Err(Self::invalid(
                "CPCV reference differs from its terminal research job",
            ));
        }
        let bytes = self.artifacts.get(&reference.artifact.uri).await?;
        if CanonicalDigest::content_hash_bytes(&bytes) != reference.artifact.content_hash {
            return Err(Self::invalid(
                "CPCV bytes differ from the Validation reference",
            ));
        }
        let artifact = FeedbackLearningStageCodec::decode(&bytes)?;
        Self::require_learning_reference(&artifact, reference)?;
        let FeedbackLearningStageResults::Cpcv(results) = &artifact.results else {
            return Err(Self::invalid(
                "Validation CPCV reference is not CPCV evidence",
            ));
        };
        let result = results
            .iter()
            .find(|result| result.candidate_recipe_hash() == candidate_recipe_hash)
            .ok_or_else(|| Self::invalid("selected challenger is absent from CPCV"))?;
        let FeedbackCpcvStageResult::Evaluated {
            model_version_id: evaluated_model,
            path_set_id,
            path_set_hash,
            ..
        } = result
        else {
            return Err(Self::invalid(
                "selected challenger has no evaluated CPCV path set",
            ));
        };
        if *evaluated_model != model_version_id {
            return Err(Self::invalid("selected CPCV model differs from Comparison"));
        }
        Ok((*path_set_id, *path_set_hash))
    }

    fn require_learning_reference(
        artifact: &FeedbackLearningStageArtifact,
        reference: &FeedbackLearningStageArtifactRef,
    ) -> QuantResult<()> {
        if artifact.feedback_cycle_id != reference.feedback_cycle_id
            || artifact.artifact_id != reference.artifact_id
            || artifact.input_hash != reference.input_hash
            || artifact.results.stage() != reference.stage
        {
            return Err(Self::invalid(
                "CPCV artifact differs from its immutable reference",
            ));
        }
        Ok(())
    }

    async fn load_governance_hash(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
    ) -> QuantResult<ContentHash> {
        let (kind, result_kind) = match stage {
            FeedbackStage::TruthFreeze => (
                ResearchJobKind::FeedbackTruthFreeze,
                ResearchJobResultKind::FeedbackTruthFreezeArtifact,
            ),
            FeedbackStage::Attribution => (
                ResearchJobKind::FeedbackAttribution,
                ResearchJobResultKind::FeedbackAttributionManifest,
            ),
            _ => {
                return Err(Self::invalid(format!(
                    "{stage} is not candidate-manifest governance evidence"
                )));
            }
        };
        let (job, reference) = self.load_stage_job(cycle, stage, kind, result_kind).await?;
        let bytes = self.artifacts.get(&reference.uri).await?;
        if FeedbackGovernanceCodec::bytes_hash(&bytes) != reference.content_hash {
            return Err(Self::invalid(format!(
                "{stage} bytes differ from their terminal hash"
            )));
        }
        match (stage, &job.params_json) {
            (FeedbackStage::TruthFreeze, ResearchJobParams::FeedbackTruthFreeze(params)) => {
                let artifact = FeedbackGovernanceCodec::decode_truth(&bytes)?;
                if artifact.feedback_cycle_id != cycle.feedback_cycle_id
                    || artifact.input_hash != params.input_hash()?
                    || !artifact.blockers.is_empty()
                {
                    return Err(Self::invalid(
                        "TruthFreeze candidate-manifest evidence is incomplete",
                    ));
                }
            }
            (FeedbackStage::Attribution, ResearchJobParams::FeedbackAttribution(params)) => {
                let artifact = FeedbackGovernanceCodec::decode_attribution(&bytes)?;
                if artifact.feedback_cycle_id != cycle.feedback_cycle_id
                    || artifact.input_hash != params.input_hash()?
                {
                    return Err(Self::invalid(
                        "Attribution candidate-manifest evidence differs from its job",
                    ));
                }
            }
            _ => {
                return Err(Self::invalid(format!(
                    "{stage} job lost its typed parameters"
                )));
            }
        }
        Ok(reference.content_hash)
    }

    async fn fit_scenario_models(
        &self,
        cycle: &FeedbackCycleInfo,
        bundle: &ActivePolicyBundle,
        candidate: &ModelVersionInfo,
        candidate_path_set_id: BacktestPathSetId,
        candidate_path_set_hash: ContentHash,
        bound_at: DateTime<Utc>,
    ) -> QuantResult<Vec<PortfolioScenarioModelArtifactBinding>> {
        if cycle.route.category().is_none() {
            return Err(Self::invalid(
                "ResearchOnly pooled Route cannot enter governed champion promotion",
            ));
        }
        let candidate_path_set = self
            .path_sets
            .find_by_id(&candidate_path_set_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_backtest_path_set", candidate_path_set_id)
            })?;
        Self::verify_path_set(
            &candidate_path_set,
            candidate,
            candidate_path_set_hash,
            cycle.label_cutoff,
        )?;
        let mut affected = bundle
            .snapshot
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .iter()
            .filter(|binding| binding.ordered_routes.contains(&cycle.route))
            .cloned()
            .collect::<Vec<_>>();
        affected.sort_by_key(|binding| {
            (
                binding.route_set_digest,
                binding.model_content_hash,
                binding.portfolio_scenario_model_artifact_id.as_uuid(),
            )
        });
        if affected.is_empty() {
            return Err(Self::invalid(
                "promotion Route has no active scenario-model binding to refit",
            ));
        }
        let mut fitted_bindings = Vec::with_capacity(affected.len());
        for active_binding in affected {
            fitted_bindings.push(
                self.refit_scenario_binding(ScenarioRefitInput {
                    cycle,
                    bundle,
                    candidate,
                    candidate_path_set: &candidate_path_set,
                    active_binding: &active_binding,
                    bound_at,
                })
                .await?,
            );
        }
        fitted_bindings.sort_by_key(|binding| {
            (
                binding.route_set_digest,
                binding.model_content_hash,
                binding.portfolio_scenario_model_artifact_id.as_uuid(),
            )
        });
        Ok(fitted_bindings)
    }

    async fn refit_scenario_binding(
        &self,
        input: ScenarioRefitInput<'_>,
    ) -> QuantResult<PortfolioScenarioModelArtifactBinding> {
        let represented =
            RepresentedRouteSet::from_routes(input.active_binding.ordered_routes.clone())
                .map_err(|error| Self::invalid(error.to_string()))?;
        if represented.routes != input.active_binding.ordered_routes
            || represented.digest != input.active_binding.route_set_digest
        {
            return Err(Self::invalid(
                "active scenario-model binding has a non-canonical Route set",
            ));
        }
        let promoted_template = self
            .load_scenario_template(
                input.active_binding,
                &represented,
                input.cycle.label_cutoff,
                input.cycle.created_at,
            )
            .await?;
        let contracts = self.scenario_contracts(input, &represented).await?;
        let current_compatibility = Self::compatibility(&represented, &contracts.current)?;
        if input.active_binding.serving_contract_digest
            != current_compatibility.serving_contract_digest
            || input.active_binding.calibration_contract_digest
                != current_compatibility.calibration_contract_digest
            || input.active_binding.trade_policy_contract_digest
                != current_compatibility.trade_policy_contract_digest
            || promoted_template.serving_contract_digest
                != current_compatibility.serving_contract_digest
            || promoted_template.calibration_contract_digest
                != current_compatibility.calibration_contract_digest
            || promoted_template.trade_policy_contract_digest
                != current_compatibility.trade_policy_contract_digest
        {
            return Err(Self::invalid(
                "active scenario model differs from current Route serving contracts",
            ));
        }
        let methodology = PortfolioScenarioMethodology::from_promoted(&promoted_template)?;
        let compatibility = Self::compatibility(&represented, &contracts.prospective)?;
        let evidence = self
            .scenario_evidence(input, &represented, &contracts)
            .await?;
        let route_inputs = contracts
            .prospective
            .iter()
            .zip(&evidence.path_sets)
            .zip(&evidence.calibrations)
            .zip(&evidence.model_lineages)
            .map(|(((contract, path_set), calibration), model_lineage)| {
                Ok(PortfolioScenarioRouteFitInput {
                    route: contract.route,
                    model_lineage: *model_lineage,
                    calibration_artifact_id: contract.calibration_artifact_id,
                    calibration_artifact_hash: contract.calibration_contract_hash,
                    trade_policy_contract_hash: contract.trade_policy_contract_hash,
                    prediction_horizon_secs: u64::try_from(contract.prediction_horizon_secs)
                        .map_err(|error| {
                            Self::invalid(format!(
                                "Route prediction horizon does not fit u64: {error}"
                            ))
                        })?,
                    path_set,
                    calibration,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let fitted = PortfolioScenarioModelFitter::fit(&PortfolioScenarioModelFitInput {
            methodology: &methodology,
            represented_routes: &represented,
            compatibility,
            routes: route_inputs,
            bound_at: input.bound_at,
        })?;
        if fitted.binding.model_content_hash == input.active_binding.model_content_hash
            || fitted.binding.portfolio_scenario_model_artifact_id
                == input.active_binding.portfolio_scenario_model_artifact_id
        {
            return Err(Self::invalid(
                "scenario refit did not produce a new content-addressed artifact",
            ));
        }
        self.persist_scenario_model(&fitted.artifact).await?;
        Ok(fitted.binding)
    }

    async fn scenario_contracts(
        &self,
        input: ScenarioRefitInput<'_>,
        represented: &RepresentedRouteSet,
    ) -> QuantResult<ScenarioContractSet> {
        let mut current = Vec::with_capacity(represented.routes.len());
        let mut prospective = Vec::with_capacity(represented.routes.len());
        let mut prospective_models = Vec::with_capacity(represented.routes.len());
        for route in &represented.routes {
            let current_model_id = input
                .bundle
                .snapshot
                .model_routing
                .model
                .route_binding(*route)
                .map_err(|error| Self::invalid(error.to_string()))?
                .champion
                .model_version_id;
            let current_model = self
                .models
                .find_model_version(&current_model_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_model_version", current_model_id))?;
            current.push(PromotedRouteContract::from_version(*route, &current_model)?);
            let model = if *route == input.cycle.route {
                input.candidate.clone()
            } else {
                current_model
            };
            prospective.push(PromotedRouteContract::from_version(*route, &model)?);
            prospective_models.push(model);
        }
        Ok(ScenarioContractSet {
            current,
            prospective,
            prospective_models,
        })
    }

    async fn scenario_evidence(
        &self,
        input: ScenarioRefitInput<'_>,
        represented: &RepresentedRouteSet,
        contracts: &ScenarioContractSet,
    ) -> QuantResult<ScenarioEvidence> {
        let mut path_sets = Vec::with_capacity(represented.routes.len());
        let mut calibrations = Vec::with_capacity(represented.routes.len());
        let mut model_lineages = Vec::with_capacity(represented.routes.len());
        for ((route, model), contract) in represented
            .routes
            .iter()
            .zip(&contracts.prospective_models)
            .zip(&contracts.prospective)
        {
            let path_set = if *route == input.cycle.route {
                input.candidate_path_set.clone()
            } else {
                self.select_route_path_set(model, input.cycle.label_cutoff)
                    .await?
            };
            Self::verify_path_set(
                &path_set,
                model,
                path_set.path_set_hash,
                input.cycle.label_cutoff,
            )?;
            let calibration = self
                .calibrations
                .find_by_id(&contract.calibration_artifact_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found(
                        "quant_calibration_artifact",
                        contract.calibration_artifact_id,
                    )
                })?;
            if calibration.content_hash != contract.calibration_contract_hash {
                return Err(Self::invalid(
                    "scenario Route calibration bytes differ from serving lineage",
                ));
            }
            let calibration_source = self
                .models
                .find_model_version(&contract.calibration_source_model_version_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found(
                        "quant_model_version",
                        contract.calibration_source_model_version_id,
                    )
                })?;
            let model_lineage = Self::verify_calibration_edge(
                *route,
                model,
                &calibration_source,
                contract,
                &calibration,
            )?;
            path_sets.push(path_set);
            calibrations.push(calibration);
            model_lineages.push(model_lineage);
        }
        Ok(ScenarioEvidence {
            path_sets,
            calibrations,
            model_lineages,
        })
    }

    async fn load_scenario_template(
        &self,
        binding: &PortfolioScenarioModelArtifactBinding,
        represented: &RepresentedRouteSet,
        decision_at: DateTime<Utc>,
        governance_frozen_at: DateTime<Utc>,
    ) -> QuantResult<PortfolioScenarioModelArtifact> {
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            binding.portfolio_scenario_model_artifact_id.to_string(),
            "json",
        )?;
        let bytes = self.artifacts.get_by_key(&key).await?;
        let model = serde_json::from_slice::<PortfolioScenarioModelArtifact>(&bytes)
            .map_err(|error| Self::invalid(format!("decode scenario model template: {error}")))?;
        PortfolioScenarioGenerator::verify_model(
            binding,
            &model,
            represented,
            decision_at,
            PortfolioScenarioVisibility::HistoricalReplay {
                governance_frozen_at,
            },
        )?;
        Ok(model)
    }

    async fn select_route_path_set(
        &self,
        model: &ModelVersionInfo,
        visible_at: DateTime<Utc>,
    ) -> QuantResult<BacktestPathSetInfo> {
        let mut path_sets = self
            .path_sets
            .list_by_model_version(&model.model_version_id)
            .await?
            .into_iter()
            .filter(|path_set| path_set.window_end <= visible_at)
            .collect::<Vec<_>>();
        path_sets.sort_by_key(|path_set| {
            (
                path_set.window_end,
                path_set.created_at,
                path_set.path_set_id.as_uuid(),
            )
        });
        path_sets.pop().ok_or_else(|| {
            Self::invalid(format!(
                "Route champion {} has no PIT-visible CPCV path set",
                model.model_version_id
            ))
        })
    }

    fn verify_path_set(
        path_set: &BacktestPathSetInfo,
        model: &ModelVersionInfo,
        expected_hash: ContentHash,
        visible_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        path_set
            .verify_hash()
            .map_err(|error| Self::invalid(error.to_string()))?;
        if path_set.model_version_id != model.model_version_id
            || path_set.path_set_hash != expected_hash
            || path_set.window_end > visible_at
            || path_set.subject.model_artifact_hash != model.artifact_hash
            || path_set.subject.serving_contract_hash != model.serving_contract_hash
            || model.training_dataset_id != Some(path_set.training_dataset_id)
        {
            return Err(Self::invalid(
                "CPCV path set differs from exact model, Dataset, or PIT cutoff",
            ));
        }
        Ok(())
    }

    fn verify_calibration_edge(
        route: BuyModelRoute,
        evaluated: &ModelVersionInfo,
        source: &ModelVersionInfo,
        contract: &PromotedRouteContract,
        calibration: &CalibrationArtifactInfo,
    ) -> QuantResult<PortfolioScenarioRouteModelLineage> {
        let evaluated_serving = evaluated
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let source_serving = source
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let evaluated_bindings = evaluated_serving.bindings();
        let source_bindings = source_serving.bindings();
        let fit = &calibration
            .verify_model_score()
            .map_err(Self::invalid)?
            .fit_contract
            .model;
        let source_derivation = source
            .verified_derivation()
            .map_err(|error| Self::invalid(error.to_string()))?;
        if contract.route != route
            || contract.model_version_id != evaluated.model_version_id
            || contract.serving_contract_hash != evaluated.serving_contract_hash
            || contract.calibration_source_model_version_id != source.model_version_id
            || source_derivation != ModelVersionDerivation::Training
            || evaluated.model_version_id == source.model_version_id
            || evaluated.artifact_hash == source.artifact_hash
            || evaluated.serving_contract_hash == source.serving_contract_hash
            || fit.model_version_id != source.model_version_id
            || fit.artifact_hash != source.artifact_hash
            || fit.serving_contract_hash != source.serving_contract_hash
            || fit.model_spec_id != source.model_spec_id
            || fit.model_spec_definition_hash != source.model_spec_definition_hash
            || fit.model_family != source.model_family
            || fit.profile_ref != source.profile_ref
            || fit.category_scope != source.category_scope
            || i64::try_from(fit.prediction_horizon_secs).ok()
                != Some(source.model_spec_prediction_horizon_secs)
            || Some(fit.training_dataset_id) != source.training_dataset_id
            || fit.training_dataset_hash != source_bindings.transform.training_dataset_hash
            || evaluated.model_spec_id != source.model_spec_id
            || evaluated.model_family != source.model_family
            || evaluated.category_scope != source.category_scope
            || evaluated.profile_ref != source.profile_ref
            || evaluated.model_spec_prediction_horizon_secs
                != source.model_spec_prediction_horizon_secs
            || evaluated.training_dataset_id != source.training_dataset_id
            || evaluated_bindings.transform.training_input_hash
                != source_bindings.transform.training_input_hash
            || evaluated_bindings.transform.training_dataset_hash
                != source_bindings.transform.training_dataset_hash
            || source_bindings.model.calibration.is_some()
            || evaluated_bindings
                .model
                .calibration
                .as_ref()
                .is_none_or(|binding| {
                    binding.artifact_id != calibration.artifact_id
                        || binding.content_hash != calibration.content_hash
                })
            || evaluated_bindings.trade_policy != source_bindings.trade_policy
        {
            return Err(Self::invalid(
                "scenario calibration source, calibrated serving model, or derivation edge differs from its immutable contract",
            ));
        }
        Ok(PortfolioScenarioRouteModelLineage {
            evaluated_model_version_id: evaluated.model_version_id,
            evaluated_model_artifact_hash: evaluated.artifact_hash,
            evaluated_serving_contract_hash: evaluated.serving_contract_hash,
            calibration_source_model_version_id: source.model_version_id,
            calibration_source_model_artifact_hash: source.artifact_hash,
            calibration_source_serving_contract_hash: source.serving_contract_hash,
        })
    }

    fn compatibility(
        represented: &RepresentedRouteSet,
        contracts: &[PromotedRouteContract],
    ) -> QuantResult<RouteCompatibilityDigests> {
        RouteCompatibilityDigests::try_new(
            represented,
            &contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.serving_contract_hash,
                })
                .collect::<Vec<_>>(),
            &contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.calibration_contract_hash,
                })
                .collect::<Vec<_>>(),
            &contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.trade_policy_contract_hash,
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|error| Self::invalid(error.to_string()))
    }

    async fn persist_scenario_model(
        &self,
        artifact: &PortfolioScenarioModelArtifact,
    ) -> QuantResult<()> {
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            artifact.portfolio_scenario_model_artifact_id.to_string(),
            "json",
        )?;
        let bytes = serde_json::to_vec(artifact)
            .map_err(|error| Self::invalid(format!("encode scenario model: {error}")))?;
        if self.artifacts.exists_by_key(&key).await? {
            let stored = self.artifacts.get_by_key(&key).await?;
            if stored != bytes {
                return Err(Self::invalid(
                    "content-addressed scenario-model key contains different bytes",
                ));
            }
        } else {
            self.artifacts.put(key.clone(), &bytes).await?;
        }
        let stored = self.artifacts.get_by_key(&key).await?;
        let decoded = serde_json::from_slice::<PortfolioScenarioModelArtifact>(&stored)
            .map_err(|error| Self::invalid(format!("re-read scenario model: {error}")))?;
        if decoded != *artifact || decoded.recomputed_hash()? != artifact.content_hash {
            return Err(Self::invalid(
                "persisted scenario model failed exact content verification",
            ));
        }
        Ok(())
    }

    async fn ensure_manifest(
        &self,
        cycle: &FeedbackCycleInfo,
        candidate: &ModelVersionInfo,
        evidence: CandidateManifestEvidence,
    ) -> QuantResult<ModelCandidateManifestInfo> {
        let contract = candidate
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let bindings = contract.bindings();
        let explanation = CandidateExplanationValidation::try_from(bindings)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let category = cycle
            .route
            .category()
            .ok_or_else(|| Self::invalid("ResearchOnly pooled route cannot bind a shadow"))?;
        let promotion_gate = PromotionGateArtifact::try_seal(PromotionGateArtifactInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            candidate_recipe_hash: evidence.candidate_recipe_hash,
            candidate_model_version_id: candidate.model_version_id,
            profile_ref: candidate.profile_ref.clone(),
            category,
            feedback_policy_hash: cycle.feedback_policy_hash,
            decision_policy_snapshot_hash: cycle.decision_policy_snapshot_hash,
            truth_freeze_hash: evidence.truth_freeze_hash,
            attribution_manifest_hash: evidence.attribution_manifest_hash,
            validation_artifact_hash: evidence.validation_artifact_hash,
            quality_gate_report_hash: evidence.quality_gate_report_hash,
            comparison_artifact_hash: evidence.comparison_artifact_hash,
            cpcv_path_set_id: evidence.cpcv_path_set_id,
            cpcv_path_set_hash: evidence.cpcv_path_set_hash,
            explanation_validation_hash: explanation.report_hash,
            scenario_model_bindings_hash: evidence.scenario_model_bindings_hash,
        })
        .map_err(|error| Self::invalid(error.to_string()))?;
        let document = ModelCandidateManifestDocument::try_new(ModelCandidateManifestInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            candidate_recipe_hash: evidence.candidate_recipe_hash,
            model_version_id: candidate.model_version_id,
            model_spec_id: candidate.model_spec_id,
            model_family: candidate.model_family,
            model_artifact_hash: candidate.artifact_hash,
            serving_contract_hash: candidate.serving_contract_hash,
            training_dataset_id: bindings.dataset.manifest.training_dataset_id,
            training_dataset_hash: bindings.transform.training_dataset_hash,
            feature_schema_hash: bindings.schemas.feature_schema_hash,
            input_contract_hash: bindings.transform.input_contract_hash,
            input_transform_hash: bindings.transform.input_transform_hash,
            calibration_artifact_id: calibration.map(|(artifact_id, _)| artifact_id),
            calibration_artifact_hash: calibration.map(|(_, content_hash)| content_hash),
            cpcv_path_set_id: evidence.cpcv_path_set_id,
            cpcv_path_set_hash: evidence.cpcv_path_set_hash,
            profile_ref: candidate.profile_ref.clone(),
            category,
            portfolio_scenario_model_bindings: evidence.portfolio_scenario_model_bindings,
            scenario_model_bindings_hash: evidence.scenario_model_bindings_hash,
            feedback_policy_hash: cycle.feedback_policy_hash,
            decision_policy_snapshot_hash: cycle.decision_policy_snapshot_hash,
            explanation_validation: explanation,
            promotion_gate,
        })
        .map_err(|error| Self::invalid(error.to_string()))?;
        let manifest = NewModelCandidateManifest::try_new(document)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let outcome = self.manifests.insert(manifest).await?;
        Ok(match outcome {
            ModelCandidateManifestWriteOutcome::Inserted(manifest)
            | ModelCandidateManifestWriteOutcome::AlreadyPresent(manifest) => manifest,
        })
    }

    async fn load_stage_job(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        result_kind: ResearchJobResultKind,
    ) -> QuantResult<(ResearchJobInfo, ResearchJobArtifactRef)> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let event = events
            .iter()
            .rev()
            .find(|event| {
                event.stage == stage && event.event_kind == FeedbackStageEventKind::Succeeded
            })
            .ok_or_else(|| Self::invalid(format!("{stage} has no WORM success event")))?;
        event.validate()?;
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid(format!("{stage} success has no job identity")))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
        let result_id = result_id(&job.params_json, stage)?;
        let result = Self::require_job_result(cycle, &job, stage, kind, result_kind, result_id)?;
        if event.evidence_uri.as_ref() != Some(&result.uri)
            || event.evidence_hash != Some(result.content_hash)
        {
            return Err(Self::invalid(format!(
                "{stage} result differs from its WORM success event"
            )));
        }
        Ok((job, result))
    }

    fn require_job_result(
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        result_kind: ResearchJobResultKind,
        result_id: Uuid,
    ) -> QuantResult<ResearchJobArtifactRef> {
        job.validate_identity()?;
        let result = job
            .result_artifact()
            .ok_or_else(|| Self::invalid(format!("{stage} job has no terminal artifact")))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.kind != kind
            || job.params_json.kind() != kind
            || job.status != ResearchJobStatus::Succeeded
            || job.result()
                != Some(ResearchJobResultRef {
                    kind: result_kind,
                    id: result_id,
                })
        {
            return Err(Self::invalid(format!(
                "{stage} job has invalid identity, status, or result lineage"
            )));
        }
        Ok(result)
    }

    fn bind_job(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: ShadowBindingJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackShadowBind,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(cycle.champion_model_spec_id),
            decision_policy_snapshot_id: Some(params.expected_snapshot_id),
            params_json: ResearchJobParams::FeedbackShadowBind(Box::new(params)),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: identity.parent_job_id(),
            recovery_attempt: 0,
            max_recovery_attempts: self.max_recovery_attempts,
        }
        .try_bind_feedback(identity)?;
        job.validate_enqueue()?;
        Ok(job)
    }

    fn require_identity(
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<()> {
        let lease_generation_exact = lease.expected_generation == cycle.generation;
        if lease.feedback_cycle_id != cycle.feedback_cycle_id
            || !lease_generation_exact
            || identity.feedback_cycle_id() != cycle.feedback_cycle_id
            || identity.feedback_stage() != FeedbackStage::ShadowBind
        {
            return Err(Self::invalid(
                "ShadowBind lease, generation, cycle, or job identity is invalid",
            ));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidCoordinatorState {
            detail: detail.into(),
        }
        .into()
    }
}

fn result_id(params: &ResearchJobParams, stage: FeedbackStage) -> QuantResult<Uuid> {
    let id = match (params, stage) {
        (ResearchJobParams::FeedbackTruthFreeze(params), FeedbackStage::TruthFreeze) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackAttribution(params), FeedbackStage::Attribution) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackValidation(params), FeedbackStage::Validation) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackComparison(params), FeedbackStage::Comparison) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackShadowBind(params), FeedbackStage::ShadowBind) => {
            params.artifact_id.as_uuid()
        }
        _ => {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!("{stage} job lost its exact result identity"),
            }
            .into());
        }
    };
    Ok(id)
}
