//! Lease-safe Comparison stage binding and terminal artifact verification.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackCandidateRecipe, FeedbackComparisonCandidateRef, FeedbackComparisonJobInput,
            FeedbackComparisonJobParams, FeedbackEvaluationUseRef,
            FeedbackLearningStageArtifactRef, FeedbackValidationArtifact,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, ModelVersionInfo, NewResearchJob,
            ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackStage, FeedbackStageEventKind, ResearchJobKind, ResearchJobResultKind,
        ResearchJobStatus,
    },
    types::{
        BacktestReportId, FeedbackComparisonArtifactId, ModelRunId, ModelVersionId,
        ResearchJobParams, RoleCode,
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
    FeedbackEvaluationWriteOutcome, ModelRegistryRepository, ResearchJobRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback_comparison::FeedbackComparisonCodec,
    feedback_learning::{
        FeedbackCpcvStageResult, FeedbackLearningStageArtifact, FeedbackLearningStageResults,
    },
};

use crate::service::{
    feedback_coordinator::FeedbackStageSuccess,
    feedback_evaluation::FeedbackEvaluationReservationService,
    feedback_governance_stage::FeedbackGovernanceStageAdapter,
    feedback_learning_stage::FeedbackLearningStageAdapter,
};

struct VerifiedCpcv {
    reference: FeedbackLearningStageArtifactRef,
    artifact: FeedbackLearningStageArtifact,
}

/// Dependencies for [`FeedbackComparisonStageAdapter`].
pub struct FeedbackComparisonStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub path_sets: Arc<dyn BacktestPathSetRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub learning_stages: Arc<FeedbackLearningStageAdapter>,
    pub governance_stages: Arc<FeedbackGovernanceStageAdapter>,
    pub evaluation_reservations: Arc<FeedbackEvaluationReservationService>,
    pub max_recovery_attempts: i32,
}

/// Owns the irreversible F08-to-F09 boundary and exact F09 read-back.
pub struct FeedbackComparisonStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    path_sets: Arc<dyn BacktestPathSetRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    learning_stages: Arc<FeedbackLearningStageAdapter>,
    governance_stages: Arc<FeedbackGovernanceStageAdapter>,
    evaluation_reservations: Arc<FeedbackEvaluationReservationService>,
    max_recovery_attempts: i32,
}

impl FeedbackComparisonStageAdapter {
    pub fn try_new(deps: FeedbackComparisonStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            models: deps.models,
            path_sets: deps.path_sets,
            artifacts: deps.artifacts,
            learning_stages: deps.learning_stages,
            governance_stages: deps.governance_stages,
            evaluation_reservations: deps.evaluation_reservations,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    /// Bind the exact CPCV-qualified family after consuming the holdout under
    /// the coordinator's live lease.
    pub async fn prepare_comparison(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, lease, identity)?;
        cycle.validate()?;
        let validation = self.governance_stages.load_validation(cycle).await?;
        let cpcv = self.load_cpcv(cycle).await?;
        if validation.reference.cpcv != cpcv.reference {
            return Err(Self::invalid(
                "Validation and Comparison resolved different CPCV predecessors",
            ));
        }
        let champion = self.load_model(cycle.champion_model_version_id).await?;
        Self::verify_champion(cycle, &champion)?;
        let candidates = self
            .build_candidates(cycle, &champion, &cpcv.artifact, &validation.artifact)
            .await?;

        // No Evaluation object bytes may be opened before this durable,
        // lease-guarded append has succeeded or returned its exact idempotent row.
        let evaluation = match self
            .evaluation_reservations
            .reserve(lease, cpcv.reference.clone())
            .await?
        {
            FeedbackEvaluationWriteOutcome::Inserted(info)
            | FeedbackEvaluationWriteOutcome::AlreadyPresent(info) => info,
        };
        evaluation.validate()?;
        let family = &cycle.candidate_family;
        let params = FeedbackComparisonJobParams::try_new(FeedbackComparisonJobInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            candidate_family_hash: cycle.candidate_family_hash,
            validation: validation.reference,
            evaluation_use: FeedbackEvaluationUseRef::from(&evaluation),
            comparison_contract: family.comparison_contract().clone(),
            decision_policy_snapshot_id: family
                .shared_evaluation()
                .source_lineage
                .decision_policy_snapshot_id,
            champion_model_version_id: champion.model_version_id,
            champion_serving_contract_hash: champion.serving_contract_hash,
            candidates,
        })?;
        self.bind_job(cycle, identity, params)
    }

    /// Validate the terminal Comparison row and its canonical object before
    /// allowing the coordinator to append stage success.
    pub async fn succeeded_comparison(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let ResearchJobParams::FeedbackComparison(params) = &job.params_json else {
            return Err(Self::invalid(
                "Comparison job lost its exact typed parameters",
            ));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackComparisonArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("Comparison job lost its terminal artifact"))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(FeedbackStage::Comparison)
            || job.kind != ResearchJobKind::FeedbackComparison
            || job.status != ResearchJobStatus::Succeeded
            || job.result() != Some(expected)
        {
            return Err(Self::invalid(
                "Comparison job has invalid cycle, kind, status, or result lineage",
            ));
        }
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackComparisonCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Comparison object bytes differ from their terminal hash",
            ));
        }
        let artifact = FeedbackComparisonCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        Ok(FeedbackStageSuccess::advance(
            artifact_ref.uri,
            artifact_ref.content_hash,
        ))
    }

    async fn load_cpcv(&self, cycle: &FeedbackCycleInfo) -> QuantResult<VerifiedCpcv> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let event = events
            .iter()
            .rev()
            .find(|event| {
                event.stage == FeedbackStage::Cpcv
                    && event.event_kind == FeedbackStageEventKind::Succeeded
            })
            .ok_or_else(|| Self::invalid("Comparison has no succeeded CPCV predecessor"))?;
        event.validate()?;
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid("succeeded CPCV event has no ResearchJob identity"))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
        let ResearchJobParams::FeedbackCpcv(params) = &job.params_json else {
            return Err(Self::invalid(
                "CPCV predecessor lost its exact typed parameters",
            ));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("succeeded CPCV job has no terminal artifact"))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(FeedbackStage::Cpcv)
            || job.kind != ResearchJobKind::FeedbackCpcv
            || job.status != ResearchJobStatus::Succeeded
            || job.result() != Some(expected)
            || event.evidence_uri.as_ref() != Some(&artifact_ref.uri)
            || event.evidence_hash != Some(artifact_ref.content_hash)
        {
            return Err(Self::invalid(
                "CPCV job and WORM success event do not carry one exact lineage",
            ));
        }
        let reference = FeedbackLearningStageArtifactRef {
            feedback_cycle_id: cycle.feedback_cycle_id,
            stage: FeedbackStage::Cpcv,
            job_id,
            artifact_id: params.artifact_id,
            input_hash: params.input_hash()?,
            artifact: artifact_ref,
        };
        let artifact = self
            .learning_stages
            .verify_reference(cycle, &reference)
            .await?;
        Ok(VerifiedCpcv {
            reference,
            artifact,
        })
    }

    async fn build_candidates(
        &self,
        cycle: &FeedbackCycleInfo,
        champion: &ModelVersionInfo,
        artifact: &FeedbackLearningStageArtifact,
        validation: &FeedbackValidationArtifact,
    ) -> QuantResult<Vec<FeedbackComparisonCandidateRef>> {
        let FeedbackLearningStageResults::Cpcv(results) = &artifact.results else {
            return Err(Self::invalid(
                "Comparison predecessor is not a CPCV result artifact",
            ));
        };
        if results.len() != validation.candidates.len() {
            return Err(Self::invalid(
                "Validation does not cover the complete CPCV trial universe",
            ));
        }
        let mut candidates = Vec::with_capacity(results.len());
        let artifact_id = FeedbackComparisonArtifactId::from_cycle_id(cycle.feedback_cycle_id);
        for (result, validation) in results.iter().zip(&validation.candidates) {
            if result.candidate_recipe_hash() != validation.candidate_recipe_hash
                || result.model_version_id() != validation.model_version_id
            {
                return Err(Self::invalid(
                    "Validation candidate identity differs from its CPCV trial",
                ));
            }
            if !validation.is_comparison_eligible() {
                continue;
            }
            let (
                candidate_recipe_hash,
                model_version_id,
                training_dataset_id,
                path_set_id,
                model_run_id,
                path_set_hash,
            ) = match result {
                FeedbackCpcvStageResult::Evaluated {
                    candidate_recipe_hash,
                    model_version_id,
                    training_dataset_id,
                    path_set_id,
                    model_run_id,
                    path_set_hash,
                } => (
                    *candidate_recipe_hash,
                    *model_version_id,
                    *training_dataset_id,
                    *path_set_id,
                    *model_run_id,
                    *path_set_hash,
                ),
                FeedbackCpcvStageResult::CalibrationInsufficient { .. } => {
                    return Err(Self::invalid(
                        "calibration-ineligible recipe passed aggregate Validation",
                    ));
                }
            };
            let recipe = cycle
                .candidate_family
                .candidate(candidate_recipe_hash)
                .ok_or_else(|| {
                    Self::invalid("CPCV result is outside the frozen candidate family")
                })?;
            let model = self.load_model(model_version_id).await?;
            Self::verify_candidate(cycle, champion, recipe, result, &model)?;
            let path_set = self
                .path_sets
                .find_by_id(&path_set_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_backtest_path_set", path_set_id))?;
            path_set.verify_hash().map_err(|error| {
                Self::invalid(format!("candidate CPCV path-set hash is invalid: {error}"))
            })?;
            let policy_id = cycle
                .candidate_family
                .shared_evaluation()
                .source_lineage
                .decision_policy_snapshot_id;
            if path_set.path_set_id != path_set_id
                || path_set.path_set_hash != path_set_hash
                || path_set.model_version_id != model_version_id
                || path_set.model_run_id != model_run_id
                || path_set.training_dataset_id != training_dataset_id
                || path_set.decision_policy_snapshot_id != policy_id
                || path_set.subject.model_artifact_hash != model.artifact_hash
                || path_set.subject.serving_contract_hash != model.serving_contract_hash
            {
                return Err(Self::invalid(
                    "candidate CPCV path set differs from its stage result or model preimage",
                ));
            }
            candidates.push(FeedbackComparisonCandidateRef {
                candidate_recipe_hash,
                model_version_id,
                serving_contract_hash: model.serving_contract_hash,
                path_set_id,
                path_set_hash,
                model_run_id: ModelRunId::from_feedback_comparison(artifact_id, model_version_id),
                backtest_report_id: BacktestReportId::from_feedback_comparison(
                    artifact_id,
                    model_version_id,
                ),
            });
        }
        Ok(candidates)
    }

    async fn load_model(&self, model_version_id: ModelVersionId) -> QuantResult<ModelVersionInfo> {
        let model = self
            .models
            .find_model_version(&model_version_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_model_version", model_version_id))?;
        model
            .verified_serving_contract()
            .map_err(|error| Self::invalid(format!("invalid model serving preimage: {error}")))?;
        Ok(model)
    }

    fn verify_champion(cycle: &FeedbackCycleInfo, champion: &ModelVersionInfo) -> QuantResult<()> {
        let evaluation = cycle.candidate_family.shared_evaluation();
        let bindings = champion.serving_contract.bindings();
        if champion.model_version_id != cycle.champion_model_version_id
            || champion.serving_contract_hash != cycle.champion_serving_contract_hash
            || champion.model_spec_id != evaluation.model_spec_id
            || champion.model_spec_definition_hash != evaluation.model_spec_definition_hash
            || champion.profile_ref != cycle.profile_ref
            || bindings.policy_snapshot.decision_policy_snapshot_id
                != evaluation.source_lineage.decision_policy_snapshot_id
        {
            return Err(Self::invalid(
                "champion model differs from the cycle, profile, policy, or model specification",
            ));
        }
        Ok(())
    }

    fn verify_candidate(
        cycle: &FeedbackCycleInfo,
        champion: &ModelVersionInfo,
        recipe: &FeedbackCandidateRecipe,
        result: &FeedbackCpcvStageResult,
        model: &ModelVersionInfo,
    ) -> QuantResult<()> {
        let FeedbackCpcvStageResult::Evaluated {
            model_version_id,
            training_dataset_id,
            ..
        } = result
        else {
            return Err(Self::invalid(
                "calibration-ineligible recipe cannot enter Comparison",
            ));
        };
        let bindings = model.serving_contract.bindings();
        if model.model_version_id != *model_version_id
            || model.model_version_id == champion.model_version_id
            || model.model_spec_id != recipe.training().model_spec_id
            || model.model_spec_definition_hash != recipe.training().model_spec_definition_hash
            || model.profile_ref != cycle.profile_ref
            || model.category_scope != champion.category_scope
            || model.training_dataset_id != Some(*training_dataset_id)
            || *training_dataset_id != recipe.training().training_dataset_id
            || bindings.policy_snapshot.decision_policy_snapshot_id
                != recipe.decision_policy_snapshot_id()
        {
            return Err(Self::invalid(
                "candidate model differs from its recipe, CPCV result, champion scope, or policy",
            ));
        }
        Ok(())
    }

    fn bind_job(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackComparisonJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackComparison,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(cycle.candidate_family.shared_evaluation().model_spec_id),
            decision_policy_snapshot_id: Some(params.decision_policy_snapshot_id),
            params_json: ResearchJobParams::FeedbackComparison(Box::new(params)),
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
        if lease.feedback_cycle_id != cycle.feedback_cycle_id {
            return Err(Self::invalid(
                "Comparison adapter lease belongs to another cycle",
            ));
        }
        if lease.expected_generation != cycle.generation {
            return Err(Self::invalid(
                "Comparison adapter lease generation is stale",
            ));
        }
        if identity.feedback_cycle_id() != cycle.feedback_cycle_id {
            return Err(Self::invalid(
                "Comparison job identity belongs to another cycle",
            ));
        }
        if identity.feedback_stage() != FeedbackStage::Comparison {
            return Err(Self::invalid(
                "Comparison job identity belongs to another stage",
            ));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidComparisonEvidence {
            detail: detail.into(),
        }
        .into()
    }
}
