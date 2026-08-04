//! Exact feedback-stage adapters for `DatasetSeal`, Training, Calibration, and CPCV.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{
            CpcvBacktestJobParams, FitModelCalibratorRequest, ModelTrainJobParams,
            RunCpcvBacktestRequest, TrainModelRequest,
        },
        ports::{
            FeedbackCalibrationCommand, FeedbackCalibrationJobParams, FeedbackCandidateFamily,
            FeedbackCandidateRecipe, FeedbackCpcvCommand, FeedbackCpcvJobParams,
            FeedbackDatasetBuildCommand, FeedbackDatasetRole, FeedbackDatasetSealJobParams,
            FeedbackLearningStageArtifactRef, FeedbackRecipeResourceBudget,
            FeedbackTrainingCommand, FeedbackTrainingJobParams, GovernanceActor,
            ModelCalibrationFitJobParams,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobArtifactRef,
            ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        DatasetPurpose, FeedbackStage, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    hashing::CanonicalDigest,
    types::{
        BacktestPathSetId, ContentHash, FeedbackCycleId, FeedbackLearningStageArtifactId,
        ModelRunId, ModelVersionId, ResearchJobParams, RoleCode,
    },
};
use quant_pivot_repository::traits::ResearchJobRepository;
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback_learning::{
        FeedbackCalibrationStageResult, FeedbackCpcvStageResult, FeedbackLearningStageArtifact,
        FeedbackLearningStageCodec, FeedbackLearningStageResults, FeedbackTrainingStageResult,
    },
};

use crate::service::{
    feedback_coordinator::FeedbackStageSuccess, feedback_recipe_stage::FeedbackRecipeStageAdapter,
};

const TRAIN_REASON: &str = "feedback_candidate_training";
const CALIBRATION_REASON: &str = "feedback_candidate_calibration";
const CPCV_REASON: &str = "feedback_candidate_cpcv";

struct LearningJobExpectation {
    kind: ResearchJobKind,
    artifact_id: FeedbackLearningStageArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    candidate_family_hash: ContentHash,
    input_hash: ContentHash,
    previous: Option<FeedbackLearningStageArtifactRef>,
}

/// Dependencies for [`FeedbackLearningStageAdapter`].
pub struct FeedbackLearningStageDeps {
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub recipes: Arc<FeedbackRecipeStageAdapter>,
    pub max_recovery_attempts: i32,
}

/// Learning-stage-only adapter consumed by the final closed-DAG dispatcher.
pub struct FeedbackLearningStageAdapter {
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    recipes: Arc<FeedbackRecipeStageAdapter>,
    max_recovery_attempts: i32,
}

impl FeedbackLearningStageAdapter {
    pub fn try_new(deps: FeedbackLearningStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            recipes: deps.recipes,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    /// Build and bind the exact server-owned job for one learning stage.
    pub async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        match identity.feedback_stage() {
            FeedbackStage::DatasetSeal => {
                let family = self.family(cycle).await?;
                let params = Self::dataset_params(cycle, &family)?;
                self.prepare_dataset_seal(cycle, identity, params, &family)
            }
            FeedbackStage::Training => {
                let params = self.training_params(cycle).await?;
                self.prepare_training(cycle, identity, params).await
            }
            FeedbackStage::Calibration => {
                let params = self.calibration_params(cycle).await?;
                self.prepare_calibration(cycle, identity, params).await
            }
            FeedbackStage::Cpcv => {
                let params = self.cpcv_params(cycle).await?;
                self.prepare_cpcv(cycle, identity, params).await
            }
            stage => Err(Self::invalid(format!(
                "learning-stage adapter cannot prepare {stage}"
            ))),
        }
    }

    /// Bind the server-frozen `DatasetSeal` batch to its deterministic job.
    pub fn prepare_dataset_seal(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackDatasetSealJobParams,
        family: &FeedbackCandidateFamily,
    ) -> QuantResult<NewResearchJob> {
        params.validate()?;
        Self::require_identity(cycle, identity, FeedbackStage::DatasetSeal)?;
        Self::require_cycle(
            cycle,
            params.feedback_cycle_id,
            params.cycle_idempotency_hash,
            params.candidate_family_hash,
            family.candidate_family_hash(),
        )?;
        Self::require_dataset_family(family, &params)?;
        self.bind_job(identity, ResearchJobParams::FeedbackDatasetSeal(params))
    }

    /// Bind the server-frozen Training batch after exact `DatasetSeal` read-back.
    pub async fn prepare_training(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackTrainingJobParams,
    ) -> QuantResult<NewResearchJob> {
        let family = self.family(cycle).await?;
        params.validate()?;
        Self::require_identity(cycle, identity, FeedbackStage::Training)?;
        Self::require_cycle(
            cycle,
            params.feedback_cycle_id,
            params.cycle_idempotency_hash,
            params.candidate_family_hash,
            family.candidate_family_hash(),
        )?;
        Self::require_training_family(&family, &params)?;
        let predecessor = self.verify_reference(cycle, &params.previous).await?;
        Self::require_training_predecessor(&params, &predecessor)?;
        self.bind_job(identity, ResearchJobParams::FeedbackTraining(params))
    }

    /// Bind the server-frozen Calibration batch after exact Training read-back.
    pub async fn prepare_calibration(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackCalibrationJobParams,
    ) -> QuantResult<NewResearchJob> {
        let family = self.family(cycle).await?;
        params.validate()?;
        Self::require_identity(cycle, identity, FeedbackStage::Calibration)?;
        Self::require_cycle(
            cycle,
            params.feedback_cycle_id,
            params.cycle_idempotency_hash,
            params.candidate_family_hash,
            family.candidate_family_hash(),
        )?;
        Self::require_calibration_family(&family, &params)?;
        let predecessor = self.verify_reference(cycle, &params.previous).await?;
        Self::require_calibration_predecessor(&params, &predecessor)?;
        self.bind_job(identity, ResearchJobParams::FeedbackCalibration(params))
    }

    /// Bind the server-frozen CPCV batch after exact Calibration read-back.
    pub async fn prepare_cpcv(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackCpcvJobParams,
    ) -> QuantResult<NewResearchJob> {
        let family = self.family(cycle).await?;
        params.validate()?;
        Self::require_identity(cycle, identity, FeedbackStage::Cpcv)?;
        Self::require_cycle(
            cycle,
            params.feedback_cycle_id,
            params.cycle_idempotency_hash,
            params.candidate_family_hash,
            family.candidate_family_hash(),
        )?;
        Self::require_cpcv_family(&family, &params)?;
        let predecessor = self.verify_reference(cycle, &params.previous).await?;
        Self::require_cpcv_predecessor(&params, &predecessor)?;
        self.bind_job(identity, ResearchJobParams::FeedbackCpcv(params))
    }

    /// Revalidate a succeeded `DatasetSeal` job and advance.
    pub async fn succeeded_dataset_seal(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        self.succeeded(cycle, job, FeedbackStage::DatasetSeal).await
    }

    /// Revalidate a succeeded Training job and advance.
    pub async fn succeeded_training(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        self.succeeded(cycle, job, FeedbackStage::Training).await
    }

    /// Revalidate a succeeded Calibration job and advance.
    pub async fn succeeded_calibration(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        self.succeeded(cycle, job, FeedbackStage::Calibration).await
    }

    /// Revalidate a succeeded CPCV job and advance.
    pub async fn succeeded_cpcv(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        self.succeeded(cycle, job, FeedbackStage::Cpcv).await
    }

    pub(crate) async fn family(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<FeedbackCandidateFamily> {
        self.recipes
            .load_plan(cycle)
            .await?
            .artifact
            .candidate_family()
            .cloned()
            .ok_or_else(|| Self::invalid("learning stage cannot follow a RecipePlan NoAction"))
    }

    fn dataset_params(
        cycle: &FeedbackCycleInfo,
        family: &FeedbackCandidateFamily,
    ) -> Result<FeedbackDatasetSealJobParams, FeedbackError> {
        let mut commands = Vec::with_capacity(family.candidates().len() * 2 + 1);
        commands.extend(
            family
                .candidates()
                .iter()
                .map(|candidate| FeedbackDatasetBuildCommand {
                    role: FeedbackDatasetRole::CandidateTraining {
                        candidate_recipe_hash: candidate.candidate_recipe_hash(),
                    },
                    resource_budget: candidate.resource_budget(),
                    request: candidate.training().clone(),
                }),
        );
        commands.extend(
            family
                .candidates()
                .iter()
                .map(|candidate| FeedbackDatasetBuildCommand {
                    role: FeedbackDatasetRole::CandidateCalibration {
                        candidate_recipe_hash: candidate.candidate_recipe_hash(),
                    },
                    resource_budget: candidate.resource_budget(),
                    request: candidate.calibration().clone(),
                }),
        );
        commands.push(FeedbackDatasetBuildCommand {
            role: FeedbackDatasetRole::SharedEvaluation,
            resource_budget: Self::aggregate_budget(family)?,
            request: family.shared_evaluation().clone(),
        });
        FeedbackDatasetSealJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family.candidate_family_hash(),
            commands,
        )
    }

    async fn training_params(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<FeedbackTrainingJobParams> {
        let family = self.family(cycle).await?;
        let (previous, _) = self.predecessor(cycle, FeedbackStage::DatasetSeal).await?;
        let commands = family
            .candidates()
            .iter()
            .map(|candidate| {
                let recipe_hash = candidate.candidate_recipe_hash();
                FeedbackTrainingCommand {
                    candidate_recipe_hash: recipe_hash,
                    resource_budget: candidate.resource_budget(),
                    params: ModelTrainJobParams {
                        model_version_id: ModelVersionId::from_feedback_candidate(
                            cycle.feedback_cycle_id,
                            recipe_hash,
                        ),
                        model_run_id: ModelRunId::from_feedback_stage(
                            cycle.feedback_cycle_id,
                            FeedbackStage::Training,
                            recipe_hash,
                        ),
                        request: TrainModelRequest {
                            training_dataset_id: candidate.training().training_dataset_id,
                            reason: TRAIN_REASON.to_owned(),
                        },
                    },
                }
            })
            .collect();
        FeedbackTrainingJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family.candidate_family_hash(),
            previous,
            commands,
        )
        .map_err(Into::into)
    }

    async fn calibration_params(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<FeedbackCalibrationJobParams> {
        let family = self.family(cycle).await?;
        let (previous, artifact) = self.predecessor(cycle, FeedbackStage::Training).await?;
        let FeedbackLearningStageResults::Training(results) = &artifact.results else {
            return Err(Self::invalid(
                "Calibration predecessor is not a Training artifact",
            ));
        };
        let mut commands = Vec::with_capacity(results.len());
        for candidate in family.candidates() {
            let recipe_hash = candidate.candidate_recipe_hash();
            let trained = Self::training_result(results, recipe_hash)?;
            commands.push(FeedbackCalibrationCommand {
                candidate_recipe_hash: recipe_hash,
                resource_budget: candidate.resource_budget(),
                params: ModelCalibrationFitJobParams {
                    model_run_id: ModelRunId::from_feedback_stage(
                        cycle.feedback_cycle_id,
                        FeedbackStage::Calibration,
                        recipe_hash,
                    ),
                    request: FitModelCalibratorRequest {
                        model_version_id: trained.model_version_id,
                        calibration_dataset_id: candidate.calibration().training_dataset_id,
                        method: candidate.calibration_method(),
                        reason: CALIBRATION_REASON.to_owned(),
                    },
                    decision_policy_snapshot_id: candidate.decision_policy_snapshot_id(),
                    downside_source: candidate.downside_source(),
                    actor: GovernanceActor::system(),
                },
            });
        }
        FeedbackCalibrationJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family.candidate_family_hash(),
            previous,
            commands,
        )
        .map_err(Into::into)
    }

    async fn cpcv_params(&self, cycle: &FeedbackCycleInfo) -> QuantResult<FeedbackCpcvJobParams> {
        let family = self.family(cycle).await?;
        let (previous, artifact) = self.predecessor(cycle, FeedbackStage::Calibration).await?;
        let FeedbackLearningStageResults::Calibration(results) = &artifact.results else {
            return Err(Self::invalid(
                "CPCV predecessor is not a Calibration artifact",
            ));
        };
        let mut commands = Vec::with_capacity(results.len());
        for result in results {
            let FeedbackCalibrationStageResult::Calibrated {
                candidate_recipe_hash,
                calibrated_model_version_id,
                ..
            } = result
            else {
                continue;
            };
            let candidate = family
                .candidate(*candidate_recipe_hash)
                .ok_or_else(|| Self::invalid("calibrated candidate left the frozen family"))?;
            commands.push(FeedbackCpcvCommand {
                candidate_recipe_hash: *candidate_recipe_hash,
                resource_budget: candidate.resource_budget(),
                cpcv_spec: candidate.cpcv_spec().clone(),
                params: CpcvBacktestJobParams {
                    model_version_id: *calibrated_model_version_id,
                    model_run_id: ModelRunId::from_feedback_stage(
                        cycle.feedback_cycle_id,
                        FeedbackStage::Cpcv,
                        *candidate_recipe_hash,
                    ),
                    request: RunCpcvBacktestRequest {
                        training_dataset_id: candidate.training().training_dataset_id,
                        decision_policy_snapshot_id: candidate.decision_policy_snapshot_id(),
                        reason: CPCV_REASON.to_owned(),
                        path_set_id: Some(BacktestPathSetId::from_feedback_candidate(
                            cycle.feedback_cycle_id,
                            *candidate_recipe_hash,
                        )),
                    },
                },
            });
        }
        FeedbackCpcvJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family.candidate_family_hash(),
            previous,
            commands,
        )
        .map_err(Into::into)
    }

    async fn predecessor(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
    ) -> QuantResult<(
        FeedbackLearningStageArtifactRef,
        FeedbackLearningStageArtifact,
    )> {
        let identity = FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, stage)?;
        let job = self
            .jobs
            .find_by_id(&identity.job_id())
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", identity.job_id()))?;
        let (artifact, result) = self.load_verified(cycle, &job, stage).await?;
        let reference = artifact.reference(job.job_id, result)?;
        Ok((reference, artifact))
    }

    fn training_result(
        results: &[FeedbackTrainingStageResult],
        recipe_hash: ContentHash,
    ) -> QuantResult<&FeedbackTrainingStageResult> {
        results
            .iter()
            .find(|result| result.candidate_recipe_hash == recipe_hash)
            .ok_or_else(|| Self::invalid("Training artifact lost one frozen candidate"))
    }

    async fn succeeded(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        stage: FeedbackStage,
    ) -> QuantResult<FeedbackStageSuccess> {
        let (artifact, result) = self.load_verified(cycle, job, stage).await?;
        artifact.validate()?;
        Ok(FeedbackStageSuccess::advance(
            result.uri,
            result.content_hash,
        ))
    }

    /// Revalidate one exact learning-stage object and its complete predecessor chain.
    pub async fn verify_reference(
        &self,
        cycle: &FeedbackCycleInfo,
        reference: &FeedbackLearningStageArtifactRef,
    ) -> QuantResult<FeedbackLearningStageArtifact> {
        let mut expected = reference.clone();
        let mut immediate = None;
        for depth in 0..4 {
            expected.validate_for(cycle.feedback_cycle_id, expected.stage)?;
            let job = self
                .jobs
                .find_by_id(&expected.job_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_research_job", expected.job_id))?;
            let (artifact, result) = self.load_verified(cycle, &job, expected.stage).await?;
            if depth == 0 {
                immediate = Some(artifact.clone());
            }
            let actual = artifact.reference(job.job_id, result)?;
            if actual != expected {
                return Err(Self::invalid(
                    "learning-stage predecessor differs from its frozen job/object reference",
                ));
            }
            match artifact.previous {
                Some(previous) => expected = previous,
                None if artifact.results.stage() == FeedbackStage::DatasetSeal => {
                    return immediate.ok_or_else(|| {
                        Self::invalid(
                            "learning-stage predecessor chain lost its immediate artifact",
                        )
                    });
                }
                None => {
                    return Err(Self::invalid(
                        "learning-stage predecessor chain ended before DatasetSeal",
                    ));
                }
            }
        }
        Err(Self::invalid(
            "learning-stage predecessor chain exceeds the closed four-stage DAG",
        ))
    }

    async fn load_verified(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        stage: FeedbackStage,
    ) -> QuantResult<(FeedbackLearningStageArtifact, ResearchJobArtifactRef)> {
        let family = self.family(cycle).await?;
        Self::require_family_params(&family, &job.params_json)?;
        let expectation = Self::job_expectation(job, stage)?;
        Self::require_cycle(
            cycle,
            expectation.feedback_cycle_id,
            expectation.cycle_idempotency_hash,
            expectation.candidate_family_hash,
            family.candidate_family_hash(),
        )?;
        let result =
            Self::require_result(cycle, job, stage, expectation.kind, expectation.artifact_id)?;
        let artifact = self.load(&result).await?;
        let cycle_id_matches = artifact.feedback_cycle_id == cycle.feedback_cycle_id;
        let cycle_hash_matches = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        let candidate_family_matches =
            artifact.candidate_family_hash == family.candidate_family_hash();
        let cycle_identity_matches =
            cycle_id_matches && cycle_hash_matches && candidate_family_matches;
        if !cycle_identity_matches
            || artifact.results.stage() != stage
            || artifact.artifact_id != expectation.artifact_id
            || artifact.input_hash != expectation.input_hash
            || artifact.previous != expectation.previous
        {
            return Err(Self::invalid(
                "learning-stage artifact differs from its job params or cycle",
            ));
        }
        Self::require_result_parity(&job.params_json, &artifact.results)?;
        Ok((artifact, result))
    }

    async fn load(
        &self,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackLearningStageArtifact> {
        let bytes = self.artifacts.get(&result.uri).await?;
        if CanonicalDigest::content_hash_bytes(&bytes) != result.content_hash {
            return Err(Self::invalid(
                "learning-stage object bytes differ from their terminal hash",
            ));
        }
        FeedbackLearningStageCodec::decode(&bytes)
    }

    fn require_result(
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        artifact_id: FeedbackLearningStageArtifactId,
    ) -> QuantResult<ResearchJobArtifactRef> {
        job.validate_identity()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
            id: artifact_id.as_uuid(),
        };
        let result = job.result();
        let artifact = job.result_artifact();
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.kind != kind
            || job.params_json.kind() != kind
            || job.status != ResearchJobStatus::Succeeded
            || result != Some(expected)
            || artifact.is_none()
        {
            return Err(Self::invalid(
                "learning-stage job has invalid lineage or terminal artifact result",
            ));
        }
        artifact.ok_or_else(|| Self::invalid("learning-stage job lost its terminal artifact"))
    }

    fn bind_job(
        &self,
        identity: FeedbackStageJobIdentity,
        params: ResearchJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: params.kind(),
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: params,
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
        identity: FeedbackStageJobIdentity,
        stage: FeedbackStage,
    ) -> QuantResult<()> {
        if identity.feedback_cycle_id() != cycle.feedback_cycle_id
            || identity.feedback_stage() != stage
        {
            return Err(Self::invalid(format!(
                "{stage} adapter received another cycle or stage identity"
            )));
        }
        Ok(())
    }

    fn require_cycle(
        cycle: &FeedbackCycleInfo,
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        expected_family_hash: ContentHash,
    ) -> QuantResult<()> {
        if feedback_cycle_id != cycle.feedback_cycle_id
            || cycle_idempotency_hash != cycle.idempotency_hash
            || candidate_family_hash != expected_family_hash
        {
            return Err(Self::invalid(
                "learning-stage params differ from their feedback cycle or candidate family",
            ));
        }
        Ok(())
    }

    fn require_dataset_family(
        family: &FeedbackCandidateFamily,
        params: &FeedbackDatasetSealJobParams,
    ) -> QuantResult<()> {
        let shared_budget = Self::aggregate_budget(family)?;
        let expected_count = family
            .candidates()
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| Self::invalid("candidate-family Dataset count overflowed"))?;
        if params.commands.len() != expected_count {
            return Err(Self::invalid(
                "DatasetSeal command count differs from the frozen candidate family",
            ));
        }
        for command in &params.commands {
            let expected = match command.role {
                FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash,
                } => family
                    .candidate(candidate_recipe_hash)
                    .map(|candidate| (candidate.training(), candidate.resource_budget())),
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash,
                } => family
                    .candidate(candidate_recipe_hash)
                    .map(|candidate| (candidate.calibration(), candidate.resource_budget())),
                FeedbackDatasetRole::SharedEvaluation => {
                    Some((family.shared_evaluation(), shared_budget))
                }
            };
            if expected != Some((&command.request, command.resource_budget)) {
                return Err(Self::invalid(
                    "DatasetSeal command or budget differs from its frozen candidate family",
                ));
            }
        }
        Ok(())
    }

    fn require_training_family(
        family: &FeedbackCandidateFamily,
        params: &FeedbackTrainingJobParams,
    ) -> QuantResult<()> {
        let candidates = family.candidates();
        if params.commands.len() != candidates.len()
            || params
                .commands
                .iter()
                .zip(candidates)
                .any(|(command, candidate)| {
                    command.candidate_recipe_hash != candidate.candidate_recipe_hash()
                        || command.resource_budget != candidate.resource_budget()
                        || command.params.request.training_dataset_id
                            != candidate.training().training_dataset_id
                })
        {
            return Err(Self::invalid(
                "Training commands differ from the frozen candidate family",
            ));
        }
        Ok(())
    }

    fn require_calibration_family(
        family: &FeedbackCandidateFamily,
        params: &FeedbackCalibrationJobParams,
    ) -> QuantResult<()> {
        let candidates = family.candidates();
        if params.commands.len() != candidates.len()
            || params
                .commands
                .iter()
                .zip(candidates)
                .any(|(command, candidate)| {
                    command.candidate_recipe_hash != candidate.candidate_recipe_hash()
                        || command.resource_budget != candidate.resource_budget()
                        || command.params.request.calibration_dataset_id
                            != candidate.calibration().training_dataset_id
                        || command.params.request.method != candidate.calibration_method()
                        || command.params.downside_source != candidate.downside_source()
                        || command.params.decision_policy_snapshot_id
                            != candidate.decision_policy_snapshot_id()
                })
        {
            return Err(Self::invalid(
                "Calibration commands differ from the frozen candidate family",
            ));
        }
        Ok(())
    }

    fn require_cpcv_family(
        family: &FeedbackCandidateFamily,
        params: &FeedbackCpcvJobParams,
    ) -> QuantResult<()> {
        if params.commands.iter().any(|command| {
            family
                .candidate(command.candidate_recipe_hash)
                .is_none_or(|candidate| {
                    command.resource_budget != candidate.resource_budget()
                        || command.params.request.training_dataset_id
                            != candidate.training().training_dataset_id
                        || command.params.request.decision_policy_snapshot_id
                            != candidate.decision_policy_snapshot_id()
                })
        }) {
            return Err(Self::invalid(
                "CPCV commands differ from the frozen candidate family",
            ));
        }
        Ok(())
    }

    fn aggregate_budget(
        family: &FeedbackCandidateFamily,
    ) -> Result<FeedbackRecipeResourceBudget, FeedbackError> {
        let mut candidates = family.candidates().iter();
        let mut aggregate = candidates
            .next()
            .map(FeedbackCandidateRecipe::resource_budget)
            .ok_or_else(|| FeedbackError::InvalidJobContract {
                detail: "candidate family has no resource budget".to_owned(),
            })?;
        for budget in candidates.map(FeedbackCandidateRecipe::resource_budget) {
            aggregate.max_concurrency = aggregate.max_concurrency.max(budget.max_concurrency);
            aggregate.max_working_set_bytes = aggregate
                .max_working_set_bytes
                .max(budget.max_working_set_bytes);
            aggregate.max_resident_model_bytes = aggregate
                .max_resident_model_bytes
                .max(budget.max_resident_model_bytes);
            aggregate.deadline_secs = aggregate.deadline_secs.max(budget.deadline_secs);
        }
        aggregate.validate()?;
        Ok(aggregate)
    }

    fn require_training_predecessor(
        params: &FeedbackTrainingJobParams,
        predecessor: &FeedbackLearningStageArtifact,
    ) -> QuantResult<()> {
        let FeedbackLearningStageResults::DatasetSeal(results) = &predecessor.results else {
            return Err(Self::invalid(
                "Training predecessor is not a DatasetSeal artifact",
            ));
        };
        if params.commands.iter().any(|command| {
            !results.iter().any(|result| {
                result.role
                    == (FeedbackDatasetRole::CandidateTraining {
                        candidate_recipe_hash: command.candidate_recipe_hash,
                    })
                    && result.training_dataset_id == command.params.request.training_dataset_id
                    && result.purpose == DatasetPurpose::Training
            })
        }) {
            return Err(Self::invalid(
                "Training commands differ from their DatasetSeal predecessor",
            ));
        }
        Ok(())
    }

    fn require_calibration_predecessor(
        params: &FeedbackCalibrationJobParams,
        predecessor: &FeedbackLearningStageArtifact,
    ) -> QuantResult<()> {
        let FeedbackLearningStageResults::Training(results) = &predecessor.results else {
            return Err(Self::invalid(
                "Calibration predecessor is not a Training artifact",
            ));
        };
        if params.commands.len() != results.len()
            || params
                .commands
                .iter()
                .zip(results)
                .any(|(command, result)| {
                    command.candidate_recipe_hash != result.candidate_recipe_hash
                        || command.params.request.model_version_id != result.model_version_id
                })
        {
            return Err(Self::invalid(
                "Calibration commands differ from their Training predecessor",
            ));
        }
        Ok(())
    }

    fn require_cpcv_predecessor(
        params: &FeedbackCpcvJobParams,
        predecessor: &FeedbackLearningStageArtifact,
    ) -> QuantResult<()> {
        let FeedbackLearningStageResults::Calibration(results) = &predecessor.results else {
            return Err(Self::invalid(
                "CPCV predecessor is not a Calibration artifact",
            ));
        };
        let calibrated = results.iter().filter_map(|result| match result {
            FeedbackCalibrationStageResult::Calibrated {
                candidate_recipe_hash,
                calibrated_model_version_id,
                ..
            } => Some((*candidate_recipe_hash, *calibrated_model_version_id)),
            FeedbackCalibrationStageResult::Insufficient { .. } => None,
        });
        if params.commands.len()
            != results
                .iter()
                .filter(|result| {
                    matches!(result, FeedbackCalibrationStageResult::Calibrated { .. })
                })
                .count()
            || params.commands.iter().zip(calibrated).any(
                |(command, (recipe_hash, model_version_id))| {
                    command.candidate_recipe_hash != recipe_hash
                        || command.params.model_version_id != model_version_id
                },
            )
        {
            return Err(Self::invalid(
                "CPCV commands differ from their eligible Calibration predecessor",
            ));
        }
        Ok(())
    }

    fn require_family_params(
        family: &FeedbackCandidateFamily,
        params: &ResearchJobParams,
    ) -> QuantResult<()> {
        match params {
            ResearchJobParams::FeedbackDatasetSeal(params) => {
                Self::require_dataset_family(family, params)
            }
            ResearchJobParams::FeedbackTraining(params) => {
                Self::require_training_family(family, params)
            }
            ResearchJobParams::FeedbackCalibration(params) => {
                Self::require_calibration_family(family, params)
            }
            ResearchJobParams::FeedbackCpcv(params) => Self::require_cpcv_family(family, params),
            _ => Err(Self::invalid(
                "learning-stage job lost its candidate-family params",
            )),
        }
    }

    fn job_expectation(
        job: &ResearchJobInfo,
        stage: FeedbackStage,
    ) -> QuantResult<LearningJobExpectation> {
        let expectation = match (&job.params_json, stage) {
            (ResearchJobParams::FeedbackDatasetSeal(params), FeedbackStage::DatasetSeal) => {
                params.validate()?;
                LearningJobExpectation {
                    kind: ResearchJobKind::FeedbackDatasetSeal,
                    artifact_id: params.artifact_id,
                    feedback_cycle_id: params.feedback_cycle_id,
                    cycle_idempotency_hash: params.cycle_idempotency_hash,
                    candidate_family_hash: params.candidate_family_hash,
                    input_hash: params.input_hash()?,
                    previous: None,
                }
            }
            (ResearchJobParams::FeedbackTraining(params), FeedbackStage::Training) => {
                params.validate()?;
                LearningJobExpectation {
                    kind: ResearchJobKind::FeedbackTraining,
                    artifact_id: params.artifact_id,
                    feedback_cycle_id: params.feedback_cycle_id,
                    cycle_idempotency_hash: params.cycle_idempotency_hash,
                    candidate_family_hash: params.candidate_family_hash,
                    input_hash: params.input_hash()?,
                    previous: Some(params.previous.clone()),
                }
            }
            (ResearchJobParams::FeedbackCalibration(params), FeedbackStage::Calibration) => {
                params.validate()?;
                LearningJobExpectation {
                    kind: ResearchJobKind::FeedbackCalibration,
                    artifact_id: params.artifact_id,
                    feedback_cycle_id: params.feedback_cycle_id,
                    cycle_idempotency_hash: params.cycle_idempotency_hash,
                    candidate_family_hash: params.candidate_family_hash,
                    input_hash: params.input_hash()?,
                    previous: Some(params.previous.clone()),
                }
            }
            (ResearchJobParams::FeedbackCpcv(params), FeedbackStage::Cpcv) => {
                params.validate()?;
                LearningJobExpectation {
                    kind: ResearchJobKind::FeedbackCpcv,
                    artifact_id: params.artifact_id,
                    feedback_cycle_id: params.feedback_cycle_id,
                    cycle_idempotency_hash: params.cycle_idempotency_hash,
                    candidate_family_hash: params.candidate_family_hash,
                    input_hash: params.input_hash()?,
                    previous: Some(params.previous.clone()),
                }
            }
            _ => {
                return Err(Self::invalid(format!(
                    "{stage} job lost its exact typed learning-stage params"
                )));
            }
        };
        Ok(expectation)
    }

    fn require_result_parity(
        params: &ResearchJobParams,
        results: &FeedbackLearningStageResults,
    ) -> QuantResult<()> {
        let exact = match (params, results) {
            (
                ResearchJobParams::FeedbackDatasetSeal(params),
                FeedbackLearningStageResults::DatasetSeal(results),
            ) => {
                params.commands.len() == results.len()
                    && params
                        .commands
                        .iter()
                        .zip(results)
                        .all(|(command, result)| {
                            command.role == result.role
                                && command.request.training_dataset_id == result.training_dataset_id
                                && command.request.purpose == result.purpose
                        })
            }
            (
                ResearchJobParams::FeedbackTraining(params),
                FeedbackLearningStageResults::Training(results),
            ) => {
                params.commands.len() == results.len()
                    && params
                        .commands
                        .iter()
                        .zip(results)
                        .all(|(command, result)| {
                            command.candidate_recipe_hash == result.candidate_recipe_hash
                                && command.params.model_version_id == result.model_version_id
                                && command.params.model_run_id == result.model_run_id
                                && command.params.request.training_dataset_id
                                    == result.training_dataset_id
                        })
            }
            (
                ResearchJobParams::FeedbackCalibration(params),
                FeedbackLearningStageResults::Calibration(results),
            ) => {
                params.commands.len() == results.len()
                    && params
                        .commands
                        .iter()
                        .zip(results)
                        .all(|(command, result)| {
                            let (
                                candidate_recipe_hash,
                                source_model_version_id,
                                model_run_id,
                                calibration_dataset_id,
                                method,
                            ) = match result {
                                FeedbackCalibrationStageResult::Calibrated {
                                    candidate_recipe_hash,
                                    source_model_version_id,
                                    model_run_id,
                                    calibration_dataset_id,
                                    method,
                                    ..
                                }
                                | FeedbackCalibrationStageResult::Insufficient {
                                    candidate_recipe_hash,
                                    source_model_version_id,
                                    model_run_id,
                                    calibration_dataset_id,
                                    method,
                                    ..
                                } => (
                                    candidate_recipe_hash,
                                    source_model_version_id,
                                    model_run_id,
                                    calibration_dataset_id,
                                    method,
                                ),
                            };
                            command.candidate_recipe_hash == *candidate_recipe_hash
                                && command.params.request.model_version_id
                                    == *source_model_version_id
                                && command.params.model_run_id == *model_run_id
                                && command.params.request.calibration_dataset_id
                                    == *calibration_dataset_id
                                && command.params.request.method == *method
                        })
            }
            (
                ResearchJobParams::FeedbackCpcv(params),
                FeedbackLearningStageResults::Cpcv(results),
            ) => {
                let evaluated = results.iter().filter_map(|result| match result {
                    FeedbackCpcvStageResult::Evaluated {
                        candidate_recipe_hash,
                        model_version_id,
                        training_dataset_id,
                        path_set_id,
                        model_run_id,
                        ..
                    } => Some((
                        candidate_recipe_hash,
                        model_version_id,
                        training_dataset_id,
                        path_set_id,
                        model_run_id,
                    )),
                    FeedbackCpcvStageResult::CalibrationInsufficient { .. } => None,
                });
                params.commands.len()
                    == results
                        .iter()
                        .filter(|result| {
                            matches!(result, FeedbackCpcvStageResult::Evaluated { .. })
                        })
                        .count()
                    && params.commands.iter().zip(evaluated).all(
                        |(
                            command,
                            (
                                candidate_recipe_hash,
                                model_version_id,
                                training_dataset_id,
                                path_set_id,
                                model_run_id,
                            ),
                        )| {
                            command.candidate_recipe_hash == *candidate_recipe_hash
                                && command.params.model_version_id == *model_version_id
                                && command.params.model_run_id == *model_run_id
                                && command.params.request.training_dataset_id
                                    == *training_dataset_id
                                && command.params.request.path_set_id == Some(*path_set_id)
                        },
                    )
            }
            _ => false,
        };
        if !exact {
            return Err(Self::invalid(
                "learning-stage result identities differ from the frozen job commands",
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
