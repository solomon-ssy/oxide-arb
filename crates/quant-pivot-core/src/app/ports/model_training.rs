//! Core implementation of [`ModelTrainingPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{
    QuantError, QuantResult,
    research::ResearchError,
    storage::{StorageError, entity::QUANT_MODEL_RUN},
};
use quant_pivot_models::{
    domain::{
        api::{ModelTrainJobParams, TrainModelRequest, TrainedModelView},
        governance::DecisionPolicySnapshotInfo,
        ports::{ModelTrainingPort, TrainingRunFinalization},
        quant::{JobProgressSink, ModelRunInfo, ModelVersionInfo},
    },
    enums::quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{
        DecisionPolicySnapshotId, ModelRunId, ModelVersionId, TrainingDatasetId,
        model_lineage::ModelVersionDerivation,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, FactorRepository, ModelRegistryRepository, ModelRunRepository,
    PolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::artifact::ArtifactStore;
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        frozen_model_parity::FrozenModelParityService,
        model_serving_preimage::{ModelPreimageReadContext, ModelServingPreimageService},
        model_training::{
            ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
        },
        trade_policy_preimage::TradePolicyPreimageVerifier,
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreModelTrainingPort {
    compute: Arc<ComputeExecutor>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    factor_repo: Arc<dyn FactorRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    trade_policy_preimages: Arc<TradePolicyPreimageVerifier>,
    serving_preimages: Arc<ModelServingPreimageService>,
    frozen_model_parity: Arc<FrozenModelParityService>,
    runtime_config: Arc<dyn PolicyRepository>,
}

struct ModelTrainingRetryIdentity {
    model_version_id: ModelVersionId,
    training_dataset_id: TrainingDatasetId,
}

impl TryFrom<&ModelVersionInfo> for ModelTrainingRetryIdentity {
    type Error = QuantError;

    fn try_from(existing: &ModelVersionInfo) -> Result<Self, Self::Error> {
        let derivation = existing.verified_derivation().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "existing model-training retry has invalid derivation lineage: {error}"
                ),
            }
        })?;
        let serving = existing.verified_serving_contract().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "existing model-training retry has an invalid serving contract: {error}"
                ),
            }
        })?;
        let Some(training_dataset_id) = existing.training_dataset_id else {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "existing model-training retry {} has no training Dataset",
                    existing.model_version_id
                ),
            }
            .into());
        };
        if !matches!(derivation, ModelVersionDerivation::Training)
            || serving.bindings().dataset.manifest.training_dataset_id != training_dataset_id
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "existing model-training retry {} is not a canonical root training version",
                    existing.model_version_id
                ),
            }
            .into());
        }
        Ok(Self {
            model_version_id: existing.model_version_id,
            training_dataset_id,
        })
    }
}

impl ModelTrainingRetryIdentity {
    fn verify_request(&self, request: &TrainModelRequest) -> QuantResult<()> {
        if request.training_dataset_id != self.training_dataset_id {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model-training retry request Dataset {} differs from existing version {} canonical training Dataset {}",
                    request.training_dataset_id,
                    self.model_version_id,
                    self.training_dataset_id,
                ),
            }
            .into());
        }
        Ok(())
    }
}

impl CoreModelTrainingPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn PolicyRepository>,
    ) -> Self {
        Self {
            compute: Arc::clone(&research.compute),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            factor_repo: Arc::clone(&research.factor_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            calibration_repo: Arc::clone(&research.calibration_artifact_repo),
            trade_policy_preimages: Arc::clone(&research.trade_policy_preimages),
            serving_preimages: Arc::clone(&research.serving_preimages),
            frozen_model_parity: Arc::clone(&research.frozen_model_parity),
            runtime_config,
        }
    }

    fn service_for(&self, policy_snapshot: DecisionPolicySnapshotInfo) -> ModelTrainerService {
        ModelTrainerService::new(
            ModelTrainerServiceDeps {
                compute: Arc::clone(&self.compute),
                dataset_repo: Arc::clone(&self.dataset_repo),
                factor_repo: Arc::clone(&self.factor_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                model_registry_repo: Arc::clone(&self.model_registry_repo),
                model_run_repo: Arc::clone(&self.model_run_repo),
                calibration_repo: Arc::clone(&self.calibration_repo),
                trade_policy_preimages: Arc::clone(&self.trade_policy_preimages),
                serving_preimages: Arc::clone(&self.serving_preimages),
            },
            ModelTrainerConfig { policy_snapshot },
        )
    }

    async fn load_policy_snapshot(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<DecisionPolicySnapshotInfo> {
        self.runtime_config
            .load_snapshot(decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "decision_policy_snapshot",
                    id: decision_policy_snapshot_id.to_string(),
                }
                .into()
            })
    }

    async fn require_retry_run(
        &self,
        model_run_id: ModelRunId,
        version: &ModelVersionInfo,
        training_dataset_id: TrainingDatasetId,
    ) -> QuantResult<()> {
        let dataset = self
            .dataset_repo
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::not_found("training_dataset", training_dataset_id))?;
        let dataset_hash =
            dataset
                .dataset_hash
                .ok_or_else(|| ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model-training retry Dataset {training_dataset_id} has no immutable hash"
                    ),
                })?;
        let run = self
            .model_run_repo
            .find_by_id(&model_run_id)
            .await?
            .ok_or_else(|| StorageError::not_found("model_run", model_run_id))?;
        if run.run_kind != ModelRunKind::Training
            || run.model_version_id != Some(version.model_version_id)
            || run.decision_policy_snapshot_id != dataset.decision_policy_snapshot_id
            || run.market_selection_id.is_some()
            || run.window_start != dataset.window_start
            || run.window_end != dataset.window_end
            || run.status != ModelRunStatus::Succeeded
            || run.input_hash != dataset_hash
            || run.output_hash != Some(version.artifact_hash)
            || run.error_code.is_some()
            || run.error_message.is_some()
            || run.finished_at.is_none()
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model-training retry run {model_run_id} does not exactly bind version {}",
                    version.model_version_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn commit_winner(run: &ModelRunInfo) -> QuantResult<TrainingRunFinalization> {
        let Some(model_version_id) = run.model_version_id else {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "succeeded training run {} has no committed model version",
                    run.model_run_id
                ),
            )
            .into());
        };
        if run.run_kind != ModelRunKind::Training
            || run.status != ModelRunStatus::Succeeded
            || run.output_hash.is_none()
            || run.error_code.is_some()
            || run.error_message.is_some()
            || run
                .finished_at
                .is_none_or(|finished_at| finished_at < run.started_at)
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "training run {} is not an exact succeeded commit winner",
                    run.model_run_id
                ),
            )
            .into());
        }
        Ok(TrainingRunFinalization::CommitWon { model_version_id })
    }

    fn is_cancelled_run(run: &ModelRunInfo) -> bool {
        run.run_kind == ModelRunKind::Training
            && run.status == ModelRunStatus::Cancelled
            && run.model_version_id.is_none()
            && run.output_hash.is_none()
            && run.error_code == Some(ModelRunErrorCode::CancelledByOperator)
            && run.error_message.is_some()
            && run
                .finished_at
                .is_some_and(|finished_at| finished_at >= run.started_at)
    }

    fn is_failed_run(run: &ModelRunInfo) -> bool {
        run.run_kind == ModelRunKind::Training
            && run.status == ModelRunStatus::Failed
            && run.model_version_id.is_none()
            && run.output_hash.is_none()
            && run.error_code == Some(ModelRunErrorCode::TrainingFailed)
            && run.error_message.is_some()
            && run
                .finished_at
                .is_some_and(|finished_at| finished_at >= run.started_at)
    }
}

#[async_trait]
impl ModelTrainingPort for CoreModelTrainingPort {
    async fn train(
        &self,
        params: ModelTrainJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
        let ModelTrainJobParams {
            model_version_id,
            model_run_id,
            request,
        } = params;
        if let Some(existing) = self
            .model_registry_repo
            .find_model_version(&model_version_id)
            .await
            .map_err(QuantError::from)?
        {
            let retry = ModelTrainingRetryIdentity::try_from(&existing)?;
            retry.verify_request(&request)?;
            self.require_retry_run(model_run_id, &existing, retry.training_dataset_id)
                .await?;
            let context = ModelPreimageReadContext::new(&cancel, None);
            self.serving_preimages.load(&existing, &context).await?;
            drop(context);
            self.frozen_model_parity
                .verify_and_record(
                    &existing,
                    "model_training_retry",
                    "verify existing candidate against its frozen training artifact",
                )
                .await?;
            let mut view = TrainedModelView::from(existing);
            view.model_run_id = Some(model_run_id);
            return Ok(view);
        }
        let _reason = request.reason;
        let dataset = self
            .dataset_repo
            .find_by_id(&request.training_dataset_id)
            .await?;
        let dataset = dataset.ok_or_else(|| StorageError::NotFound {
            entity: "training_dataset",
            id: request.training_dataset_id.to_string(),
        })?;
        let model_spec = self
            .model_registry_repo
            .find_model_spec(&dataset.model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_spec",
                id: dataset.model_spec_id.to_string(),
            })?;
        model_spec
            .training_contract
            .validate()
            .map_err(|detail| QuantError::config(format!("invalid training_contract: {detail}")))?;
        model_spec
            .input_contract
            .validate()
            .map_err(|detail| QuantError::config(format!("invalid input_contract: {detail}")))?;
        let actual_definition_hash = model_spec.definition().content_hash().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("persisted model-spec definition cannot be hashed: {error}"),
            }
        })?;
        if actual_definition_hash != model_spec.definition_hash {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "persisted model-spec definition hash mismatch: row {}, recomputed {}",
                    model_spec.definition_hash, actual_definition_hash
                ),
            }
            .into());
        }
        let policy_snapshot = self
            .load_policy_snapshot(&dataset.decision_policy_snapshot_id)
            .await?;
        let service = self.service_for(policy_snapshot);
        let outcome = Box::pin(service.train(
            TrainModelInput {
                model_version_id,
                model_run_id,
                model_spec,
                training_dataset_id: dataset.training_dataset_id,
            },
            &*progress,
            &cancel,
        ))
        .await?;
        self.frozen_model_parity
            .verify_and_record(
                &outcome.version,
                "model_training",
                "post-training full frozen dataset/model parity",
            )
            .await?;
        let mut view = TrainedModelView::from(outcome.version);
        view.model_run_id = Some(outcome.model_run_id);
        Ok(view)
    }

    async fn cancel_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<TrainingRunFinalization> {
        let Some(current) = self.model_run_repo.find_by_id(model_run_id).await? else {
            return Ok(TrainingRunFinalization::Terminalized);
        };
        match current.status {
            ModelRunStatus::Succeeded => return Self::commit_winner(&current),
            ModelRunStatus::Cancelled if Self::is_cancelled_run(&current) => {
                return Ok(TrainingRunFinalization::Terminalized);
            }
            ModelRunStatus::Running => {}
            status => {
                return Err(StorageError::state_conflict(
                    QUANT_MODEL_RUN,
                    Some(model_run_id),
                    format!("operator cancellation cannot finalize model run from {status}"),
                )
                .into());
            }
        }
        match self.model_run_repo.cancel(model_run_id, reason).await {
            Ok(terminal) if Self::is_cancelled_run(&terminal) => {
                Ok(TrainingRunFinalization::Terminalized)
            }
            Ok(terminal) => Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "operator cancellation returned a non-canonical {} terminal for {}",
                    terminal.status, terminal.model_run_id
                ),
            )
            .into()),
            Err(StorageError::StateConflict { .. }) => {
                let terminal = self
                    .model_run_repo
                    .find_by_id(model_run_id)
                    .await?
                    .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, model_run_id))?;
                match terminal.status {
                    ModelRunStatus::Succeeded => Self::commit_winner(&terminal),
                    ModelRunStatus::Cancelled if Self::is_cancelled_run(&terminal) => {
                        Ok(TrainingRunFinalization::Terminalized)
                    }
                    status => Err(StorageError::state_conflict(
                        QUANT_MODEL_RUN,
                        Some(model_run_id),
                        format!(
                            "operator cancellation lost to unexpected terminal status {status}"
                        ),
                    )
                    .into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn fail_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<TrainingRunFinalization> {
        let Some(current) = self.model_run_repo.find_by_id(model_run_id).await? else {
            return Ok(TrainingRunFinalization::Terminalized);
        };
        match current.status {
            ModelRunStatus::Succeeded => return Self::commit_winner(&current),
            ModelRunStatus::Failed if Self::is_failed_run(&current) => {
                return Ok(TrainingRunFinalization::Terminalized);
            }
            ModelRunStatus::Running => {}
            status => {
                return Err(StorageError::state_conflict(
                    QUANT_MODEL_RUN,
                    Some(model_run_id),
                    format!("retry exhaustion cannot finalize model run from {status}"),
                )
                .into());
            }
        }
        match self
            .model_run_repo
            .fail(model_run_id, ModelRunErrorCode::TrainingFailed, reason)
            .await
        {
            Ok(terminal) if Self::is_failed_run(&terminal) => {
                Ok(TrainingRunFinalization::Terminalized)
            }
            Ok(terminal) => Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "retry exhaustion returned a non-canonical {} terminal for {}",
                    terminal.status, terminal.model_run_id
                ),
            )
            .into()),
            Err(StorageError::StateConflict { .. }) => {
                let terminal = self
                    .model_run_repo
                    .find_by_id(model_run_id)
                    .await?
                    .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, model_run_id))?;
                match terminal.status {
                    ModelRunStatus::Succeeded => Self::commit_winner(&terminal),
                    ModelRunStatus::Failed if Self::is_failed_run(&terminal) => {
                        Ok(TrainingRunFinalization::Terminalized)
                    }
                    status => Err(StorageError::state_conflict(
                        QUANT_MODEL_RUN,
                        Some(model_run_id),
                        format!("retry exhaustion lost to unexpected terminal status {status}"),
                    )
                    .into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>> {
        self.model_registry_repo
            .find_model_version(model_version_id)
            .await
            .map_err(QuantError::from)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        domain::{api::TrainModelRequest, ports::TrainingRunFinalization, quant::ModelRunInfo},
        enums::quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
        types::{
            ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId, TrainingDatasetId,
        },
    };

    use super::{CoreModelTrainingPort, ModelTrainingRetryIdentity};

    fn committed_run() -> ModelRunInfo {
        let now = Utc::now();
        ModelRunInfo {
            model_run_id: ModelRunId::from_v7(),
            run_kind: ModelRunKind::Training,
            model_version_id: Some(ModelVersionId::from_v7()),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            market_selection_id: None,
            window_start: now,
            window_end: now,
            status: ModelRunStatus::Succeeded,
            input_hash: ContentHash::from_bytes([0x11; 32]),
            output_hash: Some(ContentHash::from_bytes([0x22; 32])),
            error_code: None,
            error_message: None,
            started_at: now,
            finished_at: Some(now),
        }
    }

    #[test]
    fn retry_rejects_dataset_drift() {
        let training_dataset_id = TrainingDatasetId::from_v7();
        let identity = ModelTrainingRetryIdentity {
            model_version_id: ModelVersionId::from_v7(),
            training_dataset_id,
        };
        identity
            .verify_request(&TrainModelRequest {
                training_dataset_id,
                reason: "exact retry".to_owned(),
            })
            .expect("exact Dataset retry");
        let error = identity
            .verify_request(&TrainModelRequest {
                training_dataset_id: TrainingDatasetId::from_v7(),
                reason: "different Dataset".to_owned(),
            })
            .expect_err("same version with a different Dataset must fail closed");
        assert!(
            matches!(
                error,
                QuantError::Research(ResearchError::DatasetBuild { ref detail })
                    if detail.contains("differs from existing version")
            ),
            "retry drift must remain a typed DatasetBuild error: {error}"
        );
    }

    #[test]
    fn commit_winner_is_typed() {
        let run = committed_run();
        let model_version_id = run.model_version_id.expect("committed version");
        assert_eq!(
            CoreModelTrainingPort::commit_winner(&run).expect("exact commit winner"),
            TrainingRunFinalization::CommitWon { model_version_id }
        );

        let mut incomplete = run;
        incomplete.output_hash = None;
        assert!(CoreModelTrainingPort::commit_winner(&incomplete).is_err());
        incomplete.output_hash = Some(ContentHash::from_bytes([0x22; 32]));
        incomplete.finished_at = Some(incomplete.started_at - Duration::seconds(1));
        assert!(CoreModelTrainingPort::commit_winner(&incomplete).is_err());
    }

    #[test]
    fn terminal_shapes_are_exact() {
        let mut cancelled = committed_run();
        cancelled.status = ModelRunStatus::Cancelled;
        cancelled.model_version_id = None;
        cancelled.output_hash = None;
        cancelled.error_code = Some(ModelRunErrorCode::CancelledByOperator);
        cancelled.error_message = Some("operator cancel".to_owned());
        assert!(CoreModelTrainingPort::is_cancelled_run(&cancelled));
        cancelled.output_hash = Some(ContentHash::from_bytes([0x33; 32]));
        assert!(!CoreModelTrainingPort::is_cancelled_run(&cancelled));

        let mut failed = committed_run();
        failed.status = ModelRunStatus::Failed;
        failed.model_version_id = None;
        failed.output_hash = None;
        failed.error_code = Some(ModelRunErrorCode::TrainingFailed);
        failed.error_message = Some("retry exhausted".to_owned());
        assert!(CoreModelTrainingPort::is_failed_run(&failed));
        failed.finished_at = None;
        assert!(!CoreModelTrainingPort::is_failed_run(&failed));
    }
}
