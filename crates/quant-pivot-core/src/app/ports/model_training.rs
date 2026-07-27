//! Core implementation of [`ModelTrainingPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{TrainModelRequest, TrainedModelView},
        governance::DecisionPolicySnapshotInfo,
        ports::ModelTrainingPort,
        quant::{JobProgressSink, ModelVersionInfo},
    },
    types::{
        DecisionPolicySnapshotId, ModelVersionId, TrainingDatasetId,
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
        model_serving_preimage::ModelServingPreimageService,
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
}

#[async_trait]
impl ModelTrainingPort for CoreModelTrainingPort {
    async fn train(
        &self,
        model_version_id: ModelVersionId,
        request: TrainModelRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
        if let Some(existing) = self
            .model_registry_repo
            .find_model_version(&model_version_id)
            .await
            .map_err(QuantError::from)?
        {
            ModelTrainingRetryIdentity::try_from(&existing)?.verify_request(&request)?;
            self.serving_preimages.load(&existing).await?;
            self.frozen_model_parity
                .verify_and_record(
                    &existing,
                    "model_training_retry",
                    "verify existing candidate against its frozen training artifact",
                )
                .await?;
            return Ok(TrainedModelView::from(existing));
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
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        domain::api::TrainModelRequest,
        types::{ModelVersionId, TrainingDatasetId},
    };

    use super::ModelTrainingRetryIdentity;

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
}
