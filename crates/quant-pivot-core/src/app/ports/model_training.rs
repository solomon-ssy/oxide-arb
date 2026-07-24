//! Core implementation of [`ModelTrainingPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{TrainModelRequest, TrainedModelView},
        ports::ModelTrainingPort,
        quant::{JobProgressSink, ModelVersionInfo},
    },
    runtime_config::DecisionPolicySnapshot,
    types::{DecisionPolicySnapshotId, ModelVersionId},
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, ModelRunRepository, PolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    model::{LabelSelector, objective::runtime_training_objective},
    training::LabelName,
    validation::PurgeConfig,
};
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        frozen_model_parity::FrozenModelParityService,
        historical_replay::ReplayConfig,
        model_training::{
            ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
        },
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreModelTrainingPort {
    compute: Arc<ComputeExecutor>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    frozen_model_parity: Arc<FrozenModelParityService>,
    runtime_config: Arc<dyn PolicyRepository>,
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
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            frozen_model_parity: Arc::clone(&research.frozen_model_parity),
            runtime_config,
        }
    }

    async fn service_for(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<ModelTrainerService> {
        let runtime = self
            .load_runtime_config(decision_policy_snapshot_id)
            .await?;
        Ok(ModelTrainerService::new(
            ModelTrainerServiceDeps {
                compute: Arc::clone(&self.compute),
                dataset_repo: Arc::clone(&self.dataset_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                model_registry_repo: Arc::clone(&self.model_registry_repo),
                model_run_repo: Arc::clone(&self.model_run_repo),
            },
            ModelTrainerConfig {
                factors: runtime.profile_artifacts.scoring.definition.clone(),
                objective: runtime_training_objective(
                    &runtime.profile_artifacts.research_method.research.training,
                )?,
                validation_purge: PurgeConfig {
                    embargo_pct: runtime
                        .profile_artifacts
                        .research_method
                        .research
                        .validation
                        .purge
                        .embargo_pct
                        .value,
                    min_embargo_secs: runtime
                        .profile_artifacts
                        .features
                        .definition
                        .max_lookback_secs(),
                },
            },
            ReplayConfig {
                features: runtime.profile_artifacts.features.definition,
                factors: runtime.profile_artifacts.scoring.definition,
                domain: runtime.profile_artifacts.domain.definition,
                data_quality: runtime.recommendation.data_quality,
                bias_table: None,
            },
        ))
    }

    async fn load_runtime_config(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<DecisionPolicySnapshot> {
        let version = self
            .runtime_config
            .load_snapshot(decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        Ok(version.snapshot)
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
        if model_spec.input_contract.inputs.is_empty() {
            return Err(QuantError::config(
                "model spec input_contract must contain at least one raw feature",
            ));
        }
        let prediction_horizon_secs =
            u64::try_from(model_spec.prediction_horizon_secs).map_err(|error| {
                QuantError::config(format!(
                    "model spec prediction_horizon_secs is invalid: {error}"
                ))
            })?;
        if prediction_horizon_secs == 0 {
            return Err(QuantError::config(
                "model spec prediction_horizon_secs must be positive",
            ));
        }
        let runtime = self
            .load_runtime_config(&dataset.decision_policy_snapshot_id)
            .await?;
        let service = self
            .service_for(&dataset.decision_policy_snapshot_id)
            .await?;
        let outcome = service
            .train(
                TrainModelInput {
                    model_version_id,
                    model_spec_id: dataset.model_spec_id,
                    training_dataset_id: dataset.training_dataset_id,
                    decision_policy_snapshot_id: dataset.decision_policy_snapshot_id,
                    model_family: model_spec.model_family,
                    input_contract: model_spec.input_contract.clone(),
                    label: LabelSelector {
                        name: LabelName::new(model_spec.training_contract.target_label_name),
                        horizon_secs: model_spec.training_contract.target_label_horizon_secs,
                    },
                    prediction_horizon_secs,
                    validation_folds: model_spec.training_contract.validation_folds,
                    selection_enabled_categories: runtime
                        .recommendation
                        .selection
                        .enabled_categories
                        .clone(),
                    category_scope: None,
                },
                &*progress,
                &cancel,
            )
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
