//! Core implementation of [`ModelTrainingPort`] for the Admin API (Phase 3.6).

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        JobProgressSink, ModelTrainingPort, ModelVersionInfo, TrainModelRequest, TrainedModelView,
    },
    runtime_config::RuntimeConfig,
    types::{ModelVersionId, RuntimeConfigVersionId},
};
use quant_pivot_repository::traits::{
    EventRepository, FavoriteLongshotBiasTableRepository, MarketLinkageRepository,
    MarketRepository, ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
    RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    model::{LabelSelector, ModelFamilyParseError},
    training::LabelName,
};

use crate::{
    app::bundles::ResearchBundle,
    service::{
        favorite_longshot_fit::resolve_frozen_bias_table,
        historical_replay::ReplayConfig,
        model_training::{
            ModelTrainerConfig, ModelTrainerService, ModelTrainerServiceDeps, TrainModelInput,
        },
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreModelTrainingPort {
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    bias_table_repo: Arc<dyn FavoriteLongshotBiasTableRepository>,
}

impl CoreModelTrainingPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
        bias_table_repo: Arc<dyn FavoriteLongshotBiasTableRepository>,
    ) -> Self {
        Self {
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            fact_read: Arc::clone(&research.quant_fact_read),
            market_repo: Arc::clone(&research.market_repo),
            event_repo: Arc::clone(&research.event_repo),
            linkage_repo: Arc::clone(&research.market_linkage_repo),
            runtime_config,
            bias_table_repo,
        }
    }

    async fn service_for(
        &self,
        runtime_config_version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<ModelTrainerService> {
        let runtime = self.load_runtime_config(runtime_config_version_id).await?;
        let max_book_staleness = Duration::from_millis(runtime.training.max_book_staleness_ms);
        let bias_table =
            resolve_frozen_bias_table(self.bias_table_repo.as_ref(), &runtime.factors).await?;
        Ok(ModelTrainerService::new(
            ModelTrainerServiceDeps {
                dataset_repo: Arc::clone(&self.dataset_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                model_registry_repo: Arc::clone(&self.model_registry_repo),
                model_run_repo: Arc::clone(&self.model_run_repo),
                fact_read: Arc::clone(&self.fact_read),
                market_repo: Arc::clone(&self.market_repo),
                event_repo: Arc::clone(&self.event_repo),
                linkage_repo: Arc::clone(&self.linkage_repo),
            },
            ModelTrainerConfig {
                factors: runtime.factors.clone(),
            },
            ReplayConfig {
                features: runtime.features,
                factors: runtime.factors,
                domain: runtime.domain,
                data_quality: runtime.data_quality,
                bias_table,
            },
            max_book_staleness,
        ))
    }

    async fn load_runtime_config(
        &self,
        runtime_config_version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<RuntimeConfig> {
        let version = self
            .runtime_config
            .load_version(runtime_config_version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "runtime_config_version",
                id: runtime_config_version_id.to_string(),
            })?;
        RuntimeConfig::from_json(&version.config_json).map_err(Into::into)
    }
}

#[async_trait]
impl ModelTrainingPort for CoreModelTrainingPort {
    async fn train(
        &self,
        request: TrainModelRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
        if let Some(model_version_id) = &request.model_version_id
            && let Some(existing) = self
                .model_registry_repo
                .find_model_version_by_id(model_version_id)
                .await
                .map_err(QuantError::from)?
        {
            return Ok(TrainedModelView::from(existing));
        }
        let model_version_id = request
            .model_version_id
            .clone()
            .unwrap_or_else(ModelVersionId::from_v7);
        let model_family = request
            .model_family
            .parse()
            .map_err(|error: ModelFamilyParseError| QuantError::config(error.to_string()))?;
        let service = self.service_for(&request.runtime_config_version_id).await?;
        let outcome = service
            .train(
                TrainModelInput {
                    model_version_id,
                    model_spec_id: request.model_spec_id,
                    training_dataset_id: request.training_dataset_id,
                    runtime_config_version_id: request.runtime_config_version_id,
                    model_family,
                    label: LabelSelector {
                        name: LabelName::new(request.label_name),
                        horizon_secs: request.label_horizon_secs,
                    },
                    prediction_horizon_secs: request.prediction_horizon_secs,
                    validation_folds: request.validation_folds,
                },
                &*progress,
                &cancel,
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
            .find_model_version_by_id(model_version_id)
            .await
            .map_err(QuantError::from)
    }
}
