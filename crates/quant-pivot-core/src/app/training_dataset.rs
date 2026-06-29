//! Core implementation of [`TrainingDatasetPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BuildTrainingDatasetRequest, TrainingDatasetInfo, TrainingDatasetPlanView,
        TrainingDatasetPort, TrainingDatasetView,
    },
    runtime_config::RuntimeConfig,
    types::{RuntimeConfigVersionId, TrainingDatasetId},
};
use quant_pivot_repository::traits::{
    AttributionRepository, FeatureRepository, MarketRepository, QuantFactReadRepository,
    RecommendationRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    training::{DatasetPlanRequest, TrainingDatasetBuilder, TrainingDatasetPlanner},
};

use crate::{
    app::bundles::ResearchBundle,
    service::training_dataset::{
        TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
        default_labelers,
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreTrainingDatasetPort {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
}

impl CoreTrainingDatasetPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    ) -> Self {
        Self {
            fact_read: Arc::clone(&research.quant_fact_read),
            market_repo: Arc::clone(&research.market_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            attribution_repo: Arc::clone(&research.attribution_repo),
            recommendation_repo: Arc::clone(&research.recommendation_repo),
            feature_repo: Arc::clone(&research.feature_repo),
            runtime_config,
        }
    }

    async fn service_for(
        &self,
        runtime_config_version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<TrainingDatasetService> {
        let version = self
            .runtime_config
            .load_version(runtime_config_version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "runtime_config_version",
                id: runtime_config_version_id.to_string(),
            })?;
        let runtime = RuntimeConfig::from_json(&version.config_json)?;
        TrainingDatasetService::new(
            TrainingDatasetServiceDeps {
                fact_read: Arc::clone(&self.fact_read),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.dataset_repo),
                attribution_repo: Arc::clone(&self.attribution_repo),
                recommendation_repo: Arc::clone(&self.recommendation_repo),
                feature_repo: Arc::clone(&self.feature_repo),
            },
            TrainingDatasetBuildConfig {
                features: runtime.features,
                factors: runtime.factors,
                data_quality: runtime.data_quality,
                training: runtime.training,
                labelers: default_labelers(),
            },
        )
    }

    fn plan_request(body: &BuildTrainingDatasetRequest) -> DatasetPlanRequest {
        DatasetPlanRequest {
            model_spec_id: body.model_spec_id.clone(),
            runtime_config_version_id: body.runtime_config_version_id.clone(),
            window_start: body.window_start,
            window_end: body.window_end,
            sample_interval_secs: body.sample_interval_secs,
            horizons_secs: body.horizons_secs.clone(),
            source_delay_secs: body.source_delay_secs,
            feature_schema_version: body.feature_schema_version,
            sample_sources: body.sample_sources.clone(),
            training_dataset_id: None,
        }
    }

    fn build_plan_request(body: &BuildTrainingDatasetRequest) -> DatasetPlanRequest {
        DatasetPlanRequest {
            model_spec_id: body.model_spec_id.clone(),
            runtime_config_version_id: body.runtime_config_version_id.clone(),
            window_start: body.window_start,
            window_end: body.window_end,
            sample_interval_secs: body.sample_interval_secs,
            horizons_secs: body.horizons_secs.clone(),
            source_delay_secs: body.source_delay_secs,
            feature_schema_version: body.feature_schema_version,
            sample_sources: body.sample_sources.clone(),
            training_dataset_id: body.training_dataset_id.clone(),
        }
    }
}

#[async_trait]
impl TrainingDatasetPort for CoreTrainingDatasetPort {
    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<Option<TrainingDatasetInfo>> {
        self.dataset_repo
            .find_by_id(training_dataset_id)
            .await
            .map_err(QuantError::from)
    }

    async fn plan(
        &self,
        request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetPlanView> {
        let service = self.service_for(&request.runtime_config_version_id).await?;
        let plan = service.plan(Self::plan_request(&request)).await?;
        let planned_samples = service.count_planned_samples(&plan).await?;
        Ok(TrainingDatasetPlanView {
            training_dataset_id: plan.training_dataset_id,
            model_spec_id: request.model_spec_id,
            runtime_config_version_id: request.runtime_config_version_id,
            window_start: request.window_start,
            window_end: request.window_end,
            planned_samples,
        })
    }

    async fn build(
        &self,
        request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetView> {
        let service = self.service_for(&request.runtime_config_version_id).await?;
        let plan = service.plan(Self::build_plan_request(&request)).await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        service.build(plan).await?;
        let info = self
            .dataset_repo
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        Ok(TrainingDatasetView::from(info))
    }
}
