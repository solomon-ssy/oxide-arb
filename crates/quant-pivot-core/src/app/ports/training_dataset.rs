//! Core implementation of [`TrainingDatasetPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BuildTrainingDatasetRequest, JobProgressSink, TrainingDatasetInfo, TrainingDatasetPlanView,
        TrainingDatasetPort, TrainingDatasetView,
    },
    enums::quant::TrainingDatasetStatus,
    runtime_config::RuntimeConfig,
    types::{RuntimeConfigVersionId, TrainingDatasetId},
};
use quant_pivot_repository::traits::{
    AttributionRepository, CalibrationArtifactRepository, CatalogVersionRepository,
    FeatureRepository, MarketLinkageRepository, MarketRepository, MarketSelectionRepository,
    ModelRegistryRepository, PositionRepository, QuantFactReadRepository, RecommendationRepository,
    RuntimeConfigVersionRepository, TradePolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    training::{DatasetPlanRequest, TrainingDatasetPlanner},
};

use crate::{
    app::bundles::ResearchBundle,
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreTrainingDatasetPort {
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogVersionRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    selection_repo: Arc<dyn MarketSelectionRepository>,
    position_repo: Arc<dyn PositionRepository>,
    fee_calculator: Arc<FeeCalculator>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    model_registry: Arc<dyn ModelRegistryRepository>,
    trade_policy_repo: Arc<dyn TradePolicyRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    /// Deploy guard: hard cap on the deterministic historical spine.
    max_spine_samples: u64,
    /// Deploy tunable: `as_of` slices sampled during `plan` to estimate keep-rate.
    plan_sample_slices: u32,
    /// Deploy tunable: candidate markets sampled per slice for the keep-rate estimate.
    plan_sample_markets: u32,
}

impl CoreTrainingDatasetPort {
    /// Assemble the port from an already-wired research bundle + deploy tunables.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
        bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
        max_spine_samples: u64,
        plan_sample_slices: u32,
        plan_sample_markets: u32,
    ) -> Self {
        Self {
            fact_read: Arc::clone(&research.quant_fact_read),
            catalog_repo: Arc::clone(&research.catalog_version_repo),
            market_repo: Arc::clone(&research.market_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            attribution_repo: Arc::clone(&research.attribution_repo),
            recommendation_repo: Arc::clone(&research.recommendation_repo),
            feature_repo: Arc::clone(&research.feature_repo),
            selection_repo: Arc::clone(&research.market_selection_repo),
            position_repo: Arc::clone(&research.position_repo),
            fee_calculator: Arc::clone(&research.fee_calculator),
            linkage_repo: Arc::clone(&research.market_linkage_repo),
            model_registry: Arc::clone(&research.model_registry_repo),
            trade_policy_repo: Arc::clone(&research.trade_policy_repo),
            runtime_config,
            bias_table_repo,
            max_spine_samples,
            plan_sample_slices,
            plan_sample_markets,
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
        let bias_table =
            resolve_frozen_bias_table(self.bias_table_repo.as_ref(), &runtime.factors).await?;
        TrainingDatasetService::new(
            TrainingDatasetServiceDeps {
                fact_read: Arc::clone(&self.fact_read),
                catalog_repo: Arc::clone(&self.catalog_repo),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.dataset_repo),
                attribution_repo: Arc::clone(&self.attribution_repo),
                recommendation_repo: Arc::clone(&self.recommendation_repo),
                feature_repo: Arc::clone(&self.feature_repo),
                selection_repo: Arc::clone(&self.selection_repo),
                position_repo: Arc::clone(&self.position_repo),
                fee_calculator: Arc::clone(&self.fee_calculator),
                linkage_repo: Arc::clone(&self.linkage_repo),
                model_registry: Arc::clone(&self.model_registry),
                trade_policy_repo: Arc::clone(&self.trade_policy_repo),
                calibration_repo: Arc::clone(&self.bias_table_repo),
            },
            TrainingDatasetBuildConfig {
                features: runtime.features,
                factors: runtime.factors,
                domain: runtime.domain,
                data_quality: runtime.data_quality,
                training: runtime.training,
                selection: runtime.selection,
                labelers: default_labelers(),
                bias_table,
            },
            self.max_spine_samples,
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
            knowledge_lag_secs: body.knowledge_lag_secs,
            feature_schema_version: body.feature_schema_version,
            sample_sources: body.sample_sources.clone(),
            training_dataset_id: None,
            purpose: body.purpose,
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
            knowledge_lag_secs: body.knowledge_lag_secs,
            feature_schema_version: body.feature_schema_version,
            sample_sources: body.sample_sources.clone(),
            training_dataset_id: body.training_dataset_id.clone(),
            purpose: body.purpose,
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
        // Cheap dry-run: arithmetic spine upper bound + a bounded K×M point-in-time
        // keep-rate sample (no grid materialization).
        let counts = service
            .count_plan(
                &Self::plan_request(&request),
                self.plan_sample_slices,
                self.plan_sample_markets,
            )
            .await?;
        Ok(TrainingDatasetPlanView {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id: request.model_spec_id,
            runtime_config_version_id: request.runtime_config_version_id,
            window_start: request.window_start,
            window_end: request.window_end,
            planned_samples: counts.total,
            spine_upper_bound: counts.spine_upper_bound,
            hard_cap_exceeded: counts.hard_cap_exceeded,
            estimated_eligible_samples: counts.estimated_eligible_samples,
            keep_rate: counts.keep_rate,
            keep_rate_sample_size: counts.keep_rate_sample_size,
        })
    }

    async fn build(
        &self,
        request: BuildTrainingDatasetRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView> {
        // Effectively-once recovery: completed artifacts are returned as-is;
        // planned/building rows are validated and resumed by the service.
        if let Some(training_dataset_id) = &request.training_dataset_id
            && let Some(existing) = self.dataset_repo.find_by_id(training_dataset_id).await?
        {
            match existing.status {
                TrainingDatasetStatus::Ready | TrainingDatasetStatus::InsufficientLabels => {
                    return Ok(TrainingDatasetView::from(existing));
                }
                TrainingDatasetStatus::Failed | TrainingDatasetStatus::Expired => {
                    return Err(StorageError::state_conflict(
                        "quant_training_dataset",
                        Some(training_dataset_id),
                        format!(
                            "dataset build cannot resume from terminal status {}",
                            existing.status
                        ),
                    )
                    .into());
                }
                TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => {}
            }
        }
        let service = self.service_for(&request.runtime_config_version_id).await?;
        let plan = service.plan(Self::build_plan_request(&request)).await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        service.build_with_progress(plan, progress, cancel).await?;
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
