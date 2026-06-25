//! Core implementation of [`BacktestPort`] for the Admin API (Phase 3.6).

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BacktestPort, BacktestReportInfo, BacktestReportView, ModelComparisonReportInfo,
        RunBacktestRequest,
    },
    runtime_config::RuntimeConfig,
    types::{BacktestReportId, ModelComparisonReportId, ModelVersionId, RuntimeConfigVersionId},
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, MarketRepository, ModelComparisonReportRepository,
    ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
    RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{artifact::ArtifactStore, model::ModelRuntimeFactoryBuilder};

use crate::{
    app::bundles::ResearchBundle,
    service::{
        backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
        historical_replay::ReplayConfig,
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreBacktestPort {
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    backtest_report_repo: Arc<dyn BacktestReportRepository>,
    comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
}

impl CoreBacktestPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    ) -> Self {
        Self {
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            backtest_report_repo: Arc::clone(&research.backtest_report_repo),
            comparison_report_repo: Arc::clone(&research.comparison_report_repo),
            factory_builder: Arc::clone(&research.model_runtime_factory_builder),
            fact_read: Arc::clone(&research.quant_fact_read),
            market_repo: Arc::clone(&research.market_repo),
            runtime_config,
        }
    }

    async fn service_for(
        &self,
        runtime_config_version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<BacktestService> {
        let runtime = self.load_runtime_config(runtime_config_version_id).await?;
        let max_book_staleness = Duration::from_millis(runtime.training.max_book_staleness_ms);
        Ok(BacktestService::new(
            BacktestServiceDeps {
                dataset_repo: Arc::clone(&self.dataset_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                model_registry_repo: Arc::clone(&self.model_registry_repo),
                model_run_repo: Arc::clone(&self.model_run_repo),
                backtest_report_repo: Arc::clone(&self.backtest_report_repo),
                comparison_report_repo: Arc::clone(&self.comparison_report_repo),
                factory_builder: Arc::clone(&self.factory_builder),
                fact_read: Arc::clone(&self.fact_read),
                market_repo: Arc::clone(&self.market_repo),
            },
            &runtime.portfolio,
            ReplayConfig {
                features: runtime.features,
                factors: runtime.factors,
                data_quality: runtime.data_quality,
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
        RuntimeConfig::from_json(&version.config_json)
            .map_err(|error| QuantError::config(error.to_string()))
    }
}

#[async_trait]
impl BacktestPort for CoreBacktestPort {
    async fn run(
        &self,
        model_version_id: ModelVersionId,
        request: RunBacktestRequest,
    ) -> QuantResult<BacktestReportView> {
        let service = self.service_for(&request.runtime_config_version_id).await?;
        let input = BacktestInput {
            model_version_id,
            training_dataset_id: request.training_dataset_id,
            runtime_config_version_id: request.runtime_config_version_id,
            calibrate: request.calibrate,
        };
        match request.comparison_model_version_id {
            Some(baseline) => {
                let (info, comparison) = service.run_comparison(input, baseline).await?;
                let mut view = BacktestReportView::from(info);
                view.comparison_report_id = Some(comparison.comparison_report_id);
                Ok(view)
            }
            None => Ok(BacktestReportView::from(service.run(input).await?)),
        }
    }

    async fn find_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReportInfo>> {
        self.backtest_report_repo
            .find_by_id(backtest_report_id)
            .await
            .map_err(QuantError::from)
    }

    async fn find_comparison_report(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>> {
        self.comparison_report_repo
            .find_by_id(comparison_report_id)
            .await
            .map_err(QuantError::from)
    }
}
