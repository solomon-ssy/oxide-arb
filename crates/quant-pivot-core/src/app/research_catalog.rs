//! Core implementation of [`ResearchCatalogPort`] — the read-only research
//! catalog surface backing the operator workbench.
//!
//! Thin delegation over the research repositories already assembled in
//! [`ResearchBundle`]; every method pages a ledger or loads a by-id row and never
//! mutates state.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        BacktestReportInfo, BacktestReportListQuery, ComparisonReportListQuery,
        FactorDefinitionInfo, FactorDefinitionListQuery, ModelComparisonReportInfo, ModelSpecInfo,
        ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery, Paginated,
        ResearchCatalogPort, TrainingDatasetInfo, TrainingDatasetListQuery,
    },
    types::FactorDefinitionId,
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, FactorRepository, ModelComparisonReportRepository,
    ModelRegistryRepository, TrainingDatasetRepository,
};

use crate::app::bundles::ResearchBundle;

/// Read-only research catalog port wired from the research bundle repositories.
pub struct CoreResearchCatalogPort {
    datasets: Arc<dyn TrainingDatasetRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    backtests: Arc<dyn BacktestReportRepository>,
    comparisons: Arc<dyn ModelComparisonReportRepository>,
    factors: Arc<dyn FactorRepository>,
}

impl CoreResearchCatalogPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(research: &ResearchBundle) -> Self {
        Self {
            datasets: Arc::clone(&research.training_dataset_repo),
            models: Arc::clone(&research.model_registry_repo),
            backtests: Arc::clone(&research.backtest_report_repo),
            comparisons: Arc::clone(&research.comparison_report_repo),
            factors: Arc::clone(&research.factor_repo),
        }
    }
}

#[async_trait]
impl ResearchCatalogPort for CoreResearchCatalogPort {
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>> {
        self.datasets.page(query).await.map_err(QuantError::from)
    }

    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>> {
        self.models
            .page_versions(query)
            .await
            .map_err(QuantError::from)
    }

    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>> {
        self.models
            .page_specs(query)
            .await
            .map_err(QuantError::from)
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        self.backtests.page(query).await.map_err(QuantError::from)
    }

    async fn list_comparison_reports(
        &self,
        query: ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>> {
        self.comparisons.page(query).await.map_err(QuantError::from)
    }

    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>> {
        self.factors
            .page_definitions(query)
            .await
            .map_err(QuantError::from)
    }

    async fn find_factor(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        self.factors
            .find_definition(factor_definition_id)
            .await
            .map_err(QuantError::from)
    }
}
