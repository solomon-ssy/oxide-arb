//! Read-only catalog port for the research plane.
//!
//! The offline research workflow (dataset plan/build, train, backtest, factor
//! governance) is ID-driven at the mutation boundary, but the operator console
//! needs paginated catalogs to browse the artifacts a workflow produces. This
//! port is the single read surface for those catalogs, mirroring
//! [`AccountReadPort`](crate::domain::AccountReadPort) and
//! [`ExecutionReadPort`](crate::domain::ExecutionReadPort): every method returns
//! a [`Paginated`] projection or a by-id row, and it never mutates state.

use async_trait::async_trait;

use crate::{
    domain::{
        BacktestReportInfo, BacktestReportListQuery, ComparisonReportListQuery,
        FactorCollinearityView, FactorDefinitionInfo, FactorDefinitionListQuery,
        ModelComparisonReportInfo, ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo,
        ModelVersionListQuery, Paginated, TrainingDatasetInfo, TrainingDatasetListQuery,
    },
    types::FactorDefinitionId,
};
use quant_pivot_error::QuantResult;
use rust_decimal::Decimal;

/// Dependency-inversion boundary between the HTTP layer and the core research
/// repositories for read-only catalog browsing.
///
/// Implemented in `quant-pivot-core` and injected into `quant_pivot_web::AppState`.
#[async_trait]
pub trait ResearchCatalogPort: Send + Sync {
    /// Page the frozen training-dataset ledger, newest first.
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>>;

    /// Page the trained-model registry, newest first.
    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>>;

    /// Page the model-spec catalog (dataset/training selector source), newest first.
    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>>;

    /// Page the append-only backtest-report ledger, newest first.
    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>>;

    /// Page the append-only pairwise comparison-report ledger, newest first.
    async fn list_comparison_reports(
        &self,
        query: ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>>;

    /// Page the factor-definition governance catalog, newest first.
    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>>;

    /// Load a single factor definition by id (detail drawer target).
    async fn find_factor(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>>;

    /// Analyze pairwise factor collinearity over the recent factor-value window.
    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: Decimal,
    ) -> QuantResult<FactorCollinearityView>;
}
