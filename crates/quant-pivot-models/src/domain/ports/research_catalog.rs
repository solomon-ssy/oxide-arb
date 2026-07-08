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
        FactorCollinearitySource, FactorCollinearityView, FactorDefinitionInfo,
        FactorDefinitionListQuery, ModelComparisonReportInfo, ModelPublishedCatalogQuery,
        ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery, Paginated,
        PublishedModelOptionView, TrainingDatasetInfo, TrainingDatasetListQuery,
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

    /// The `Published`, side-and-category-eligible candidates for one
    /// `FieldWidget::ModelVersionSelect` runtime-config field (11.2.2
    /// remediation R8). Unlike every other catalog method this is **not**
    /// paginated: the eligible set is bounded by the governed model registry
    /// (a human-curated handful of specs), never a market-scale collection.
    async fn list_published_model_options(
        &self,
        query: ModelPublishedCatalogQuery,
    ) -> QuantResult<Vec<PublishedModelOptionView>>;

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
    ///
    /// `source` selects the value plane: `Raw` correlates pre-normalization values
    /// (the methodologically correct default for detecting same-signal factors),
    /// `Normalized` correlates the post-normalization scores.
    ///
    /// When `neutralize_by_category` is set (the `factors.orthogonalize.neutralize_by`
    /// operator), each factor is residualized against its market category before
    /// correlating, so shared category composition does not masquerade as factor
    /// redundancy.
    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: Decimal,
        source: FactorCollinearitySource,
        neutralize_by_category: bool,
    ) -> QuantResult<FactorCollinearityView>;
}
