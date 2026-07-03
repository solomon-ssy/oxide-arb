//! Pairwise model-comparison report ledger repository trait.

use std::collections::HashMap;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ComparisonReportListQuery, ModelComparisonReportInfo, NewModelComparisonReport, Paginated,
    },
    types::{BacktestReportId, ModelComparisonReportId, ModelVersionId},
};

/// Persistence port for the append-only, content-addressed comparison-report ledger.
#[async_trait::async_trait]
pub trait ModelComparisonReportRepository: Send + Sync {
    /// Insert a new comparison-report row, returning the persisted projection.
    async fn create(
        &self,
        report: NewModelComparisonReport,
    ) -> Result<ModelComparisonReportInfo, StorageError>;

    /// Look up a comparison report by id.
    async fn find_by_id(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError>;

    /// List comparison reports for a candidate model version, most recent first.
    async fn list_by_candidate_version(
        &self,
        candidate_model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelComparisonReportInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: ComparisonReportListQuery,
    ) -> Result<Paginated<ModelComparisonReportInfo>, StorageError>;

    /// Resolve the pairwise comparison (if any) that references a backtest report
    /// as either candidate or baseline, newest first when multiple exist.
    async fn find_by_backtest_report_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError>;

    /// Batch-resolve comparison ids for catalog enrichment (one lookup per page).
    async fn comparison_ids_for_backtest_reports(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> Result<HashMap<BacktestReportId, ModelComparisonReportId>, StorageError>;
}
