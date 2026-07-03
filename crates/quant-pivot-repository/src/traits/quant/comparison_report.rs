//! Pairwise model-comparison report ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ComparisonReportListQuery, ModelComparisonReportInfo, NewModelComparisonReport, Paginated,
    },
    types::{ModelComparisonReportId, ModelVersionId},
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
}
