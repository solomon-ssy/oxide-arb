//! Backtest-report ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BacktestReportInfo, NewBacktestReport},
    types::{BacktestReportId, ModelVersionId},
};

/// Persistence port for the append-only, content-addressed backtest-report ledger.
#[async_trait::async_trait]
pub trait BacktestReportRepository: Send + Sync {
    /// Insert a new backtest-report row, returning the persisted projection.
    async fn create(&self, report: NewBacktestReport) -> Result<BacktestReportInfo, StorageError>;

    /// Look up a backtest report by id.
    async fn find_by_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<BacktestReportInfo>, StorageError>;

    /// List reports for a model version, most recent first.
    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestReportInfo>, StorageError>;
}
