//! Admin port for offline PIT backtests (Phase 3.6).

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use std::{collections::HashMap, sync::Arc};

use crate::{
    domain::{BacktestReportView, JobProgressSink, ModelComparisonReportInfo, RunBacktestRequest},
    types::{BacktestReportId, ModelComparisonReportId, ModelVersionId},
};
use quant_pivot_error::QuantResult;

/// Dependency-inversion boundary between the HTTP layer and the core backtester.
///
/// Implemented in `quant-pivot-core` and injected into `quant_pivot_web::AppState`.
#[async_trait]
pub trait BacktestPort: Send + Sync {
    /// Replay a model version over a frozen dataset and persist a report.
    ///
    /// When `request.comparison_model_version_id` is set, runs pair mode: the
    /// baseline is replayed over the same window and a comparison report is
    /// persisted; the returned view's `comparison_report_id` is populated.
    async fn run(
        &self,
        model_version_id: ModelVersionId,
        request: RunBacktestRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestReportView>;

    /// Load a persisted backtest report (comparison id enriched when present).
    async fn find_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReportView>>;

    /// Batch-resolve comparison ids for catalog list enrichment.
    async fn comparison_ids_for_backtest_reports(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> QuantResult<HashMap<BacktestReportId, ModelComparisonReportId>>;

    /// Load a persisted pairwise comparison report.
    async fn find_comparison_report(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>>;
}
