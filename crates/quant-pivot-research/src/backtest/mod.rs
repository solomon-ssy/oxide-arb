//! Backtest plane: the [`Backtester`] contract and its request/report shells.
//!
//! Offline closure (3.6). 3.0 fixes the trait + minimal I/O; the PIT replay
//! loop, greedy allocator, and portfolio metrics land in 3.6. A backtest must
//! never touch the live `BookStore` — only historical PIT.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{BacktestReportId, ContentHash, ModelVersionId};
use serde::{Deserialize, Serialize};

/// Request to backtest a model version over a historical window.
///
/// Extended in 3.6 with the selection/feature/factor inputs and cost model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestRequest {
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
}

/// A point-in-time backtest report.
///
/// Extended in 3.6 with portfolio-level performance and risk metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestReport {
    /// Report id.
    pub backtest_report_id: BacktestReportId,
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
    /// Canonical hash of the report inputs + outputs.
    pub report_hash: ContentHash,
}

/// Runs a point-in-time backtest of a model version.
#[async_trait]
pub trait Backtester: Send + Sync {
    /// Execute the backtest and produce a report.
    async fn run(&self, request: BacktestRequest) -> QuantResult<BacktestReport>;
}
