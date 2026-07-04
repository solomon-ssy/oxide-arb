//! Backtest admin HTTP contract (Phase 3.6).
//!
//! UI surface for offline PIT backtests of a registered model version:
//!
//! 1. `POST /research/models/{id}/backtest` — replay the model over a historical
//!    window (PIT only, never the live `BookStore`), persist a
//!    [`BacktestReportView`], and optionally fit a calibrated return curve.
//! 2. `GET /research/backtest-reports/{id}` — fetch a stored report.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{BacktestReportInfo, pagination::PageRequest},
    types::{
        BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId,
        Probability, RuntimeConfigVersionId, TrainingDatasetId,
    },
};

/// Inbound body for `POST /research/models/{id}/backtest` (the model version id
/// is taken from the path).
///
/// The replay window and tick grid are defined by the frozen training dataset
/// (PIT-materialized), so the request only selects the dataset + config.
///
/// `Serialize` is derived so the request can be frozen into a durable research
/// job's `params_json` and replayed on execute.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct RunBacktestRequest {
    /// Frozen, PIT-materialized dataset to replay the model over.
    pub training_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version governing portfolio caps + provenance.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Whether to fit a calibrated return curve from realized outcomes and
    /// register a calibrated child candidate version.
    #[serde(default)]
    pub calibrate: bool,
    /// When set, run **pair mode**: replay this baseline over the same window
    /// alongside the path model (the candidate), persist both backtest reports,
    /// and persist a [`ModelComparisonReportView`] of candidate − baseline. The
    /// candidate's report (with `comparison_report_id` populated) is returned.
    #[serde(default)]
    pub comparison_model_version_id: Option<ModelVersionId>,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    /// Pre-assigned candidate report id frozen at async enqueue for
    /// effectively-once recovery; omit on direct calls — the job engine mints
    /// one before persisting params (pair mode attaches comparison on execute).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backtest_report_id: Option<BacktestReportId>,
}

/// Stored backtest report returned after a run and on fetch.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestReportView {
    pub backtest_report_id: BacktestReportId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: i64,
    pub missing_feature_count: i64,
    pub rank_ic: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: serde_json::Value,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: serde_json::Value,
    pub tail_loss: Decimal,
    pub report_pnl_simulation: serde_json::Value,
    pub report_hash: ContentHash,
    pub parquet_uri: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Set in pair mode: the persisted comparison of this candidate vs. baseline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_report_id: Option<ModelComparisonReportId>,
}

/// Paginated filter for the append-only backtest-report ledger catalog.
///
/// `from` / `to` bound `created_at`; `model_version_id` scopes to the reports
/// for one trained version. The pagination window is the shared [`PageRequest`].
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct BacktestReportListQuery {
    pub model_version_id: Option<ModelVersionId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<BacktestReportInfo> for BacktestReportView {
    fn from(info: BacktestReportInfo) -> Self {
        Self::from_info(info, None)
    }
}

impl BacktestReportView {
    /// Build a wire view, optionally attaching the pairwise comparison id when
    /// this report participated in pair-mode backtest.
    pub fn from_info(
        info: BacktestReportInfo,
        comparison_report_id: Option<ModelComparisonReportId>,
    ) -> Self {
        Self {
            backtest_report_id: info.backtest_report_id,
            model_version_id: info.model_version_id,
            model_run_id: info.model_run_id,
            runtime_config_version_id: info.runtime_config_version_id,
            window_start: info.window_start,
            window_end: info.window_end,
            coverage: info.coverage,
            sample_count: info.sample_count,
            missing_feature_count: info.missing_feature_count,
            rank_ic: info.rank_ic,
            hit_rate: info.hit_rate,
            expected_vs_realized: info.expected_vs_realized,
            max_drawdown: info.max_drawdown,
            turnover: info.turnover,
            liquidity_feasibility: info.liquidity_feasibility,
            category_breakdown: info.category_breakdown,
            tail_loss: info.tail_loss,
            report_pnl_simulation: info.report_pnl_simulation,
            report_hash: info.report_hash,
            parquet_uri: info.parquet_uri,
            created_at: info.created_at,
            comparison_report_id,
        }
    }
}
