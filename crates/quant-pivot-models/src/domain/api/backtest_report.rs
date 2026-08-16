//! Backtest admin HTTP contract.
//!
//! UI surface for offline PIT backtests of a registered model version:
//!
//! 1. `POST /research/models/{id}/backtest` — replay the model over a historical
//!    window (PIT only, never the live `BookStore`), persist a
//!    [`BacktestReportView`] without changing any model artifact.
//! 2. `GET /research/backtest-reports/{id}` — fetch a stored report.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{pagination::PageRequest, quant::BacktestReportInfo},
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelComparisonReportId,
        ModelRunId, ModelVersionId, Probability, TrainingDatasetId,
        backtest::{BacktestPortfolioFunnel, CategoryMetrics, ExpectedVsRealized, PnlSimulation},
    },
};

/// Inbound body for `POST /research/models/{id}/backtest` (the model version id
/// is taken from the path).
///
/// The replay window and tick grid are defined by the frozen Evaluation dataset
/// (PIT-materialized), so the request only selects the dataset + config.
///
/// `Serialize` is derived so the request can be frozen into a durable research
/// job's `params_json` and replayed on execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct RunBacktestRequest {
    /// Frozen, reusable Evaluation holdout to replay the model over.
    pub evaluation_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version governing portfolio caps + provenance.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// When set, run **pair mode**: replay this baseline over the same window
    /// alongside the path model (the candidate), persist both backtest reports,
    /// and persist a `ModelComparisonReportView` of candidate − baseline. The
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
    pub evaluation_dataset_id: TrainingDatasetId,
    pub model_run_id: ModelRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: i64,
    pub missing_feature_count: i64,
    pub realized_return_rank_correlation: Decimal,
    /// Unannualized Sharpe ratio of the single-path per-tick return series
    /// — the debug-view sibling of the CPCV path-set's
    /// Sharpe distribution, never the alpha-significance gate's data source.
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: CategoryMetrics,
    pub tail_loss: Decimal,
    pub report_pnl_simulation: PnlSimulation,
    pub portfolio_funnel: BacktestPortfolioFunnel,
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
            evaluation_dataset_id: info.evaluation_dataset_id,
            model_run_id: info.model_run_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            window_start: info.window_start,
            window_end: info.window_end,
            coverage: info.coverage,
            sample_count: info.sample_count,
            missing_feature_count: info.missing_feature_count,
            realized_return_rank_correlation: info.realized_return_rank_correlation,
            sharpe: info.sharpe,
            hit_rate: info.hit_rate,
            expected_vs_realized: info.expected_vs_realized,
            max_drawdown: info.max_drawdown,
            turnover: info.turnover,
            liquidity_feasibility: info.liquidity_feasibility,
            category_breakdown: info.category_breakdown,
            tail_loss: info.tail_loss,
            report_pnl_simulation: info.report_pnl_simulation,
            portfolio_funnel: info.portfolio_funnel,
            report_hash: info.report_hash,
            parquet_uri: info.parquet_uri,
            created_at: info.created_at,
            comparison_report_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RunBacktestRequest;

    #[test]
    fn rejects_legacy_authority() {
        let legacy = json!({
            "evaluation_dataset_id": "01900000-0000-7000-8000-000000000001",
            "decision_policy_snapshot_id": "01900000-0000-7000-8000-000000000002",
            "calibrate": true,
            "reason": "evaluation only"
        });

        assert!(
            serde_json::from_value::<RunBacktestRequest>(legacy).is_err(),
            "Evaluation backtests must reject the retired calibration authority"
        );
    }
}
