//! Backtest-report ledger persistence DTOs.

use crate::{
    entities::quant_backtest_report,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        Probability,
        backtest::{CategoryMetrics, ExpectedVsRealized, PnlSimulation},
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Frozen, content-addressed backtest-report row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_backtest_report::Entity")]
pub struct BacktestReportInfo {
    pub backtest_report_id: BacktestReportId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: i64,
    pub missing_feature_count: i64,
    pub rank_ic: Decimal,
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: CategoryMetrics,
    pub tail_loss: Decimal,
    pub report_pnl_simulation: PnlSimulation,
    pub report_hash: ContentHash,
    pub parquet_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    BacktestReportInfo,
    quant_backtest_report::Model,
    {
        backtest_report_id,
        model_version_id,
        model_run_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
        coverage,
        sample_count,
        missing_feature_count,
        rank_ic,
        sharpe,
        hit_rate,
        expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown,
        tail_loss,
        report_pnl_simulation,
        report_hash,
        parquet_uri,
        created_at,
    }
);

/// Insert payload for `quant_backtest_report`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_backtest_report::ActiveModel")]
pub struct NewBacktestReport {
    pub backtest_report_id: BacktestReportId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: i64,
    pub missing_feature_count: i64,
    pub rank_ic: Decimal,
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: CategoryMetrics,
    pub tail_loss: Decimal,
    pub report_pnl_simulation: PnlSimulation,
    pub report_hash: ContentHash,
    pub parquet_uri: Option<String>,
}
