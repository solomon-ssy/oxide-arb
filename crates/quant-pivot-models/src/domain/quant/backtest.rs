//! Backtest-report ledger persistence DTOs.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_backtest_report,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        Probability, TrainingDatasetId,
        backtest::{BacktestReportHashInput, CategoryMetrics, ExpectedVsRealized, PnlSimulation},
    },
};

/// Frozen, content-addressed backtest-report row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_backtest_report::Entity")]
pub struct BacktestReportInfo {
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
        evaluation_dataset_id,
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

impl BacktestReportInfo {
    /// Recompute the canonical compute-artifact hash from the persisted
    /// semantic payload.
    pub fn recomputed_hash(&self) -> Result<ContentHash, String> {
        BacktestReportHashInput::try_from(self)?
            .content_hash()
            .map_err(|error| format!("backtest report hash failed: {error}"))
    }

    /// Verify that the stored hash seals the exact persisted report payload.
    pub fn verify_hash(&self) -> Result<(), String> {
        let recomputed = self.recomputed_hash()?;
        if self.report_hash == recomputed {
            Ok(())
        } else {
            Err(format!(
                "backtest report hash mismatch: stored {}, recomputed {recomputed}",
                self.report_hash
            ))
        }
    }
}

/// Insert payload for `quant_backtest_report`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_backtest_report::ActiveModel")]
pub struct NewBacktestReport {
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

impl NewBacktestReport {
    /// Recompute the canonical compute-artifact hash before insertion.
    pub fn recomputed_hash(&self) -> Result<ContentHash, String> {
        BacktestReportHashInput::try_from(self)?
            .content_hash()
            .map_err(|error| format!("backtest report hash failed: {error}"))
    }

    /// Verify that the caller-provided hash is the exact canonical report
    /// identity. Repositories must call this inside the insertion transaction.
    pub fn verify_hash(&self) -> Result<(), String> {
        let recomputed = self.recomputed_hash()?;
        if self.report_hash == recomputed {
            Ok(())
        } else {
            Err(format!(
                "backtest report hash mismatch: stored {}, recomputed {recomputed}",
                self.report_hash
            ))
        }
    }
}

impl<'a> TryFrom<&'a BacktestReportInfo> for BacktestReportHashInput<'a> {
    type Error = String;

    fn try_from(report: &'a BacktestReportInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            backtest_report_id: &report.backtest_report_id,
            model_version_id: &report.model_version_id,
            dataset_id: &report.evaluation_dataset_id,
            decision_policy_snapshot_id: &report.decision_policy_snapshot_id,
            window_start: report.window_start,
            window_end: report.window_end,
            coverage: report.coverage,
            sample_count: u64::try_from(report.sample_count)
                .map_err(|error| format!("backtest sample_count must be non-negative: {error}"))?,
            missing_feature_count: u64::try_from(report.missing_feature_count).map_err(
                |error| format!("backtest missing_feature_count must be non-negative: {error}"),
            )?,
            rank_ic: report.rank_ic,
            sharpe: report.sharpe,
            hit_rate: report.hit_rate,
            expected_vs_realized: &report.expected_vs_realized,
            max_drawdown: report.max_drawdown,
            turnover: report.turnover,
            liquidity_feasibility: report.liquidity_feasibility,
            category_breakdown: &report.category_breakdown,
            tail_loss: report.tail_loss,
            report_pnl_simulation: &report.report_pnl_simulation,
        })
    }
}

impl<'a> TryFrom<&'a NewBacktestReport> for BacktestReportHashInput<'a> {
    type Error = String;

    fn try_from(report: &'a NewBacktestReport) -> Result<Self, Self::Error> {
        Ok(Self {
            backtest_report_id: &report.backtest_report_id,
            model_version_id: &report.model_version_id,
            dataset_id: &report.evaluation_dataset_id,
            decision_policy_snapshot_id: &report.decision_policy_snapshot_id,
            window_start: report.window_start,
            window_end: report.window_end,
            coverage: report.coverage,
            sample_count: u64::try_from(report.sample_count)
                .map_err(|error| format!("backtest sample_count must be non-negative: {error}"))?,
            missing_feature_count: u64::try_from(report.missing_feature_count).map_err(
                |error| format!("backtest missing_feature_count must be non-negative: {error}"),
            )?,
            rank_ic: report.rank_ic,
            sharpe: report.sharpe,
            hit_rate: report.hit_rate,
            expected_vs_realized: &report.expected_vs_realized,
            max_drawdown: report.max_drawdown,
            turnover: report.turnover,
            liquidity_feasibility: report.liquidity_feasibility,
            category_breakdown: &report.category_breakdown,
            tail_loss: report.tail_loss,
            report_pnl_simulation: &report.report_pnl_simulation,
        })
    }
}
