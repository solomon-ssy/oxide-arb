//! Combinatorial Purged Cross-Validation (CPCV) path-set ledger persistence
//! DTOs (Phase 11.5 §3.3/§6).

use crate::{
    entities::quant_backtest_path_set,
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        TrainingDatasetId,
        backtest::{BacktestPaths, SharpeDistribution},
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Frozen CPCV + governed trial-grid validation result row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_backtest_path_set::Entity")]
pub struct BacktestPathSetInfo {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    /// `phi(N, k)` — the number of reconstructed complete paths.
    pub path_count: i64,
    /// `C(N, k)` — the number of purge/embargo/train/evaluate folds run.
    pub combination_count: i64,
    /// Median of the paths' own rank IC — the Phase 11.5 hard `RankIc` gate's
    /// data source.
    pub median_rank_ic: Decimal,
    /// `SharpeDistribution { min, p25, median, p75, max }`.
    pub sharpe_distribution: SharpeDistribution,
    /// `Vec<BacktestPath>` (`path_index`, `group_returns`, `sharpe`, `rank_ic`,
    /// `max_drawdown`, `tail_loss`) — the full reconstructed path detail.
    pub paths: BacktestPaths,
    /// The Deflated Sharpe Ratio (`PSR` evaluated at the trial-grid-corrected
    /// benchmark) — in `[0, 1]`, the Phase 11.5 hard `DeflatedSharpe` gate's
    /// data source.
    pub deflated_sharpe: Decimal,
    /// The expected-maximum-Sharpe benchmark (`SR*`) `deflated_sharpe` was
    /// evaluated against (audit visibility).
    pub dsr_benchmark_sharpe: Decimal,
    /// Probability of Backtest Overfitting — the Phase 11.5 hard `Pbo` gate's
    /// data source.
    pub pbo: Decimal,
    /// Minimum Track Record Length, in seconds — `None` when the
    /// representative path's Sharpe is non-positive (soft/informational gate).
    pub min_track_record_length_secs: Option<i64>,
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as V.
    pub trial_count: i64,
    /// Governed trial-grid configurations evaluated for CSCV/PBO + DSR N/V.
    pub trial_grid_count: i64,
    /// Audit-only: production `coordinate_search` effective trials (not in DSR N).
    pub coord_search_effective_n: i64,
    /// Content-addressed digest of the persisted path-set payload.
    pub path_set_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    BacktestPathSetInfo,
    quant_backtest_path_set::Model,
    {
        path_set_id,
        model_version_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
        path_count,
        combination_count,
        median_rank_ic,
        sharpe_distribution,
        paths,
        deflated_sharpe,
        dsr_benchmark_sharpe,
        pbo,
        min_track_record_length_secs,
        trial_count,
        trial_grid_count,
        coord_search_effective_n,
        path_set_hash,
        created_at,
    }
);

/// Insert payload for `quant_backtest_path_set`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_backtest_path_set::ActiveModel")]
pub struct NewBacktestPathSet {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    pub trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
    pub path_set_hash: ContentHash,
}
