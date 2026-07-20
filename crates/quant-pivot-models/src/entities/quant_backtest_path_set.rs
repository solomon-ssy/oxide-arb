//! `quant_backtest_path_set` table entity (Phase 11.5 CPCV result).

use crate::types::{
    BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
    TrainingDatasetId,
    backtest::{BacktestPaths, SharpeDistribution},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_backtest_path_set")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "JsonBinary")]
    pub sharpe_distribution: SharpeDistribution,
    #[sea_orm(column_type = "JsonBinary")]
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as the
    /// trial-grid Sharpe variance V used in the Deflated Sharpe Ratio.
    pub trial_count: i64,
    /// Governed trial-grid configurations evaluated for CSCV/PBO + DSR N/V.
    pub trial_grid_count: i64,
    /// Audit-only: production `coordinate_search` effective trials (not in DSR N).
    pub coord_search_effective_n: i64,
    pub path_set_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<super::quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<super::quant_model_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<super::quant_training_dataset::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<super::decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
