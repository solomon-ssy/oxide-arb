//! `quant_backtest_report` table entity.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, quant_model_run, quant_model_version};
use crate::types::{
    BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
    Probability,
    backtest::{CategoryMetrics, ExpectedVsRealized, PnlSimulation},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_backtest_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "JsonBinary")]
    pub expected_vs_realized: ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub category_breakdown: CategoryMetrics,
    pub tail_loss: Decimal,
    #[sea_orm(column_type = "JsonBinary")]
    pub report_pnl_simulation: PnlSimulation,
    pub report_hash: ContentHash,
    pub parquet_uri: Option<String>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<quant_model_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
