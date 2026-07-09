//! `quant_backtest_report` table entity.

use crate::types::{
    BacktestReportId, ContentHash, ModelRunId, ModelVersionId, Probability, RuntimeConfigVersionId,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_backtest_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub expected_vs_realized: Json,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub category_breakdown: Json,
    pub tail_loss: Decimal,
    #[sea_orm(column_type = "JsonBinary")]
    pub report_pnl_simulation: Json,
    pub report_hash: ContentHash,
    pub parquet_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ModelVersion,
    #[sea_orm(
        belongs_to = "super::quant_model_run::Entity",
        from = "Column::ModelRunId",
        to = "super::quant_model_run::Column::ModelRunId"
    )]
    ModelRun,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
