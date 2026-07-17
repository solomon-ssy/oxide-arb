//! `quant_model_comparison_report` table entity.

use crate::types::{
    BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_comparison_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub comparison_report_id: ModelComparisonReportId,
    pub baseline_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub baseline_report_id: BacktestReportId,
    pub candidate_report_id: BacktestReportId,
    pub model_run_id: ModelRunId,
    pub rank_ic_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub category_breakdown_diff: Json,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<super::quant_model_run::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
