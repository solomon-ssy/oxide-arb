//! `quant_model_comparison_report` table entity.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::{quant_backtest_report, quant_model_run, quant_model_version};
use crate::types::{
    BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId,
    backtest::CategoryRealizedReturnRankCorrelationDeltas,
};

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
    pub realized_return_rank_correlation_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub category_breakdown_diff: CategoryRealizedReturnRankCorrelationDeltas,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<quant_model_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "BaselineModelVersion",
        from = "baseline_model_version_id",
        to = "model_version_id"
    )]
    pub baseline_model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CandidateModelVersion",
        from = "candidate_model_version_id",
        to = "model_version_id"
    )]
    pub candidate_model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "BaselineReport",
        from = "baseline_report_id",
        to = "backtest_report_id"
    )]
    pub baseline_report: BelongsTo<quant_backtest_report::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CandidateReport",
        from = "candidate_report_id",
        to = "backtest_report_id"
    )]
    pub candidate_report: BelongsTo<quant_backtest_report::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
