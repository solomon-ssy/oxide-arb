//! `quant_report_data_quality_snapshot` table entity.

use crate::types::{
    DecisionPolicySnapshotId, ReportDataQualitySnapshotId, ReportDataQualityTokens,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_data_quality_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub report_data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub decision_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    #[sea_orm(column_type = "JsonBinary")]
    pub tokens_json: ReportDataQualityTokens,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<super::decision_policy_snapshot::Entity>,
    #[sea_orm(has_many, relation_enum = "RecommendationReport")]
    pub recommendation_report: HasMany<super::quant_recommendation_report::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
