//! `quant_report_data_quality_snapshot` table entity.

use crate::types::{ReportDataQualitySnapshotId, ReportDataQualityTokens, RuntimeConfigVersionId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_data_quality_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub report_data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub decision_at: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    #[sea_orm(column_type = "JsonBinary")]
    pub tokens_json: ReportDataQualityTokens,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RuntimeConfigVersion",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub runtime_config_version: BelongsTo<super::runtime_config_version::Entity>,
    #[sea_orm(has_many, relation_enum = "RecommendationReport")]
    pub recommendation_report: HasMany<super::quant_recommendation_report::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
