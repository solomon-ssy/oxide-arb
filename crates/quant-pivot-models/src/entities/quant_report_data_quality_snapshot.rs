//! `quant_report_data_quality_snapshot` table entity.

use crate::types::{ReportDataQualitySnapshotId, ReportDataQualityTokens, RuntimeConfigVersionId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::runtime_config_version::Entity",
        from = "Column::RuntimeConfigVersionId",
        to = "super::runtime_config_version::Column::RuntimeConfigVersionId"
    )]
    RuntimeConfigVersion,
    #[sea_orm(has_many = "super::quant_recommendation_report::Entity")]
    RecommendationReport,
}

impl Related<super::runtime_config_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RuntimeConfigVersion.def()
    }
}

impl Related<super::quant_recommendation_report::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecommendationReport.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
