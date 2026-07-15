//! Durable report-fact bundle outbox and `ClickHouse` verification acknowledgement.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::{
    enums::quant::ReportFactDeliveryStatus,
    types::{ArtifactUri, ContentHash, RecommendationReportId},
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_fact_delivery")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_report_id: RecommendationReportId,
    pub status: ReportFactDeliveryStatus,
    pub bundle_uri: ArtifactUri,
    pub bundle_hash: ContentHash,
    pub bundle_bytes: i64,
    pub recommendation_row_count: i64,
    pub recommendation_row_chain_hash: ContentHash,
    pub funnel_row_count: i64,
    pub funnel_row_chain_hash: ContentHash,
    pub attempt_count: i32,
    pub claim_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub verified_at: Option<DateTime<Utc>>,
    pub announced_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_recommendation_report::Entity",
        from = "Column::RecommendationReportId",
        to = "super::quant_recommendation_report::Column::RecommendationReportId"
    )]
    RecommendationReport,
}

impl Related<super::quant_recommendation_report::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecommendationReport.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
