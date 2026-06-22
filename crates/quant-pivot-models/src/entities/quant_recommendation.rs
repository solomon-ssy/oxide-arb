//! `quant_recommendation` table entity.

use crate::{
    enums::quant::{RecommendationStatus, SignalSide},
    types::{EventId, MarketId, Probability, RecommendationId, RecommendationReportId, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub rank: i32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: SignalSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_plan: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub sizing_plan: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub exit_plan: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub risk_envelope: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub factor_breakdown: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence_refs: Json,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_recommendation_report::Entity",
        from = "Column::RecommendationReportId",
        to = "super::quant_recommendation_report::Column::RecommendationReportId"
    )]
    RecommendationReport,
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
    #[sea_orm(
        belongs_to = "super::event::Entity",
        from = "Column::EventId",
        to = "super::event::Column::EventId"
    )]
    Event,
    #[sea_orm(has_many = "super::quant_order_intent::Entity")]
    OrderIntent,
    #[sea_orm(has_one = "super::quant_recommendation_attribution::Entity")]
    Attribution,
}

impl Related<super::quant_recommendation_report::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecommendationReport.def()
    }
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl Related<super::quant_recommendation_attribution::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Attribution.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
