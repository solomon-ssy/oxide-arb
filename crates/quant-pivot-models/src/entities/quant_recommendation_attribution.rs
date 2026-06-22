//! `quant_recommendation_attribution` table entity.

use crate::{
    enums::quant::RecommendationOutcome,
    types::{RecommendationId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_attribution")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationOutcome,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_outcome_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub exit_outcome_json: Json,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub attribution_json: Json,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_recommendation::Entity",
        from = "Column::RecommendationId",
        to = "super::quant_recommendation::Column::RecommendationId"
    )]
    Recommendation,
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
