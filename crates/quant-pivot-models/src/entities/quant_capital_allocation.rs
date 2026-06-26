//! `quant_capital_allocation` table entity.

use crate::{
    enums::execution::CapitalAllocationState,
    types::{CapitalAllocationId, OrderIntentId, RecommendationId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_capital_allocation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub capital_allocation_id: CapitalAllocationId,
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub state: CapitalAllocationState,
    pub planned_usd: Usd,
    pub allocated_usd: Usd,
    pub locked_usd: Usd,
    pub spent_usd: Usd,
    pub released_usd: Usd,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_order_intent::Entity",
        from = "Column::OrderIntentId",
        to = "super::quant_order_intent::Column::OrderIntentId"
    )]
    OrderIntent,
    #[sea_orm(
        belongs_to = "super::quant_recommendation::Entity",
        from = "Column::RecommendationId",
        to = "super::quant_recommendation::Column::RecommendationId"
    )]
    Recommendation,
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
