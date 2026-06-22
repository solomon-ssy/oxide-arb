//! `quant_execution_order` table entity.

use crate::{
    enums::quant::{ExecutionOrderState, SignalSide},
    types::{ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_order")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    #[sea_orm(column_type = "Text")]
    pub order_phase: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: SignalSide,
    #[sea_orm(column_type = "Text")]
    pub order_type: String,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub venue_order_id: Option<OrderId>,
    #[sea_orm(column_type = "Text", nullable)]
    pub venue_status: Option<String>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
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
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
