//! `quant_position` table entity.

use crate::{
    enums::{
        common::MarketCategory,
        execution::PositionLedgerState,
        quant::{AccountSource, OutcomeSide},
    },
    types::{EventId, MarketId, OrderIntentId, PositionId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_position")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub state: PositionLedgerState,
    pub shares: Shares,
    pub avg_price: Price,
    pub cost_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub source: AccountSource,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
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
    #[sea_orm(
        belongs_to = "super::event::Entity",
        from = "Column::EventId",
        to = "super::event::Column::EventId"
    )]
    Event,
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

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
