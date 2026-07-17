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

#[sea_orm::model]
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

    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<super::quant_order_intent::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<Option<super::event::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
