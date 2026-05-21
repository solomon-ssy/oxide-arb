//! `trades` table entity.

use crate::enums::common::{Side, TradeOutcome};
use crate::types::{
    Bps, EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd,
};
use chrono::{DateTime, Utc};
use oxide_arb_macros::ActiveModelDefaults;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, ActiveModelDefaults)]
#[sea_orm(table_name = "trade")]
#[active_defaults(
    generate(trade_id, TradeId::generate()),
    default(outcome, TradeOutcome::Pending),
    timestamp(created_at),
    timestamp(updated_at, always)
)]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub trade_id: TradeId,
    pub execution_id: ExecutionId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub detected_edge_bps: Option<Bps>,
    pub detected_profit_usd: Option<Usd>,
    pub net_profit_usd: Option<Usd>,
    #[sea_orm(column_type = "Text", nullable)]
    pub order_id: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub tx_hash: Option<String>,
    pub outcome: TradeOutcome,
    #[sea_orm(column_type = "Text")]
    pub execution_mode: String,
    pub latency_ms: Option<i32>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
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
