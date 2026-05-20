//! `trades` table entity.

use crate::enums::common::TradeOutcome;
use crate::types::{Bps, EventId, MarketId, TradeId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "trade")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub trade_id: TradeId,
    pub created_at: DateTime<Utc>,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub status: TradeOutcome,
    pub detected_edge_bps: Bps,
    pub detected_profit_usd: Usd,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub total_gas_usd: Usd,
    pub net_profit_usd: Usd,
    pub net_profit_projected_usd: Usd,
    pub detection_to_exec_ms: Option<i32>,
    pub tx_hash: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text")]
    pub opportunity_snapshot: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub validation_snapshot: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub execution_record: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::ConditionId"
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

impl ActiveModelBehavior for ActiveModel {}
