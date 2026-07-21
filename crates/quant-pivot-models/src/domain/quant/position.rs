//! Position ledger persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        execution::{ExitReason, PositionLedgerState},
        quant::{AccountSource, OutcomeSide},
    },
    types::{EventId, MarketId, OrderIntentId, PositionId, Price, Shares, TokenId, Usd},
};

/// Persisted current-position ledger row (one lot per filled entry intent).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_position::Entity")]
pub struct PositionInfo {
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

info_from_model!(PositionInfo, crate::entities::quant_position::Model, {
    position_id, order_intent_id, token_id, market_id, event_id, category, side,
    state, shares, avg_price, cost_usd, realized_pnl_usd, source, opened_at,
    updated_at, closed_at,
});

/// Insert payload for `quant_position`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_position::ActiveModel")]
pub struct NewPosition {
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
    pub closed_at: Option<DateTime<Utc>>,
}

/// Fill fact used to upsert/open a per-intent position lot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFill {
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub filled_at: DateTime<Utc>,
    pub source: AccountSource,
}

/// Exit fact used to reduce or close an existing position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionExit {
    pub shares: Shares,
    pub avg_price: Price,
    pub proceeds_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub exited_at: DateTime<Utc>,
    pub reason: ExitReason,
}
