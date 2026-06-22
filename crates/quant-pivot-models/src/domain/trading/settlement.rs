//! Market settlement domain messages and accounting inputs.

use crate::{
    enums::common::{SettlementTrigger, Side},
    types::{MarketId, Price, ResolutionEventId, Shares, TokenId, TradeId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::DeriveIntoActiveModel;
use serde::{Deserialize, Serialize};

/// Event-driven or retry-driven request to settle all open positions for a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSettlementRequest {
    pub market_id: MarketId,
    pub winning_token_id: TokenId,
    pub winning_outcome: String,
    pub source: SettlementTrigger,
    pub observed_at: DateTime<Utc>,
}

/// Deterministic payout calculation for a single position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementEconomics {
    pub won: bool,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
}

/// Risk-engine input once a market-resolution settlement has completed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSettlementInput {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub entry_price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub winning_token_id: TokenId,
    pub settlement_trigger: SettlementTrigger,
}

/// Minimal persisted audit row for a market-resolution signal or oracle audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionEventInfo {
    pub resolution_id: ResolutionEventId,
    pub market_id: MarketId,
    pub outcome: String,
    pub source: String,
    pub gamma_agrees: Option<bool>,
    pub ctf_agrees: Option<bool>,
    pub evidence: Option<serde_json::Value>,
    pub resolved_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ResolutionEventInfo, crate::entities::resolution_event::Model, {
    resolution_id, market_id, outcome, source, gamma_agrees, ctf_agrees,
    evidence, resolved_at, created_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::resolution_event::ActiveModel")]
pub struct NewResolutionEvent {
    pub resolution_id: ResolutionEventId,
    pub market_id: MarketId,
    pub outcome: String,
    pub source: String,
    pub gamma_agrees: Option<bool>,
    pub ctf_agrees: Option<bool>,
    pub evidence: Option<serde_json::Value>,
    pub resolved_at: DateTime<Utc>,
}
