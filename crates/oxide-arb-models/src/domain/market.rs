//! Market and event registry domain models.
//!
//! These models represent the market data as ingested from Polymarket's
//! Gamma API and enriched by the data pipeline.

use crate::enums::common::{MarketCategory, TickSize};
use crate::enums::market::MarketStatus;
use crate::types::{EventId, MarketId, Price, TokenId, Usd};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single conditional token within a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDescriptor {
    pub token_id: TokenId,
    pub outcome: String,
    /// Whether this is a neg-risk market token.
    pub neg_risk: bool,
}

/// A market (condition) in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEntry {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub neg_risk: bool,
    pub tick_size: TickSize,
    pub tokens: Vec<TokenDescriptor>,
    /// Current best bid price.
    pub best_bid: Option<Price>,
    /// Current best ask price.
    pub best_ask: Option<Price>,
    /// Total order book depth in USD.
    pub depth_usd: Option<Usd>,
    /// Minimum order size in USDC.
    pub min_order_size: Decimal,
    /// Volume in the last 24h.
    pub volume_24h: Usd,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A Polymarket event grouping multiple markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEntry {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub market_ids: Vec<MarketId>,
    pub neg_risk: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
