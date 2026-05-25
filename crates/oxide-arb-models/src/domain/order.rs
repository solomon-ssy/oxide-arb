//! Order domain models for Polymarket CLOB interaction.

use crate::enums::common::{OrderType, Side};
use crate::enums::order::OrderStatus;
use crate::types::{MarketId, OrderId, Price, Shares, TokenId, Usd};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Request to place an order on the Polymarket CLOB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub price: Price,
    pub order_type: OrderType,
    /// Whether this market uses neg-risk CTF exchange.
    pub neg_risk: bool,
}

/// Response from the CLOB after order submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: OrderId,
    pub status: OrderStatus,
    /// Transaction hash if on-chain settlement occurred.
    pub tx_hash: Option<String>,
    pub filled_shares: Shares,
    pub avg_fill_price: Option<Price>,
    pub fee_paid: Usd,
    pub submitted_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
}
