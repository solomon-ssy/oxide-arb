//! Order domain models for Polymarket CLOB interaction.

use crate::enums::common::{OrderType, Side};
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

/// Status of an order on the CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Order accepted and fully filled.
    Filled,
    /// Order partially filled (FOK would have been killed).
    PartiallyFilled,
    /// Order rejected by the exchange.
    Rejected,
    /// Order cancelled (e.g. FAK remainder).
    Cancelled,
    /// Order is resting on the book (GTC/GTD).
    Open,
    /// Order expired (GTD past deadline).
    Expired,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Filled => write!(f, "filled"),
            Self::PartiallyFilled => write!(f, "partially_filled"),
            Self::Rejected => write!(f, "rejected"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::Open => write!(f, "open"),
            Self::Expired => write!(f, "expired"),
        }
    }
}
