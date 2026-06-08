//! Order domain models for Polymarket CLOB interaction.

use crate::{
    enums::{
        common::{OrderType, Side},
        order::OrderStatus,
    },
    types::{MarketId, OrderId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "unit", content = "value")]
pub enum OrderAmount {
    Usd(Usd),
    Shares(Shares),
}

impl OrderAmount {
    #[must_use]
    pub const fn as_usd(self) -> Option<Usd> {
        match self {
            Self::Usd(value) => Some(value),
            Self::Shares(_) => None,
        }
    }

    #[must_use]
    pub const fn as_shares(self) -> Option<Shares> {
        match self {
            Self::Shares(value) => Some(value),
            Self::Usd(_) => None,
        }
    }
}

/// Request to place an order on the Polymarket CLOB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub amount: OrderAmount,
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
