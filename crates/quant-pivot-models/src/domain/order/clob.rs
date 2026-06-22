//! Polymarket CLOB wire order types.

use crate::{
    enums::{
        common::{OrderType, Side},
        order::OrderStatus,
    },
    types::{MarketId, OrderId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Tagged order amount for CLOB submission (USD notional or share count).
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

/// Outbound CLOB order submission request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub amount: OrderAmount,
    pub price: Price,
    pub order_type: OrderType,
    pub neg_risk: bool,
}

/// CLOB order submission response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub tx_hash: Option<String>,
    pub filled_shares: Shares,
    pub avg_fill_price: Option<Price>,
    pub fee_paid: Usd,
    pub submitted_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
}
