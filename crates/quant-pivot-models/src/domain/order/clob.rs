//! Polymarket CLOB wire order types.

use crate::{
    enums::{
        common::{OrderType, Side},
        execution::VenueOrderStatus,
    },
    types::{MarketId, OrderAmount, OrderId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Outbound CLOB order submission request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub amount: OrderAmount,
    pub price: Price,
    pub order_type: OrderType,
    /// Maker-only admission at the venue; valid only for GTC/GTD limit orders.
    pub post_only: bool,
}

/// CLOB order submission response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: OrderId,
    pub status: VenueOrderStatus,
    pub tx_hash: Option<String>,
    pub filled_shares: Shares,
    pub avg_fill_price: Option<Price>,
    pub fee_paid: Usd,
    pub submitted_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
}
