//! Polymarket CLOB wire order types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::{OrderType, Side},
        execution::VenueOrderStatus,
    },
    types::{
        EvmTransactionHash, MarketId, OrderId, Price, Shares, TokenId, Usd, VenueOrderAmount,
        VenueTradeId,
    },
};

/// Outbound CLOB order submission request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub amount: VenueOrderAmount,
    /// Exact fee frozen by final execution admission for this venue order.
    pub expected_fee: Usd,
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
    pub trade_ids: Vec<VenueTradeId>,
    pub transaction_hashes: Vec<EvmTransactionHash>,
    pub filled_shares: Shares,
    pub avg_fill_price: Option<Price>,
    pub submitted_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
}
