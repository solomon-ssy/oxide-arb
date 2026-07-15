//! Order submission, cancellation, and query types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use quant_pivot_models::{
    enums::{common::Side, fee::FeeLiquidityRole},
    types::{Bps, MarketId, OrderId, Price, Shares, TokenId},
};

/// Result of cancelling a single order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResult {
    pub order_id: OrderId,
    pub success: bool,
    pub reason: Option<String>,
}

/// Result of cancelling all orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAllResult {
    pub canceled: Vec<OrderId>,
    pub not_canceled: Vec<(OrderId, String)>,
}

/// An open order resting on the book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: OrderId,
    pub token_id: TokenId,
    pub side: Side,
    pub price: Price,
    pub size: Shares,
    pub filled: Shares,
}

/// Authenticated account trade from CLOB data history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobTrade {
    pub trade_id: String,
    pub order_id: OrderId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub size: Shares,
    pub price: Price,
    pub fee_rate_bps: Bps,
    pub trader_side: FeeLiquidityRole,
    pub maker_orders: Vec<ClobMakerOrder>,
    pub tx_hash: String,
    pub matched_at: DateTime<Utc>,
}

/// Maker leg retained from an authenticated CLOB trade response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobMakerOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub size: Shares,
    pub price: Price,
    pub fee_rate_bps: Bps,
    pub token_id: TokenId,
}
