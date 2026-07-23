//! Order submission, cancellation, and query types.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{common::Side, execution::VenueTradeStatus, fee::FeeLiquidityRole},
    types::{Bps, EvmTransactionHash, MarketId, OrderId, Price, Shares, TokenId, VenueTradeId},
};
use serde::{Deserialize, Serialize};

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

/// Exact authenticated order snapshot used to discover the order's canonical
/// trade identities without scanning account history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobOrder {
    pub order_id: OrderId,
    pub is_working: bool,
    pub original_size: Shares,
    pub matched_size: Shares,
    pub associated_trade_ids: Vec<VenueTradeId>,
}

/// Authenticated account trade from CLOB data history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobTrade {
    pub trade_id: VenueTradeId,
    pub order_id: OrderId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub size: Shares,
    pub price: Price,
    pub fee_rate_bps: Bps,
    pub trader_side: FeeLiquidityRole,
    pub maker_orders: Vec<ClobMakerOrder>,
    pub status: VenueTradeStatus,
    pub transaction_hash: Option<EvmTransactionHash>,
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
