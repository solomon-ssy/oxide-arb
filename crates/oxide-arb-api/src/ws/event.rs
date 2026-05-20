//! Normalized WebSocket event types.

use oxide_arb_models::enums::common::TickSize;
use oxide_arb_models::types::{Price, Shares, TokenId};
use serde::{Deserialize, Serialize};

/// A single price level in the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Price,
    pub size: Shares,
}

/// A change to a price level (size=0 means removal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevelDelta {
    pub price: Price,
    pub size: Shares,
}

/// Connection status for a single shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting { attempt: u32 },
}

/// Normalized event emitted by the WebSocket manager.
#[derive(Debug, Clone)]
pub enum WsEvent {
    BookSnapshot {
        asset_id: TokenId,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        timestamp_ms: u64,
        hash: String,
    },
    PriceChange {
        asset_id: TokenId,
        changes: Vec<PriceLevelDelta>,
        timestamp_ms: u64,
    },
    BestBidAsk {
        asset_id: TokenId,
        best_bid: Price,
        best_ask: Price,
        timestamp_ms: u64,
    },
    TickSizeChange {
        asset_id: TokenId,
        old_tick: TickSize,
        new_tick: TickSize,
    },
    LastTradePrice {
        asset_id: TokenId,
        price: Price,
        timestamp_ms: u64,
    },
    ShardStatus {
        shard_id: usize,
        status: ShardConnectionStatus,
    },
}
