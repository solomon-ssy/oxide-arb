//! Data-plane event types for the WS → book-apply pipeline.
//!
//! Replaces heap-heavy legacy WS book payloads in PR-3+.
//! Control-plane variants mirror WS manager output; book payloads use `Arc<[BookLevel]>`.

use crate::{
    domain::book::BookLevel,
    enums::common::{Side, TickSize},
    types::{MarketId, Price, Shares, TokenId},
};
use std::{sync::Arc, time::Instant};

/// Monotonic + exchange timestamps captured at WS ingress (not serialized).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressTrace {
    pub mono: Instant,
    pub ws_timestamp_ms: u64,
}

impl IngressTrace {
    #[must_use]
    pub const fn new(mono: Instant, ws_timestamp_ms: u64) -> Self {
        Self {
            mono,
            ws_timestamp_ms,
        }
    }
}

/// A change to a price level (`size = 0` means removal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevelDelta {
    pub price: Price,
    pub size: Shares,
    pub side: Side,
}

/// One side of a book snapshot (zero-copy via `Arc`).
#[derive(Debug, Clone)]
pub struct BookSideData {
    pub levels: Arc<[BookLevel]>,
}

impl BookSideData {
    #[must_use]
    pub const fn from_levels(levels: Arc<[BookLevel]>) -> Self {
        Self { levels }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self {
            levels: Arc::from([]),
        }
    }
}

/// Full book snapshot command for a single token.
#[derive(Debug, Clone)]
pub struct BookSnapshotCmd {
    pub asset_id: TokenId,
    pub bids: BookSideData,
    pub asks: BookSideData,
    pub timestamp_ms: u64,
    pub trace: IngressTrace,
}

/// Incremental price-level updates for a single token.
#[derive(Debug, Clone)]
pub struct PriceDeltaCmd {
    pub asset_id: TokenId,
    pub changes: Arc<[PriceLevelDelta]>,
    pub timestamp_ms: u64,
    pub trace: IngressTrace,
}

/// Connection status for a single WS shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting { attempt: u32 },
}

/// Unified pipeline event: book data + control-plane WS events.
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    BookSnapshot(BookSnapshotCmd),
    PriceDelta(PriceDeltaCmd),
    BestBidAsk {
        asset_id: TokenId,
        best_bid: Price,
        best_ask: Price,
        timestamp_ms: u64,
        trace: IngressTrace,
    },
    TickSizeChange {
        asset_id: TokenId,
        old_tick: TickSize,
        new_tick: TickSize,
        trace: IngressTrace,
    },
    LastTradePrice {
        asset_id: TokenId,
        price: Price,
        timestamp_ms: u64,
        trace: IngressTrace,
    },
    MarketResolved {
        market_id: MarketId,
        winning_token_id: TokenId,
        winning_outcome: String,
        asset_ids: Arc<[TokenId]>,
        timestamp_ms: u64,
        trace: IngressTrace,
    },
    ShardStatus {
        shard_id: usize,
        status: ShardConnectionStatus,
    },
}
