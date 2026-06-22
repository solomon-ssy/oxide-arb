//! Data-plane event types for the WS → book-apply pipeline.

use crate::{
    domain::market::book::BookLevel,
    enums::{
        common::{Side, TickSize},
        system::ShardConnectionStatus,
    },
    types::{MarketId, Price, Shares, TokenId},
};
use std::{sync::Arc, time::Instant};

/// Monotonic ingress trace for a WS payload.
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

/// Single price-level delta on one book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevelDelta {
    pub price: Price,
    pub size: Shares,
    pub side: Side,
}

/// One side of an L2 book snapshot command.
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

/// Full L2 book snapshot command from the WS ingest plane.
#[derive(Debug, Clone)]
pub struct BookSnapshotCmd {
    pub asset_id: TokenId,
    pub bids: BookSideData,
    pub asks: BookSideData,
    pub timestamp_ms: u64,
    pub trace: IngressTrace,
}

/// Incremental price delta command from the WS ingest plane.
#[derive(Debug, Clone)]
pub struct PriceDeltaCmd {
    pub asset_id: TokenId,
    pub changes: Arc<[PriceLevelDelta]>,
    pub timestamp_ms: u64,
    pub trace: IngressTrace,
}

/// Normalized market-data events consumed by the data pipeline.
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

impl PipelineEvent {
    #[must_use]
    pub const fn is_book_coalescable(&self) -> bool {
        matches!(self, Self::BookSnapshot(_) | Self::PriceDelta(_))
    }

    #[must_use]
    pub const fn is_market_data_event(&self) -> bool {
        self.is_book_coalescable()
    }

    #[must_use]
    pub const fn asset_id(&self) -> Option<&TokenId> {
        match self {
            Self::BookSnapshot(cmd) => Some(&cmd.asset_id),
            Self::PriceDelta(cmd) => Some(&cmd.asset_id),
            Self::BestBidAsk { asset_id, .. }
            | Self::TickSizeChange { asset_id, .. }
            | Self::LastTradePrice { asset_id, .. } => Some(asset_id),
            Self::MarketResolved { .. } | Self::ShardStatus { .. } => None,
        }
    }
}
