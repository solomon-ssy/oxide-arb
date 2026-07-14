//! Data-plane event types for the WS → book-apply pipeline.

use crate::{
    domain::market::book::BookLevel,
    enums::{
        common::{Side, TickSize},
        system::ShardConnectionStatus,
    },
    types::{ContentHash, MarketId, Price, Shares, TokenId},
};
use chrono::Utc;
use rust_decimal::Decimal;
use std::{sync::Arc, time::Instant};
use uuid::Uuid;

/// Monotonic ingress trace for a WS payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressTrace {
    pub mono: Instant,
    /// Local wall-clock instant captured when the SDK payload entered our process.
    pub ingress_time_ms: i64,
    pub ws_timestamp_ms: u64,
    pub stream_session_id: Uuid,
    pub shard_id: u32,
    pub token_sequence: u64,
}

impl IngressTrace {
    #[must_use]
    pub fn new(mono: Instant, ws_timestamp_ms: u64) -> Self {
        Self {
            mono,
            ingress_time_ms: Utc::now().timestamp_millis(),
            ws_timestamp_ms,
            stream_session_id: Uuid::nil(),
            shard_id: 0,
            token_sequence: 0,
        }
    }

    pub const fn assign_stream(
        &mut self,
        stream_session_id: Uuid,
        shard_id: u32,
        token_sequence: u64,
    ) {
        self.stream_session_id = stream_session_id;
        self.shard_id = shard_id;
        self.token_sequence = token_sequence;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSessionEndReason {
    Normal,
    Resubscribe,
    Overflow,
    Disconnect,
    Shutdown,
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
        market_id: MarketId,
        asset_id: TokenId,
        price: Price,
        side: Option<Side>,
        size: Option<Shares>,
        fee_rate_bps: Option<Decimal>,
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
    StreamSessionOpened {
        stream_session_id: Uuid,
        shard_id: u32,
        subscription_token_hash: ContentHash,
        subscription_token_count: u32,
        opened_at_ms: i64,
    },
    StreamSessionClosed {
        stream_session_id: Uuid,
        shard_id: u32,
        subscription_token_hash: ContentHash,
        subscription_token_count: u32,
        received_sequences: Arc<[(TokenId, u64)]>,
        opened_at_ms: i64,
        closed_at_ms: i64,
        reason: StreamSessionEndReason,
    },
    StreamGap {
        asset_id: TokenId,
        stream_session_id: Uuid,
        shard_id: u32,
        last_received_sequence: u64,
        timestamp_ms: u64,
    },
}

impl PipelineEvent {
    #[must_use]
    pub const fn is_book_coalescable(&self) -> bool {
        matches!(self, Self::BookSnapshot(_) | Self::PriceDelta(_))
    }

    #[must_use]
    pub const fn is_market_data_event(&self) -> bool {
        matches!(
            self,
            Self::BookSnapshot(_)
                | Self::PriceDelta(_)
                | Self::BestBidAsk { .. }
                | Self::TickSizeChange { .. }
                | Self::LastTradePrice { .. }
        )
    }

    #[must_use]
    pub const fn asset_id(&self) -> Option<&TokenId> {
        match self {
            Self::BookSnapshot(cmd) => Some(&cmd.asset_id),
            Self::PriceDelta(cmd) => Some(&cmd.asset_id),
            Self::BestBidAsk { asset_id, .. }
            | Self::TickSizeChange { asset_id, .. }
            | Self::LastTradePrice { asset_id, .. }
            | Self::StreamGap { asset_id, .. } => Some(asset_id),
            Self::MarketResolved { .. }
            | Self::ShardStatus { .. }
            | Self::StreamSessionOpened { .. }
            | Self::StreamSessionClosed { .. } => None,
        }
    }

    /// Assign canonical per-token stream provenance after normalization.
    pub const fn assign_stream_provenance(
        &mut self,
        stream_session_id: Uuid,
        shard_id: u32,
        token_sequence: u64,
    ) {
        let trace = match self {
            Self::BookSnapshot(command) => Some(&mut command.trace),
            Self::PriceDelta(command) => Some(&mut command.trace),
            Self::BestBidAsk { trace, .. }
            | Self::TickSizeChange { trace, .. }
            | Self::LastTradePrice { trace, .. }
            | Self::MarketResolved { trace, .. } => Some(trace),
            Self::ShardStatus { .. }
            | Self::StreamSessionOpened { .. }
            | Self::StreamSessionClosed { .. }
            | Self::StreamGap { .. } => None,
        };
        if let Some(trace) = trace {
            trace.assign_stream(stream_session_id, shard_id, token_sequence);
        }
    }
}
