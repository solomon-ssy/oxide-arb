//! Historical point-in-time query contract: [`PitQueryEngine`].
//!
//! The async, `ClickHouse`-backed historical counterpart to the live, synchronous
//! [`PointInTimeDataSource`](quant_pivot_models::domain::PointInTimeDataSource).
//! PIT correctness is a hard invariant: an implementation must never return
//! state newer than the requested `as_of`. The return types carry the full book
//! / metadata payload so feature builders run identically over either source;
//! the `ClickHouse`-backed implementation lands in 3.5.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::market::book::BookLevel,
    enums::market::MarketStatus,
    types::{MarketId, TokenId},
};

/// A book snapshot resolved strictly as of a past decision time.
///
/// Carries the full level payload so it normalizes into
/// [`ResolvedBook`](crate::features::ResolvedBook) exactly like a live snapshot.
///
/// There is **no** separate `observed_at`: the publish time is `timestamp_ms`,
/// the single source of truth for when the datum was observed. Both the live and
/// historical paths derive `observed_at` from it identically (see
/// [`ResolvedBook`](crate::features::ResolvedBook)), so `book.age_ms` — and thus
/// the feature hash — can never diverge between online and offline builds. A PIT
/// engine must guarantee `timestamp_ms <= as_of` (never look-ahead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshotAt {
    /// Token the snapshot describes.
    pub token_id: TokenId,
    /// Requested decision time (the PIT cutoff the engine resolved against).
    pub as_of: DateTime<Utc>,
    /// Bid levels, best-first.
    pub bids: Arc<[BookLevel]>,
    /// Ask levels, best-first.
    pub asks: Arc<[BookLevel]>,
    /// Publish timestamp of the resolved snapshot, in epoch milliseconds
    /// (`<= as_of`); the canonical observed time.
    pub timestamp_ms: u64,
    /// Monotonic publish version of the resolved snapshot.
    pub version: u64,
}

/// Market catalog context resolved strictly as of a past decision time.
///
/// Carries the metadata payload so it normalizes into
/// [`ResolvedMarketContext`](crate::features::ResolvedMarketContext).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketContextAt {
    /// Market the context describes.
    pub market_id: MarketId,
    /// Requested decision time.
    pub as_of: DateTime<Utc>,
    /// Timestamp of the resolved datum (`<= as_of`).
    pub observed_at: DateTime<Utc>,
    /// Lifecycle status.
    pub status: MarketStatus,
    /// Whether the market is a neg-risk market.
    pub neg_risk: bool,
    /// Scheduled resolution time, when known.
    pub end_date: Option<DateTime<Utc>>,
    /// Catalog creation time (event-age proxy).
    pub created_at: DateTime<Utc>,
    /// Number of outcome tokens.
    pub outcome_count: u32,
}

/// Resolves historical book / market context with no look-ahead.
#[async_trait]
pub trait PitQueryEngine: Send + Sync {
    /// The book for `token_id` visible at `as_of`, if any.
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>>;

    /// The market context for `market_id` visible at `as_of`, if any.
    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>>;
}
