//! Historical point-in-time query contract: [`PitQueryEngine`].
//!
//! The async, `ClickHouse`-backed historical counterpart to the live, synchronous
//! [`PointInTimeDataSource`](quant_pivot_models::domain::PointInTimeDataSource).
//! PIT correctness is a hard invariant: an implementation must never return
//! state newer than the requested `as_of`. The engine + snapshot bodies land in
//! 3.5; 3.0 fixes the trait + return shells.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{MarketId, TokenId};

/// A book snapshot resolved strictly as of a past decision time.
///
/// The book body (levels, depth) is filled in 3.5; the identity + visibility
/// timestamps are fixed here so the trait contract is stable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshotAt {
    /// Token the snapshot describes.
    pub token_id: TokenId,
    /// Requested decision time.
    pub as_of: DateTime<Utc>,
    /// Timestamp of the resolved datum (`<= as_of`, never look-ahead).
    pub observed_at: DateTime<Utc>,
}

/// Market catalog context resolved strictly as of a past decision time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketContextAt {
    /// Market the context describes.
    pub market_id: MarketId,
    /// Requested decision time.
    pub as_of: DateTime<Utc>,
    /// Timestamp of the resolved datum (`<= as_of`).
    pub observed_at: DateTime<Utc>,
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
