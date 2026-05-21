//! Orderbook domain types shared across the workspace.
//!
//! [`BookLevel`] is the canonical representation of a single price/size level
//! in an L2 orderbook. It is defined here (rather than in the API crate) so
//! that the algorithm crate can consume it without depending on `oxide-arb-api`.

use crate::types::{Price, Shares};
use serde::{Deserialize, Serialize};

/// A single price level in an orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Price,
    pub size: Shares,
}

/// One side (bids or asks) of an orderbook for a single token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSide {
    /// Levels sorted by price: ascending for asks, descending for bids.
    pub levels: Vec<BookLevel>,
    /// Timestamp of the last update (epoch millis from exchange).
    pub timestamp_ms: u64,
}

impl OrderbookSide {
    /// Best (first) price on this side, if any levels exist.
    #[must_use]
    pub fn best_price(&self) -> Option<Price> {
        self.levels.first().map(|l| l.price)
    }

    /// Total size across all levels.
    #[must_use]
    pub fn total_size(&self) -> Shares {
        self.levels.iter().map(|l| l.size).sum()
    }

    /// Whether this side has no levels at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

/// Complete orderbook state for a binary market (YES + NO tokens).
///
/// The algorithm crate receives this as input to the detection pipeline.
/// The `no_*` sides are `Option` because not all markets expose a separate
/// NO-token book (single-token markets derive NO from YES complement).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketBookSnapshot {
    pub yes_bids: OrderbookSide,
    pub yes_asks: OrderbookSide,
    pub no_bids: Option<OrderbookSide>,
    pub no_asks: Option<OrderbookSide>,
}
