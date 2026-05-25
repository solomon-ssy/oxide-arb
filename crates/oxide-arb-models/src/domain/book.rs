//! Orderbook domain types shared across the workspace.
//!
//! [`BookLevel`] is the canonical representation of a single price/size level
//! in an L2 orderbook. It is defined here (rather than in the API crate) so
//! that the algorithm crate can consume it without depending on `oxide-arb-api`.

use crate::types::{Price, Shares, TokenId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Orderbook quality issue detected before feeding the opportunity pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BookGateError {
    /// At least one side has no levels (WS snapshot received but empty).
    #[error("empty {side} for token {token_id}")]
    EmptySide {
        token_id: TokenId,
        side: &'static str,
    },
    /// Data age exceeds the expired staleness threshold.
    #[error("stale data for {token_id}: {age_ms}ms > {threshold_ms}ms")]
    Stale {
        token_id: TokenId,
        age_ms: u64,
        threshold_ms: u64,
    },
    /// Crossed book: best bid >= best ask (abnormal market state).
    #[error("crossed book for {token_id}: bid={best_bid} >= ask={best_ask}")]
    CrossedBook {
        token_id: TokenId,
        best_bid: Price,
        best_ask: Price,
    },
}

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

/// Complete YES+NO orderbook snapshot required by the endgame detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndgameBookSnapshot {
    /// YES-token bid side, sorted descending by price.
    pub yes_bids: OrderbookSide,
    /// YES-token ask side, sorted ascending by price.
    pub yes_asks: OrderbookSide,
    /// NO-token bid side, sorted descending by price.
    pub no_bids: OrderbookSide,
    /// NO-token ask side, sorted ascending by price.
    pub no_asks: OrderbookSide,
}

impl EndgameBookSnapshot {
    /// Return the maximum age, in milliseconds, across all four book sides.
    #[must_use]
    pub fn max_staleness_ms(&self, now_ms: u64) -> u64 {
        [
            self.yes_bids.timestamp_ms,
            self.yes_asks.timestamp_ms,
            self.no_bids.timestamp_ms,
            self.no_asks.timestamp_ms,
        ]
        .into_iter()
        .map(|timestamp_ms| now_ms.saturating_sub(timestamp_ms))
        .max()
        .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Price, Shares};
    use rust_decimal_macros::dec;

    fn side(price: rust_decimal::Decimal, timestamp_ms: u64) -> OrderbookSide {
        OrderbookSide {
            levels: vec![BookLevel {
                price: Price::new(price),
                size: Shares::new(dec!(10)),
            }],
            timestamp_ms,
        }
    }

    #[test]
    fn max_staleness_uses_oldest_side() {
        let book = EndgameBookSnapshot {
            yes_bids: side(dec!(0.96), 900),
            yes_asks: side(dec!(0.97), 950),
            no_bids: side(dec!(0.03), 700),
            no_asks: side(dec!(0.04), 990),
        };

        assert_eq!(book.max_staleness_ms(1000), 300);
    }
}
