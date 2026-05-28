//! Orderbook domain types shared across the workspace.
//!
//! [`BookLevel`] stores fixed-point [`MicroPrice`] / [`MicroShares`] for hot paths.
//! [`EndgameBookView`] provides zero-copy detection views; serde/API boundaries
//! convert via [`BookLevel::from_decimal`] / [`BookLevel::price_decimal`].

use crate::types::{
    MicroConversionError, MicroPrice, MicroShares, MicroUsd, Price, Shares, TokenId,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// Which YES/NO leg triggered a pair-level book gate failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookGateLeg {
    Yes,
    No,
}

/// Orderbook quality issue detected before feeding the opportunity pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BookGateError {
    /// At least one side has no levels (WS snapshot received but empty).
    #[error("empty {side} for token {token_id}")]
    EmptySide {
        token_id: TokenId,
        side: &'static str,
    },
    /// Data age exceeds the staleness threshold for the stalest leg.
    #[error("stale {leg:?} book for {token_id}: {age_ms}ms > {threshold_ms}ms")]
    Stale {
        leg: BookGateLeg,
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

/// A single price level in an orderbook (fixed-point hot-path representation).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookLevel {
    pub price: MicroPrice,
    pub size: MicroShares,
}

impl BookLevel {
    pub fn from_decimal(price: Price, size: Shares) -> Result<Self, MicroConversionError> {
        let price_d = price.inner();
        if price_d <= rust_decimal::Decimal::ZERO || price_d > rust_decimal::Decimal::ONE {
            return Err(MicroConversionError);
        }
        Ok(Self {
            price: MicroPrice::try_from_decimal(price_d)?,
            size: MicroShares::try_from_decimal(size.inner())?,
        })
    }

    /// Ingest-only helper: returns `None` when price/size are out of range.
    #[must_use]
    #[inline]
    pub fn try_from_decimal(price: Price, size: Shares) -> Option<Self> {
        Self::from_decimal(price, size).ok()
    }

    /// Infallible conversion when caller guarantees valid decimal ranges (test/bench seed only).
    #[doc(hidden)]
    #[must_use]
    pub fn from_decimal_unchecked(price: Price, size: Shares) -> Self {
        Self {
            price: MicroPrice::try_from_decimal(price.inner()).unwrap_or(MicroPrice::ZERO),
            size: MicroShares::try_from_decimal(size.inner()).unwrap_or(MicroShares::ZERO),
        }
    }

    #[must_use]
    #[inline]
    pub fn price_decimal(self) -> Price {
        Price::new(self.price.to_decimal())
    }

    #[must_use]
    #[inline]
    pub fn size_decimal(self) -> Shares {
        Shares::new(self.size.to_decimal())
    }

    #[must_use]
    #[inline]
    pub fn depth_usd(self) -> MicroUsd {
        self.price.mul_shares(self.size)
    }
}

/// Sum USD depth across levels (precomputed at publish time).
#[must_use]
#[inline]
pub fn total_depth_usd(levels: &[BookLevel]) -> MicroUsd {
    levels
        .iter()
        .fold(MicroUsd::ZERO, |acc, l| acc + l.depth_usd())
}

/// Sum ask-side share depth at prices at or below `limit_price` (buy walk).
#[must_use]
pub fn ask_depth_up_to(levels: &[BookLevel], limit_price: Price) -> Shares {
    levels
        .iter()
        .take_while(|level| level.price_decimal() <= limit_price)
        .fold(Shares::ZERO, |acc, level| {
            Shares::new(acc.inner() + level.size.to_decimal())
        })
}

/// Sum bid-side share depth at prices at or above `limit_price` (sell walk).
#[must_use]
pub fn bid_depth_down_to(levels: &[BookLevel], limit_price: Price) -> Shares {
    levels
        .iter()
        .take_while(|level| level.price_decimal() >= limit_price)
        .fold(Shares::ZERO, |acc, level| {
            Shares::new(acc.inner() + level.size.to_decimal())
        })
}

/// Immutable published snapshot for a single token (Arc-backed, lock-free read).
///
/// Sides use [`Arc<[BookLevel]>`] so readers share level storage without copying;
/// writers use copy-on-write on the mutable [`OrderBook`] before publish.
#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub bids: Arc<[BookLevel]>,
    pub asks: Arc<[BookLevel]>,
    pub timestamp_ms: u64,
    /// Monotonic per-token sequence bumped on each publish (SLO-2 freshness).
    pub version: u64,
    /// Sum of `price × size` across all ask levels (micro-USD).
    pub total_ask_depth_usd: MicroUsd,
    /// Sum of `price × size` across all bid levels (micro-USD).
    pub total_bid_depth_usd: MicroUsd,
}

impl BookSnapshot {
    #[must_use]
    pub fn new(
        bids: Arc<[BookLevel]>,
        asks: Arc<[BookLevel]>,
        timestamp_ms: u64,
        version: u64,
    ) -> Self {
        Self {
            total_bid_depth_usd: total_depth_usd(&bids),
            total_ask_depth_usd: total_depth_usd(&asks),
            bids,
            asks,
            timestamp_ms,
            version,
        }
    }

    #[must_use]
    #[inline]
    pub fn bid_view(&self) -> BookSideView<'_> {
        BookSideView {
            levels: &self.bids,
            timestamp_ms: self.timestamp_ms,
            total_depth_usd: self.total_bid_depth_usd,
        }
    }

    #[must_use]
    #[inline]
    pub fn ask_view(&self) -> BookSideView<'_> {
        BookSideView {
            levels: &self.asks,
            timestamp_ms: self.timestamp_ms,
            total_depth_usd: self.total_ask_depth_usd,
        }
    }

    #[must_use]
    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|l| l.price_decimal())
    }

    #[must_use]
    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|l| l.price_decimal())
    }

    /// Available ask depth fillable at or below `limit_price`.
    #[must_use]
    #[inline]
    pub fn ask_depth_up_to(&self, limit_price: Price) -> Shares {
        ask_depth_up_to(&self.asks, limit_price)
    }

    /// Available bid depth fillable at or above `limit_price`.
    #[must_use]
    #[inline]
    pub fn bid_depth_down_to(&self, limit_price: Price) -> Shares {
        bid_depth_down_to(&self.bids, limit_price)
    }
}

/// Zero-copy view of one side of an orderbook.
#[derive(Debug, Clone, Copy)]
pub struct BookSideView<'a> {
    pub levels: &'a [BookLevel],
    pub timestamp_ms: u64,
    pub total_depth_usd: MicroUsd,
}

impl BookSideView<'_> {
    #[must_use]
    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        self.levels.first().map(|l| l.price_decimal())
    }

    #[must_use]
    #[inline]
    pub fn best_price_micro(&self) -> Option<MicroPrice> {
        self.levels.first().map(|l| l.price)
    }

    #[must_use]
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }
}

/// Zero-copy YES+NO book view for the endgame detector hot path.
#[derive(Debug, Clone, Copy)]
pub struct EndgameBookView<'a> {
    pub yes_bids: BookSideView<'a>,
    pub yes_asks: BookSideView<'a>,
    pub no_bids: BookSideView<'a>,
    pub no_asks: BookSideView<'a>,
}

impl EndgameBookView<'_> {
    #[must_use]
    #[inline]
    pub fn max_staleness_ms(&self, now_ms: u64) -> u64 {
        let a = now_ms.saturating_sub(self.yes_bids.timestamp_ms);
        let b = now_ms.saturating_sub(self.yes_asks.timestamp_ms);
        let c = now_ms.saturating_sub(self.no_bids.timestamp_ms);
        let d = now_ms.saturating_sub(self.no_asks.timestamp_ms);
        a.max(b).max(c).max(d)
    }

    /// Identify the stalest YES/NO leg for error attribution.
    #[must_use]
    pub fn stalest_leg(
        &self,
        now_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> (BookGateLeg, TokenId, u64) {
        let yes_age = now_ms
            .saturating_sub(self.yes_bids.timestamp_ms)
            .max(now_ms.saturating_sub(self.yes_asks.timestamp_ms));
        let no_age = now_ms
            .saturating_sub(self.no_bids.timestamp_ms)
            .max(now_ms.saturating_sub(self.no_asks.timestamp_ms));
        if yes_age >= no_age {
            (BookGateLeg::Yes, token_yes.clone(), yes_age)
        } else {
            (BookGateLeg::No, token_no.clone(), no_age)
        }
    }
}

/// Arc-backed YES+NO pair loaded from `BookStore` (cheap to pass across threads).
#[derive(Debug, Clone)]
pub struct EndgameBookPair {
    pub yes: Arc<BookSnapshot>,
    pub no: Arc<BookSnapshot>,
}

impl EndgameBookPair {
    #[must_use]
    #[inline]
    pub fn view(&self) -> EndgameBookView<'_> {
        EndgameBookView {
            yes_bids: self.yes.bid_view(),
            yes_asks: self.yes.ask_view(),
            no_bids: self.no.bid_view(),
            no_asks: self.no.ask_view(),
        }
    }

    #[must_use]
    #[inline]
    pub fn max_staleness_ms(&self, now_ms: u64) -> u64 {
        self.view().max_staleness_ms(now_ms)
    }
}

/// Top-of-book prices for execution validation (no depth materialization).
#[derive(Debug, Clone, Copy)]
pub struct TopOfBook {
    pub yes_best_bid: Option<Price>,
    pub yes_best_ask: Option<Price>,
    pub no_best_bid: Option<Price>,
    pub no_best_ask: Option<Price>,
    pub max_staleness_ms: u64,
    pub yes_version: u64,
    pub no_version: u64,
}

/// One side (bids or asks) of an orderbook for a single token (serde/API boundary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSide {
    pub levels: Vec<BookLevel>,
    pub timestamp_ms: u64,
}

impl OrderbookSide {
    #[must_use]
    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        self.levels.first().map(|l| l.price_decimal())
    }

    #[must_use]
    #[inline]
    pub fn total_size(&self) -> Shares {
        self.levels.iter().fold(Shares::ZERO, |acc, l| {
            Shares::new(acc.inner() + l.size.to_decimal())
        })
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    #[must_use]
    pub fn from_view(view: BookSideView<'_>) -> Self {
        Self {
            levels: view.levels.to_vec(),
            timestamp_ms: view.timestamp_ms,
        }
    }
}

/// Owned YES+NO snapshot for serde/API boundaries (not used on detect hot path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndgameBookSnapshot {
    pub yes_bids: OrderbookSide,
    pub yes_asks: OrderbookSide,
    pub no_bids: OrderbookSide,
    pub no_asks: OrderbookSide,
}

impl EndgameBookSnapshot {
    #[must_use]
    #[inline]
    pub fn max_staleness_ms(&self, now_ms: u64) -> u64 {
        EndgameBookView {
            yes_bids: BookSideView {
                levels: &self.yes_bids.levels,
                timestamp_ms: self.yes_bids.timestamp_ms,
                total_depth_usd: total_depth_usd(&self.yes_bids.levels),
            },
            yes_asks: BookSideView {
                levels: &self.yes_asks.levels,
                timestamp_ms: self.yes_asks.timestamp_ms,
                total_depth_usd: total_depth_usd(&self.yes_asks.levels),
            },
            no_bids: BookSideView {
                levels: &self.no_bids.levels,
                timestamp_ms: self.no_bids.timestamp_ms,
                total_depth_usd: total_depth_usd(&self.no_bids.levels),
            },
            no_asks: BookSideView {
                levels: &self.no_asks.levels,
                timestamp_ms: self.no_asks.timestamp_ms,
                total_depth_usd: total_depth_usd(&self.no_asks.levels),
            },
        }
        .max_staleness_ms(now_ms)
    }

    #[must_use]
    pub fn from_pair(pair: &EndgameBookPair) -> Self {
        let v = pair.view();
        Self {
            yes_bids: OrderbookSide::from_view(v.yes_bids),
            yes_asks: OrderbookSide::from_view(v.yes_asks),
            no_bids: OrderbookSide::from_view(v.no_bids),
            no_asks: OrderbookSide::from_view(v.no_asks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    fn level(price: rust_decimal::Decimal) -> BookLevel {
        BookLevel::from_decimal(Price::new(price), Shares::new(dec!(10))).unwrap()
    }

    fn side(price: rust_decimal::Decimal, timestamp_ms: u64) -> OrderbookSide {
        OrderbookSide {
            levels: vec![level(price)],
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

    #[test]
    fn endgame_view_max_staleness() {
        let yes = Arc::new(BookSnapshot::new(
            Arc::from([level(dec!(0.96))]),
            Arc::from([level(dec!(0.97))]),
            900,
            0,
        ));
        let no = Arc::new(BookSnapshot::new(
            Arc::from([level(dec!(0.03))]),
            Arc::from([level(dec!(0.04))]),
            700,
            0,
        ));
        let pair = EndgameBookPair { yes, no };
        assert_eq!(pair.max_staleness_ms(1000), 300);
    }

    #[test]
    fn total_depth_precomputed() {
        let asks = Arc::from([level(dec!(0.97))]);
        let snap = BookSnapshot::new(Arc::from([]), asks, 0, 0);
        assert_eq!(snap.total_ask_depth_usd.to_decimal(), dec!(9.7));
    }

    #[test]
    fn ask_depth_up_to_walks_sorted_levels() {
        let asks = Arc::from([
            level_with_size(dec!(0.90), dec!(5)),
            level_with_size(dec!(0.92), dec!(10)),
            level_with_size(dec!(0.95), dec!(20)),
        ]);
        let snap = BookSnapshot::new(Arc::from([]), asks, 0, 0);
        assert_eq!(
            snap.ask_depth_up_to(Price::new(dec!(0.92))),
            Shares::new(dec!(15))
        );
        assert_eq!(snap.ask_depth_up_to(Price::new(dec!(0.89))), Shares::ZERO);
    }

    #[test]
    fn bid_depth_down_to_walks_sorted_levels() {
        let bids = Arc::from([
            level_with_size(dec!(0.95), dec!(8)),
            level_with_size(dec!(0.93), dec!(12)),
            level_with_size(dec!(0.90), dec!(4)),
        ]);
        let snap = BookSnapshot::new(bids, Arc::from([]), 0, 0);
        assert_eq!(
            snap.bid_depth_down_to(Price::new(dec!(0.93))),
            Shares::new(dec!(20))
        );
        assert_eq!(snap.bid_depth_down_to(Price::new(dec!(0.96))), Shares::ZERO);
    }

    fn level_with_size(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }
}
