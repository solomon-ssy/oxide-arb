//! Orderbook domain types shared across the workspace.
//!
//! [`BookLevel`] stores fixed-point [`MicroPrice`] / [`MicroShares`] for hot paths.
//! [`QuantBookView`] provides zero-copy detection views; serde/API boundaries
//! convert via [`BookLevel::from_decimal`] / [`BookLevel::price_decimal`].

use std::sync::Arc;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{
    MicroConversionError, MicroPrice, MicroShares, MicroUsd, Price, Shares, TokenId,
};

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
        if price_d <= Decimal::ZERO || price_d > Decimal::ONE {
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

/// Default near-touch depth (levels per side) for queue imbalance.
///
/// Kept small so the metric reflects executable near-touch pressure rather than
/// deep resting liquidity, per OBI/MLOFI microstructure practice. The live KPI
/// (frontend `metrics.ts`) MUST use the same depth so the instantaneous KPI and
/// the persisted `imbalance_avg` series stay comparable.
pub const IMBALANCE_DEPTH_LEVELS: usize = 5;

/// Sum USD depth across levels (precomputed at publish time).
#[must_use]
#[inline]
pub fn total_depth_usd(levels: &[BookLevel]) -> MicroUsd {
    levels
        .iter()
        .fold(MicroUsd::ZERO, |acc, l| acc + l.depth_usd())
}

/// Sum share depth (Σ size) across the best `n` levels (best-first ordering).
///
/// Basis for the top-N share-weighted queue-imbalance signal. Shares (not USD
/// notional) are used deliberately: USD weighting is biased toward the ask side
/// because ask prices exceed bid prices, which structurally skews imbalance
/// negative regardless of true resting pressure.
#[must_use]
#[inline]
pub fn top_n_share_depth(levels: &[BookLevel], n: usize) -> Shares {
    levels.iter().take(n).fold(Shares::ZERO, |acc, level| {
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

/// Immutable published snapshot for a single token (Arc-backed guard read).
///
/// Sides use [`Arc<[BookLevel]>`] so readers share level storage without copying;
/// writers use copy-on-write on the mutable order book before publish.
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
    /// Constant-time values computed once when this snapshot is published.
    pub summary: BookSummary,
}

/// Precomputed hot-read and telemetry values for one immutable book snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookSummary {
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub spread: Option<Price>,
    pub mid: Option<Price>,
    pub top1_depth_usd: MicroUsd,
    pub top5_depth_usd: MicroUsd,
    pub top20_depth_usd: MicroUsd,
    pub imbalance: Option<Decimal>,
    pub crossed: bool,
}

impl BookSnapshot {
    #[must_use]
    pub fn new(
        bids: Arc<[BookLevel]>,
        asks: Arc<[BookLevel]>,
        timestamp_ms: u64,
        version: u64,
    ) -> Self {
        let bid = summarize_side(&bids);
        let ask = summarize_side(&asks);
        let best_bid = bids.first().map(|level| level.price_decimal());
        let best_ask = asks.first().map(|level| level.price_decimal());
        let (spread, mid, crossed) = match (best_bid, best_ask) {
            (Some(best_bid), Some(best_ask)) => (
                Some(Price::new(best_ask.inner() - best_bid.inner())),
                Some(Price::new(
                    (best_bid.inner() + best_ask.inner()) / Decimal::TWO,
                )),
                best_bid >= best_ask,
            ),
            _ => (None, None, false),
        };
        let bid_top5 = Decimal::from(bid.top5_shares.micro());
        let ask_top5 = Decimal::from(ask.top5_shares.micro());
        let imbalance_total = bid_top5 + ask_top5;
        let imbalance = if imbalance_total.is_zero() {
            None
        } else {
            Some((bid_top5 - ask_top5) / imbalance_total)
        };
        Self {
            total_bid_depth_usd: bid.total_depth_usd,
            total_ask_depth_usd: ask.total_depth_usd,
            summary: BookSummary {
                best_bid,
                best_ask,
                spread,
                mid,
                top1_depth_usd: bid.top1_depth_usd + ask.top1_depth_usd,
                top5_depth_usd: bid.top5_depth_usd + ask.top5_depth_usd,
                top20_depth_usd: bid.top20_depth_usd + ask.top20_depth_usd,
                imbalance,
                crossed,
            },
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
    pub const fn best_bid(&self) -> Option<Price> {
        self.summary.best_bid
    }

    #[must_use]
    #[inline]
    pub const fn best_ask(&self) -> Option<Price> {
        self.summary.best_ask
    }

    /// Available bid depth fillable at or above `limit_price`.
    #[must_use]
    #[inline]
    pub fn bid_depth_down_to(&self, limit_price: Price) -> Shares {
        bid_depth_down_to(&self.bids, limit_price)
    }
}

#[derive(Debug, Clone, Copy)]
struct SideSummary {
    total_depth_usd: MicroUsd,
    top1_depth_usd: MicroUsd,
    top5_depth_usd: MicroUsd,
    top20_depth_usd: MicroUsd,
    top5_shares: MicroShares,
}

fn summarize_side(levels: &[BookLevel]) -> SideSummary {
    let mut summary = SideSummary {
        total_depth_usd: MicroUsd::ZERO,
        top1_depth_usd: MicroUsd::ZERO,
        top5_depth_usd: MicroUsd::ZERO,
        top20_depth_usd: MicroUsd::ZERO,
        top5_shares: MicroShares::ZERO,
    };
    for (index, level) in levels.iter().copied().enumerate() {
        let depth = level.depth_usd();
        summary.total_depth_usd += depth;
        if index < 20 {
            summary.top20_depth_usd += depth;
        }
        if index < 5 {
            summary.top5_depth_usd += depth;
            summary.top5_shares += level.size;
        }
        if index == 0 {
            summary.top1_depth_usd = depth;
        }
    }
    summary
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

/// Zero-copy YES+NO book view for paired binary-market calculations.
#[derive(Debug, Clone, Copy)]
pub struct QuantBookView<'a> {
    pub yes_bids: BookSideView<'a>,
    pub yes_asks: BookSideView<'a>,
    pub no_bids: BookSideView<'a>,
    pub no_asks: BookSideView<'a>,
}

impl QuantBookView<'_> {
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
pub struct BinaryBookPair {
    pub yes: Arc<BookSnapshot>,
    pub no: Arc<BookSnapshot>,
}

impl BinaryBookPair {
    #[must_use]
    #[inline]
    pub fn view(&self) -> QuantBookView<'_> {
        QuantBookView {
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
    pub const fn is_empty(&self) -> bool {
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
pub struct QuantBookSnapshot {
    pub yes_bids: OrderbookSide,
    pub yes_asks: OrderbookSide,
    pub no_bids: OrderbookSide,
    pub no_asks: OrderbookSide,
}

impl QuantBookSnapshot {
    #[must_use]
    #[inline]
    pub fn max_staleness_ms(&self, now_ms: u64) -> u64 {
        QuantBookView {
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
    pub fn from_pair(pair: &BinaryBookPair) -> Self {
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
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    fn level(price: Decimal) -> BookLevel {
        BookLevel::from_decimal(Price::new(price), Shares::new(dec!(10))).unwrap()
    }

    fn side(price: Decimal, timestamp_ms: u64) -> OrderbookSide {
        OrderbookSide {
            levels: vec![level(price)],
            timestamp_ms,
        }
    }

    #[test]
    fn max_staleness_uses_side() {
        let book = QuantBookSnapshot {
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
        let pair = BinaryBookPair { yes, no };
        assert_eq!(pair.max_staleness_ms(1000), 300);
    }

    #[test]
    fn total_depth_precomputed() {
        let asks = Arc::from([level(dec!(0.97))]);
        let snap = BookSnapshot::new(Arc::from([]), asks, 0, 0);
        assert_eq!(snap.total_ask_depth_usd.to_decimal(), dec!(9.7));
    }

    #[test]
    fn publish_summary_matches_scans() {
        let bids = Arc::from([
            level_with_size(dec!(0.55), dec!(10)),
            level_with_size(dec!(0.54), dec!(20)),
        ]);
        let asks = Arc::from([
            level_with_size(dec!(0.60), dec!(5)),
            level_with_size(dec!(0.61), dec!(15)),
        ]);
        let snap = BookSnapshot::new(bids, asks, 0, 1);

        assert_eq!(snap.summary.best_bid, Some(Price::new(dec!(0.55))));
        assert_eq!(snap.summary.best_ask, Some(Price::new(dec!(0.60))));
        assert_eq!(snap.summary.spread, Some(Price::new(dec!(0.05))));
        assert_eq!(snap.summary.mid, Some(Price::new(dec!(0.575))));
        assert_eq!(snap.summary.top1_depth_usd.to_decimal(), dec!(8.5));
        assert_eq!(snap.summary.top5_depth_usd.to_decimal(), dec!(28.45));
        assert_eq!(snap.summary.top20_depth_usd, snap.summary.top5_depth_usd);
        assert_eq!(snap.summary.imbalance, Some(dec!(0.2)));
        assert!(!snap.summary.crossed);
    }

    #[test]
    fn bid_depth_down_levels() {
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

    fn level_with_size(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    #[test]
    fn top_n_share_shares() {
        let levels = [
            level_with_size(dec!(0.50), dec!(10)),
            level_with_size(dec!(0.49), dec!(20)),
            level_with_size(dec!(0.48), dec!(30)),
        ];
        // Share-weighted (not USD): far levels do not get a price multiplier.
        assert_eq!(top_n_share_depth(&levels, 2), Shares::new(dec!(30)));
        assert_eq!(top_n_share_depth(&levels, 10), Shares::new(dec!(60)));
        assert_eq!(top_n_share_depth(&[], 5), Shares::ZERO);
    }
}
