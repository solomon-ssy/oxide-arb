//! Unified, source-agnostic point-in-time inputs for feature builders.
//!
//! Live ([`PointInTimeDataSource`](quant_pivot_models::domain::PointInTimeDataSource))
//! and historical ([`PitQueryEngine`](crate::pit::PitQueryEngine)) sources both
//! normalize into [`ResolvedBook`] / [`ResolvedMarketContext`], so one builder
//! definition produces byte-identical features online and offline. Windowed
//! time-series / microstructure features read a [`MarketWindowSnapshot`] that the
//! orchestrator pre-fetches per round (never a database query in the build loop).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_models::{
    domain::TradeTapePrint,
    domain::market::{
        book::{BookLevel, BookSnapshot, IMBALANCE_DEPTH_LEVELS, top_n_share_depth},
        registry::MarketRegistryInfo,
    },
    enums::market::MarketStatus,
    types::{Bps, MarketId, MicroUsd, Price, TokenId, Usd},
};
use rust_decimal::Decimal;

use crate::pit::{BookSnapshotAt, MarketContextAt};

/// Convert epoch milliseconds to a UTC instant, falling back to `default` on
/// overflow (never panics).
fn millis_to_utc(timestamp_ms: u64, default: DateTime<Utc>) -> DateTime<Utc> {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .unwrap_or(default)
}

/// A pre-fetched, point-in-time-bounded trade-tape window for one market.
#[derive(Debug, Clone)]
pub struct TradeTapeWindowSnapshot {
    /// Market the window describes (YES+NO token fills aggregated).
    pub market_id: MarketId,
    /// Decision time the window was resolved as of.
    pub as_of: DateTime<Utc>,
    /// Source visibility delay applied to the trade tape.
    pub source_delay: Duration,
    /// Whether the trade-tape source was queried and considered available.
    pub source_available: bool,
    /// Participant rows ascending by event time, all before the PIT cutoff.
    pub prints: Vec<TradeTapePrint>,
}

impl TradeTapeWindowSnapshot {
    #[must_use]
    pub const fn empty(market_id: MarketId, as_of: DateTime<Utc>, source_delay: Duration) -> Self {
        Self {
            market_id,
            as_of,
            source_delay,
            source_available: false,
            prints: Vec::new(),
        }
    }

    #[must_use]
    pub const fn available(
        market_id: MarketId,
        as_of: DateTime<Utc>,
        source_delay: Duration,
        prints: Vec<TradeTapePrint>,
    ) -> Self {
        Self {
            market_id,
            as_of,
            source_delay,
            source_available: true,
            prints,
        }
    }

    #[must_use]
    pub fn cutoff(&self) -> DateTime<Utc> {
        self.as_of
            - ChronoDuration::from_std(self.source_delay).unwrap_or_else(|_| ChronoDuration::zero())
    }

    #[must_use]
    pub fn freshest_trade_time(&self) -> Option<DateTime<Utc>> {
        self.prints.last().map(|print| print.event_time)
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.prints.is_empty()
    }

    /// Re-label ingest availability without discarding prefetched prints.
    #[must_use]
    pub const fn with_source_available(mut self, source_available: bool) -> Self {
        self.source_available = source_available;
        self
    }

    #[must_use]
    pub fn prints_in(&self, window: Duration) -> Vec<&TradeTapePrint> {
        let cutoff = self.cutoff();
        let start =
            cutoff - ChronoDuration::from_std(window).unwrap_or_else(|_| ChronoDuration::zero());
        self.prints
            .iter()
            .filter(|print| print.event_time >= start && print.event_time < cutoff)
            .collect()
    }
}

/// A book resolved as of a decision time, independent of live vs historical
/// transport. All price/size accessors are computed from the level vectors.
#[derive(Debug, Clone)]
pub struct ResolvedBook {
    /// Token the book describes.
    pub token_id: TokenId,
    /// Bid levels, best-first.
    pub bids: Arc<[BookLevel]>,
    /// Ask levels, best-first.
    pub asks: Arc<[BookLevel]>,
    /// Publish timestamp of the underlying snapshot, in epoch milliseconds.
    pub timestamp_ms: u64,
    /// Monotonic publish version of the underlying snapshot.
    pub version: u64,
    /// When the datum was observed (`<= as_of`).
    pub observed_at: DateTime<Utc>,
}

impl ResolvedBook {
    /// Build from a live published snapshot.
    #[must_use]
    pub fn from_live(token_id: &TokenId, snapshot: &BookSnapshot, as_of: DateTime<Utc>) -> Self {
        Self {
            token_id: token_id.clone(),
            bids: Arc::clone(&snapshot.bids),
            asks: Arc::clone(&snapshot.asks),
            timestamp_ms: snapshot.timestamp_ms,
            version: snapshot.version,
            observed_at: millis_to_utc(snapshot.timestamp_ms, as_of),
        }
    }

    /// Best (highest) bid price.
    #[must_use]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|level| level.price_decimal())
    }

    /// Best (lowest) ask price.
    #[must_use]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|level| level.price_decimal())
    }

    /// Mid price `(bid + ask) / 2`, when both sides are quoted.
    #[must_use]
    pub fn mid(&self) -> Option<Price> {
        let bid = self.best_bid()?.inner();
        let ask = self.best_ask()?.inner();
        Some(Price::new((bid + ask) / Decimal::from(2)))
    }

    /// Whether either side has no levels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() || self.asks.is_empty()
    }

    /// Whether the book is crossed (best bid `>=` best ask).
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(bid), Some(ask)) if bid >= ask)
    }

    /// Total visible USD depth across both sides.
    #[must_use]
    pub fn visible_liquidity_usd(&self) -> Usd {
        Usd::new(self.bid_depth_usd().inner() + self.ask_depth_usd().inner())
    }

    /// Total bid-side USD depth.
    #[must_use]
    pub fn bid_depth_usd(&self) -> Usd {
        side_depth_usd(&self.bids, self.bids.len())
    }

    /// Total ask-side USD depth.
    #[must_use]
    pub fn ask_depth_usd(&self) -> Usd {
        side_depth_usd(&self.asks, self.asks.len())
    }

    /// Combined top-`levels` USD depth across both sides.
    #[must_use]
    pub fn top_n_depth_usd(&self, levels: u32) -> Usd {
        let n = levels as usize;
        Usd::new(side_depth_usd(&self.bids, n).inner() + side_depth_usd(&self.asks, n).inner())
    }

    /// Top-N share-weighted queue imbalance `(bid - ask) / (bid + ask)` in
    /// `[-1, 1]`, positive = bid-heavy.
    ///
    /// Uses near-touch share depth (best [`IMBALANCE_DEPTH_LEVELS`] levels per
    /// side), not full-book USD notional. Full-book USD weighting is
    /// structurally ask-biased (ask prices exceed bid prices) and dominated by
    /// deep resting liquidity, which strips the signal of predictive meaning.
    /// Kept identical to the ingest-side `imbalance()` so live, persisted, and
    /// materialized values share one definition.
    #[must_use]
    pub fn depth_imbalance(&self) -> Option<Decimal> {
        let bid = top_n_share_depth(&self.bids, IMBALANCE_DEPTH_LEVELS).inner();
        let ask = top_n_share_depth(&self.asks, IMBALANCE_DEPTH_LEVELS).inner();
        let total = bid + ask;
        if total.is_zero() {
            return None;
        }
        Some((bid - ask) / total)
    }

    /// Ask-side price impact slope: price increase per cumulative share across
    /// available levels. `None` when fewer than two levels or zero cumulative
    /// size.
    #[must_use]
    pub fn slope(&self) -> Option<Decimal> {
        if self.asks.len() < 2 {
            return None;
        }
        let first = self.asks.first()?.price_decimal().inner();
        let last = self.asks.last()?.price_decimal().inner();
        let cumulative: Decimal = self
            .asks
            .iter()
            .map(|level| level.size_decimal().inner())
            .sum();
        if cumulative.is_zero() {
            return None;
        }
        Some((last - first) / cumulative)
    }
}

impl From<BookSnapshotAt> for ResolvedBook {
    fn from(snapshot: BookSnapshotAt) -> Self {
        // `observed_at` is derived from `timestamp_ms` with the exact same rule
        // as `from_live`, so an identical book produces an identical `book.age_ms`
        // (and feature hash) online and offline.
        let observed_at = millis_to_utc(snapshot.timestamp_ms, snapshot.as_of);
        Self {
            token_id: snapshot.token_id,
            bids: snapshot.bids,
            asks: snapshot.asks,
            timestamp_ms: snapshot.timestamp_ms,
            version: snapshot.version,
            observed_at,
        }
    }
}

/// Sum USD notional across the first `take` levels of a side.
fn side_depth_usd(levels: &[BookLevel], take: usize) -> Usd {
    let micro = levels
        .iter()
        .take(take)
        .fold(MicroUsd::ZERO, |acc, level| acc + level.depth_usd());
    Usd::new(micro.to_decimal())
}

/// Market catalog context resolved as of a decision time, source-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMarketContext {
    /// Market the context describes.
    pub market_id: MarketId,
    /// When the datum was observed (`<= as_of`).
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

impl ResolvedMarketContext {
    /// Build from the live market registry projection.
    #[must_use]
    pub fn from_live(info: &MarketRegistryInfo) -> Self {
        Self {
            market_id: info.market_id.clone(),
            observed_at: info.updated_at,
            status: info.status,
            neg_risk: info.neg_risk,
            end_date: info.end_date,
            created_at: info.created_at,
            outcome_count: u32::try_from(info.tokens.len()).unwrap_or(u32::MAX),
        }
    }
}

impl From<MarketContextAt> for ResolvedMarketContext {
    fn from(context: MarketContextAt) -> Self {
        Self {
            market_id: context.market_id,
            observed_at: context.observed_at,
            status: context.status,
            neg_risk: context.neg_risk,
            end_date: context.end_date,
            created_at: context.created_at,
            outcome_count: context.outcome_count,
        }
    }
}

/// One pre-aggregated microstructure bucket (mirrors `book_microstructure_1s`).
///
/// All optional fields are `None` when the underlying aggregate had no data, so
/// builders distinguish "no quote" from a real zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicrostructureBucket {
    /// Bucket start time (`<= as_of - source_delay`).
    pub bucket_time: DateTime<Utc>,
    /// Closing mid price in the bucket.
    pub mid_close: Option<Price>,
    /// Average spread in basis points.
    pub spread_bps_avg: Option<Bps>,
    /// Average top-1 depth in USD.
    pub top1_depth_usd_avg: Option<Usd>,
    /// Average top-5 depth in USD.
    pub top5_depth_usd_avg: Option<Usd>,
    /// Average bid/ask depth imbalance.
    pub imbalance_avg: Option<Decimal>,
    /// Number of book updates in the bucket.
    pub update_count: u64,
    /// Number of full snapshots in the bucket.
    pub snapshot_count: u64,
    /// Number of deltas in the bucket.
    pub delta_count: u64,
    /// Number of crossed-book observations in the bucket.
    pub crossed_count: u64,
    /// Number of sequence gaps in the bucket.
    pub gap_count: u64,
    /// Worst book age observed in the bucket, in milliseconds.
    pub max_book_age_ms: u64,
}

/// A pre-fetched, point-in-time-bounded window of microstructure buckets for one
/// token, ascending by `bucket_time`. Every bucket satisfies the PIT cutoff
/// `bucket_time <= as_of - source_delay`.
#[derive(Debug, Clone)]
pub struct MarketWindowSnapshot {
    /// Token the window describes.
    pub token_id: TokenId,
    /// Decision time the window was resolved as of.
    pub as_of: DateTime<Utc>,
    /// Source visibility delay applied to the window.
    pub source_delay: Duration,
    /// Buckets ascending by time, all at or before the PIT cutoff.
    pub buckets: Vec<MicrostructureBucket>,
}

impl MarketWindowSnapshot {
    /// An empty window (no history available).
    #[must_use]
    pub const fn empty(token_id: TokenId, as_of: DateTime<Utc>, source_delay: Duration) -> Self {
        Self {
            token_id,
            as_of,
            source_delay,
            buckets: Vec::new(),
        }
    }

    /// The PIT cutoff: facts at or after this instant are invisible.
    #[must_use]
    pub fn cutoff(&self) -> DateTime<Utc> {
        self.as_of
            - ChronoDuration::from_std(self.source_delay).unwrap_or_else(|_| ChronoDuration::zero())
    }

    /// Whether the window has any buckets.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// The freshest (latest) bucket time, or `None` when the window is empty.
    ///
    /// This is the observed time of the freshest fact the window can offer, used
    /// as the provenance / staleness anchor for windowed features (`as_of` minus
    /// this is the fact lag).
    #[must_use]
    pub fn freshest_bucket_time(&self) -> Option<DateTime<Utc>> {
        self.buckets.last().map(|bucket| bucket.bucket_time)
    }

    /// Buckets whose `bucket_time` falls within the trailing `window` ending at
    /// the PIT cutoff, ascending.
    #[must_use]
    pub fn buckets_in(&self, window: Duration) -> Vec<&MicrostructureBucket> {
        let cutoff = self.cutoff();
        let start =
            cutoff - ChronoDuration::from_std(window).unwrap_or_else(|_| ChronoDuration::zero());
        self.buckets
            .iter()
            .filter(|bucket| bucket.bucket_time >= start && bucket.bucket_time <= cutoff)
            .collect()
    }

    /// The mid prices present within the trailing `window`, ascending.
    #[must_use]
    pub fn mids_in(&self, window: Duration) -> Vec<Price> {
        self.buckets_in(window)
            .into_iter()
            .filter_map(|bucket| bucket.mid_close)
            .collect()
    }

    /// The `(bucket_time, mid)` samples present within the trailing `window`,
    /// ascending — the time-native input for duration-based EMA / MACD
    /// estimators (which must weight by real elapsed time, not point count,
    /// because the book is sampled sparsely and unevenly).
    #[must_use]
    pub fn mids_ts_in(&self, window: Duration) -> Vec<(DateTime<Utc>, Price)> {
        self.buckets_in(window)
            .into_iter()
            .filter_map(|bucket| bucket.mid_close.map(|mid| (bucket.bucket_time, mid)))
            .collect()
    }
}
