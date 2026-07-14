//! Market API contract: inbound list query + outbound response views
//! (including the order-book projection).

use crate::{
    clickhouse::{BookMicrostructureRow, ChBps, ChDecimal64, ChPrice, ChUsd, TradeTapeRow},
    domain::{
        BookLevel, MarketInfo, NormalizePageQuery, market::book::BookSnapshot,
        pagination::PageRequest,
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{Bps, EventId, MarketId, MicroUsd, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::query::QueryError;
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use validator::Validate;

/// Filter + pagination query for the markets list endpoint.
///
/// `keyword` matches the question or slug (case-insensitive substring); the
/// other filters are exact and AND-combined. Call [`MarketPageQuery::prepare`]
/// at the HTTP boundary before persistence when `subscribed` filtering is used;
/// SQL pagination is hardened separately via [`PageWindow`](crate::domain::PageWindow).
#[derive(Debug, Clone, Default, Serialize, Deserialize, NormalizePageQuery)]
pub struct MarketPageQuery {
    pub keyword: Option<String>,
    pub status: Option<MarketStatus>,
    pub category: Option<MarketCategory>,
    /// When `true`, match markets with an empty `categories` array (UI "未知").
    pub category_unknown: Option<bool>,
    pub event_id: Option<EventId>,
    /// When set, filter markets whose YES/NO tokens are both live on the CLOB WS
    /// transport (`true`) or not (`false`). Resolved server-side against the
    /// runtime subscription union — never client-supplied token sets.
    pub subscribed: Option<bool>,
    #[serde(skip)]
    pub resolved_subscribed_tokens: Option<HashSet<TokenId>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl MarketPageQuery {
    /// Harden pagination and inject the live WS subscription union when
    /// `subscribed` is set. Wire callers must never set `resolved_subscribed_tokens`.
    #[must_use]
    pub fn prepare(self, live_subscribed: HashSet<TokenId>) -> Self {
        let mut query = self.normalized();
        if query.subscribed.is_some() {
            query.resolved_subscribed_tokens = Some(live_subscribed);
        }
        query
    }
}

/// Governed request to manually block a market.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct BlockMarketRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Governed request to unblock a market into an explicit operator-selected state.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct UnblockMarketRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    pub restore_status: MarketStatus,
}

/// Live order-book digest attached to a [`MarketView`].
///
/// Computed from the published (lock-free) YES / NO book snapshots at
/// response time — never persisted. `None` on the parent view means neither
/// token has a published book yet.
#[derive(Debug, Clone, Serialize)]
pub struct MarketBookSummaryView {
    pub yes_best_bid: Option<Price>,
    pub yes_best_ask: Option<Price>,
    pub no_best_bid: Option<Price>,
    pub no_best_ask: Option<Price>,
    /// Total resting notional (bid + ask) across both tokens' books (USD).
    pub depth_usd: Usd,
    /// Publish timestamp of the freshest contributing book (epoch millis).
    pub updated_at_ms: u64,
}

impl MarketBookSummaryView {
    /// Digest the published YES / NO snapshots; `None` when neither exists.
    #[must_use]
    pub fn from_snapshots(yes: Option<&BookSnapshot>, no: Option<&BookSnapshot>) -> Option<Self> {
        if yes.is_none() && no.is_none() {
            return None;
        }
        let depth = |snapshot: Option<&BookSnapshot>| {
            snapshot.map_or(MicroUsd::ZERO, |s| {
                s.total_bid_depth_usd + s.total_ask_depth_usd
            })
        };
        let depth_usd = Usd::new((depth(yes) + depth(no)).to_decimal());
        Some(Self {
            yes_best_bid: yes.and_then(BookSnapshot::best_bid),
            yes_best_ask: yes.and_then(BookSnapshot::best_ask),
            no_best_bid: no.and_then(BookSnapshot::best_bid),
            no_best_ask: no.and_then(BookSnapshot::best_ask),
            depth_usd,
            updated_at_ms: yes
                .map_or(0, |s| s.timestamp_ms)
                .max(no.map_or(0, |s| s.timestamp_ms)),
        })
    }
}

/// Outbound projection of a market row for the web dashboard.
///
/// Combines the persisted catalog row with the runtime overlay (live book
/// digest + CLOB WS subscription state), so it is built via
/// [`MarketView::project`] rather than a bare `From<MarketInfo>`.
#[derive(Debug, Clone, Serialize)]
pub struct MarketView {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    /// Category memberships (any-match filterable via `MarketPageQuery::category`).
    pub categories: Vec<MarketCategory>,
    pub status: MarketStatus,
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub fees_enabled: bool,
    /// Whether both of the market's tokens are live on the CLOB WS transport
    /// (engine baseline or operator overlay).
    pub subscribed: bool,
    /// Live order-book digest; `None` when no book has been published yet.
    pub book: Option<MarketBookSummaryView>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MarketView {
    /// Project a persisted market row plus its runtime overlay into the wire
    /// shape. The overlay inputs come from the market-data port (published
    /// books + WS subscription union) and are resolved by the handler.
    #[must_use]
    pub fn project(m: MarketInfo, subscribed: bool, book: Option<MarketBookSummaryView>) -> Self {
        Self {
            categories: m.categories,
            market_id: m.market_id,
            event_id: m.event_id,
            question: m.question,
            slug: m.slug,
            status: m.status,
            outcome: m.outcome,
            yes_token_id: m.yes_token_id,
            no_token_id: m.no_token_id,
            tick_size: m.tick_size,
            neg_risk: m.neg_risk,
            fees_enabled: m.fees_enabled,
            subscribed,
            book,
            start_date: m.start_date,
            end_date: m.end_date,
            resolved_at: m.resolved_at,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

/// A single decimal-valued order-book level (wire-friendly, not the hot-path
/// fixed-point representation).
#[derive(Debug, Clone, Serialize)]
pub struct BookLevelView {
    pub price: Price,
    pub size: Shares,
}

/// One token's published order book (bids + asks) at a point in time.
#[derive(Debug, Clone, Serialize)]
pub struct MarketBookSideView {
    pub token_id: TokenId,
    pub bids: Vec<BookLevelView>,
    pub asks: Vec<BookLevelView>,
    pub timestamp_ms: u64,
    pub version: u64,
}

impl MarketBookSideView {
    /// Project a published [`BookSnapshot`] into decimal wire levels.
    #[must_use]
    pub fn from_snapshot(token_id: TokenId, snapshot: &BookSnapshot) -> Self {
        let map = |levels: &[BookLevel]| {
            levels
                .iter()
                .map(|level| BookLevelView {
                    price: level.price_decimal(),
                    size: level.size_decimal(),
                })
                .collect()
        };
        Self {
            token_id,
            bids: map(&snapshot.bids),
            asks: map(&snapshot.asks),
            timestamp_ms: snapshot.timestamp_ms,
            version: snapshot.version,
        }
    }
}

/// The YES + NO published books for a market.
#[derive(Debug, Clone, Serialize)]
pub struct MarketBookView {
    pub market_id: MarketId,
    pub yes: Option<MarketBookSideView>,
    pub no: Option<MarketBookSideView>,
}

/// Bucket resolution chosen for a microstructure series response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MicrostructureResolution {
    /// One-second buckets (`book_microstructure_1s`).
    Second,
    /// One-minute buckets (`book_microstructure_1m`).
    Minute,
}

impl MicrostructureResolution {
    /// Whether the 1-minute rollup table should be read.
    #[must_use]
    pub const fn is_minute(self) -> bool {
        matches!(self, Self::Minute)
    }
}

/// Inbound query for the market microstructure history endpoint.
///
/// Both bounds are optional: `to` defaults to now and `from` to a one-hour
/// look-back. [`MarketMicrostructureQuery::resolve`] hardens the window and
/// picks the bucket resolution by span (1s vs 1m rollup).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MarketMicrostructureQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

/// Resolved, validated microstructure window plus the chosen bucket table.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedMicrostructureWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub resolution: MicrostructureResolution,
}

impl MarketMicrostructureQuery {
    /// Default look-back when `from` is omitted (1 hour).
    const DEFAULT_LOOKBACK_MINUTES: i64 = 60;
    /// Hard span ceiling to keep row counts bounded.
    const MAX_DAYS: i64 = 7;
    /// Windows wider than this read the 1-minute rollup instead of 1-second.
    const MINUTE_THRESHOLD_HOURS: i64 = 3;

    /// Resolve to a validated window plus bucket resolution. `to` defaults to
    /// now, `from` to `to - 1h`; inverted or over-`MAX_DAYS` windows return a
    /// domain [`QueryError`] (mapped web-side via `From`).
    pub fn resolve(&self) -> Result<ResolvedMicrostructureWindow, QueryError> {
        let to = self.to.unwrap_or_else(Utc::now);
        let from = self
            .from
            .unwrap_or(to - Duration::minutes(Self::DEFAULT_LOOKBACK_MINUTES));
        if to < from {
            return Err(QueryError::Inverted);
        }
        if to - from > Duration::days(Self::MAX_DAYS) {
            return Err(QueryError::TooWide {
                max_days: Self::MAX_DAYS,
            });
        }
        let resolution = if to - from > Duration::hours(Self::MINUTE_THRESHOLD_HOURS) {
            MicrostructureResolution::Minute
        } else {
            MicrostructureResolution::Second
        };
        Ok(ResolvedMicrostructureWindow {
            from,
            to,
            resolution,
        })
    }
}

/// One microstructure observation bucket projected for the dashboard chart.
///
/// Money / price / bps fields serialize as canonical decimal strings; the
/// top-N share-weighted queue `imbalance` is a raw ratio in `[-1, 1]`.
#[derive(Debug, Clone, Serialize)]
pub struct MicrostructureBucket {
    /// Bucket start time (epoch millis).
    pub bucket_ms: i64,
    pub mid_open: Option<Price>,
    pub mid_close: Option<Price>,
    pub best_bid_close: Option<Price>,
    pub best_ask_close: Option<Price>,
    pub spread_bps_min: Option<Bps>,
    pub spread_bps_avg: Option<Bps>,
    pub spread_bps_max: Option<Bps>,
    pub depth_top1_usd: Option<Usd>,
    pub depth_top5_usd: Option<Usd>,
    pub depth_top20_usd: Option<Usd>,
    /// Top-N share-weighted queue imbalance `(bid - ask) / (bid + ask)` over the
    /// best few levels per side, bid-heavy positive.
    pub imbalance: Option<Decimal>,
    pub last_trade_count: u64,
    pub update_count: u64,
    pub gap_count: u64,
    pub crossed_count: u64,
}

impl MicrostructureBucket {
    /// Project a persisted 1s/1m microstructure row into the wire bucket.
    #[must_use]
    pub fn from_row(row: &BookMicrostructureRow) -> Self {
        Self {
            bucket_ms: row.bucket_time,
            mid_open: row.mid_price_open.map(ChPrice::to_price),
            mid_close: row.mid_price_close.map(ChPrice::to_price),
            best_bid_close: row.best_bid_close.map(ChPrice::to_price),
            best_ask_close: row.best_ask_close.map(ChPrice::to_price),
            spread_bps_min: row.spread_bps_min.map(ChBps::to_bps),
            spread_bps_avg: row.spread_bps_avg.map(ChBps::to_bps),
            spread_bps_max: row.spread_bps_max.map(ChBps::to_bps),
            depth_top1_usd: row.top1_depth_usd_avg.map(ChUsd::to_usd),
            depth_top5_usd: row.top5_depth_usd_avg.map(ChUsd::to_usd),
            depth_top20_usd: row.top20_depth_usd_avg.map(ChUsd::to_usd),
            imbalance: row.imbalance_avg.map(ChDecimal64::to_decimal),
            last_trade_count: row.last_trade_count,
            update_count: row.update_count,
            gap_count: row.gap_count,
            crossed_count: row.crossed_count,
        }
    }
}

/// A single last-trade print for the price-chart overlay.
#[derive(Debug, Clone, Serialize)]
pub struct MarketTradeTick {
    pub token_id: TokenId,
    /// Trade event time (epoch millis).
    pub ts_ms: i64,
    pub price: Price,
}

impl MarketTradeTick {
    /// Project a canonical Market-WS trade-tape row.
    #[must_use]
    pub fn from_row(row: TradeTapeRow) -> Self {
        Self {
            token_id: row.token_id,
            ts_ms: row.event_time,
            price: row.price.to_price(),
        }
    }
}

/// Historical microstructure series (YES + NO) for a single market, plus recent
/// last-trade prints for overlay markers.
#[derive(Debug, Clone, Serialize)]
pub struct MarketMicrostructureView {
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub resolution: MicrostructureResolution,
    /// Window start (epoch millis).
    pub from_ms: i64,
    /// Window end (epoch millis).
    pub to_ms: i64,
    pub yes: Vec<MicrostructureBucket>,
    pub no: Vec<MicrostructureBucket>,
    pub trades: Vec<MarketTradeTick>,
}

#[cfg(test)]
mod tests {
    use super::MarketBookSummaryView;
    use crate::{
        domain::market::book::{BookLevel, BookSnapshot},
        types::{Price, Shares, Usd},
    };
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    fn snapshot(bid: rust_decimal::Decimal, ask: rust_decimal::Decimal) -> BookSnapshot {
        let level = |price| {
            BookLevel::from_decimal(Price::new(price), Shares::new(dec!(10))).expect("valid level")
        };
        BookSnapshot::new(
            Arc::from([level(bid)]),
            Arc::from([level(ask)]),
            1_700_000_000_000,
            7,
        )
    }

    #[test]
    fn summary_digests_both_sides_and_sums_depth() {
        let yes = snapshot(dec!(0.96), dec!(0.97));
        let no = snapshot(dec!(0.03), dec!(0.04));
        let summary =
            MarketBookSummaryView::from_snapshots(Some(&yes), Some(&no)).expect("summary");

        assert_eq!(summary.yes_best_bid, Some(Price::new(dec!(0.96))));
        assert_eq!(summary.yes_best_ask, Some(Price::new(dec!(0.97))));
        assert_eq!(summary.no_best_bid, Some(Price::new(dec!(0.03))));
        // (0.96 + 0.97 + 0.03 + 0.04) × 10 shares of resting notional.
        assert_eq!(summary.depth_usd, Usd::new(dec!(20)));
        assert_eq!(summary.updated_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn summary_is_absent_without_any_published_book() {
        assert!(MarketBookSummaryView::from_snapshots(None, None).is_none());
    }
}
