//! Market API contract: inbound list query + outbound response views
//! (including the order-book projection).

use crate::{
    domain::{BookLevel, MarketInfo, market::book::BookSnapshot, pagination::PageRequest},
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, MicroUsd, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Filter + pagination query for the markets list endpoint.
///
/// `keyword` matches the question or slug (case-insensitive substring); the
/// other filters are exact and AND-combined. The window is hardened via
/// [`MarketPageQuery::normalized`] before reaching SQL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketPageQuery {
    pub keyword: Option<String>,
    pub status: Option<MarketStatus>,
    pub category: Option<MarketCategory>,
    pub event_id: Option<EventId>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl MarketPageQuery {
    /// Return a copy with a normalized (safe) pagination window.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
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

#[cfg(test)]
mod tests {
    use super::MarketBookSummaryView;
    use crate::domain::market::book::{BookLevel, BookSnapshot};
    use crate::types::{Price, Shares, Usd};
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
