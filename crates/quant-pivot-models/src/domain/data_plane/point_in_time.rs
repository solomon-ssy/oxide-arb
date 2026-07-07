//! Point-in-time data access for feature, factor, and report builders.
//!
//! A [`PointInTimeDataSource`] resolves the book and market context visible at a
//! given decision time (`as_of`). The **live** implementation (core) serves the
//! current `BookStore` / `MarketRegistry` state, where `as_of` is the report
//! decision instant and the caller bounds staleness via the data-quality gate.
//!
//! **Historical replay** uses [`PitQueryEngine`](quant_pivot_research::pit::PitQueryEngine)
//! (`quant-pivot-research::pit`); the ClickHouse-backed streaming resolver is
//! [`ChHistoricalPitSource`](quant_pivot_core::pit::platform::ch_historical::ChHistoricalPitSource)
//! in `quant-pivot-core::pit::platform::ch_historical`. Offline dataset builds
//! batch-prefetch facts and serve from [`MaterializedPitEngine`](quant_pivot_research::pit::MaterializedPitEngine).

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{
    domain::market::{
        book::BookSnapshot,
        registry::{MarketRegistryInfo, NegRiskLegSet},
    },
    types::{EventId, MarketId, TokenId},
};

/// Resolves point-in-time book and market context.
pub trait PointInTimeDataSource: Send + Sync {
    /// The book snapshot for `token_id` visible at `as_of`.
    ///
    /// The live source returns the current published book (no look-ahead bound
    /// beyond "now"); a historical source must never return state newer than
    /// `as_of`.
    fn book_snapshot(&self, token_id: &TokenId, as_of: DateTime<Utc>) -> Option<Arc<BookSnapshot>>;

    /// The market catalog context for `market_id` visible at `as_of`.
    fn market_context(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>>;

    /// Expected vs resolved YES legs of a neg-risk event for structural full-leg
    /// aggregates (Phase 11.2.1).
    fn neg_risk_leg_set(&self, event_id: &EventId) -> NegRiskLegSet;
}
