//! Point-in-time data access for feature, factor, and report builders.
//!
//! A [`PointInTimeDataSource`] resolves the book and market context visible at a
//! given decision time (`as_of`). The **live** implementation (core) serves the
//! current `BookStore` / `MarketRegistry` state, where `as_of` is the report
//! decision instant and the caller bounds staleness via the data-quality gate.
//!
//! TODO(phase-3): a historical, ClickHouse-backed implementation that resolves
//! the book/market state strictly as of a past `as_of` (no look-ahead) for PIT
//! backtests and training datasets — see
//! `docs/plans/quant-pivot/03-data-factor-model-pipeline.md`.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{
    domain::market::{book::BookSnapshot, registry::MarketRegistryInfo},
    types::{MarketId, TokenId},
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
}
