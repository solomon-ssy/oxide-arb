//! Live point-in-time data source backed by in-memory book/market state.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{
        PointInTimeDataSource,
        market::{
            book::BookSnapshot,
            registry::{MarketRegistryInfo, NegRiskLegSet},
        },
    },
    types::{EventId, MarketId, TokenId},
};

use crate::pipeline::{book_store::BookStore, market_registry::MarketRegistry};

/// Live PIT source backed by current in-memory book/market state.
///
/// Serves the published `BookStore` / `MarketRegistry`. `as_of` is accepted for
/// interface parity with the (Phase 3) historical source but the live source
/// always reflects "now"; the data-quality gate bounds acceptable staleness.
pub struct LiveBookDataSource {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
}

impl LiveBookDataSource {
    #[must_use]
    pub const fn new(book_store: Arc<BookStore>, market_registry: Arc<MarketRegistry>) -> Self {
        Self {
            book_store,
            market_registry,
        }
    }
}

impl PointInTimeDataSource for LiveBookDataSource {
    fn book_snapshot(
        &self,
        token_id: &TokenId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<BookSnapshot>> {
        self.book_store.load(token_id)
    }

    fn market_context(
        &self,
        market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>> {
        self.market_registry.get_market(market_id)
    }

    fn neg_risk_leg_set(&self, event_id: &EventId) -> NegRiskLegSet {
        self.market_registry.neg_risk_leg_set(event_id)
    }
}
