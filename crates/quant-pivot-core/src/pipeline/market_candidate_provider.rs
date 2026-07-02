//! Core-side projector that freezes [`MarketCandidate`] slices for the research
//! market selector.
//!
//! [`MarketCandidateProvider`] is the producer half of the models-domain
//! decoupling: it reads the registry, the lock-free [`BookStore`], and the global
//! [`IngestPipelineLagTracker`] **once** per round and emits neutral, serializable
//! [`MarketCandidate`] values. The research selector consumes that slice as a
//! pure function — it never sees a core type. The projection takes a single
//! consistent reading per market and performs no database I/O.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_api::ws::WsShardHealthPort;
use quant_pivot_models::{
    domain::{MarketCandidate, MarketRegistryInfo},
    types::Usd,
};

use crate::{
    observability::fact_lag::IngestPipelineLagTracker,
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
};

/// Projects the decision-time market world into frozen candidate facts.
pub struct MarketCandidateProvider {
    registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    ws_health: Arc<dyn WsShardHealthPort>,
    ingest_lag: Arc<IngestPipelineLagTracker>,
}

impl MarketCandidateProvider {
    /// Build the provider over the live registry, book store, WS health, and
    /// ingest-lag tracker.
    #[must_use]
    pub const fn new(
        registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        ws_health: Arc<dyn WsShardHealthPort>,
        ingest_lag: Arc<IngestPipelineLagTracker>,
    ) -> Self {
        Self {
            registry,
            book_store,
            ws_health,
            ingest_lag,
        }
    }

    /// Freeze every active market into a [`MarketCandidate`] as of `as_of`.
    ///
    /// The ingest-lag reading is process-global and taken once, so all candidates
    /// in a round share the same `ingest_lag_ms`. Markets that vanish from the
    /// registry between the id snapshot and the metadata read are skipped.
    #[must_use]
    pub fn candidates(&self, as_of: DateTime<Utc>) -> Vec<MarketCandidate> {
        let now_ms = u64::try_from(as_of.timestamp_millis()).unwrap_or(0);
        let ingest_lag_ms = self.ingest_lag.peek_worst_ms();
        let connection_healthy = self.ws_health.market_data_healthy();
        let market_ids = self.registry.active_markets();

        let mut candidates = Vec::with_capacity(market_ids.len());
        for market_id in market_ids.iter() {
            if let Some(info) = self.registry.get_market(market_id) {
                candidates.push(self.project(
                    &info,
                    as_of,
                    now_ms,
                    connection_healthy,
                    ingest_lag_ms,
                ));
            }
        }
        candidates
    }

    /// Project one registry row plus its primary-token book into a candidate.
    fn project(
        &self,
        info: &MarketRegistryInfo,
        as_of: DateTime<Utc>,
        now_ms: u64,
        connection_healthy: bool,
        ingest_lag_ms: u64,
    ) -> MarketCandidate {
        let book = self.book_store.load(&info.token_yes);
        let (best_bid, best_ask, depth_usd, crossed, empty) =
            book.as_ref()
                .map_or((None, None, None, false, true), |snapshot| {
                    let best_bid = snapshot.best_bid();
                    let best_ask = snapshot.best_ask();
                    let depth = Usd::new(
                        (snapshot.total_bid_depth_usd + snapshot.total_ask_depth_usd).to_decimal(),
                    );
                    let crossed =
                        matches!((best_bid, best_ask), (Some(bid), Some(ask)) if bid >= ask);
                    let empty = snapshot.bids.is_empty() || snapshot.asks.is_empty();
                    (best_bid, best_ask, Some(depth), crossed, empty)
                });
        // Local WS receipt-clock age (venue clock skew / reconnect re-writes
        // excluded), consistent with the data-quality plane; falls back to the
        // published venue timestamp, and `None` when no book exists at all.
        let book_age_ms = self
            .ws_health
            .token_message_age_ms(&info.token_yes)
            .or_else(|| {
                book.as_ref()
                    .map(|snapshot| now_ms.saturating_sub(snapshot.timestamp_ms))
            });

        MarketCandidate {
            market_id: info.market_id.clone(),
            event_id: info.event_id.clone(),
            category: info.fee_category(),
            status: info.status,
            primary_token_id: info.token_yes.clone(),
            secondary_token_id: Some(info.token_no.clone()),
            end_date: info.end_date,
            liquidity_usd: info.liquidity_usd,
            volume_24h_usd: info.volume_24h,
            best_bid,
            best_ask,
            depth_usd,
            book_age_ms,
            crossed,
            empty,
            connection_healthy,
            ingest_lag_ms,
            observed_at: as_of,
        }
    }
}
