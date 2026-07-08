//! Core-side projector that freezes [`MarketCandidate`] slices for the research
//! market selector.
//!
//! [`MarketCandidateProvider`] is the producer half of the models-domain
//! decoupling: it reads the registry, the lock-free [`BookStore`], the global
//! [`IngestPipelineLagTracker`], and (for category-mapped markets) the frozen
//! linkage ledger + domain-observation facts **once** per round, and emits
//! neutral, serializable [`MarketCandidate`] values. The research selector
//! consumes that slice as a pure function — it never sees a core type. All
//! database reads happen up front in one batch; the per-market projection is
//! pure over the frozen readings.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_api::ws::WsShardHealthPort;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{DomainAvailability, LinkageOutcome, MarketCandidate, MarketRegistryInfo},
    enums::domain::{DomainFamily, DomainMetric, LinkageStatus},
    runtime_config::DomainConfig,
    types::{DomainInstrumentKey, MarketId, Usd},
};
use quant_pivot_repository::traits::{MarketLinkageRepository, QuantFactReadRepository};

use crate::{
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::fact_lag::IngestPipelineLagTracker,
};

/// Projects the decision-time market world into frozen candidate facts.
pub struct MarketCandidateProvider {
    registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    ws_health: Arc<dyn WsShardHealthPort>,
    ingest_lag: Arc<IngestPipelineLagTracker>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl MarketCandidateProvider {
    /// Build the provider over the live registry, book store, WS health,
    /// ingest-lag tracker, linkage ledger, and domain fact reader.
    #[must_use]
    pub const fn new(
        registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        ws_health: Arc<dyn WsShardHealthPort>,
        ingest_lag: Arc<IngestPipelineLagTracker>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        fact_read: Arc<dyn QuantFactReadRepository>,
    ) -> Self {
        Self {
            registry,
            book_store,
            ws_health,
            ingest_lag,
            linkage_repo,
            fact_read,
        }
    }

    /// Freeze every active market into a [`MarketCandidate`] as of `as_of`.
    ///
    /// The ingest-lag reading is process-global and taken once, so all candidates
    /// in a round share the same `ingest_lag_ms`. Domain availability is read in
    /// one batch over the category-mapped subset. Markets that vanish from the
    /// registry between the id snapshot and the metadata read are skipped.
    ///
    /// # Errors
    ///
    /// Propagates linkage-ledger / domain-fact read failures (the domain plane
    /// fails closed as a whole rather than serving guessed availability).
    pub async fn candidates(
        &self,
        as_of: DateTime<Utc>,
        domain: &DomainConfig,
    ) -> QuantResult<Vec<MarketCandidate>> {
        let now_ms = u64::try_from(as_of.timestamp_millis()).unwrap_or(0);
        let ingest_lag_ms = self.ingest_lag.peek_worst_ms();
        let connection_healthy = self.ws_health.market_data_healthy();
        let market_ids = self.registry.active_markets();

        let mut infos = Vec::with_capacity(market_ids.len());
        for market_id in market_ids.iter() {
            if let Some(info) = self.registry.get_market(market_id) {
                infos.push(info);
            }
        }
        let availability = self
            .project_domain_availability(&infos, as_of, domain)
            .await?;

        Ok(infos
            .iter()
            .map(|info| {
                let domain_availability = availability
                    .get(&info.market_id)
                    .copied()
                    .unwrap_or(DomainAvailability::NotMapped);
                self.project(
                    info,
                    as_of,
                    now_ms,
                    connection_healthy,
                    ingest_lag_ms,
                    domain_availability,
                )
            })
            .collect())
    }

    /// One batched domain-availability reading for the category-mapped subset
    /// (Phase 11.2.2 §3.8): mapped ∧ enabled ∧ `Resolved` linkage ∧ the linked
    /// instrument has a visible PIT observation at `as_of`.
    async fn project_domain_availability(
        &self,
        infos: &[Arc<MarketRegistryInfo>],
        as_of: DateTime<Utc>,
        domain: &DomainConfig,
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        let mapped: Vec<MarketId> = infos
            .iter()
            .filter(|info| {
                DomainFamily::for_category(info.fee_category())
                    .is_some_and(|family| domain.family_enabled(family))
            })
            .map(|info| info.market_id.clone())
            .collect();
        if mapped.is_empty() {
            return Ok(HashMap::new());
        }

        // PIT-correct: the record that was actually valid at `as_of`, never
        // the ledger's latest-ever row for the market — a metadata revision
        // resolved *after* `as_of` must never leak into this decision
        // (`latest_for_markets` is reserved for resolver idempotence only).
        let valid_at = self
            .linkage_repo
            .valid_at_for_markets(&mapped, as_of)
            .await
            .map_err(QuantError::from)?;
        let mut by_market: HashMap<MarketId, DomainAvailability> = mapped
            .iter()
            .map(|market_id| (market_id.clone(), DomainAvailability::Unresolved))
            .collect();

        // Resolve source presence once per distinct instrument.
        let mut instrument_by_market: HashMap<MarketId, DomainInstrumentKey> = HashMap::new();
        let mut instruments: HashSet<DomainInstrumentKey> = HashSet::new();
        for info in valid_at {
            if info.status == LinkageStatus::Unresolved {
                continue;
            }
            let market_id = info.market_id.clone();
            let outcome: LinkageOutcome =
                serde_json::from_value(info.outcome).map_err(|error| {
                    QuantError::config(format!(
                        "linkage ledger row for market {market_id} has an undecodable \
                         outcome payload: {error}"
                    ))
                })?;
            if let LinkageOutcome::Resolved(binding) = outcome {
                instruments.insert(binding.instrument_key.clone());
                instrument_by_market.insert(market_id, binding.instrument_key);
            }
        }

        let cutoff_ms = (as_of
            - ChronoDuration::seconds(i64::try_from(domain.crypto.source_delay_secs).unwrap_or(0)))
        .timestamp_millis();
        let mut instrument_has_data: HashMap<DomainInstrumentKey, bool> = HashMap::new();
        for instrument in instruments {
            let observation = self
                .fact_read
                .domain_observation_at(&instrument, DomainMetric::Close.as_str(), cutoff_ms)
                .await
                .map_err(QuantError::from)?;
            instrument_has_data.insert(instrument, observation.is_some());
        }

        for (market_id, instrument) in instrument_by_market {
            let availability = if instrument_has_data
                .get(&instrument)
                .copied()
                .unwrap_or(false)
            {
                DomainAvailability::Available
            } else {
                DomainAvailability::SourceEmpty
            };
            by_market.insert(market_id, availability);
        }
        Ok(by_market)
    }

    /// Project one registry row plus its primary-token book into a candidate.
    fn project(
        &self,
        info: &MarketRegistryInfo,
        as_of: DateTime<Utc>,
        now_ms: u64,
        connection_healthy: bool,
        ingest_lag_ms: u64,
        domain_availability: DomainAvailability,
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
            domain_availability,
            observed_at: as_of,
        }
    }
}
