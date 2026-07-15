//! Immutable in-memory PIT source for integration tests.
//!
//! Production decisions resolve from durable ClickHouse/Postgres ledgers. Tests
//! that intentionally exercise the report pipeline without external storage may
//! freeze an already-populated `BookStore` and `MarketRegistry` through this
//! adapter. Construction copies every decision input, so later registry/book
//! mutations cannot create a torn test snapshot.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::DateTime;
use quant_pivot_core::ingest::{book_store::BookStore, market_registry::MarketRegistry};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionSource,
        market::{
            MarketFeeSchedule,
            book::BookSnapshot,
            registry::{EventRegistryInfo, MarketRegistryInfo, NegRiskLegSet},
        },
    },
    hashing::CanonicalDigest,
    types::{
        CatalogSyncBatchId, EventCatalogVersionId, EventId, MarketCatalogVersionId, MarketId,
        TokenId,
    },
};
use quant_pivot_research::pit::{
    BookSnapshotAt, CanonicalBookEventRef, MarketContextAt, PointInTimeSnapshotSource,
    ResolvedMarketSnapshot,
};
use rust_decimal::Decimal;

/// Frozen test-only projection of current in-memory ingest state.
pub struct InMemoryDecisionSnapshotSource {
    books: HashMap<TokenId, Arc<BookSnapshot>>,
    markets: HashMap<MarketId, Arc<MarketRegistryInfo>>,
    events: HashMap<EventId, Arc<EventRegistryInfo>>,
    leg_sets: HashMap<EventId, NegRiskLegSet>,
    fee_schedules: HashMap<MarketId, MarketFeeSchedule>,
}

impl InMemoryDecisionSnapshotSource {
    /// Freeze every active market, event, and published YES/NO book.
    #[must_use]
    pub fn freeze(registry: &MarketRegistry, book_store: &BookStore) -> Self {
        Self::freeze_inner(registry, book_store, false)
    }

    /// Freeze a test venue whose catalog explicitly proves that fees are disabled.
    #[must_use]
    pub fn freeze_with_zero_fee_schedule(
        registry: &MarketRegistry,
        book_store: &BookStore,
    ) -> Self {
        Self::freeze_inner(registry, book_store, true)
    }

    fn freeze_inner(
        registry: &MarketRegistry,
        book_store: &BookStore,
        include_zero_fee_schedule: bool,
    ) -> Self {
        let mut books = HashMap::new();
        let mut markets = HashMap::new();
        let mut events = HashMap::new();
        let mut leg_sets = HashMap::new();
        let mut fee_schedules = HashMap::new();
        for market_id in registry.active_markets().iter() {
            let Some(market) = registry.get_market(market_id) else {
                continue;
            };
            for token_id in [&market.token_yes, &market.token_no] {
                if let Some(book) = book_store.load(token_id) {
                    books.insert(token_id.clone(), book);
                }
            }
            if let Some(event) = registry.get_event(&market.event_id) {
                leg_sets
                    .entry(event.event_id.clone())
                    .or_insert_with(|| registry.neg_risk_leg_set(&event.event_id));
                events.insert(event.event_id.clone(), Arc::new(event));
            }
            if include_zero_fee_schedule {
                fee_schedules.insert(
                    market.market_id.clone(),
                    MarketFeeSchedule {
                        market_id: market.market_id.clone(),
                        fees_enabled: false,
                        fee_rate: Decimal::ZERO,
                        exponent: Decimal::from(2),
                        taker_only: true,
                        rebate_rate: None,
                        observed_at: market.updated_at,
                    },
                );
            }
            markets.insert(market.market_id.clone(), market);
        }
        Self {
            books,
            markets,
            events,
            leg_sets,
            fee_schedules,
        }
    }
}

#[async_trait]
impl PointInTimeSnapshotSource for InMemoryDecisionSnapshotSource {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let Some(snapshot) = self.books.get(token_id) else {
            return Ok(None);
        };
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let cutoff_ms = u64::try_from(source_cutoff.timestamp_millis()).map_err(|_| {
            ResearchError::PitResolution {
                detail: format!("book source cutoff {source_cutoff} predates the Unix epoch"),
            }
        })?;
        let timestamp_ms =
            i64::try_from(snapshot.timestamp_ms).map_err(|_| ResearchError::PitResolution {
                detail: format!(
                    "test book for token {token_id} has an unrepresentable publish timestamp"
                ),
            })?;
        let available_at = DateTime::from_timestamp_millis(timestamp_ms).ok_or_else(|| {
            ResearchError::PitResolution {
                detail: format!(
                    "test book for token {token_id} has an unrepresentable publish timestamp"
                ),
            }
        })?;
        if snapshot.timestamp_ms > cutoff_ms || available_at > boundary.decision_at() {
            return Ok(None);
        }
        Ok(Some(BookSnapshotAt {
            token_id: token_id.clone(),
            source_cutoff,
            decision_at: boundary.decision_at(),
            bids: Arc::clone(&snapshot.bids),
            asks: Arc::clone(&snapshot.asks),
            timestamp_ms: snapshot.timestamp_ms,
            version: snapshot.version,
            sequence: snapshot.version,
            source_event: Some(CanonicalBookEventRef {
                stream_session_id: uuid::Uuid::from_u128(1),
                token_sequence: snapshot.version,
                source_event_hash: CanonicalDigest::content_hash_json(&(
                    token_id,
                    snapshot.timestamp_ms,
                    snapshot.version,
                    snapshot.bids.as_ref(),
                    snapshot.asks.as_ref(),
                ))?,
            }),
            available_at,
        }))
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        let Some(market) = self.markets.get(market_id) else {
            return Ok(None);
        };
        let Some(event) = self.events.get(&market.event_id) else {
            return Ok(None);
        };
        let catalog_cutoff = boundary.cutoff_for(DecisionSource::Catalog);
        if market.updated_at > catalog_cutoff || event.updated_at > catalog_cutoff {
            return Ok(None);
        }
        let context = MarketContextAt {
            market_id: market.market_id.clone(),
            effective_at: market.updated_at,
            available_at: market.updated_at,
            status: market.status,
            neg_risk: market.neg_risk,
            start_date: market.start_date,
            end_date: market.end_date,
            created_at: market.created_at,
            fee_schedule: self.fee_schedules.get(market_id).cloned(),
        };
        Ok(Some(ResolvedMarketSnapshot {
            boundary: boundary.clone(),
            market: Arc::clone(market),
            event: Arc::clone(event),
            context,
            neg_risk_leg_set: self
                .leg_sets
                .get(&event.event_id)
                .cloned()
                .unwrap_or_else(NegRiskLegSet::empty),
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            event_catalog_version_id: EventCatalogVersionId::from_v7(),
            market_content_hash: CanonicalDigest::content_hash_json(market.as_ref())?,
            event_content_hash: CanonicalDigest::content_hash_json(event.as_ref())?,
            membership_hash: CanonicalDigest::content_hash_json(&event.market_ids)?,
            market_timestamp_quality: "source".to_owned(),
            event_timestamp_quality: "source".to_owned(),
            market_effective_at: market.updated_at,
            market_available_at: market.updated_at,
            event_effective_at: event.updated_at,
            event_available_at: event.updated_at,
        }))
    }

    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        let mut market_ids = self.markets.keys().cloned().collect::<Vec<_>>();
        market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut snapshots = Vec::with_capacity(market_ids.len());
        for market_id in market_ids {
            if let Some(snapshot) = self.market_snapshot_at(&market_id, boundary).await? {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }
}
