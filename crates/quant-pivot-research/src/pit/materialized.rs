//! In-memory point-in-time engine over a pre-fetched window.
//!
//! The offline dataset builder batch-reads every book snapshot / market context
//! it will need once, then serves per-sample point-in-time lookups from memory
//! through this engine — so the build loop issues **zero** database queries and
//! produces byte-identical features to the online path.
//!
//! **Immutability:** this type is a frozen snapshot for one dataset build. It is
//! not updated when live books or catalog rows change at runtime; online /
//! streaming replay uses the durable `ClickHouse`/Postgres PIT source instead.
//! That separation keeps offline builds reproducible (prefetch window + plan
//! hash fully determine the materialized state) while the streaming source can
//! observe fresh facts.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        CatalogSnapshotInfo, CatalogWindowInfo, DecisionBoundary, DecisionSource,
        EventCatalogVersionInfo, MarketCatalogVersionInfo,
    },
    types::{EventCatalogVersionId, MarketId, TokenId},
};

use super::{
    BookSnapshotAt, PointInTimeSnapshotSource, ResolvedMarketSnapshot, resolve_catalog_snapshot,
};

/// Serves point-in-time book / market lookups from a pre-fetched window.
///
/// Each per-key series is kept ascending by observed time; a lookup returns the
/// freshest entry at or before the source cutoff whose availability does not
/// exceed the decision time. A book older than `max_book_staleness` relative to
/// that source cutoff is treated as missing, so online and replay agree.
pub struct MaterializedPitEngine {
    books: HashMap<TokenId, Vec<BookSnapshotAt>>,
    markets: HashMap<MarketId, Vec<MarketCatalogVersionInfo>>,
    events: HashMap<EventCatalogVersionId, EventCatalogVersionInfo>,
    max_book_staleness: Duration,
}

impl MaterializedPitEngine {
    /// Build from pre-fetched books and the complete immutable catalog window.
    ///
    /// Each series is sorted defensively by effective time, availability time,
    /// and stable version id. Malformed payloads and dangling event revisions
    /// fail construction; replay never substitutes current metadata.
    pub fn new(
        mut books: HashMap<TokenId, Vec<BookSnapshotAt>>,
        catalog: CatalogWindowInfo,
        max_book_staleness: Duration,
    ) -> QuantResult<Self> {
        for series in books.values_mut() {
            series.sort_by(|left, right| {
                left.timestamp_ms
                    .cmp(&right.timestamp_ms)
                    .then_with(|| left.available_at.cmp(&right.available_at))
                    .then_with(|| left.sequence.cmp(&right.sequence))
            });
        }
        let events: HashMap<_, _> = catalog
            .event_versions
            .into_iter()
            .map(|event| (event.event_catalog_version_id.clone(), event))
            .collect();
        let mut markets: HashMap<MarketId, Vec<MarketCatalogVersionInfo>> = HashMap::new();
        for market in catalog.market_versions {
            if !events.contains_key(&market.event_catalog_version_id) {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "materialized market version {} references absent event version {}",
                        market.market_catalog_version_id, market.event_catalog_version_id
                    ),
                }
                .into());
            }
            markets
                .entry(market.market_id.clone())
                .or_default()
                .push(market);
        }
        for series in markets.values_mut() {
            series.sort_by(|left, right| {
                left.source_effective_at
                    .cmp(&right.source_effective_at)
                    .then_with(|| left.available_at.cmp(&right.available_at))
                    .then_with(|| {
                        left.market_catalog_version_id
                            .to_string()
                            .cmp(&right.market_catalog_version_id.to_string())
                    })
            });
        }
        Ok(Self {
            books,
            markets,
            events,
            max_book_staleness,
        })
    }
}

#[async_trait]
impl PointInTimeSnapshotSource for MaterializedPitEngine {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        Ok(self.visible_book(
            token_id,
            boundary.cutoff_for(DecisionSource::Book),
            boundary.decision_at(),
        ))
    }

    async fn books_at_boundary(
        &self,
        token_ids: &[TokenId],
        boundary: &DecisionBoundary,
    ) -> QuantResult<HashMap<TokenId, BookSnapshotAt>> {
        Ok(token_ids
            .iter()
            .filter_map(|token_id| {
                self.visible_book(
                    token_id,
                    boundary.cutoff_for(DecisionSource::Book),
                    boundary.decision_at(),
                )
                .map(|book| (token_id.clone(), book))
            })
            .collect())
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        let Some(market) = self.visible_market(market_id, boundary) else {
            return Ok(None);
        };
        let event = self
            .events
            .get(&market.event_catalog_version_id)
            .filter(|event| event_visible(event, boundary))
            .cloned()
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "visible market version {} has no visible event version {}",
                    market.market_catalog_version_id, market.event_catalog_version_id
                ),
            })?;
        let event_markets = self
            .markets
            .values()
            .filter_map(|series| {
                series.iter().rev().find(|candidate| {
                    candidate.event_catalog_version_id == event.event_catalog_version_id
                        && market_visible(candidate, boundary)
                })
            })
            .cloned()
            .collect();
        resolve_catalog_snapshot(
            CatalogSnapshotInfo {
                market: market.clone(),
                event,
                event_markets,
            },
            boundary,
        )
        .map(Some)
    }
}

impl MaterializedPitEngine {
    fn visible_book(
        &self,
        token_id: &TokenId,
        source_cutoff: DateTime<Utc>,
        decision_at: DateTime<Utc>,
    ) -> Option<BookSnapshotAt> {
        let cutoff_ms = source_cutoff.timestamp_millis();
        let min_ms = (source_cutoff - self.max_book_staleness).timestamp_millis();
        self.books.get(token_id).and_then(|series| {
            series
                .iter()
                .rev()
                .find(|snapshot| {
                    ms_le(snapshot.timestamp_ms, cutoff_ms) && snapshot.available_at <= decision_at
                })
                .filter(|snapshot| ms_ge(snapshot.timestamp_ms, min_ms))
                .map(|snapshot| BookSnapshotAt {
                    source_cutoff,
                    decision_at,
                    ..snapshot.clone()
                })
        })
    }

    fn visible_market(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Option<&MarketCatalogVersionInfo> {
        self.markets.get(market_id)?.iter().rev().find(|market| {
            market_visible(market, boundary)
                && self
                    .events
                    .get(&market.event_catalog_version_id)
                    .is_some_and(|event| event_visible(event, boundary))
        })
    }
}

fn market_visible(market: &MarketCatalogVersionInfo, boundary: &DecisionBoundary) -> bool {
    market.source_effective_at <= boundary.cutoff_for(DecisionSource::Catalog)
        && market.available_at <= boundary.decision_at()
}

fn event_visible(event: &EventCatalogVersionInfo, boundary: &DecisionBoundary) -> bool {
    event.source_effective_at <= boundary.cutoff_for(DecisionSource::Catalog)
        && event.available_at <= boundary.decision_at()
}

/// Whether an epoch-millisecond `timestamp_ms` is at or before `cutoff_ms`.
fn ms_le(timestamp_ms: u64, cutoff_ms: i64) -> bool {
    i64::try_from(timestamp_ms).is_ok_and(|ms| ms <= cutoff_ms)
}

/// Whether an epoch-millisecond `timestamp_ms` is at or after `min_ms`.
fn ms_ge(timestamp_ms: u64, min_ms: i64) -> bool {
    i64::try_from(timestamp_ms).map_or(true, |ms| ms >= min_ms)
}

#[cfg(test)]
mod tests {
    use super::MaterializedPitEngine;
    use crate::pit::{BookSnapshotAt, PointInTimeSnapshotSource};
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CatalogWindowInfo, DecisionClock, EventCatalogVersionInfo, EventRegistryInfo,
            MarketCatalogVersionInfo,
            market::{book::BookLevel, registry::MarketRegistryInfo},
        },
        enums::{
            common::{CategorySet, MarketCategory, TickSize},
            market::{EventStatus, MarketStatus},
        },
        types::{
            CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId,
            MarketCatalogVersionId, MarketId, Price, Shares, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use std::{collections::HashMap, sync::Arc};

    fn level(price: &str) -> BookLevel {
        BookLevel::from_decimal_unchecked(
            Price::new(Decimal::from_str_exact(price).expect("decimal")),
            Shares::new(Decimal::from(100)),
        )
    }

    fn snapshot(token: &str, ts_ms: i64) -> BookSnapshotAt {
        let timestamp = Utc.timestamp_millis_opt(ts_ms).single().expect("ts");
        BookSnapshotAt {
            token_id: TokenId::new(token),
            source_cutoff: timestamp,
            decision_at: timestamp,
            bids: Arc::from([level("0.48")]),
            asks: Arc::from([level("0.52")]),
            timestamp_ms: u64::try_from(ts_ms).expect("positive test timestamp"),
            version: 1,
            sequence: 1,
            source_event: None,
            available_at: timestamp,
        }
    }

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn catalog_window(
        market_id: &MarketId,
        revisions: &[(chrono::DateTime<Utc>, MarketStatus)],
    ) -> CatalogWindowInfo {
        let event_id = EventId::new("event");
        let event_version_id = EventCatalogVersionId::from_v7();
        let batch_id = CatalogSyncBatchId::from_v7();
        let event_time = revisions[0].0;
        let event = EventRegistryInfo {
            event_id: event_id.clone(),
            title: "event".to_owned(),
            slug: "event".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: vec![market_id.clone()],
            categories: CategorySet::from(MarketCategory::Sports),
            tags: Vec::new(),
            neg_risk: false,
            end_date: None,
            created_at: event_time,
            updated_at: event_time,
        };
        let event_version = EventCatalogVersionInfo {
            event_catalog_version_id: event_version_id.clone(),
            catalog_sync_batch_id: batch_id.clone(),
            event_id: event_id.clone(),
            source_effective_at: event_time,
            source_timestamp_quality: "source".to_owned(),
            available_at: event_time,
            origin: "gamma_sync".to_owned(),
            content_hash: hash('e'),
            payload: serde_json::to_value(event).expect("event payload"),
            created_at: event_time,
        };
        let market_versions = revisions
            .iter()
            .map(|(at, status)| {
                let market = MarketRegistryInfo {
                    market_id: market_id.clone(),
                    event_id: event_id.clone(),
                    token_yes: TokenId::new("yes"),
                    token_no: TokenId::new("no"),
                    question: "question".to_owned(),
                    slug: "market".to_owned(),
                    description: None,
                    categories: CategorySet::from(MarketCategory::Sports),
                    status: *status,
                    outcome: None,
                    neg_risk: false,
                    tick_size: TickSize::Hundredth,
                    tokens: Vec::new(),
                    best_bid: None,
                    best_ask: None,
                    depth_usd: None,
                    min_order_size: Decimal::ONE,
                    liquidity_usd: None,
                    volume_24h: None,
                    start_date: None,
                    end_date: None,
                    resolved_at: None,
                    created_at: Some(event_time),
                    updated_at: *at,
                };
                MarketCatalogVersionInfo {
                    market_catalog_version_id: MarketCatalogVersionId::from_v7(),
                    catalog_sync_batch_id: batch_id.clone(),
                    event_catalog_version_id: event_version_id.clone(),
                    market_id: market_id.clone(),
                    event_id: event_id.clone(),
                    source_effective_at: *at,
                    source_timestamp_quality: "source".to_owned(),
                    source_created_at: Some(event_time),
                    available_at: *at,
                    origin: "gamma_sync".to_owned(),
                    content_hash: hash(if *status == MarketStatus::Active {
                        'a'
                    } else {
                        'b'
                    }),
                    payload: serde_json::to_value(market).expect("market payload"),
                    created_at: *at,
                }
            })
            .collect();
        CatalogWindowInfo {
            market_versions,
            event_versions: vec![event_version],
        }
    }

    #[tokio::test]
    async fn boundary_book_lookup_enforces_effective_availability_and_staleness() {
        let token = TokenId::new("t");
        let decision_at = Utc.timestamp_millis_opt(12_000).single().expect("ts");
        let boundary = DecisionClock::new(2)
            .boundary(decision_at)
            .expect("boundary");
        let mut delayed = snapshot("t", 9_500);
        delayed.available_at = Utc.timestamp_millis_opt(13_000).single().expect("ts");
        let mut books = HashMap::new();
        books.insert(
            token.clone(),
            vec![
                snapshot("t", 5_000),
                snapshot("t", 12_000),
                snapshot("t", 9_000),
                delayed,
            ],
        );
        let engine = MaterializedPitEngine::new(
            books,
            CatalogWindowInfo {
                market_versions: Vec::new(),
                event_versions: Vec::new(),
            },
            Duration::seconds(2),
        )
        .expect("engine");

        let resolved = engine
            .book_at_boundary(&token, &boundary)
            .await
            .expect("book");
        let book = resolved.expect("snapshot");
        assert_eq!(book.timestamp_ms, 9_000);
        assert_eq!(book.source_cutoff.timestamp_millis(), 10_000);
        assert_eq!(book.decision_at, decision_at);

        let stale_only = MaterializedPitEngine::new(
            HashMap::from([(token.clone(), vec![snapshot("t", 1_000)])]),
            CatalogWindowInfo {
                market_versions: Vec::new(),
                event_versions: Vec::new(),
            },
            Duration::seconds(2),
        )
        .expect("engine");
        assert!(
            stale_only
                .book_at_boundary(&token, &boundary)
                .await
                .expect("book")
                .is_none()
        );
    }

    #[tokio::test]
    async fn boundary_market_lookup_enforces_effective_and_availability_clocks() {
        let market_id = MarketId::new("m");
        let t0 = Utc.timestamp_millis_opt(1_000).single().expect("ts");
        let t1 = Utc.timestamp_millis_opt(5_000).single().expect("ts");
        let mut catalog = catalog_window(
            &market_id,
            &[(t0, MarketStatus::Active), (t1, MarketStatus::Settled)],
        );
        catalog.market_versions[1].available_at =
            Utc.timestamp_millis_opt(7_000).single().expect("ts");
        let engine = MaterializedPitEngine::new(HashMap::new(), catalog, Duration::seconds(60))
            .expect("engine");
        let before_available = DecisionClock::new(0)
            .boundary(Utc.timestamp_millis_opt(6_000).single().expect("ts"))
            .expect("boundary");
        let snapshot = engine
            .market_snapshot_at(&market_id, &before_available)
            .await
            .expect("market")
            .expect("snapshot");
        assert_eq!(snapshot.context.status, MarketStatus::Active);

        let after_available = DecisionClock::new(0)
            .boundary(Utc.timestamp_millis_opt(8_000).single().expect("ts"))
            .expect("boundary");
        let snapshot = engine
            .market_snapshot_at(&market_id, &after_available)
            .await
            .expect("market")
            .expect("snapshot");
        assert_eq!(snapshot.context.status, MarketStatus::Settled);

        let before_first = DecisionClock::new(0)
            .boundary(Utc.timestamp_millis_opt(500).single().expect("ts"))
            .expect("boundary");
        assert!(
            engine
                .market_snapshot_at(&market_id, &before_first)
                .await
                .expect("market")
                .is_none()
        );
    }
}
