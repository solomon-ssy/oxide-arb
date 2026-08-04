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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        market::{
            CatalogEventChangeInfo, CatalogMarketChangeInfo, CatalogSnapshotInfo, CatalogWindowInfo,
        },
    },
    types::{CatalogEventChangeId, ClobMarketInfoVersion, MarketId, TokenId},
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
    markets: HashMap<MarketId, Vec<CatalogMarketChangeInfo>>,
    events: HashMap<CatalogEventChangeId, CatalogEventChangeInfo>,
    market_info: HashMap<MarketId, Vec<ClobMarketInfoVersion>>,
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
        market_info_versions: Vec<ClobMarketInfoVersion>,
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
            .event_changes
            .into_iter()
            .map(|event| (event.event_change_id, event))
            .collect();
        let mut markets: HashMap<MarketId, Vec<CatalogMarketChangeInfo>> = HashMap::new();
        for market in catalog.market_changes {
            if !events.contains_key(&market.event_change_id) {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "materialized market version {} references absent event version {}",
                        market.market_change_id, market.event_change_id
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
                        left.market_change_id
                            .to_string()
                            .cmp(&right.market_change_id.to_string())
                    })
            });
        }
        let mut market_info: HashMap<MarketId, Vec<ClobMarketInfoVersion>> = HashMap::new();
        for version in market_info_versions {
            version
                .validate()
                .map_err(|detail| ResearchError::PitResolution {
                    detail: format!(
                        "materialized CLOB market-info version {} is invalid: {detail}",
                        version.version_id
                    ),
                })?;
            market_info
                .entry(version.market_id.clone())
                .or_default()
                .push(version);
        }
        for series in market_info.values_mut() {
            series.sort_by(|left, right| {
                left.effective_at
                    .cmp(&right.effective_at)
                    .then_with(|| left.available_at.cmp(&right.available_at))
                    .then_with(|| left.payload_hash.cmp(&right.payload_hash))
            });
        }
        Ok(Self {
            books,
            markets,
            events,
            market_info,
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
            .get(&market.event_change_id)
            .filter(|event| event_visible(event, boundary))
            .cloned()
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "visible market version {} has no visible event version {}",
                    market.market_change_id, market.event_change_id
                ),
            })?;
        let event_markets = self
            .markets
            .values()
            .filter_map(|series| {
                series.iter().rev().find(|candidate| {
                    candidate.event_change_id == event.event_change_id
                        && market_visible(candidate, boundary)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut resolved = resolve_catalog_snapshot(
            &CatalogSnapshotInfo {
                market: market.clone(),
                event: Arc::new(event),
                event_markets: Arc::from(event_markets),
            },
            boundary,
        )?;
        resolved.context.fee_schedule = self
            .visible_market_info(market_id, boundary)
            .map(ClobMarketInfoVersion::fee_schedule);
        Ok(Some(resolved))
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
    ) -> Option<&CatalogMarketChangeInfo> {
        self.markets.get(market_id)?.iter().rev().find(|market| {
            market_visible(market, boundary)
                && self
                    .events
                    .get(&market.event_change_id)
                    .is_some_and(|event| event_visible(event, boundary))
        })
    }

    fn visible_market_info(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Option<&ClobMarketInfoVersion> {
        self.market_info
            .get(market_id)?
            .iter()
            .rev()
            .find(|version| {
                version.effective_at <= boundary.knowledge_cutoff()
                    && version.available_at <= boundary.decision_at()
            })
    }
}

fn market_visible(market: &CatalogMarketChangeInfo, boundary: &DecisionBoundary) -> bool {
    market.source_effective_at <= boundary.cutoff_for(DecisionSource::Catalog)
        && market.available_at <= boundary.decision_at()
}

fn event_visible(event: &CatalogEventChangeInfo, boundary: &DecisionBoundary) -> bool {
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
    use std::{collections::HashMap, sync::Arc};

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::DecisionClock,
            market::{
                CATALOG_OBJECT_SCHEMA_VERSION, CatalogEventChangeInfo, CatalogMarketChangeInfo,
                CatalogWindowInfo, EventRegistryInfo, book::BookLevel,
                registry::MarketRegistryInfo,
            },
        },
        enums::{
            catalog::{CatalogChangeType, CatalogFilterReasonSet, CatalogTimestampQuality},
            common::{CategorySet, MarketCategory, TickSize},
            market::{EventStatus, MarketStatus},
        },
        hashing::CanonicalDigest,
        types::{
            CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId,
            CatalogMarketObjectId, CatalogSyncBatchId, ClobFeeDetails, ClobMarketInfoVersion,
            ClobMarketInfoVersionId, ClobTokenDescriptor, ContentHash, EventId, MarketId, Price,
            Shares, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::MaterializedPitEngine;
    use crate::pit::{BookSnapshotAt, PointInTimeSnapshotSource};

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
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn market_info(
        market_id: &MarketId,
        effective_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        platform_rate: Decimal,
    ) -> ClobMarketInfoVersion {
        let raw_payload = json!({ "platform_rate": platform_rate.to_string() });
        ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: market_id.clone(),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: TokenId::new("yes"),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: TokenId::new("no"),
                    outcome: "No".to_owned(),
                },
            ],
            tick_size: TickSize::Hundredth,
            minimum_order_size: Decimal::ONE,
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: platform_rate,
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at,
            available_at,
            payload_hash: CanonicalDigest::content_hash_json(&raw_payload).expect("payload hash"),
            raw_payload,
        }
    }

    fn catalog_window(
        market_id: &MarketId,
        revisions: &[(DateTime<Utc>, MarketStatus)],
    ) -> CatalogWindowInfo {
        let event_id = EventId::new("event");
        let event_version_id = CatalogEventChangeId::from_v7();
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
        let event_hash = hash('e');
        let event_version = CatalogEventChangeInfo {
            event_change_id: event_version_id,
            catalog_sync_batch_id: batch_id,
            event_object_id: CatalogEventObjectId::from_content_hash(&event_hash),
            event_id: event_id.clone(),
            source_effective_at: event_time,
            source_timestamp_quality: CatalogTimestampQuality::Source,
            available_at: event_time,
            change_type: CatalogChangeType::GammaScanUpsert,
            content_hash: event_hash,
            schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
            payload: serde_json::to_value(event).expect("event payload").into(),
            created_at: event_time,
        };
        let market_changes = revisions
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
                    filter_reasons: CatalogFilterReasonSet::default(),
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
                let market_hash = hash(if *status == MarketStatus::Active {
                    'a'
                } else {
                    'b'
                });
                CatalogMarketChangeInfo {
                    market_change_id: CatalogMarketChangeId::from_v7(),
                    catalog_sync_batch_id: batch_id,
                    event_change_id: event_version_id,
                    market_object_id: CatalogMarketObjectId::from_content_hash(&market_hash),
                    market_id: market_id.clone(),
                    event_id: event_id.clone(),
                    source_effective_at: *at,
                    source_timestamp_quality: CatalogTimestampQuality::Source,
                    source_created_at: Some(event_time),
                    available_at: *at,
                    change_type: CatalogChangeType::GammaScanUpsert,
                    content_hash: market_hash,
                    schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                    payload: serde_json::to_value(market).expect("market payload").into(),
                    created_at: *at,
                }
            })
            .collect();
        CatalogWindowInfo {
            market_changes,
            event_changes: vec![event_version],
        }
    }

    #[tokio::test]
    async fn boundary_book_enforces_staleness() {
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
                market_changes: Vec::new(),
                event_changes: Vec::new(),
            },
            Vec::new(),
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
                market_changes: Vec::new(),
                event_changes: Vec::new(),
            },
            Vec::new(),
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
    async fn boundary_market_enforces_clocks() {
        let market_id = MarketId::new("m");
        let t0 = Utc.timestamp_millis_opt(1_000).single().expect("ts");
        let t1 = Utc.timestamp_millis_opt(5_000).single().expect("ts");
        let mut catalog = catalog_window(
            &market_id,
            &[(t0, MarketStatus::Active), (t1, MarketStatus::Settled)],
        );
        catalog.market_changes[1].available_at =
            Utc.timestamp_millis_opt(7_000).single().expect("ts");
        let engine =
            MaterializedPitEngine::new(HashMap::new(), catalog, Vec::new(), Duration::seconds(60))
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

    #[tokio::test]
    async fn market_fee_resolves_bitemporally() {
        let market_id = MarketId::new("fee-market");
        let other_market_id = MarketId::new("other-market");
        let catalog_at = Utc
            .timestamp_millis_opt(1_000)
            .single()
            .expect("catalog time");
        let first_at = Utc
            .timestamp_millis_opt(2_000)
            .single()
            .expect("first time");
        let second_at = Utc
            .timestamp_millis_opt(4_000)
            .single()
            .expect("second time");
        let second_available = Utc
            .timestamp_millis_opt(7_000)
            .single()
            .expect("second availability");
        let exact_at = Utc
            .timestamp_millis_opt(8_000)
            .single()
            .expect("exact time");
        let future_at = Utc
            .timestamp_millis_opt(9_000)
            .single()
            .expect("future time");
        let versions = vec![
            market_info(&market_id, first_at, first_at, Decimal::new(1, 2)),
            market_info(&market_id, second_at, second_available, Decimal::new(2, 2)),
            market_info(&market_id, exact_at, exact_at, Decimal::new(3, 2)),
            market_info(&market_id, future_at, future_at, Decimal::new(4, 2)),
            market_info(&other_market_id, first_at, first_at, Decimal::new(9, 2)),
        ];
        let engine = MaterializedPitEngine::new(
            HashMap::new(),
            catalog_window(&market_id, &[(catalog_at, MarketStatus::Active)]),
            versions,
            Duration::seconds(60),
        )
        .expect("engine");

        let before_delayed_available = DecisionClock::new(0)
            .boundary(Utc.timestamp_millis_opt(6_000).single().expect("boundary"))
            .expect("boundary");
        let first = engine
            .market_snapshot_at(&market_id, &before_delayed_available)
            .await
            .expect("market")
            .expect("snapshot")
            .context
            .fee_schedule
            .expect("fee schedule");
        assert_eq!(first.platform_rate, Decimal::new(1, 2));

        let after_delayed_available = DecisionClock::new(0)
            .boundary(Utc.timestamp_millis_opt(7_500).single().expect("boundary"))
            .expect("boundary");
        let second = engine
            .market_snapshot_at(&market_id, &after_delayed_available)
            .await
            .expect("market")
            .expect("snapshot")
            .context
            .fee_schedule
            .expect("fee schedule");
        assert_eq!(second.platform_rate, Decimal::new(2, 2));

        let at_exact_boundary = DecisionClock::new(0).boundary(exact_at).expect("boundary");
        let exact = engine
            .market_snapshot_at(&market_id, &at_exact_boundary)
            .await
            .expect("market")
            .expect("snapshot")
            .context
            .fee_schedule
            .expect("fee schedule");
        assert_eq!(exact.platform_rate, Decimal::new(3, 2));
    }
}
