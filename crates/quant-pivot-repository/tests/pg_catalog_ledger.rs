//! Bitemporal Gamma catalog ledger integration tests.

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CATALOG_OBJECT_SCHEMA_VERSION, CatalogBatchCommit, CatalogBatchFailure,
        CatalogEventCandidate, CatalogMarketCandidate, CatalogSnapshotInfo, DecisionClock,
        EventRegistryInfo, EventTags, MarketRegistryInfo, NewCatalogEventChange,
        NewCatalogEventObject, NewCatalogMarketObject, NewCatalogSyncBatch, TokenInfo, UpsertEvent,
        UpsertMarket,
    },
    entities::{
        catalog_event_change, catalog_event_object, catalog_market_change, catalog_market_object,
        catalog_sync_batch, event, market,
    },
    enums::{
        catalog::{
            CatalogChangeType, CatalogFilterReason, CatalogFilterReasonSet,
            CatalogSyncFailureStage, CatalogSyncKind, CatalogSyncStatus, CatalogTimestampQuality,
        },
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    hashing::CanonicalDigest,
    types::{
        CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId, CatalogMarketObjectId,
        CatalogSyncBatchId, ContentHash, EventId, MarketId, TokenId,
    },
};
use quant_pivot_repository::{
    postgres::PgCatalogLedgerRepository, traits::CatalogLedgerRepository,
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal::Decimal;
use sea_orm::{EntityTrait, PaginatorTrait};
use std::{collections::BTreeSet, iter, sync::Arc};
use tokio::sync::Barrier;

const EVENT_ID: &str = "evt-batch-catalog-ledger";
const MARKET_A: &str = "0xbatch-catalog-a";
const MARKET_B: &str = "0xbatch-catalog-b";

#[tokio::test]
#[ignore = "requires Docker"]
async fn correction_is_invisible_until_its_availability_time() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCatalogLedgerRepository::new(pool.connection().clone());
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();

    let original_batch = repo
        .commit(commit(1, t0, t0, "original"))
        .await
        .expect("first catalog commit");
    let correction_batch = repo
        .commit(commit(
            2,
            t0 + Duration::seconds(20),
            t0 + Duration::seconds(5),
            "correction",
        ))
        .await
        .expect("correction commit");
    let original_visible_at = original_batch.committed_at.expect("committed batch");
    let correction_visible_at = correction_batch.committed_at.expect("committed batch");

    assert_eq!(
        repo.coverage_start().await.expect("coverage"),
        Some(original_visible_at)
    );

    let before_available = repo
        .market_at(
            &MarketId::new("0xcatalog-ledger"),
            &DecisionClock::new(0)
                .boundary(correction_visible_at - Duration::milliseconds(1))
                .expect("boundary"),
        )
        .await
        .expect("PIT lookup")
        .expect("original visible");
    assert_eq!(before_available.payload["revision"], "original");
    assert_eq!(before_available.available_at, original_visible_at);

    let after_available = repo
        .market_at(
            &MarketId::new("0xcatalog-ledger"),
            &DecisionClock::new(0)
                .boundary(correction_visible_at)
                .expect("boundary"),
        )
        .await
        .expect("PIT lookup")
        .expect("correction visible");
    assert_eq!(after_available.payload["revision"], "correction");
    assert_eq!(after_available.available_at, correction_visible_at);

    let effective_decision = correction_visible_at + Duration::seconds(1);
    let lag_secs = u64::try_from((effective_decision - (t0 + Duration::seconds(2))).num_seconds())
        .expect("positive fixture knowledge lag");
    let before_effective = repo
        .market_at(
            &MarketId::new("0xcatalog-ledger"),
            &DecisionClock::new(lag_secs)
                .boundary(effective_decision)
                .expect("boundary"),
        )
        .await
        .expect("PIT lookup")
        .expect("original remains visible");
    assert_eq!(before_effective.payload["revision"], "original");

    let coverage = repo
        .research_history_coverage(correction_visible_at)
        .await
        .expect("research history coverage");
    assert_eq!(coverage.len(), 2);
    assert_eq!(
        coverage
            .iter()
            .map(|entry| (entry.object.as_str(), entry.row_count))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("catalog_event_change", 2), ("catalog_market_change", 2)])
    );
    assert!(coverage.iter().all(|entry| {
        entry.time_column == "source_effective_at"
            && entry.earliest_event_time == Some(t0)
            && entry.latest_event_time == Some(t0 + Duration::seconds(5))
    }));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_snapshot_observes_one_exact_event_revision_and_membership() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCatalogLedgerRepository::new(pool.connection().clone());
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 1, 0, 0).unwrap();

    let original = membership_commit(10, t0, t0, "original");
    let original_event_change_id = original.events[0].change.event_change_id.clone();
    let original_batch = repo
        .commit(original)
        .await
        .expect("original catalog commit");

    let correction = membership_commit(
        11,
        t0 + Duration::seconds(20),
        t0 + Duration::seconds(5),
        "correction",
    );
    let corrected_event_change_id = correction.events[0].change.event_change_id.clone();
    let correction_batch = repo
        .commit(correction)
        .await
        .expect("corrected catalog commit");
    let original_visible_at = original_batch.committed_at.expect("committed batch");
    let correction_visible_at = correction_batch.committed_at.expect("committed batch");

    let before_available = repo
        .snapshots_at_boundary(
            &DecisionClock::new(0)
                .boundary(correction_visible_at - Duration::milliseconds(1))
                .expect("boundary"),
        )
        .await
        .expect("batch PIT lookup before correction availability");
    assert_coherent_membership(&before_available, &original_event_change_id, "original");
    assert!(before_available.iter().all(|snapshot| {
        snapshot.market.available_at == original_visible_at
            && snapshot.event.available_at == original_visible_at
    }));

    let after_available = repo
        .snapshots_at_boundary(
            &DecisionClock::new(0)
                .boundary(correction_visible_at)
                .expect("boundary"),
        )
        .await
        .expect("batch PIT lookup after correction availability");
    assert_coherent_membership(&after_available, &corrected_event_change_id, "correction");
    assert!(after_available.iter().all(|snapshot| {
        snapshot.market.available_at == correction_visible_at
            && snapshot.event.available_at == correction_visible_at
    }));

    let effective_decision = correction_visible_at + Duration::seconds(1);
    let lag_secs = u64::try_from((effective_decision - (t0 + Duration::seconds(2))).num_seconds())
        .expect("positive fixture knowledge lag");
    let before_effective = repo
        .snapshots_at_boundary(
            &DecisionClock::new(lag_secs)
                .boundary(effective_decision)
                .expect("boundary"),
        )
        .await
        .expect("batch PIT lookup before correction effective time");
    assert_coherent_membership(&before_effective, &original_event_change_id, "original");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_snapshot_rejects_decisions_before_catalog_coverage() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCatalogLedgerRepository::new(pool.connection().clone());
    let coverage_start = Utc.with_ymd_and_hms(2026, 7, 10, 2, 0, 0).unwrap();

    let batch = repo
        .commit(membership_commit(
            20,
            coverage_start,
            coverage_start,
            "baseline",
        ))
        .await
        .expect("baseline catalog commit");
    let coverage_start = batch.committed_at.expect("committed batch");

    let error = repo
        .snapshots_at_boundary(
            &DecisionClock::new(0)
                .boundary(coverage_start - Duration::milliseconds(1))
                .expect("boundary"),
        )
        .await
        .expect_err("pre-coverage replay must fail closed");
    match error {
        StorageError::StaleData(detail) => {
            assert!(detail.contains("predates coverage start"), "{detail}");
        }
        other => panic!("expected stale-data error, got {other:?}"),
    }

    let at_coverage_start = repo
        .snapshots_at_boundary(
            &DecisionClock::new(0)
                .boundary(coverage_start)
                .expect("boundary"),
        )
        .await
        .expect("coverage-start boundary is admissible");
    assert_eq!(at_coverage_start.len(), 2);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn concurrent_batch_reads_never_observe_a_torn_catalog_commit() {
    const READER_COUNT: usize = 24;

    let (pool, _container) = setup_pg().await;
    let repo = Arc::new(PgCatalogLedgerRepository::new(pool.connection().clone()));
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 3, 0, 0).unwrap();
    repo.commit(membership_commit(30, t0, t0, "original"))
        .await
        .expect("original catalog commit");

    let boundary = DecisionClock::new(0)
        .boundary(Utc::now() + Duration::minutes(1))
        .expect("boundary");
    let barrier = Arc::new(Barrier::new(READER_COUNT + 1));
    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let repo = Arc::clone(&repo);
        let boundary = boundary.clone();
        let barrier = Arc::clone(&barrier);
        readers.push(tokio::spawn(async move {
            barrier.wait().await;
            repo.snapshots_at_boundary(&boundary).await
        }));
    }

    barrier.wait().await;
    repo.commit(membership_commit(
        31,
        t0 + Duration::seconds(10),
        t0 + Duration::seconds(10),
        "correction",
    ))
    .await
    .expect("atomic correction commit");

    for reader in readers {
        let snapshots = reader
            .await
            .expect("reader task")
            .expect("concurrent batch PIT lookup");
        let revisions = snapshot_revisions(&snapshots);
        assert!(
            revisions == BTreeSet::from(["original"])
                || revisions == BTreeSet::from(["correction"]),
            "a repeatable-read snapshot mixed catalog revisions: {revisions:?}"
        );
    }

    let committed = repo
        .snapshots_at_boundary(&boundary)
        .await
        .expect("post-commit batch PIT lookup");
    assert_eq!(
        snapshot_revisions(&committed),
        BTreeSet::from(["correction"])
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn failed_attempt_is_audited_but_never_creates_catalog_coverage() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCatalogLedgerRepository::new(pool.connection().clone());
    let started_at = Utc::now();
    let failure = repo
        .record_failure(CatalogBatchFailure {
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            sync_kind: CatalogSyncKind::Reconcile,
            started_at,
            fetched_at: None,
            failure_stage: CatalogSyncFailureStage::Fetch,
            failure_detail: "gamma request timed out".to_owned(),
            rejections: Vec::new(),
        })
        .await
        .expect("failed attempt audit");

    assert_eq!(failure.status, CatalogSyncStatus::Failed);
    assert_eq!(failure.failure_stage, Some(CatalogSyncFailureStage::Fetch));
    assert_eq!(
        failure.failure_detail.as_deref(),
        Some("gamma request timed out")
    );
    assert!(failure.committed_at.is_none());
    assert_eq!(repo.coverage_start().await.expect("coverage"), None);
    assert_eq!(repo.watermark().await.expect("watermark"), None);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn identical_reconcile_only_appends_batch_audit() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();
    let repo = PgCatalogLedgerRepository::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();

    repo.commit(commit(1, t0, t0, "original"))
        .await
        .expect("baseline commit");
    let before = catalog_row_counts(db).await;

    let mut reconcile = commit(1, t0 + Duration::minutes(5), t0, "original");
    reconcile.batch.sync_kind = CatalogSyncKind::Reconcile;
    repo.commit(reconcile)
        .await
        .expect("identical reconcile must be an idempotent success");
    let after = catalog_row_counts(db).await;

    assert_eq!(after.batches, before.batches + 1);
    assert_eq!(after.event_objects, before.event_objects);
    assert_eq!(after.event_changes, before.event_changes);
    assert_eq!(after.market_objects, before.market_objects);
    assert_eq!(after.market_changes, before.market_changes);
    assert_eq!(after.events, before.events);
    assert_eq!(after.markets, before.markets);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn projection_upsert_updates_filter_reasons_atomically_with_status() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();
    let repo = PgCatalogLedgerRepository::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();

    repo.commit(commit(1, t0, t0, "original"))
        .await
        .expect("active baseline");

    let mut filtered = commit(2, t0 + Duration::minutes(5), t0, "filtered");
    set_market_disposition(
        &mut filtered,
        MarketStatus::Filtered,
        iter::once(CatalogFilterReason::Inactive).collect(),
    );
    repo.commit(filtered)
        .await
        .expect("active to filtered transition");
    let filtered_projection = market::Entity::find_by_id(MarketId::new("0xcatalog-ledger"))
        .one(db)
        .await
        .expect("load filtered projection")
        .expect("filtered projection exists");
    assert_eq!(filtered_projection.status, MarketStatus::Filtered);
    assert_eq!(
        filtered_projection.filter_reasons,
        vec![CatalogFilterReason::Inactive]
    );

    let active = commit(3, t0 + Duration::minutes(10), t0, "active-again");
    repo.commit(active)
        .await
        .expect("filtered to active transition");
    let active_projection = market::Entity::find_by_id(MarketId::new("0xcatalog-ledger"))
        .one(db)
        .await
        .expect("load active projection")
        .expect("active projection exists");
    assert_eq!(active_projection.status, MarketStatus::Active);
    assert!(active_projection.filter_reasons.is_empty());
}

#[derive(Debug, PartialEq, Eq)]
struct CatalogRowCounts {
    batches: u64,
    event_objects: u64,
    event_changes: u64,
    market_objects: u64,
    market_changes: u64,
    events: u64,
    markets: u64,
}

async fn catalog_row_counts(db: &sea_orm::DatabaseConnection) -> CatalogRowCounts {
    CatalogRowCounts {
        batches: catalog_sync_batch::Entity::find()
            .count(db)
            .await
            .expect("count catalog batches"),
        event_objects: catalog_event_object::Entity::find()
            .count(db)
            .await
            .expect("count catalog event objects"),
        event_changes: catalog_event_change::Entity::find()
            .count(db)
            .await
            .expect("count catalog event changes"),
        market_objects: catalog_market_object::Entity::find()
            .count(db)
            .await
            .expect("count catalog market objects"),
        market_changes: catalog_market_change::Entity::find()
            .count(db)
            .await
            .expect("count catalog market changes"),
        events: event::Entity::find()
            .count(db)
            .await
            .expect("count event projections"),
        markets: market::Entity::find()
            .count(db)
            .await
            .expect("count market projections"),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn object_payload_hash_drift_is_rejected_before_catalog_commit() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCatalogLedgerRepository::new(pool.connection().clone());
    let now = Utc::now();
    let mut invalid = commit(1, now, now, "original");
    invalid.events[0].object.payload["revision"] = "tampered".into();

    let error = repo
        .commit(invalid)
        .await
        .expect_err("payload not covered by its content hash must fail");
    assert!(matches!(error, StorageError::InvariantViolation { .. }));
    assert_eq!(repo.coverage_start().await.expect("coverage"), None);
}

fn commit(
    revision: u8,
    available_at: chrono::DateTime<Utc>,
    source_effective_at: chrono::DateTime<Utc>,
    label: &str,
) -> CatalogBatchCommit {
    let sync_kind = if label == "original" {
        CatalogSyncKind::Baseline
    } else {
        CatalogSyncKind::Reconcile
    };
    let batch_id = CatalogSyncBatchId::from_v7();
    let event_id = EventId::new("evt-catalog-ledger");
    let market_id = MarketId::new("0xcatalog-ledger");
    let (event, event_object) = event_object_fixture(
        event_id.clone(),
        vec![market_id.clone()],
        source_effective_at,
        label,
    );
    let event_object_id = event_object.event_object_id.clone();
    let (market, market_object) =
        market_object_fixture(market_id, event_id.clone(), source_effective_at, label);
    CatalogBatchCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: batch_id.clone(),
            sync_kind,
            started_at: available_at,
            fetched_at: available_at,
            event_count: 1,
            market_count: 1,
            rejected_count: 0,
            batch_hash: hash(revision),
        },
        events: vec![CatalogEventCandidate {
            projection: event,
            object: event_object,
            change: NewCatalogEventChange {
                event_change_id: CatalogEventChangeId::from_v7(),
                catalog_sync_batch_id: batch_id.clone(),
                event_object_id: event_object_id.clone(),
                event_id,
                source_effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                change_type: CatalogChangeType::GammaScanUpsert,
            },
        }],
        markets: vec![CatalogMarketCandidate {
            projection: market,
            object: market_object,
            market_change_id: CatalogMarketChangeId::from_v7(),
            catalog_sync_batch_id: batch_id,
            event_object_id,
            source_effective_at,
            source_timestamp_quality: CatalogTimestampQuality::Source,
            source_created_at: Some(source_effective_at - Duration::days(1)),
            change_type: CatalogChangeType::GammaScanUpsert,
        }],
    }
}

fn membership_commit(
    revision: u8,
    available_at: chrono::DateTime<Utc>,
    source_effective_at: chrono::DateTime<Utc>,
    label: &str,
) -> CatalogBatchCommit {
    let sync_kind = if matches!(label, "original" | "baseline") {
        CatalogSyncKind::Baseline
    } else {
        CatalogSyncKind::Reconcile
    };
    let batch_id = CatalogSyncBatchId::from_v7();
    let event_change_id = CatalogEventChangeId::from_v7();
    let event_id = EventId::new(EVENT_ID);
    let market_ids = [MarketId::new(MARKET_A), MarketId::new(MARKET_B)];
    let (current_event, event_object) = event_object_fixture(
        event_id.clone(),
        market_ids.to_vec(),
        source_effective_at,
        label,
    );
    let event_object_id = event_object.event_object_id.clone();

    let market_candidates = market_ids
        .iter()
        .map(|market_id| {
            let (projection, object) = market_object_fixture(
                market_id.clone(),
                event_id.clone(),
                source_effective_at,
                label,
            );
            CatalogMarketCandidate {
                projection,
                object,
                market_change_id: CatalogMarketChangeId::from_v7(),
                catalog_sync_batch_id: batch_id.clone(),
                event_object_id: event_object_id.clone(),
                source_effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                source_created_at: Some(source_effective_at - Duration::days(1)),
                change_type: CatalogChangeType::GammaScanUpsert,
            }
        })
        .collect::<Vec<_>>();

    CatalogBatchCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: batch_id.clone(),
            sync_kind,
            started_at: available_at,
            fetched_at: available_at,
            event_count: 1,
            market_count: i64::try_from(market_ids.len()).expect("two fixture markets"),
            rejected_count: 0,
            batch_hash: hash(revision),
        },
        events: vec![CatalogEventCandidate {
            projection: current_event,
            object: event_object,
            change: NewCatalogEventChange {
                event_change_id,
                catalog_sync_batch_id: batch_id,
                event_object_id,
                event_id,
                source_effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                change_type: CatalogChangeType::GammaScanUpsert,
            },
        }],
        markets: market_candidates,
    }
}

fn set_market_disposition(
    commit: &mut CatalogBatchCommit,
    status: MarketStatus,
    filter_reasons: CatalogFilterReasonSet,
) {
    let candidate = commit
        .markets
        .first_mut()
        .expect("catalog fixture has one market");
    let mut source = serde_json::from_value::<MarketRegistryInfo>(candidate.object.payload.clone())
        .expect("decode typed market fixture");
    source.status = status;
    source.filter_reasons = filter_reasons;
    let payload = serde_json::to_value(&source).expect("encode typed market fixture");
    let content_hash =
        CanonicalDigest::content_hash_typed("quant-pivot/catalog-market-object", 1, &payload)
            .expect("hash typed market fixture");
    candidate.object.market_object_id = CatalogMarketObjectId::from_content_hash(&content_hash);
    candidate.object.content_hash = content_hash.clone();
    candidate.object.payload = payload;
    candidate.projection = UpsertMarket::from_registry(&source).expect("normalize market fixture");
    candidate.projection.content_hash = content_hash;
}

fn event_object_fixture(
    event_id: EventId,
    market_ids: Vec<MarketId>,
    source_effective_at: chrono::DateTime<Utc>,
    revision: &str,
) -> (UpsertEvent, NewCatalogEventObject) {
    let source = EventRegistryInfo {
        event_id: event_id.clone(),
        title: "Catalog Event".to_owned(),
        slug: format!("catalog-event-{}", event_id.as_str()),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: market_ids.clone(),
        categories: CategorySet::from(MarketCategory::Politics),
        tags: vec![MarketCategory::Politics.as_str().to_owned()],
        neg_risk: false,
        end_date: None,
        created_at: source_effective_at - Duration::days(1),
        updated_at: source_effective_at,
    };
    let mut payload = serde_json::to_value(&source).expect("serialize typed event fixture");
    payload
        .as_object_mut()
        .expect("event fixture object")
        .insert("revision".to_owned(), revision.into());
    let content_hash =
        CanonicalDigest::content_hash_typed("quant-pivot/catalog-event-object", 1, &payload)
            .expect("hash typed event fixture");
    let event_object_id = CatalogEventObjectId::from_content_hash(&content_hash);
    (
        UpsertEvent {
            event_id,
            title: source.title,
            slug: source.slug,
            series_slug: source.series_slug,
            status: source.status,
            tags: EventTags(source.tags),
            neg_risk: source.neg_risk,
            catalog_market_ids: market_ids.into(),
            end_date: source.end_date,
            content_hash: content_hash.clone(),
        },
        NewCatalogEventObject {
            event_object_id,
            content_hash,
            schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
            payload,
        },
    )
}

fn market_object_fixture(
    market_id: MarketId,
    event_id: EventId,
    source_effective_at: chrono::DateTime<Utc>,
    revision: &str,
) -> (UpsertMarket, NewCatalogMarketObject) {
    let yes_token = TokenId::new(format!("{}-yes", market_id.as_str()));
    let no_token = TokenId::new(format!("{}-no", market_id.as_str()));
    let source = MarketRegistryInfo {
        market_id,
        event_id,
        token_yes: yes_token.clone(),
        token_no: no_token.clone(),
        question: "Catalog market?".to_owned(),
        slug: "catalog-market".to_owned(),
        description: None,
        categories: CategorySet::from(MarketCategory::Politics),
        status: MarketStatus::Active,
        filter_reasons: CatalogFilterReasonSet::default(),
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: yes_token,
                outcome: "Yes".to_owned(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: no_token,
                outcome: "No".to_owned(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: None,
        volume_24h: None,
        start_date: None,
        end_date: None,
        resolved_at: None,
        created_at: Some(source_effective_at - Duration::days(1)),
        updated_at: source_effective_at,
    };
    let mut payload = serde_json::to_value(&source).expect("serialize typed market fixture");
    payload
        .as_object_mut()
        .expect("market fixture object")
        .insert("revision".to_owned(), revision.into());
    let content_hash =
        CanonicalDigest::content_hash_typed("quant-pivot/catalog-market-object", 1, &payload)
            .expect("hash typed market fixture");
    let market_object_id = CatalogMarketObjectId::from_content_hash(&content_hash);
    let mut projection = UpsertMarket::from_registry(&source).expect("normalize market fixture");
    projection.content_hash = content_hash.clone();
    (
        projection,
        NewCatalogMarketObject {
            market_object_id,
            content_hash,
            schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
            payload,
        },
    )
}

fn assert_coherent_membership(
    snapshots: &[CatalogSnapshotInfo],
    event_change_id: &CatalogEventChangeId,
    expected_revision: &str,
) {
    assert_eq!(snapshots.len(), 2);
    let expected_market_ids = BTreeSet::from([MARKET_A, MARKET_B]);
    let snapshot_market_ids = snapshots
        .iter()
        .map(|snapshot| snapshot.market.market_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(snapshot_market_ids, expected_market_ids);

    for snapshot in snapshots {
        assert_eq!(snapshot.market.event_change_id, *event_change_id);
        assert_eq!(snapshot.event.event_change_id, *event_change_id);
        assert_eq!(snapshot.market.event_id, snapshot.event.event_id);
        assert_eq!(snapshot.market.payload["revision"], expected_revision);
        assert_eq!(snapshot.event.payload["revision"], expected_revision);
        assert_eq!(snapshot.event_markets.len(), 2);
        assert_eq!(
            snapshot
                .event_markets
                .iter()
                .map(|market| market.market_id.as_str())
                .collect::<BTreeSet<_>>(),
            expected_market_ids
        );
        assert!(snapshot.event_markets.iter().all(|market| {
            market.event_change_id == *event_change_id
                && market.event_id == snapshot.event.event_id
                && market.payload["revision"] == expected_revision
        }));
    }
}

fn snapshot_revisions(snapshots: &[CatalogSnapshotInfo]) -> BTreeSet<&str> {
    snapshots
        .iter()
        .flat_map(|snapshot| {
            std::iter::once(&snapshot.market)
                .chain(snapshot.event_markets.iter())
                .map(|market| market.payload["revision"].as_str())
                .chain(std::iter::once(snapshot.event.payload["revision"].as_str()))
        })
        .flatten()
        .collect()
}

fn hash(seed: u8) -> ContentHash {
    ContentHash::parse(format!("blake3:{seed:064x}")).expect("valid content hash")
}
