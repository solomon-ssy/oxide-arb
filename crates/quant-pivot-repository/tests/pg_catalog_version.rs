//! Bitemporal Gamma catalog ledger integration tests.

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::GammaConfig,
    domain::{
        CatalogCommit, CatalogSnapshotInfo, CatalogSyncFailureStage, DecisionClock,
        NewCatalogSyncBatch, NewEventCatalogVersion, NewFailedCatalogSyncBatch,
        NewMarketCatalogVersion,
    },
    enums::common::MarketCategory,
    types::{
        CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId, MarketCatalogVersionId,
        MarketId,
    },
};
use quant_pivot_repository::{
    postgres::PgCatalogVersionRepository, traits::CatalogVersionRepository,
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::Barrier;

const EVENT_ID: &str = "evt-batch-catalog-version";
const MARKET_A: &str = "0xbatch-catalog-a";
const MARKET_B: &str = "0xbatch-catalog-b";

#[tokio::test]
#[ignore = "requires Docker"]
async fn correction_is_invisible_until_its_availability_time() {
    let (pool, _container) = setup_pg().await;
    let guard = GammaConfig::default().catalog_visibility_guard_secs;
    let repo = PgCatalogVersionRepository::new(pool.connection().clone(), guard);
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
            &MarketId::new("0xcatalog-version"),
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
            &MarketId::new("0xcatalog-version"),
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
            &MarketId::new("0xcatalog-version"),
            &DecisionClock::new(lag_secs)
                .boundary(effective_decision)
                .expect("boundary"),
        )
        .await
        .expect("PIT lookup")
        .expect("original remains visible");
    assert_eq!(before_effective.payload["revision"], "original");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_snapshot_observes_one_exact_event_revision_and_membership() {
    let (pool, _container) = setup_pg().await;
    let guard = GammaConfig::default().catalog_visibility_guard_secs;
    let repo = PgCatalogVersionRepository::new(pool.connection().clone(), guard);
    let t0 = Utc.with_ymd_and_hms(2026, 7, 10, 1, 0, 0).unwrap();

    let original = membership_commit(10, t0, t0, "original");
    let original_event_version_id = original.event_versions[0].event_catalog_version_id.clone();
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
    let corrected_event_version_id = correction.event_versions[0]
        .event_catalog_version_id
        .clone();
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
    assert_coherent_membership(&before_available, &original_event_version_id, "original");
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
    assert_coherent_membership(&after_available, &corrected_event_version_id, "correction");
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
    assert_coherent_membership(&before_effective, &original_event_version_id, "original");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_snapshot_rejects_decisions_before_catalog_coverage() {
    let (pool, _container) = setup_pg().await;
    let guard = GammaConfig::default().catalog_visibility_guard_secs;
    let repo = PgCatalogVersionRepository::new(pool.connection().clone(), guard);
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
    let guard = GammaConfig::default().catalog_visibility_guard_secs;
    let repo = Arc::new(PgCatalogVersionRepository::new(
        pool.connection().clone(),
        guard,
    ));
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
    let guard = GammaConfig::default().catalog_visibility_guard_secs;
    let repo = PgCatalogVersionRepository::new(pool.connection().clone(), guard);
    let started_at = Utc::now();
    let failure = repo
        .record_failure(NewFailedCatalogSyncBatch {
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            sync_kind: "incremental".to_owned(),
            source_cursor: Some(started_at - Duration::minutes(5)),
            started_at,
            fetched_at: None,
            failure_stage: CatalogSyncFailureStage::Fetch,
            failure_detail: "gamma request timed out".to_owned(),
        })
        .await
        .expect("failed attempt audit");

    assert_eq!(failure.status, "failed");
    assert_eq!(failure.failure_stage.as_deref(), Some("fetch"));
    assert_eq!(
        failure.failure_detail.as_deref(),
        Some("gamma request timed out")
    );
    assert!(failure.committed_at.is_none());
    assert_eq!(repo.coverage_start().await.expect("coverage"), None);
    assert_eq!(repo.watermark().await.expect("watermark"), None);
}

fn commit(
    revision: u8,
    available_at: chrono::DateTime<Utc>,
    source_effective_at: chrono::DateTime<Utc>,
    label: &str,
) -> CatalogCommit {
    let batch_id = CatalogSyncBatchId::from_v7();
    let event_version_id = EventCatalogVersionId::from_v7();
    let event_id = EventId::new("evt-catalog-version");
    let market_id = MarketId::new("0xcatalog-version");
    CatalogCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: batch_id.clone(),
            sync_kind: "incremental".to_owned(),
            source_cursor: None,
            started_at: available_at,
            fetched_at: available_at,
            event_count: 1,
            market_count: 1,
            rejected_count: 0,
            batch_hash: hash(revision),
        },
        current_events: vec![make_event(
            event_id.as_str(),
            "Catalog Event",
            "catalog-event",
            MarketCategory::Politics,
        )],
        event_versions: vec![NewEventCatalogVersion {
            event_catalog_version_id: event_version_id.clone(),
            catalog_sync_batch_id: batch_id.clone(),
            event_id: event_id.clone(),
            source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            available_at,
            origin: "gamma_sync".to_owned(),
            content_hash: hash(revision.checked_add(10).expect("fixture hash seed")),
            payload: serde_json::json!({"revision": label}),
        }],
        current_markets: vec![make_market(
            market_id.as_str(),
            event_id.as_str(),
            "Catalog market?",
            "catalog-market",
            MarketCategory::Politics,
            None,
        )],
        market_versions: vec![NewMarketCatalogVersion {
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            catalog_sync_batch_id: batch_id,
            event_catalog_version_id: event_version_id,
            market_id,
            event_id,
            source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            source_created_at: Some(source_effective_at - Duration::days(1)),
            available_at,
            origin: "gamma_sync".to_owned(),
            content_hash: hash(revision.checked_add(20).expect("fixture hash seed")),
            payload: serde_json::json!({"revision": label}),
        }],
    }
}

fn membership_commit(
    revision: u8,
    available_at: chrono::DateTime<Utc>,
    source_effective_at: chrono::DateTime<Utc>,
    label: &str,
) -> CatalogCommit {
    let batch_id = CatalogSyncBatchId::from_v7();
    let event_version_id = EventCatalogVersionId::from_v7();
    let event_id = EventId::new(EVENT_ID);
    let market_ids = [MarketId::new(MARKET_A), MarketId::new(MARKET_B)];
    let mut current_event = make_event(
        EVENT_ID,
        "Batch Catalog Event",
        "batch-catalog-event",
        MarketCategory::Politics,
    );
    current_event.catalog_market_ids = market_ids.to_vec().into();

    let current_markets = market_ids
        .iter()
        .map(|market_id| {
            make_market(
                market_id.as_str(),
                EVENT_ID,
                &format!("Catalog market {}?", market_id.as_str()),
                &format!("catalog-market-{}", market_id.as_str()),
                MarketCategory::Politics,
                None,
            )
        })
        .collect::<Vec<_>>();
    let market_versions = market_ids
        .iter()
        .enumerate()
        .map(|(index, market_id)| NewMarketCatalogVersion {
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            catalog_sync_batch_id: batch_id.clone(),
            event_catalog_version_id: event_version_id.clone(),
            market_id: market_id.clone(),
            event_id: event_id.clone(),
            source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            source_created_at: Some(source_effective_at - Duration::days(1)),
            available_at,
            origin: "gamma_sync".to_owned(),
            content_hash: hash(
                revision
                    .checked_add(u8::try_from(index).expect("two fixture markets"))
                    .and_then(|seed| seed.checked_add(40))
                    .expect("fixture hash seed"),
            ),
            payload: serde_json::json!({
                "revision": label,
                "market_id": market_id.as_str(),
            }),
        })
        .collect::<Vec<_>>();

    CatalogCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id: batch_id.clone(),
            sync_kind: "incremental".to_owned(),
            source_cursor: None,
            started_at: available_at,
            fetched_at: available_at,
            event_count: 1,
            market_count: i64::try_from(market_ids.len()).expect("two fixture markets"),
            rejected_count: 0,
            batch_hash: hash(revision),
        },
        current_events: vec![current_event],
        event_versions: vec![NewEventCatalogVersion {
            event_catalog_version_id: event_version_id,
            catalog_sync_batch_id: batch_id,
            event_id,
            source_effective_at,
            source_timestamp_quality: "source".to_owned(),
            available_at,
            origin: "gamma_sync".to_owned(),
            content_hash: hash(revision.checked_add(20).expect("fixture hash seed")),
            payload: serde_json::json!({
                "revision": label,
                "market_ids": [MARKET_A, MARKET_B],
            }),
        }],
        current_markets,
        market_versions,
    }
}

fn assert_coherent_membership(
    snapshots: &[CatalogSnapshotInfo],
    event_version_id: &EventCatalogVersionId,
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
        assert_eq!(snapshot.market.event_catalog_version_id, *event_version_id);
        assert_eq!(snapshot.event.event_catalog_version_id, *event_version_id);
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
            market.event_catalog_version_id == *event_version_id
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
