//! Domain-source cursor compare-and-set persistence contracts.

use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
        DomainSourceCursorInfo, UpsertDomainSourceCursor,
    },
    enums::domain::KlineInterval,
    hashing::CanonicalDigest,
    types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::{
    postgres::PgDomainSourceCursorRepository, traits::DomainSourceCursorRepository,
};
use quant_pivot_system_tests::postgres::setup_pg;

fn cursor(close_time: DateTime<Utc>) -> UpsertDomainSourceCursor {
    let checkpoint_json = DomainSourceCheckpoint::BinanceKline { close_time };
    let checkpoint_hash =
        CanonicalDigest::content_hash_json(&checkpoint_json).expect("hash checkpoint");
    UpsertDomainSourceCursor {
        source_id: DomainSourceId::binance(),
        instrument_key: DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        ),
        checkpoint_json,
        checkpoint_hash,
        status: DomainCursorStatus::Live,
        last_error: None,
        updated_at: Utc::now(),
    }
}

fn expect_advanced(outcome: DomainSourceCursorCasOutcome) -> DomainSourceCursorInfo {
    match outcome {
        DomainSourceCursorCasOutcome::Advanced(cursor) => cursor,
        DomainSourceCursorCasOutcome::Conflict(cursor) => {
            panic!("expected cursor advance, found conflict at {cursor:?}")
        }
    }
}

pub async fn compare_validates_concurrent_winner() {
    let (pool, _container) = setup_pg().await;
    let first = PgDomainSourceCursorRepository::new(pool.connection().clone());
    let second = PgDomainSourceCursorRepository::new(pool.connection().clone());
    let initial = cursor(
        Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0)
            .single()
            .expect("initial time"),
    );
    let mut failed_without_error = initial.clone();
    failed_without_error.status = DomainCursorStatus::Failed;
    let failed_shape_error = first
        .compare_and_set(None, failed_without_error)
        .await
        .expect_err("failed cursor without an error must fail");
    assert!(matches!(
        failed_shape_error,
        StorageError::InvariantViolation { .. }
    ));
    let mut live_with_error = initial.clone();
    live_with_error.last_error = Some("stale failure".to_owned());
    let live_shape_error = first
        .compare_and_set(None, live_with_error)
        .await
        .expect_err("live cursor with an error must fail");
    assert!(matches!(
        live_shape_error,
        StorageError::InvariantViolation { .. }
    ));

    let initialized = expect_advanced(
        first
            .compare_and_set(None, initial.clone())
            .await
            .expect("initialize cursor"),
    );
    assert_eq!(initialized.checkpoint_hash, initial.checkpoint_hash);

    let initialize_again = first
        .compare_and_set(None, initial.clone())
        .await
        .expect("observe initialization conflict");
    assert!(matches!(
        initialize_again,
        DomainSourceCursorCasOutcome::Conflict(ref current)
            if current.checkpoint_hash == initial.checkpoint_hash
    ));

    let mut forged = cursor(
        Utc.with_ymd_and_hms(2026, 7, 24, 0, 1, 0)
            .single()
            .expect("forged time"),
    );
    forged.checkpoint_hash =
        ContentHash::parse(&format!("blake3:{}", "f".repeat(64))).expect("forged hash");
    let forged_error = first
        .compare_and_set(Some(initial.checkpoint_hash), forged)
        .await
        .expect_err("forged checkpoint hash must fail");
    assert!(matches!(
        forged_error,
        StorageError::InvariantViolation { .. }
    ));

    let left = cursor(
        Utc.with_ymd_and_hms(2026, 7, 24, 0, 2, 0)
            .single()
            .expect("left time"),
    );
    let right = cursor(
        Utc.with_ymd_and_hms(2026, 7, 24, 0, 3, 0)
            .single()
            .expect("right time"),
    );
    let (left_result, right_result) = tokio::join!(
        first.compare_and_set(Some(initial.checkpoint_hash), left),
        second.compare_and_set(Some(initial.checkpoint_hash), right),
    );
    let outcomes = [
        left_result.expect("left compare-and-set"),
        right_result.expect("right compare-and-set"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DomainSourceCursorCasOutcome::Advanced(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DomainSourceCursorCasOutcome::Conflict(_)))
            .count(),
        1
    );

    let winner = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            DomainSourceCursorCasOutcome::Advanced(cursor) => Some(cursor),
            DomainSourceCursorCasOutcome::Conflict(_) => None,
        })
        .expect("one winner");
    let observed_conflict = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            DomainSourceCursorCasOutcome::Advanced(_) => None,
            DomainSourceCursorCasOutcome::Conflict(cursor) => Some(cursor),
        })
        .expect("one conflict");
    assert_eq!(
        observed_conflict.checkpoint_hash, winner.checkpoint_hash,
        "loser must observe the durable winner"
    );

    let stale = first
        .compare_and_set(
            Some(initial.checkpoint_hash),
            cursor(
                Utc.with_ymd_and_hms(2026, 7, 24, 0, 4, 0)
                    .single()
                    .expect("stale time"),
            ),
        )
        .await
        .expect("stale compare-and-set");
    assert!(matches!(
        stale,
        DomainSourceCursorCasOutcome::Conflict(ref current)
            if current.checkpoint_hash == winner.checkpoint_hash
    ));
    let stored = first
        .find(&initial.source_id, &initial.instrument_key)
        .await
        .expect("find cursor")
        .expect("cursor exists");
    assert_eq!(stored.checkpoint_hash, winner.checkpoint_hash);
}
