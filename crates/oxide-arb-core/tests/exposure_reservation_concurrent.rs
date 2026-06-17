//! Concurrent exposure reservation stress tests.

use oxide_arb_core::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::{
    runtime_config::ExposureReservationConfig,
    types::{MarketId, ReservationId, Usd},
};
use rust_decimal_macros::dec;
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

const fn tight_config() -> ExposureReservationConfig {
    ExposureReservationConfig {
        max_total_exposure_cents: 1_000, // $10
        max_per_market_cents: 500,       // $5
        default_ttl_secs: 300,
        gc_interval_secs: 30,
    }
}

#[test]
fn concurrent_reserve_respects_global_limit() {
    let backend = Arc::new(InMemoryExposureReservation::new(tight_config()));
    let amount = Usd::new(dec!(2));
    let threads = 16usize;
    let mut handles = Vec::with_capacity(threads);

    for i in 0..threads {
        let backend = Arc::clone(&backend);
        handles.push(thread::spawn(move || {
            let market = MarketId::new(format!("m{i}"));
            backend
                .try_reserve_sync(&market, amount, Duration::from_secs(60))
                .is_ok()
        }));
    }

    let successes = handles
        .into_iter()
        .map(|h| h.join().expect("thread join"))
        .filter(|ok| *ok)
        .count();
    let failures = threads - successes;

    assert_eq!(successes, 5, "global limit $10 / $2 = 5 successes");
    assert_eq!(failures, 11);
    assert!(backend.total_reserved_usd_sync().inner() <= dec!(10));
}

#[test]
fn concurrent_reserve_release_no_panic() {
    let backend = Arc::new(InMemoryExposureReservation::new(tight_config()));
    let market = MarketId::new("m1");
    let threads = 32usize;
    let mut handles = Vec::with_capacity(threads);

    for _thread in 0..threads {
        let backend = Arc::clone(&backend);
        let market = market.clone();
        handles.push(thread::spawn(move || {
            let amount = Usd::new(dec!(1));
            if let Ok(id) = backend.try_reserve_sync(&market, amount, Duration::from_secs(60)) {
                let _ = backend.release_sync(&id);
            }
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }
    assert_eq!(backend.active_count_sync(), 0);
    assert_eq!(backend.total_reserved_usd_sync(), Usd::ZERO);
}

#[test]
fn gc_expired_clears_counters() {
    let backend = Arc::new(InMemoryExposureReservation::new(tight_config()));
    let market = MarketId::new("m1");
    let _ = backend
        .try_reserve_sync(&market, Usd::new(dec!(3)), Duration::from_millis(1))
        .expect("reserve");
    thread::sleep(Duration::from_millis(5));
    let expired = backend.gc_expired();
    assert_eq!(expired, 1);
    assert_eq!(backend.total_reserved_usd_sync(), Usd::ZERO);
    assert_eq!(backend.active_count_sync(), 0);
}

#[test]
fn gc_preserves_reconciliation_pinned_reservations() {
    let backend = Arc::new(InMemoryExposureReservation::new(tight_config()));
    let market = MarketId::new("m1");
    let id = backend
        .try_reserve_sync(&market, Usd::new(dec!(3)), Duration::from_millis(1))
        .expect("reserve");
    backend
        .pin_for_reconciliation_sync(&id)
        .expect("pin reservation");
    thread::sleep(Duration::from_millis(5));

    let expired = backend.gc_expired();
    assert_eq!(expired, 0);
    assert_eq!(backend.active_count_sync(), 1);
    assert_eq!(backend.total_reserved_usd_sync(), Usd::new(dec!(3)));

    backend.release_sync(&id).expect("release pinned");
    assert_eq!(backend.active_count_sync(), 0);
    assert_eq!(backend.total_reserved_usd_sync(), Usd::ZERO);
}

#[test]
fn per_market_limit_enforced_under_contention() {
    let backend = Arc::new(InMemoryExposureReservation::new(tight_config()));
    let market = MarketId::new("m1");
    let amount = Usd::new(dec!(3));
    let mut ok = 0u32;
    let mut limit_hits = 0u32;

    for _ in 0..4 {
        match backend.try_reserve_sync(&market, amount, Duration::from_secs(60)) {
            Ok(_) => ok += 1,
            Err(ReservationError::ExceedsLimit { .. }) => limit_hits += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    assert_eq!(ok, 1, "per-market cap $5 allows one $3 reservation");
    assert_eq!(limit_hits, 3);
}

#[test]
fn restore_sync_is_idempotent_for_same_reservation_id() {
    let backend = InMemoryExposureReservation::new(tight_config());
    let market = MarketId::new("m-restore");
    let id = ReservationId::from_v7();
    let amount = Usd::new(dec!(4));
    let expires = Instant::now() + Duration::from_secs(300);

    backend
        .restore_sync(id.clone(), market.clone(), amount, false, expires)
        .expect("first restore");
    backend
        .restore_sync(id, market, amount, false, expires)
        .expect("duplicate restore must be idempotent");

    assert_eq!(backend.active_count_sync(), 1);
    assert_eq!(backend.total_reserved_usd_sync(), amount);
}
