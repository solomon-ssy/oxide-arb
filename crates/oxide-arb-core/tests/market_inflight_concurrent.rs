//! Concurrent stress tests for [`MarketInFlightRegistry`].

use oxide_arb_core::execution::market_inflight::{InFlightGuard, MarketInFlightRegistry};
use oxide_arb_models::types::MarketId;
use std::{
    sync::{Arc, Barrier},
    thread,
};

#[test]
fn concurrent_same_market_only_one_wins() {
    let reg = Arc::new(MarketInFlightRegistry::new());
    let market = MarketId::new("m1");
    let barrier = Arc::new(Barrier::new(32));
    let results = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(32)));
    let guards = Arc::new(parking_lot::Mutex::new(Vec::<InFlightGuard>::new()));

    let handles: Vec<_> = (0..32)
        .map(|_| {
            let reg = Arc::clone(&reg);
            let market = market.clone();
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);
            let guards = Arc::clone(&guards);
            thread::spawn(move || {
                barrier.wait();
                match reg.try_acquire(&market) {
                    Some(guard) => {
                        results.lock().push(true);
                        guards.lock().push(guard);
                    }
                    None => results.lock().push(false),
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread join");
    }

    let wins = results.lock().iter().filter(|&&x| x).count();
    assert_eq!(wins, 1, "exactly one thread may acquire the same market");
    assert_eq!(guards.lock().len(), 1);
}

#[test]
fn guard_drop_releases_slot() {
    let reg = Arc::new(MarketInFlightRegistry::new());
    let market = MarketId::new("m-release");

    {
        let guard = reg.try_acquire(&market).expect("first acquire");
        assert_eq!(reg.active_count(), 1);
        drop(guard);
    }

    assert_eq!(reg.active_count(), 0);
    assert!(
        reg.try_acquire(&market).is_some(),
        "slot must be reusable after guard drop"
    );
}

#[test]
fn different_markets_parallel() {
    let reg = Arc::new(MarketInFlightRegistry::new());
    let markets: Vec<MarketId> = (0..8).map(|i| MarketId::new(format!("m{i}"))).collect();
    let guards: Vec<_> = markets
        .iter()
        .map(|m| {
            reg.try_acquire(m)
                .expect("distinct markets should not block")
        })
        .collect();

    assert_eq!(reg.active_count(), 8);
    drop(guards);
    assert_eq!(reg.active_count(), 0);
}

#[test]
fn active_count_tracks_inflight() {
    let reg = Arc::new(MarketInFlightRegistry::new());
    assert_eq!(reg.active_count(), 0);

    let g1 = reg.try_acquire(&MarketId::new("a")).expect("acquire a");
    assert_eq!(reg.active_count(), 1);

    let g2 = reg.try_acquire(&MarketId::new("b")).expect("acquire b");
    assert_eq!(reg.active_count(), 2);

    drop(g1);
    assert_eq!(reg.active_count(), 1);

    drop(g2);
    assert_eq!(reg.active_count(), 0);
}
