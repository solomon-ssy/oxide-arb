//! Concurrent stress tests for [`BookStore`] token creation and writes.

use oxide_arb_core::{observability::metrics_hub::MetricsHub, pipeline::book_store::BookStore};
use oxide_arb_models::{
    domain::book::BookLevel,
    enums::common::Side,
    types::{Price, Shares, TokenId},
};
use rust_decimal_macros::dec;
use std::{
    sync::{Arc, Barrier},
    thread,
};

fn make_level(price: rust_decimal::Decimal) -> BookLevel {
    BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(dec!(10)))
}

#[test]
fn concurrent_same_token_deltas_monotonic_version() {
    let store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let token = TokenId::new("shared-token");
    store.apply_snapshot(&token, vec![make_level(dec!(0.50))], vec![], 1, None);

    let barrier = Arc::new(Barrier::new(32));
    let handles: Vec<_> = (0..32)
        .map(|i| {
            let store = Arc::clone(&store);
            let token = token.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let price = dec!(0.50) + dec!(0.0001) * rust_decimal::Decimal::from(i);
                store.apply_delta(
                    &token,
                    [(Side::Buy, Price::new(price), Shares::new(dec!(10)))],
                    2 + u64::try_from(i).unwrap_or(0),
                    None,
                );
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread join");
    }

    assert_eq!(store.book_version(&token), 33, "1 snapshot + 32 deltas");
    assert!(store.load(&token).is_some());
}

#[test]
fn concurrent_distinct_tokens_unique_states() {
    let store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let barrier = Arc::new(Barrier::new(32));

    let handles: Vec<_> = (0..32)
        .map(|i| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let token = TokenId::new(format!("token-{i}"));
                store.apply_snapshot(&token, vec![make_level(dec!(0.50))], vec![], 1, None);
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread join");
    }

    assert_eq!(store.token_count(), 32);
}
