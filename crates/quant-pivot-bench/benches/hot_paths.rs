//! hot-path benchmarks (ingest + book store only).

mod support;

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::{
    domain::{
        data_plane::pipeline::StreamSessionTicket,
        market::book::{BookLevel, BookSnapshot},
    },
    types::{Price, Shares, TokenId},
};
use rust_decimal_macros::dec;
use uuid::Uuid;

use support::registered_data_plane;

fn bench_book_store_snapshot(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let (data_plane, token) = registered_data_plane("12345");
    let store = BookStore::new(data_plane, metrics);
    let session = StreamSessionTicket::new(Uuid::from_u128(1), 1).expect("valid benchmark session");
    assert!(
        store
            .session_directory()
            .open(session, Arc::from([TokenId::new("12345")]))
    );
    let bids = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.55)),
        Shares::new(dec!(100)),
    )]);
    let asks = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.56)),
        Shares::new(dec!(100)),
    )]);

    c.bench_function("book_store_publish_snapshot", |b| {
        b.iter(|| {
            store.publish_snapshot_session(
                token,
                BookSnapshot::new(Arc::clone(&bids), Arc::clone(&asks), 1, 1),
                1,
                session,
                None,
            );
        });
    });
}

criterion_group!(benches, bench_book_store_snapshot);
criterion_main!(benches);
