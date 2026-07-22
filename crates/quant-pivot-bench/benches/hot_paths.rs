//! hot-path benchmarks (ingest + book store only).

mod support;

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::{
    domain::market::book::{BookLevel, BookSnapshot},
    types::{Price, Shares},
};
use rust_decimal_macros::dec;

use support::registered_data_plane;

fn bench_book_store_publish_snapshot(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let (data_plane, token) = registered_data_plane("12345");
    let store = BookStore::new(data_plane, metrics);
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
            store.publish(
                token,
                BookSnapshot::new(Arc::clone(&bids), Arc::clone(&asks), 1, 1),
                1,
                1,
                None,
            );
        });
    });
}

criterion_group!(benches, bench_book_store_publish_snapshot);
criterion_main!(benches);
