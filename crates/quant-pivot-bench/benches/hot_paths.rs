//! hot-path benchmarks (ingest + book store only).

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    types::{Price, Shares, TokenId},
};
use rust_decimal_macros::dec;

fn bench_book_store_apply_snapshot(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let store = BookStore::new(metrics);
    let token = TokenId::new("12345");
    let bids = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.55)),
        Shares::new(dec!(100)),
    )]);
    let asks = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.56)),
        Shares::new(dec!(100)),
    )]);

    c.bench_function("book_store_apply_snapshot", |b| {
        b.iter(|| {
            store.apply_snapshot(&token, Arc::clone(&bids), Arc::clone(&asks), 1, None);
        });
    });
}

criterion_group!(benches, bench_book_store_apply_snapshot);
criterion_main!(benches);
