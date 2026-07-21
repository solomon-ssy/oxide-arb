//! end-to-end micro-benchmarks (data plane skeleton).

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::types::TokenId;

fn bench_book_store_load_empty(c: &mut Criterion) {
    let store = BookStore::new(Arc::new(MetricsHub::new()));
    let token = TokenId::new("999");

    c.bench_function("book_store_load_empty", |b| {
        b.iter(|| {
            let _ = store.load(&token);
        });
    });
}

criterion_group!(benches, bench_book_store_load_empty);
criterion_main!(benches);
