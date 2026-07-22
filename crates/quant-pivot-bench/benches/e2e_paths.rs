//! end-to-end micro-benchmarks (data plane skeleton).

mod support;

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};

use support::registered_data_plane;

fn bench_book_store_read_borrowed(c: &mut Criterion) {
    let (data_plane, token) = registered_data_plane("999");
    let store = BookStore::new(data_plane, Arc::new(MetricsHub::new()));

    c.bench_function("book_store_read_borrowed", |b| {
        b.iter(|| {
            let _ = store.read(token, |snapshot| snapshot.version);
        });
    });
}

criterion_group!(benches, bench_book_store_read_borrowed);
criterion_main!(benches);
