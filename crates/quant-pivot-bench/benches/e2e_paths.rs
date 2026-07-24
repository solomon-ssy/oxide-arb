//! end-to-end micro-benchmarks (data plane skeleton).

mod support;

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::{
    domain::{data_plane::pipeline::StreamSessionTicket, market::book::BookSnapshot},
    types::TokenId,
};
use uuid::Uuid;

use support::registered_data_plane;

fn bench_book_store_borrowed(c: &mut Criterion) {
    let (data_plane, token) = registered_data_plane("999");
    let store = BookStore::new(data_plane, Arc::new(MetricsHub::new()));
    let session = StreamSessionTicket::new(Uuid::from_u128(1), 1).expect("valid benchmark session");
    assert!(
        store
            .session_directory()
            .open(session, Arc::from([TokenId::new("999")]))
    );
    assert!(store.publish_snapshot_session(
        token,
        BookSnapshot::new(Arc::from([]), Arc::from([]), 1, 1),
        1,
        session,
        None,
    ));

    c.bench_function("book_store_read_borrowed", |b| {
        b.iter(|| {
            let _ = store.read_fresh(token, |snapshot, _| snapshot.version);
        });
    });
}

criterion_group!(benches, bench_book_store_borrowed);
criterion_main!(benches);
