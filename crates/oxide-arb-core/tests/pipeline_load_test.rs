//! Load test: flood snapshots and verify backpressure without emergency halt.

#[path = "support/test_util/mock_event_source.rs"]
mod mock_event_source;

use std::sync::Arc;
use std::time::{Duration, Instant};

use mock_event_source::MockEventSource;
use oxide_arb_core::observability::backpressure::BackpressurePolicy;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::outbox::in_memory::InMemoryEventStore;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::data_pipeline::{DataPipeline, DataPipelineDeps};
use oxide_arb_core::pipeline::market_registry::MarketRegistry;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::domain::pipeline::{
    BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent,
};
use oxide_arb_models::types::{Price, Shares, TokenId};
use rust_decimal_macros::dec;
use tokio_util::sync::CancellationToken;

fn snapshot_cmd(token: &TokenId, ts: u64) -> PipelineEvent {
    let level = BookLevel::from_decimal(Price::new(dec!(0.95)), Shares::new(dec!(100))).unwrap();
    PipelineEvent::BookSnapshot(BookSnapshotCmd {
        asset_id: token.clone(),
        bids: BookSideData::empty(),
        asks: BookSideData::from_levels(Arc::from([level])),
        timestamp_ms: ts,
        trace: IngressTrace::new(Instant::now(), ts),
    })
}

#[tokio::test]
async fn hundred_tokens_thousand_snapshots_monotonic_versions() {
    let metrics = Arc::new(MetricsHub::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        4,
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let (coalescer_tx, _coalescer_rx) = flume::bounded(256);
    let shutdown = CancellationToken::new();

    let (source, inject) = MockEventSource::paired(8192);
    let event_source: Arc<dyn oxide_arb_core::pipeline::event_source::PipelineEventSource> =
        Arc::new(source);

    let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(&book_store),
        market_registry,
        coalescer_tx,
        metrics: Arc::clone(&metrics),
        backpressure,
        book_shard_count: 4,
        book_channel_capacity: 8,
        shutdown: shutdown.clone(),
    }));

    let pipeline_handle = {
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move { pipeline.run().await })
    };

    let mut last_versions = ahash::AHashMap::new();
    for round in 0..1000u64 {
        for i in 0..100usize {
            let token = TokenId::new(format!("tok-{i}"));
            inject.send(snapshot_cmd(&token, round * 100 + i as u64));
            let version = book_store.book_version(&token);
            let prev = last_versions.entry(token).or_insert(0);
            assert!(version >= *prev, "book version must be monotonic");
            *prev = version;
        }
        if round % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        metrics.book_apply_coalesced_total.get() > 0,
        "expected coalesce under backpressure"
    );
    assert_eq!(book_store.token_count(), 100);

    shutdown.cancel();
    let _ = pipeline_handle.await;
}
