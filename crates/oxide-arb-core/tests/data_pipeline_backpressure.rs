//! Data pipeline backpressure under flood load — FSM must not enter emergency.

use oxide_arb_core::{
    execution::fsm::ExecutionFSM,
    observability::{backpressure::BackpressurePolicy, metrics_hub::MetricsHub},
    outbox::in_memory::InMemoryEventStore,
    pipeline::{
        book_store::BookStore,
        data_pipeline::{DataPipeline, DataPipelineDeps},
        event_source,
        market_registry::MarketRegistry,
    },
};
use oxide_arb_models::{
    domain::{
        book::BookLevel,
        pipeline::{BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent},
    },
    types::{Price, Shares, TokenId},
};
use oxide_arb_test_support::mock_event::MockEventSource;
use rust_decimal_macros::dec;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

fn snapshot_cmd(token: &TokenId, ask_price: rust_decimal::Decimal, ts: u64) -> PipelineEvent {
    let level = BookLevel::from_decimal_unchecked(Price::new(ask_price), Shares::new(dec!(1000)));
    PipelineEvent::BookSnapshot(BookSnapshotCmd {
        asset_id: token.clone(),
        bids: BookSideData::empty(),
        asks: BookSideData::from_levels(Arc::from([level])),
        timestamp_ms: ts,
        trace: IngressTrace::new(Instant::now(), ts),
    })
}

#[tokio::test]
async fn flood_does_not_halt_fsm() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        1,
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let (coalescer_tx, _coalescer_rx) = flume::bounded(4);
    let (settlement_tx, _settlement_rx) = flume::bounded(4);
    let shutdown = CancellationToken::new();

    let (source, inject) = MockEventSource::paired(8192);
    let event_source: Arc<dyn event_source::PipelineEventSource> = Arc::new(source);

    let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(&book_store),
        market_registry,
        coalescer_tx,
        settlement_tx,
        metrics: Arc::clone(&metrics),
        backpressure: Arc::clone(&backpressure),
        book_shard_count: 1,
        book_channel_capacity: 4,
        shutdown: shutdown.clone(),
    }));

    let pipeline_handle = {
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move { pipeline.run().await })
    };

    let token = TokenId::new("flood-token");
    let last_price = dec!(0.99);
    let cmd = snapshot_cmd(&token, last_price, 1);

    for i in 0..1000 {
        let mut event = cmd.clone();
        if let PipelineEvent::BookSnapshot(ref mut snap) = event {
            snap.timestamp_ms = i;
            snap.trace = IngressTrace::new(Instant::now(), i);
        }
        inject.send(event);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    let _ = pipeline_handle.await;

    assert!(!fsm.is_emergency(), "flood must not trigger emergency halt");
    assert!(
        metrics.book_apply_coalesced_total.get() > 0,
        "expected coalesced book events under backpressure"
    );

    let best = book_store
        .load(&token)
        .and_then(|snap| snap.best_ask())
        .expect("book should have latest snapshot applied");
    assert_eq!(best.inner(), last_price, "latest-wins coalesce must win");
}

#[tokio::test]
async fn success_path_does_not_coalesce() {
    let metrics = Arc::new(MetricsHub::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        1,
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let (coalescer_tx, _coalescer_rx) = flume::bounded(64);
    let (settlement_tx, _settlement_rx) = flume::bounded(64);
    let shutdown = CancellationToken::new();

    let (source, inject) = MockEventSource::paired(8192);
    let event_source: Arc<dyn event_source::PipelineEventSource> = Arc::new(source);

    let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(&book_store),
        market_registry,
        coalescer_tx,
        settlement_tx,
        metrics: Arc::clone(&metrics),
        backpressure,
        book_shard_count: 1,
        book_channel_capacity: 256,
        shutdown: shutdown.clone(),
    }));

    let pipeline_handle = {
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move { pipeline.run().await })
    };

    let token = TokenId::new("no-coalesce-token");
    for i in 0..50 {
        inject.send(snapshot_cmd(
            &token,
            dec!(0.90) + dec!(0.0001) * rust_decimal::Decimal::from(i),
            i,
        ));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    let _ = pipeline_handle.await;

    assert_eq!(
        metrics.book_apply_coalesced_total.get(),
        0,
        "successful dispatch must not trigger coalesce backpressure"
    );
    assert_eq!(book_store.book_version(&token), 50);
}
