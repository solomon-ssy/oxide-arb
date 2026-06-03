//! Production soak gate for market-data ingestion.

use chrono::Utc;
use oxide_arb_core::{
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        metrics_hub::MetricsHub,
    },
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
        market::{MarketRegistryInfo, TokenInfo},
        pipeline::{BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent},
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, Price, Shares, TokenId, Usd},
};
use oxide_arb_test_support::mock_event::MockEventSource;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const MARKET_COUNT: usize = 500;
const TOKEN_COUNT: usize = MARKET_COUNT * 2;

fn snapshot_cmd(token: &TokenId, ask_price: Decimal, timestamp_ms: u64) -> PipelineEvent {
    let level = BookLevel::from_decimal_unchecked(Price::new(ask_price), Shares::new(dec!(1000)));
    PipelineEvent::BookSnapshot(BookSnapshotCmd {
        asset_id: token.clone(),
        bids: BookSideData::empty(),
        asks: BookSideData::from_levels(Arc::from([level])),
        timestamp_ms,
        trace: IngressTrace::new(Instant::now(), timestamp_ms),
    })
}

fn register_markets(registry: &MarketRegistry) -> Vec<TokenId> {
    let mut tokens = Vec::with_capacity(TOKEN_COUNT);
    for i in 0..MARKET_COUNT {
        let market_id = MarketId::new(format!("0xsoak-market-{i}"));
        let event_id = EventId::new(format!("evt-soak-{i}"));
        let yes = TokenId::new(format!("soak-token-{i}-yes"));
        let no = TokenId::new(format!("soak-token-{i}-no"));
        registry.register_market(MarketRegistryInfo {
            market_id,
            event_id,
            token_yes: yes.clone(),
            token_no: no.clone(),
            question: format!("Soak market {i}?"),
            slug: format!("soak-market-{i}"),
            category: MarketCategory::Other,
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: yes.clone(),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: no.clone(),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            volume_24h: Usd::ZERO,
            fee_schedule: None,
            end_date: None,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        tokens.push(yes);
        tokens.push(no);
    }
    tokens
}

#[tokio::test]
#[ignore = "production soak gate: run before Live promotion"]
async fn five_hundred_markets_thousand_tokens_ingest_without_book_drops() {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(None, None, None, 0));
    let backpressure = Arc::new(BackpressurePolicy::new(Arc::clone(&metrics), 4));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let tokens = register_markets(&market_registry);
    let (coalescer_tx, _coalescer_rx) = flume::bounded(65_536);
    let (settlement_tx, _settlement_rx) = flume::bounded(1_024);
    let shutdown = CancellationToken::new();
    let (source, inject) = MockEventSource::paired(65_536);
    let event_source: Arc<dyn event_source::PipelineEventSource> = Arc::new(source);
    let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(&book_store),
        market_registry,
        coalescer_tx,
        settlement_tx,
        metrics: Arc::clone(&metrics),
        alerts,
        backpressure,
        book_fact_writer: None,
        book_shard_count: 4,
        book_channel_capacity: 4096,
        shutdown: shutdown.clone(),
    }));
    let handle = {
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move { pipeline.run().await })
    };

    let rounds = std::env::var("OXIDE_ARB_SOAK_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10);
    for round in 0..rounds {
        for (idx, token) in tokens.iter().enumerate() {
            let price = dec!(0.90) + dec!(0.000001) * Decimal::from(idx);
            inject.send(snapshot_cmd(
                token,
                price,
                round * TOKEN_COUNT as u64 + idx as u64,
            ));
        }
        tokio::task::yield_now().await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    shutdown.cancel();
    let _ = handle.await;

    assert_eq!(book_store.token_count(), TOKEN_COUNT);
    assert_eq!(
        metrics.book_apply_dropped.get(),
        0,
        "soak gate must not drop book events"
    );
}
