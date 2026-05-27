//! Deterministic integration tests for book/coalescer/execution hot paths.

use std::sync::Arc;

use chrono::Utc;
use oxide_arb_core::detection::coalescer::Coalescer;
use oxide_arb_core::execution::fsm::ExecutionFSM;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::market_registry::MarketRegistry;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::domain::market::{MarketRegistryInfo, TokenInfo};
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::types::{MarketId, Price, Shares, TokenId};
use rust_decimal_macros::dec;
use tokio_util::sync::CancellationToken;

fn level(p: rust_decimal::Decimal, s: rust_decimal::Decimal) -> BookLevel {
    BookLevel::from_decimal_unchecked(Price::new(p), Shares::new(s))
}

fn sample_market(id: &str) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(id),
        event_id: oxide_arb_models::types::EventId::new("evt"),
        token_yes: TokenId::new(format!("{id}-yes")),
        token_no: TokenId::new(format!("{id}-no")),
        question: "Q".into(),
        slug: "q".into(),
        category: oxide_arb_models::enums::common::MarketCategory::Other,
        status: MarketStatus::Active,
        neg_risk: false,
        tick_size: oxide_arb_models::enums::common::TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(format!("{id}-yes")),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(format!("{id}-no")),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: oxide_arb_models::types::Usd::ZERO,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn book_store_publish_increments_version() {
    let metrics = Arc::new(MetricsHub::new());
    let store = BookStore::new(metrics);
    let tid = TokenId::new("tok");
    store.apply_snapshot(&tid, vec![level(dec!(0.5), dec!(1))], vec![], 100, None);
    let v1 = store.book_version(&tid);
    store.apply_delta(
        &tid,
        [(Side::Buy, Price::new(dec!(0.55)), Shares::new(dec!(2)))],
        200,
        None,
    );
    let v2 = store.book_version(&tid);
    assert_eq!(v1, 1);
    assert_eq!(v2, 2);
}

#[tokio::test]
async fn coalescer_flushes_after_window() {
    let metrics = Arc::new(MetricsHub::new());
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(sample_market("m1"));

    let (tx, rx) = flume::bounded(16);
    let shutdown = CancellationToken::new();
    let coalescer = Coalescer::new(
        registry,
        std::time::Duration::from_millis(30),
        tx,
        Arc::clone(&metrics),
        shutdown.clone(),
    );

    let yes = TokenId::new("m1-yes");
    coalescer.notify_token_update(&yes);

    let handle = tokio::spawn(async move { coalescer.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    shutdown.cancel();
    let _ = handle.await;

    let market = rx.try_recv().expect("market should flush");
    assert_eq!(market.as_str(), "m1");
}

#[tokio::test]
async fn coalescer_pair_complete_flushes_immediately() {
    let metrics = Arc::new(MetricsHub::new());
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(sample_market("m2"));

    let (tx, rx) = flume::bounded(16);
    let shutdown = CancellationToken::new();
    let coalescer = Coalescer::new(
        registry,
        std::time::Duration::from_millis(500),
        tx,
        Arc::clone(&metrics),
        shutdown,
    );

    coalescer.notify_token_update(&TokenId::new("m2-yes"));
    coalescer.notify_token_update(&TokenId::new("m2-no"));

    let market = rx
        .try_recv()
        .expect("pair-complete should flush immediately");
    assert_eq!(market.as_str(), "m2");
}

#[test]
fn backpressure_book_coalesce_does_not_halt() {
    use std::sync::Arc;
    use std::time::Instant;

    use oxide_arb_core::observability::backpressure::{BackpressureAction, BackpressurePolicy};
    use oxide_arb_core::outbox::in_memory::InMemoryEventStore;
    use oxide_arb_models::domain::pipeline::{
        IngressTrace, PipelineEvent, PriceDeltaCmd, PriceLevelDelta,
    };
    use oxide_arb_models::enums::common::Side;
    use oxide_arb_models::types::{Price, Shares, TokenId};
    use rust_decimal_macros::dec;

    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let bp = BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        1,
    );

    let event = PipelineEvent::PriceDelta(PriceDeltaCmd {
        asset_id: TokenId::new("t1"),
        changes: Arc::from([PriceLevelDelta {
            price: Price::new(dec!(0.5)),
            size: Shares::new(dec!(100)),
            side: Side::Buy,
        }]),
        timestamp_ms: 1,
        trace: IngressTrace::new(Instant::now(), 1),
    });

    assert_eq!(
        bp.on_book_channel_full(0, event),
        BackpressureAction::Coalesced
    );
    assert!(!fsm.is_emergency());
    assert_eq!(metrics.book_apply_coalesced_total.get(), 1);
}
