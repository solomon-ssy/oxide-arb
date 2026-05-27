//! Outcome drain integration: spill FIFO replay via `spawn_outcome_drain`.

#[path = "support/test_util/risk_config.rs"]
mod risk_config;
#[path = "support/test_util/risk_metrics.rs"]
mod risk_metrics;
#[path = "support/test_util/risk_persistence.rs"]
mod risk_persistence;

use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{
        execution_pipeline::{ExecutionPipeline, PostTradeJob},
        fsm::ExecutionFSM,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::metrics_hub::MetricsHub,
    outbox::in_memory::{InMemoryEventStore, SharedInMemoryEventStore},
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    config::{ExposureReservationConfig, PolymarketConfig, WebSocketConfig},
    enums::{common::ExecutionMode, execution::ExecutionOutcome},
    types::{MarketId, Price, TokenId, TradeId, Usd},
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskPersistence,
};
use risk_config::test_risk_config;
use risk_metrics::TestRiskMetrics;
use risk_persistence::TestRiskPersistence;
use rust_decimal_macros::dec;
use std::time::Duration as StdTimeDuration;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

struct DrainFixture {
    outcome_tx: flume::Sender<PostTradeJob>,
    outcome_rx: flume::Receiver<PostTradeJob>,
    spill: SharedInMemoryEventStore,
    persistence: Arc<TestRiskPersistence>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    metrics: Arc<MetricsHub>,
}

fn spill_job(id: &str) -> PostTradeJob {
    PostTradeJob {
        trade_id: TradeId::new(id),
        market_id: MarketId::new("m1"),
        token_id: TokenId::new("t1"),
        entry_price: Price::new(dec!(0.5)),
        net_profit: Usd::new(dec!(1)),
        outcome: ExecutionOutcome::Miss {
            reason: "test".into(),
            execution_mode: ExecutionMode::Paper,
        },
    }
}

fn build_drain_fixture() -> DrainFixture {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let persistence: Arc<TestRiskPersistence> = Arc::new(TestRiskPersistence::new());
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        StdTimeDuration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        ws_manager,
    ));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .persistence(Arc::clone(&persistence) as Arc<dyn RiskPersistence>)
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );
    let (outcome_tx, outcome_rx) = ExecutionPipeline::outcome_channel();

    DrainFixture {
        outcome_tx,
        outcome_rx,
        spill,
        persistence,
        risk_engine,
        risk_metrics,
        fsm,
        metrics,
    }
}

fn audit_trade_ids(persistence: &TestRiskPersistence) -> Vec<String> {
    persistence
        .take_audits()
        .into_iter()
        .filter_map(|audit| audit.trade_id.map(|id| id.as_str().to_owned()))
        .collect()
}

#[tokio::test]
async fn spawn_outcome_drain_replays_spill_fifo_before_channel_jobs() {
    let fixture = build_drain_fixture();
    for id in ["job-a", "job-b", "job-c"] {
        fixture
            .spill
            .enqueue_sync_post_trade(spill_job(id), &fixture.metrics)
            .expect("prefill spill");
    }

    let shutdown = CancellationToken::new();
    let drain = tokio::spawn({
        let rx = fixture.outcome_rx.clone();
        let risk_engine = Arc::clone(&fixture.risk_engine);
        let risk_metrics = Arc::clone(&fixture.risk_metrics);
        let fsm = Arc::clone(&fixture.fsm);
        let spill = Arc::clone(&fixture.spill);
        let shutdown = shutdown.clone();
        async move {
            ExecutionPipeline::spawn_outcome_drain(
                rx,
                risk_engine,
                risk_metrics,
                fsm,
                spill,
                shutdown,
            )
            .await
            .expect("drain");
        }
    });

    fixture
        .outcome_tx
        .send_async(spill_job("job-d"))
        .await
        .expect("channel job");

    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    drain.await.expect("drain task");

    assert_eq!(
        audit_trade_ids(&fixture.persistence),
        vec![
            "job-a".to_owned(),
            "job-b".to_owned(),
            "job-c".to_owned(),
            "job-d".to_owned(),
        ]
    );
    assert_eq!(fixture.spill.pending_post_trade_count(), 0);
}
