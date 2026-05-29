//! Outcome drain integration: spill FIFO replay via `spawn_outcome_drain`.

use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{execution_pipeline::PostTradeDrainDeps, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    outbox::in_memory::{InMemoryEventStore, SharedInMemoryEventStore},
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    config::{PolymarketConfig, WebSocketConfig},
    domain::{NewTrade, execution::PostTradeJob},
    enums::common::ExecutionMode,
    types::Usd,
};
use oxide_arb_repository::traits::{PositionRepository, TradeRepository};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskPersistence,
};
use oxide_arb_test_support::{
    fixtures::minimal_post_trade_job,
    mocks::{MockPositionRepository, MockTradeRepository},
    persistence::{spawn_test_outcome_drain, test_persistence},
    risk::{TestRiskMetrics, TestRiskPersistence, test_risk_config},
};
use rust_decimal_macros::dec;
use std::time::Duration as StdTimeDuration;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

struct DrainFixture {
    outcome_tx: flume::Sender<PostTradeJob>,
    outcome_rx: flume::Receiver<PostTradeJob>,
    spill: SharedInMemoryEventStore,
    persistence: Arc<TestRiskPersistence>,
    trade_repo: Arc<MockTradeRepository>,
    position_repo: Arc<MockPositionRepository>,
    audit_writer: Arc<ExecutionAuditWriter>,
    alerts: Arc<AlertDispatcher>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    metrics_state: Arc<RiskMetricsState>,
    fsm: Arc<ExecutionFSM>,
    metrics: Arc<MetricsHub>,
    _shutdown: CancellationToken,
}

async fn seed_pending_trade(trade_repo: &MockTradeRepository, job: &PostTradeJob) {
    trade_repo
        .create(NewTrade {
            trade_id: job.trade_id.clone(),
            execution_id: job.execution_id.clone(),
            opportunity_id: job.opportunity_id.clone(),
            market_id: job.market_id.clone(),
            event_id: job.event_id.clone(),
            token_id: job.token_id.clone(),
            side: job.side,
            shares: job.plan_shares,
            price: job.entry_price,
            cost_usd: Usd::new(job.plan_shares.inner() * job.entry_price.inner()),
            fee_usd: Usd::ZERO,
            detected_edge_bps: job.edge_bps,
            detected_profit_usd: job.detected_profit,
            execution_mode: ExecutionMode::Paper,
        })
        .await
        .expect("seed pending trade");
}

fn build_drain_fixture() -> DrainFixture {
    let shutdown = CancellationToken::new();
    let test_persist = test_persistence(shutdown.clone());
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let persistence: Arc<TestRiskPersistence> = Arc::new(TestRiskPersistence::new());
    let exposure = Arc::new(InMemoryExposureReservation::new(
        test_risk_config().exposure_reservation_config(),
    ));
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        shutdown.clone(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        StdTimeDuration::from_secs(60),
    ))));
    metrics_state.seed_simulated_snapshot(ExecutionMode::Paper, Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        ws_manager,
        ExecutionMode::Paper,
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
    let (outcome_tx, outcome_rx) = flume::bounded(1024);

    DrainFixture {
        outcome_tx,
        outcome_rx,
        spill,
        persistence,
        trade_repo: Arc::clone(&test_persist.trade_repo),
        position_repo: Arc::clone(&test_persist.position_repo),
        audit_writer: Arc::clone(&test_persist.audit_writer),
        alerts: Arc::clone(&test_persist.alerts),
        risk_engine,
        risk_metrics,
        metrics_state,
        fsm,
        metrics,
        _shutdown: shutdown,
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
        let job = minimal_post_trade_job(id);
        seed_pending_trade(fixture.trade_repo.as_ref(), &job).await;
        fixture
            .spill
            .enqueue_sync_post_trade(job, &fixture.metrics)
            .expect("prefill spill");
    }

    let shutdown = CancellationToken::new();
    let channel_job = minimal_post_trade_job("job-d");
    seed_pending_trade(fixture.trade_repo.as_ref(), &channel_job).await;

    let drain = tokio::spawn({
        let rx = fixture.outcome_rx.clone();
        let risk_engine = Arc::clone(&fixture.risk_engine);
        let risk_metrics = Arc::clone(&fixture.risk_metrics);
        let fsm = Arc::clone(&fixture.fsm);
        let trade_repo = Arc::clone(&fixture.trade_repo);
        let position_repo: Arc<dyn PositionRepository> = fixture.position_repo.clone();
        let audit_writer = Arc::clone(&fixture.audit_writer);
        let alerts = Arc::clone(&fixture.alerts);
        let spill = Arc::clone(&fixture.spill);
        let metrics_state = Arc::clone(&fixture.metrics_state);
        let shutdown = shutdown.clone();
        async move {
            spawn_test_outcome_drain(
                rx,
                PostTradeDrainDeps {
                    risk_engine,
                    risk_metrics,
                    fsm,
                    trade_repo,
                    position_repo,
                    audit_writer,
                    alerts,
                    post_trade_spill: spill,
                    metrics_state,
                    metrics_refresh: None,
                    execution_mode: ExecutionMode::Paper,
                },
                shutdown,
            )
            .await
            .expect("drain");
        }
    });

    fixture
        .outcome_tx
        .send_async(channel_job)
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
