//! Post-trade consumer and relay recovery tests.

use chrono::{Duration as ChronoDuration, Utc};
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    infra::async_writer::AsyncWriter,
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    post_trade::{
        consumer::PostTradeConsumer,
        relay::{PostTradeRelay, PostTradeRelayDeps},
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    config::{PolymarketConfig, RiskConfig, WebSocketConfig},
    domain::{NewTrade, TradeObservation},
    enums::common::{ExecutionMode, MarketCategory, Side, TradeBusinessOutcome, TradeState},
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, Price, ReservationId, Shares, TokenId,
        TradeId, Usd,
    },
};
use oxide_arb_repository::traits::TradeRepository;
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::mocks::{MockPositionRepository, MockTradeRepository};
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Harness {
    trade_repo: Arc<MockTradeRepository>,
    position_repo: Arc<MockPositionRepository>,
    consumer: PostTradeConsumer,
    capital_manager: Arc<CapitalManager>,
    metrics: Arc<MetricsHub>,
}

fn new_trade(trade_id: TradeId, reservation_id: ReservationId) -> NewTrade {
    NewTrade {
        trade_id,
        execution_id: ExecutionId::generate(),
        reservation_id,
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xpost-trade-market"),
        event_id: EventId::new("evt-post-trade"),
        token_id: TokenId::new("12345"),
        side: Side::Buy,
        shares: Shares::new(dec!(100)),
        price: Price::new(dec!(0.92)),
        cost_usd: Usd::new(dec!(92)),
        fee_usd: Usd::new(dec!(1)),
        detected_edge_bps: Some(Bps::new(dec!(250))),
        detected_profit_usd: Some(Usd::new(dec!(5))),
        scored_snapshot: serde_json::json!({
            "resolution_prob": 0.99,
            "price_zone": "z97",
            "duration_bucket": "medium",
            "staleness": "fresh"
        }),
        category: MarketCategory::Politics,
        execution_mode: ExecutionMode::Live,
    }
}

fn fill_observation() -> TradeObservation {
    TradeObservation {
        state: TradeState::FillObserved,
        shares: Shares::new(dec!(100)),
        price: Price::new(dec!(0.92)),
        cost_usd: Usd::new(dec!(92)),
        fee_usd: Usd::new(dec!(1)),
        order_id: None,
        tx_hash: None,
        net_profit_usd: Some(Usd::new(dec!(6))),
        latency_ms: Some(12),
        error_message: None,
        confirmed_at: Utc::now(),
    }
}

fn risk_engine() -> Arc<RiskEngine> {
    Arc::new(
        RiskEngineBuilder::new()
            .config(RiskConfig::default())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&StaticRiskMetrics)
            .expect("risk engine build"),
    )
}

struct StaticRiskMetrics;

impl oxide_arb_risk::traits::RiskMetrics for StaticRiskMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::ZERO
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<oxide_arb_models::domain::position::PositionInfo> {
        Vec::new()
    }
    fn cash_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn position_mark_value(&self) -> Usd {
        Usd::ZERO
    }
    fn equity(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }
    fn open_directional_count(&self, _: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
        0
    }
    fn record_trade_outcome(&self, _: Side, _: &MarketId, _: bool) {}
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
    fn metrics_age_secs(&self) -> u64 {
        0
    }
    fn is_stale(&self) -> bool {
        false
    }
    fn is_authoritative(&self) -> bool {
        true
    }
}

fn audit_writer(metrics: Arc<MetricsHub>) -> Arc<ExecutionAuditWriter> {
    let (writer, _worker) = AsyncWriter::new(
        "post-trade-audit-test",
        128,
        Duration::from_secs(3600),
        move |_batch: Vec<OpportunityAuditRow>| Box::pin(async move { Ok(()) }),
        metrics,
        CancellationToken::new(),
    );
    Arc::new(ExecutionAuditWriter::new(Arc::new(writer)))
}

fn risk_metrics(
    exposure: Arc<InMemoryExposureReservation>,
    metrics_state: Arc<RiskMetricsState>,
) -> Arc<CoreRiskMetrics> {
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    metrics_state.seed_simulated_snapshot(ExecutionMode::Live, Usd::new(dec!(5000)));
    Arc::new(CoreRiskMetrics::new(
        metrics_state,
        exposure,
        ws_manager,
        ExecutionMode::Live,
    ))
}

fn harness() -> Harness {
    let metrics = Arc::new(MetricsHub::new());
    let trade_repo = Arc::new(MockTradeRepository::default());
    let position_repo = Arc::new(MockPositionRepository::default());
    let reservation_config = RiskConfig::default().exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation_config.clone()));
    let capital_manager = Arc::new(CapitalManager::new(exposure.clone(), reservation_config));
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    let fsm = Arc::new(ExecutionFSM::new(
        metrics.clone(),
        Arc::new(AlertDispatcher::new(None, None, None, 0)),
    ));

    let consumer = PostTradeConsumer {
        risk_engine: risk_engine(),
        risk_metrics: risk_metrics(exposure, metrics_state.clone()),
        fsm,
        trade_repo: trade_repo.clone(),
        position_repo: position_repo.clone(),
        audit_writer: audit_writer(metrics.clone()),
        metrics_state,
        metrics_refresh: None,
        metrics: metrics.clone(),
        execution_mode: ExecutionMode::Live,
    };

    Harness {
        trade_repo,
        position_repo,
        consumer,
        capital_manager,
        metrics,
    }
}

async fn create_observed_fill(repo: &MockTradeRepository) -> TradeId {
    let trade_id = TradeId::generate();
    let reservation_id = ReservationId::new_id();
    repo.create(new_trade(trade_id.clone(), reservation_id))
        .await
        .expect("create trade");
    assert!(
        repo.mark_submitted(&trade_id, Utc::now())
            .await
            .expect("mark submitted")
    );
    repo.mark_observed(&trade_id, fill_observation())
        .await
        .expect("mark observed");
    trade_id
}

#[tokio::test]
async fn consumer_is_idempotent_for_replayed_fill() {
    let harness = harness();
    let trade_id = create_observed_fill(&harness.trade_repo).await;
    let observed = harness.trade_repo.find(&trade_id).expect("observed trade");

    harness.consumer.process(&observed).await;
    harness.consumer.process(&observed).await;

    let terminal = harness.trade_repo.find(&trade_id).expect("terminal trade");
    assert_eq!(terminal.state, TradeState::Settled);
    assert_eq!(
        terminal.business_outcome,
        Some(TradeBusinessOutcome::Success)
    );
    assert_eq!(harness.position_repo.positions_snapshot().len(), 1);
    assert_eq!(harness.metrics.post_trade_relay_processed.get(), 1);
}

#[tokio::test]
async fn repository_claim_is_linearized_under_race() {
    let harness = harness();
    create_observed_fill(&harness.trade_repo).await;
    let now = Utc::now();
    let expired_before = now - ChronoDuration::seconds(5);

    let (a, b) = tokio::join!(
        harness
            .trade_repo
            .claim_unprocessed(10, "relay-a", now, expired_before),
        harness
            .trade_repo
            .claim_unprocessed(10, "relay-b", now, expired_before),
    );
    let total_claimed = a.expect("relay-a claim").len() + b.expect("relay-b claim").len();
    assert_eq!(total_claimed, 1);
}

#[tokio::test]
async fn relay_processes_observed_trade_to_terminal_state() {
    let harness = harness();
    let trade_id = create_observed_fill(&harness.trade_repo).await;
    let notify = Arc::new(Notify::new());
    let shutdown = CancellationToken::new();
    let relay = PostTradeRelay::new(PostTradeRelayDeps {
        consumer: harness.consumer,
        trade_repo: harness.trade_repo.clone(),
        notify: notify.clone(),
        capital_manager: harness.capital_manager,
        batch_size: 10,
        poll_interval: Duration::from_millis(10),
        stale_submitted_after: Duration::from_secs(60),
        metrics: harness.metrics,
    });
    let handle = tokio::spawn(relay.run(shutdown.clone()));
    notify.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    handle.await.expect("relay join").expect("relay run");

    let terminal = harness.trade_repo.find(&trade_id).expect("terminal trade");
    assert_eq!(terminal.state, TradeState::Settled);
}

#[tokio::test]
async fn relay_marks_stale_submitted_trade_orphaned() {
    let harness = harness();
    let trade_id = TradeId::generate();
    let reservation_id = ReservationId::new_id();
    harness
        .trade_repo
        .create(new_trade(trade_id.clone(), reservation_id))
        .await
        .expect("create trade");
    assert!(
        harness
            .trade_repo
            .mark_submitted(&trade_id, Utc::now() - ChronoDuration::minutes(5))
            .await
            .expect("mark submitted")
    );

    let shutdown = CancellationToken::new();
    let relay = PostTradeRelay::new(PostTradeRelayDeps {
        consumer: harness.consumer,
        trade_repo: harness.trade_repo.clone(),
        notify: Arc::new(Notify::new()),
        capital_manager: harness.capital_manager,
        batch_size: 10,
        poll_interval: Duration::from_millis(10),
        stale_submitted_after: Duration::from_millis(1),
        metrics: harness.metrics,
    });
    let handle = tokio::spawn(relay.run(shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    handle.await.expect("relay join").expect("relay run");

    let orphaned = harness.trade_repo.find(&trade_id).expect("orphaned trade");
    assert_eq!(orphaned.state, TradeState::Orphaned);
    assert_eq!(
        orphaned.business_outcome,
        Some(TradeBusinessOutcome::Failed)
    );
    assert!(orphaned.needs_reconcile);
}
