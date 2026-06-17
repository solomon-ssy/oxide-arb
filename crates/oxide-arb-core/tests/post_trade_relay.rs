//! Post-trade consumer and relay recovery tests.

#[path = "common/mod.rs"]
mod common;

use chrono::{Duration as ChronoDuration, Utc};
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics},
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    infra::async_writer::{AsyncWriter, AsyncWriterConfig},
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::market_registry::MarketRegistry,
    post_trade::{
        consumer::PostTradeConsumer,
        relay::{PostTradeRelay, PostTradeRelayDeps},
    },
    runtime_config::RuntimeConfigStore,
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::runtime_config::{NotificationConfig, RuntimeConfig};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    config::{PolymarketConfig, WebSocketConfig},
    domain::{
        CoreEvent, CoreEventPublisher, NewTrade, PositionInfo, TradeObservation,
        market::{MarketRegistryInfo, TokenInfo},
    },
    enums::{
        common::{
            CategorySet, ExecutionMode, MarketCategory, RedeemResolutionSource, RedeemStatus, Side,
            TickSize, TradeBusinessOutcome, TradeState,
        },
        market::MarketStatus,
    },
    runtime_config::RiskConfig,
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, Price, ReservationId, Shares, TokenId,
        TradeId, Usd,
    },
};
use oxide_arb_repository::traits::TradeRepository;
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskMetrics,
};
use oxide_arb_test_support::mocks::{
    MockCalibrationRepository, MockPositionRepository, MockTradeRepository,
};
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Harness {
    trade_repo: Arc<MockTradeRepository>,
    position_repo: Arc<MockPositionRepository>,
    calibration_repo: Arc<MockCalibrationRepository>,
    consumer: PostTradeConsumer,
    capital_manager: Arc<CapitalManager>,
    metrics: Arc<MetricsHub>,
    events_rx: flume::Receiver<CoreEvent>,
}

fn scored_snapshot_json(opportunity_id: &OpportunityId) -> serde_json::Value {
    serde_json::json!({
        "opportunity_id": opportunity_id,
        "market_id": "0xpost-trade-market",
        "event_id": "evt-post-trade",
        "token_id": "12345",
        "token_yes": "12345",
        "token_no": "67890",
        "side": "BUY",
        "category": "politics",
        "entry_price": "0.92",
        "edge_bps": "250",
        "expected_net_profit": "5",
        "net_profit_if_correct": "6",
        "shares": "100",
        "total_cost": "92",
        "total_fees": "1",
        "resolution_prob": 0.99,
        "resolution_prob_decimal": "0.99",
        "confidence": 0.99,
        "confidence_decimal": "0.99",
        "fill_probability": null,
        "score": null,
        "urgency_factor": null,
        "category_weight": null,
        "staleness_discount": null,
        "convergence_secs": 3600,
        "price_zone": "z97",
        "duration_bucket": "medium",
        "depth_used_pct": 10.0,
        "depth_used_pct_decimal": "10",
        "staleness": "fresh",
        "calibration": {
            "sample_size": 50,
            "alpha_prior": "2",
            "beta_prior": "1",
            "posterior_mean": "0.93",
            "fallback_tier": 1,
            "snapshot_hash": null
        },
        "book": null,
        "factors": null,
        "missing_fields": [],
        "detected_at": Utc::now(),
        "schema_version": 2
    })
}

fn new_trade(trade_id: TradeId, reservation_id: ReservationId) -> NewTrade {
    let opportunity_id = OpportunityId::from_v7();
    NewTrade {
        trade_id,
        execution_id: ExecutionId::from_v7(),
        reservation_id,
        opportunity_id: opportunity_id.clone(),
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
        scored_snapshot: scored_snapshot_json(&opportunity_id),
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

impl RiskMetrics for StaticRiskMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::ZERO
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<PositionInfo> {
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
        AsyncWriterConfig::new("post-trade-audit-test")
            .batch_size(128)
            .flush_interval(Duration::from_secs(3600)),
        move |_batch: Vec<OpportunityAuditRow>| Box::pin(async move { Ok(()) }),
        metrics,
        CancellationToken::new(),
    );
    Arc::new(ExecutionAuditWriter::new(Arc::new(writer)))
}

fn risk_metrics(
    exposure: Arc<InMemoryExposureReservation>,
    metrics_state: Arc<RiskMetricsState>,
    market_registry: Arc<MarketRegistry>,
) -> Arc<CoreRiskMetrics> {
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    ws_manager.seed_test_token_connectivity(&TokenId::new("post-trade-yes"));
    ws_manager.seed_test_token_connectivity(&TokenId::new("post-trade-no"));
    metrics_state.seed_simulated_snapshot(ExecutionMode::Live, Usd::new(dec!(5000)));
    Arc::new(CoreRiskMetrics::new(
        metrics_state,
        exposure,
        market_registry,
        ws_manager,
        ExecutionModeHandle::new(ExecutionMode::Live),
    ))
}

fn post_trade_market_registry() -> Arc<MarketRegistry> {
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(MarketRegistryInfo {
        market_id: MarketId::new("0xpost-trade-market"),
        event_id: EventId::new("evt-post-trade"),
        token_yes: TokenId::new("post-trade-yes"),
        token_no: TokenId::new("post-trade-no"),
        question: "Post trade test?".into(),
        slug: "post-trade".into(),
        categories: CategorySet::from(MarketCategory::Politics),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new("post-trade-yes"),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new("post-trade-no"),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(5),
        volume_24h: Usd::ZERO,
        fee_schedule: None,
        end_date: None,
        resolved_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    registry
}

fn harness_with_config(config: RuntimeConfig) -> Harness {
    let metrics = Arc::new(MetricsHub::new());
    let trade_repo = Arc::new(MockTradeRepository::default());
    let position_repo = Arc::new(MockPositionRepository::default());
    let calibration_repo = Arc::new(MockCalibrationRepository::default());
    let reservation_config = RiskConfig::default().exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation_config.clone()));
    let capital_manager = Arc::new(CapitalManager::new(exposure.clone(), &reservation_config));
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    let fsm = Arc::new(ExecutionFSM::new(
        metrics.clone(),
        Arc::new(AlertDispatcher::new(&NotificationConfig::default())),
    ));

    let (events, events_rx) = CoreEventPublisher::bounded(64);
    let metrics_refresh = common::disconnected_metrics_refresh(
        Arc::clone(&metrics_state),
        ExecutionMode::Live,
        Arc::clone(&metrics),
    );
    let market_registry = post_trade_market_registry();
    let consumer = PostTradeConsumer {
        risk_engine: risk_engine(),
        risk_metrics: risk_metrics(
            exposure,
            metrics_state.clone(),
            Arc::clone(&market_registry),
        ),
        fsm,
        capital_manager: Arc::clone(&capital_manager),
        trade_repo: trade_repo.clone(),
        position_repo: position_repo.clone(),
        calibration_repo: calibration_repo.clone(),
        audit_writer: audit_writer(metrics.clone()),
        metrics_state,
        metrics_refresh,
        metrics: metrics.clone(),
        events,
        market_registry,
        runtime_config: Arc::new(RuntimeConfigStore::new(config)),
    };

    Harness {
        trade_repo,
        position_repo,
        calibration_repo,
        consumer,
        capital_manager,
        metrics,
        events_rx,
    }
}

fn harness() -> Harness {
    harness_with_config(RuntimeConfig::default())
}

async fn create_observed_fill(repo: &MockTradeRepository) -> TradeId {
    let trade_id = TradeId::from_v7();
    let reservation_id = ReservationId::from_v7();
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
    let positions = harness.position_repo.positions_snapshot();
    assert_eq!(positions.len(), 1);
    // The opened position inherits the trade's execution mode so ledger
    // aggregates stay mode-scoped.
    assert_eq!(positions[0].execution_mode, ExecutionMode::Live);
    assert_eq!(harness.calibration_repo.outcome_count(), 1);
    assert_eq!(harness.metrics.post_trade_relay_processed.get(), 1);
}

#[tokio::test]
async fn simulated_fill_snapshots_default_redeem_plan_when_policy_has_no_live_classes() {
    let mut config = RuntimeConfig::default();
    config.settlement.redeem.standard = None;
    config.settlement.redeem.neg_risk = None;
    let harness = harness_with_config(config);
    let trade_id = TradeId::from_v7();
    let reservation_id = ReservationId::from_v7();
    let mut trade = new_trade(trade_id.clone(), reservation_id);
    trade.execution_mode = ExecutionMode::Paper;
    harness
        .trade_repo
        .create(trade)
        .await
        .expect("create paper trade");
    assert!(
        harness
            .trade_repo
            .mark_submitted(&trade_id, Utc::now())
            .await
            .expect("mark submitted")
    );
    harness
        .trade_repo
        .mark_observed(&trade_id, fill_observation())
        .await
        .expect("mark observed");
    let observed = harness.trade_repo.find(&trade_id).expect("observed trade");

    harness.consumer.process(&observed).await;

    let positions = harness.position_repo.positions_snapshot();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].execution_mode, ExecutionMode::Paper);
    assert_eq!(positions[0].redeem_status, RedeemStatus::NotRequired);
    assert!(!positions[0].redeem_neg_risk);
    assert_eq!(positions[0].redeem_route, "standard_ctf");
    assert_eq!(
        positions[0].redeem_resolution,
        RedeemResolutionSource::ClassStandard
    );
}

#[tokio::test]
async fn successful_fill_emits_trade_filled_and_position_changed() {
    let harness = harness();
    let trade_id = create_observed_fill(&harness.trade_repo).await;
    let observed = harness.trade_repo.find(&trade_id).expect("observed trade");

    harness.consumer.process(&observed).await;

    let mut events = Vec::new();
    while let Ok(event) = harness.events_rx.try_recv() {
        events.push(event);
    }
    match events
        .iter()
        .find(|event| matches!(event, CoreEvent::TradeFilled(_)))
    {
        Some(CoreEvent::TradeFilled(trade)) => assert_eq!(trade.trade_id, trade_id),
        _ => panic!("expected a TradeFilled event, got {events:?}"),
    }
    assert!(
        events
            .iter()
            .any(|event| matches!(event, CoreEvent::PositionChanged(_))),
        "expected a PositionChanged event, got {events:?}"
    );

    // At-least-once replay must not re-emit: only the worker that actually
    // advanced the row publishes.
    harness.consumer.process(&observed).await;
    assert!(
        harness.events_rx.try_recv().is_err(),
        "idempotent replay must not re-emit"
    );
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
        runtime: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
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
    let trade_id = TradeId::from_v7();
    let reservation_id = ReservationId::from_v7();
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
    // Tight one-second confirmation budget: the trade was submitted five
    // minutes ago, so the first stale scan must orphan it.
    let mut config = RuntimeConfig::default();
    config.execution.timeout.trade_confirm_timeout_secs = 1;
    let relay = PostTradeRelay::new(PostTradeRelayDeps {
        consumer: harness.consumer,
        trade_repo: harness.trade_repo.clone(),
        notify: Arc::new(Notify::new()),
        capital_manager: harness.capital_manager,
        batch_size: 10,
        runtime: Arc::new(RuntimeConfigStore::new(config)),
        metrics: harness.metrics,
    });
    let handle = tokio::spawn(relay.run(shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    handle.await.expect("relay join").expect("relay run");

    let orphaned = harness.trade_repo.find(&trade_id).expect("orphaned trade");
    assert_eq!(orphaned.state, TradeState::Orphaned);
    assert_eq!(orphaned.business_outcome, None);
    assert!(orphaned.needs_reconcile);
}
