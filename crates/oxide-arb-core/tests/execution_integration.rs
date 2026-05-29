//! Execution pipeline integration tests.

use alloy::signers::{Signer as _, local::LocalSigner};
use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::{
    clob::ClobClient, fees::FeeCalculator, keystore::OrderSigner, ws::ClobWsManager,
};
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{
        capital_manager::CapitalManager,
        clob_outcome::map_order_response,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps, PostTradeDrainDeps},
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::{
        backpressure::{BackpressureAction, BackpressurePolicy},
        metrics_hub::MetricsHub,
    },
    outbox::in_memory::InMemoryEventStore,
    pipeline::{
        book_store::BookStore, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    config::{MarketDataConfig, PolymarketConfig, WebSocketConfig},
    domain::{
        book::BookLevel,
        execution::{self, ExecutionPlan},
        order::OrderResponse,
    },
    enums::{
        common::{ExecutionMode, MarketCategory, Side, TradeOutcome},
        execution::ExecutionOutcome,
        order::OrderStatus,
    },
    types::{
        EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId, Shares,
        TokenId, Usd,
    },
};
use oxide_arb_repository::traits::PositionRepository;
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock};
use oxide_arb_test_support::{
    book::seed_book_store,
    fixtures::{minimal_post_trade_job, sample_scored},
    mocks::MockTradeRepository,
    persistence::{TestPersistence, spawn_test_outcome_drain, test_persistence},
    pipeline::build_pipeline,
    risk::{TestRiskMetrics, test_risk_config},
};
use polymarket_client_sdk_v2::{
    POLYGON,
    clob::{Client as SdkClient, Config as SdkConfig},
};
use rust_decimal_macros::dec;
use std::{
    str::FromStr as _,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[test]
fn clob_outcome_maps_partial_fill() {
    let plan = ExecutionPlan {
        execution_id: ExecutionId::generate(),
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("m1"),
        event_id: EventId::new("e1"),
        token_id: TokenId::new("t1"),
        side: Side::Buy,
        shares: Shares::new(dec!(100)),
        limit_price: Price::new(dec!(0.5)),
        estimated_cost: Usd::new(dec!(50)),
        estimated_fee: Usd::ZERO,
        category: MarketCategory::Politics,
        neg_risk: false,
        reservation_id: ReservationId::new_id(),
        detected_at: Utc::now(),
        planned_at: Utc::now(),
    };

    let fee_calculator = FeeCalculator::default();
    let outcome = map_order_response(
        OrderResponse {
            order_id: OrderId::new("ord-1"),
            status: OrderStatus::PartiallyFilled,
            tx_hash: None,
            filled_shares: Shares::new(dec!(40)),
            avg_fill_price: Some(Price::new(dec!(0.5))),
            fee_paid: Usd::ZERO,
            submitted_at: Utc::now(),
            responded_at: Utc::now(),
        },
        &plan,
        ExecutionMode::Live,
        Instant::now(),
        &fee_calculator,
        plan.category,
        &plan.token_id,
    );

    match outcome {
        ExecutionOutcome::Filled { filled_shares, .. } => {
            assert_eq!(filled_shares, Shares::new(dec!(40)));
        }
        other => panic!("expected filled partial, got {other:?}"),
    }
}

#[test]
fn fill_expected_net_profit_uses_execution_economics() {
    use oxide_arb_models::domain::execution::fill_expected_net_profit;

    let fused_p = dec!(0.95);
    let shares = Shares::new(dec!(100));
    let cost = Usd::new(dec!(92));
    let fee = Usd::new(dec!(0.40));
    let ev = fill_expected_net_profit(fused_p, shares, cost, fee);
    // expected_payout = 100 * 0.95 = 95, minus 92 cost, minus 0.40 fee = 2.60
    assert_eq!(ev, Usd::new(dec!(2.60)));
}

#[tokio::test]
async fn paper_execution_fills_when_risk_and_books_pass() {
    let harness = build_pipeline();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected paper fill, got {result:?}");
}

#[tokio::test]
async fn execution_rejects_when_fsm_emergency() {
    let harness = build_pipeline();
    harness.fsm.enter_emergency("test halt");
    let result = harness.pipeline.execute(harness.scored).await;
    assert!(result.is_rejected());
    assert_eq!(result.rejection_stage.as_deref(), Some("halted"));
}

#[tokio::test]
async fn fill_enqueues_post_trade_job() {
    let harness = build_pipeline();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = harness
        .outcome_rx
        .try_recv()
        .expect("post-trade job should be enqueued");
    assert!(matches!(job.outcome, ExecutionOutcome::Filled { .. }));
}

#[test]
fn post_trade_spill_does_not_halt() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let bp = BackpressurePolicy::new(Arc::clone(&metrics), None, Arc::clone(&spill), 1);

    let job = minimal_post_trade_job("trade-1");

    assert_eq!(
        bp.on_post_trade_channel_full(job),
        BackpressureAction::Spilled
    );
    assert!(!fsm.is_emergency());
    assert_eq!(metrics.post_trade_spilled_total.get(), 1);
    assert_eq!(spill.pending_post_trade_count(), 1);
}

#[test]
fn outbox_spill_fifo_under_pressure() {
    let metrics = Arc::new(MetricsHub::new());
    let spill = Arc::new(InMemoryEventStore::new());
    let bp = BackpressurePolicy::new(Arc::clone(&metrics), None, Arc::clone(&spill), 1);

    for i in 0..10 {
        let job = minimal_post_trade_job(&format!("job-{i}"));
        assert_eq!(
            bp.on_post_trade_channel_full(job),
            BackpressureAction::Spilled
        );
    }

    assert_eq!(metrics.post_trade_spilled_total.get(), 10);
    assert_eq!(spill.pending_post_trade_count(), 10);

    let drained = spill.drain_post_trade_jobs();
    assert_eq!(drained.len(), 10);
    for (i, job) in drained.iter().enumerate() {
        assert_eq!(job.trade_id.as_str(), format!("job-{i}"));
    }
}

#[tokio::test]
async fn live_mode_wiremock_fill() {
    const TOKEN: &str =
        "15871154585880608648532107628464183779895785213830018178010423617714102767076";

    let server = MockServer::start().await;
    mount_live_clob_wiremock(&server, None).await;
    let clob = live_clob_client(&server).await;
    let pipeline = live_execution_pipeline(clob, 30_000);

    let mut scored = sample_scored();
    Arc::make_mut(&mut scored).opportunity = Arc::new({
        let mut opp = (*scored.opportunity).clone();
        opp.token_id = TokenId::new(TOKEN);
        opp
    });
    seed_book_store(pipeline.book_store(), scored.as_ref());

    let result = pipeline.execute(scored).await;
    assert!(
        result.is_filled(),
        "expected live wiremock fill, got {result:?}"
    );
}

#[tokio::test]
async fn live_mode_wiremock_timeout() {
    const TOKEN: &str =
        "15871154585880608648532107628464183779895785213830018178010423617714102767076";
    const TIMEOUT_MS: u64 = 100;

    let server = MockServer::start().await;
    mount_live_clob_wiremock(&server, Some(Duration::from_millis(500))).await;
    let clob = live_clob_client(&server).await;
    let pipeline = live_execution_pipeline(clob, TIMEOUT_MS);

    let mut scored = sample_scored();
    Arc::make_mut(&mut scored).opportunity = Arc::new({
        let mut opp = (*scored.opportunity).clone();
        opp.token_id = TokenId::new(TOKEN);
        opp
    });
    seed_book_store(pipeline.book_store(), scored.as_ref());

    let result = pipeline.execute(scored).await;
    assert!(
        matches!(
            result.outcome_summary,
            Some(execution::ExecutionOutcomeSummary::Failed)
        ),
        "expected timeout failure, got {result:?}"
    );
}

#[tokio::test]
async fn paper_execution_misses_when_insufficient_depth() {
    let harness = build_pipeline();

    let now_ms = chrono::Utc::now()
        .timestamp_millis()
        .max(0)
        .to_u64()
        .unwrap_or(0);
    let thin_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(5)),
    )];
    harness
        .book_store
        .apply_snapshot(&harness.scored.token_yes, vec![], thin_asks, now_ms, None);

    let result = harness.pipeline.execute(harness.scored).await;
    assert!(
        result.is_miss(),
        "expected paper miss on thin book, got {result:?}"
    );
}

#[tokio::test]
async fn fill_enqueues_post_trade_with_independent_trade_id() {
    let harness = build_pipeline();
    let opp_id = harness.scored.opportunity.opportunity_id.clone();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = harness
        .outcome_rx
        .try_recv()
        .expect("post-trade job should be enqueued");
    assert!(matches!(job.outcome, ExecutionOutcome::Filled { .. }));
    assert_ne!(
        job.trade_id.as_str(),
        opp_id.as_str(),
        "trade_id must be independent from opportunity_id"
    );
    assert_eq!(
        harness
            .persistence
            .trade_repo
            .find(&job.trade_id)
            .unwrap()
            .outcome,
        TradeOutcome::Pending
    );
}

#[tokio::test]
async fn trade_insert_fail_closed_aborts_dispatch() {
    let harness = build_pipeline();
    harness.persistence.trade_repo.fail_create();
    let before = harness.persistence.trade_repo.trade_count();
    let result = harness.pipeline.execute(harness.scored).await;
    assert!(result.is_rejected());
    assert_eq!(result.rejection_stage.as_deref(), Some("trade_persist"));
    assert_eq!(harness.persistence.trade_repo.trade_count(), before);
    assert!(harness.outcome_rx.is_empty());
}

#[tokio::test]
async fn post_trade_drain_updates_trade_and_writes_ch_audit() {
    let harness = build_pipeline();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = harness.outcome_rx.recv_async().await.expect("job");
    let trade_id = job.trade_id.clone();

    let shutdown = CancellationToken::new();
    let (outcome_tx, outcome_rx) = flume::bounded(1024);
    outcome_tx.try_send(job).expect("send job");

    let drain = tokio::spawn({
        let risk_engine = Arc::clone(&harness.risk_engine);
        let risk_metrics = Arc::clone(&harness.risk_metrics);
        let fsm = Arc::clone(&harness.fsm);
        let trade_repo = Arc::clone(&harness.persistence.trade_repo);
        let position_repo: Arc<dyn PositionRepository> = harness.persistence.position_repo.clone();
        let audit_writer = Arc::clone(&harness.persistence.audit_writer);
        let alerts = Arc::clone(&harness.persistence.alerts);
        let spill = harness.pipeline.post_trade_spill().clone();
        let shutdown = shutdown.clone();
        let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
            Duration::from_secs(60),
        ))));
        metrics_state.seed_simulated_snapshot(ExecutionMode::Paper, Usd::new(dec!(5000)));
        async move {
            spawn_test_outcome_drain(
                outcome_rx,
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

    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    drain.await.expect("drain task");

    let trade = harness
        .persistence
        .trade_repo
        .find(&trade_id)
        .expect("trade");
    assert_eq!(trade.outcome, TradeOutcome::Success);
    assert_eq!(harness.persistence.audit_rows.lock().unwrap().len(), 1);
}

async fn mount_live_clob_wiremock(server: &wiremock::MockServer, order_delay: Option<Duration>) {
    Mock::given(method("GET"))
        .and(path("/auth/derive-api-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apiKey": "00000000-0000-0000-0000-000000000000",
            "passphrase": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "secret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "version": 2 })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/neg-risk"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "neg_risk": false })),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/fee-rate"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "base_fee": 0 })),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tick-size"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "minimum_tick_size": "0.01" })),
        )
        .mount(server)
        .await;
    let mut order_response = ResponseTemplate::new(200).set_body_string(
        r#"{
                "success": true,
                "orderID": "0xfill",
                "status": "matched",
                "makingAmount": "100",
                "takingAmount": "92"
            }"#,
    );
    if let Some(delay) = order_delay {
        order_response = order_response.set_delay(delay);
    }
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(order_response)
        .mount(server)
        .await;
}

async fn live_clob_client(server: &wiremock::MockServer) -> Arc<ClobClient> {
    const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    let local = LocalSigner::from_str(PRIVATE_KEY)
        .expect("signer")
        .with_chain_id(Some(POLYGON));
    let sdk = SdkClient::new(
        &server.uri(),
        SdkConfig::builder().use_server_time(true).build(),
    )
    .expect("sdk")
    .authentication_builder(&local)
    .authenticate()
    .await
    .expect("auth");
    let key_bytes = hex::decode(PRIVATE_KEY.trim_start_matches("0x")).expect("hex");
    let order_signer = Arc::new(
        OrderSigner::from_bytes(&key_bytes)
            .expect("order signer")
            .with_chain_id(Some(POLYGON)),
    );
    Arc::new(ClobClient::from_sdk_for_test(Arc::new(sdk), order_signer))
}

fn live_execution_pipeline(
    clob: Arc<ClobClient>,
    dispatcher_timeout_ms: u64,
) -> LivePipelineHarness {
    let shutdown = CancellationToken::new();
    let persistence = test_persistence(shutdown);
    let (outcome_tx, _rx) = flume::bounded(1024);
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        1,
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let reservation_config = test_risk_config().exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation_config.clone()));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        reservation_config,
    ));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    metrics_state.seed_simulated_snapshot(ExecutionMode::Paper, Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        metrics_state,
        exposure,
        ws_manager,
        ExecutionMode::Paper,
    ));

    let fee_calculator = Arc::new(FeeCalculator::default());
    let pipeline = ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(&book_store),
            StalenessClassifier::new(&MarketDataConfig::default()),
            dec!(50),
            5_000,
            Arc::clone(&metrics),
        ),
        plan_builder: PlanBuilder::new(
            Arc::clone(&fee_calculator),
            Arc::new(MarketRegistry::new()),
        ),
        dispatcher: Dispatcher::new(
            ExecutionMode::Live,
            None,
            Arc::clone(&fee_calculator),
            Arc::clone(&metrics),
        ),
        order_strategy: FokOrderStrategy::new(
            ExecutionMode::Live,
            Some(clob),
            fee_calculator,
            dispatcher_timeout_ms,
            Arc::clone(&metrics),
        ),
        capital_manager: capital,
        risk_engine,
        risk_metrics,
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics: Arc::clone(&metrics),
        execution_mode: ExecutionMode::Live,
        trade_repo: Arc::clone(&persistence.trade_repo),
        audit_writer: Arc::clone(&persistence.audit_writer),
        event_store: Arc::new(InMemoryEventStore::new()),
        outcome_tx,
        backpressure,
    });

    LivePipelineHarness {
        pipeline,
        book_store,
        _persistence: persistence,
    }
}

struct LivePipelineHarness {
    pipeline: ExecutionPipeline<MockTradeRepository>,
    book_store: Arc<BookStore>,
    _persistence: TestPersistence,
}

impl LivePipelineHarness {
    const fn book_store(&self) -> &Arc<BookStore> {
        &self.book_store
    }

    async fn execute(&self, scored: Arc<ScoredOpportunity>) -> execution::ExecutionResult {
        self.pipeline.execute(scored).await
    }
}
