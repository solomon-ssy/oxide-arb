//! Execution pipeline integration tests.

#[path = "support/test_util/book_store_seed.rs"]
mod book_store_seed;
#[path = "support/test_util/risk_config.rs"]
mod risk_config;
#[path = "support/test_util/risk_metrics.rs"]
mod risk_metrics;
#[path = "support/test_util/scored_opportunity.rs"]
mod scored_opportunity;

use std::sync::Arc;
use std::time::Instant;

use book_store_seed::seed_book_store;
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::bridge::risk_metrics::CoreRiskMetrics;
use oxide_arb_core::execution::capital_manager::CapitalManager;
use oxide_arb_core::execution::clob_outcome::{filled_net_profit, map_order_response};
use oxide_arb_core::execution::dispatcher::Dispatcher;
use oxide_arb_core::execution::execution_pipeline::{
    ExecutionPipeline, ExecutionPipelineDeps, PostTradeJob,
};
use oxide_arb_core::execution::fsm::ExecutionFSM;
use oxide_arb_core::execution::market_inflight::MarketInFlightRegistry;
use oxide_arb_core::execution::plan_builder::PlanBuilder;
use oxide_arb_core::execution::tiered_strategy::OrderStrategy;
use oxide_arb_core::execution::validator::Validator;
use oxide_arb_core::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_core::observability::backpressure::{BackpressureAction, BackpressurePolicy};
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::outbox::in_memory::InMemoryEventStore;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::staleness_classifier::StalenessClassifier;
use oxide_arb_core::service::risk_metrics::{ApiHealthTracker, RiskMetricsState};
use oxide_arb_models::config::{
    ExposureReservationConfig, MarketDataConfig, PolymarketConfig, WebSocketConfig,
};
use oxide_arb_models::domain::execution::ExecutionPlan;
use oxide_arb_models::domain::order::OrderResponse;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::enums::execution::ExecutionOutcome;
use oxide_arb_models::enums::order::OrderStatus;
use oxide_arb_models::types::{
    EventId, ExecutionId, MarketId, OrderId, Price, ReservationId, Shares, TokenId, TradeId, Usd,
};
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use risk_config::test_risk_config;
use risk_metrics::TestRiskMetrics;
use rust_decimal_macros::dec;
use scored_opportunity::{sample_opportunity, sample_scored};
use tokio_util::sync::CancellationToken;

fn build_pipeline(
    outcome_tx: flume::Sender<PostTradeJob>,
) -> (
    ExecutionPipeline,
    Arc<ExecutionFSM>,
    Arc<MetricsHub>,
    Arc<ScoredOpportunity>,
) {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        ExposureReservationConfig::default(),
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
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        std::time::Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(metrics_state, exposure, ws_manager));

    let pipeline = ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(&book_store),
            StalenessClassifier::new(&MarketDataConfig::default()),
            dec!(50),
            5_000,
            Arc::clone(&metrics),
        ),
        plan_builder: PlanBuilder::new(Arc::new(FeeCalculator::default())),
        dispatcher: Dispatcher::new(ExecutionMode::Paper, Arc::clone(&metrics)),
        order_strategy: OrderStrategy::new(ExecutionMode::Paper, None, Arc::clone(&metrics)),
        capital_manager: capital,
        risk_engine,
        risk_metrics,
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics: Arc::clone(&metrics),
        execution_mode: ExecutionMode::Paper,
        outcome_tx,
        backpressure,
    });

    let scored = sample_scored();
    seed_book_store(&book_store, scored.as_ref());

    (pipeline, fsm, metrics, scored)
}

#[test]
fn clob_outcome_maps_partial_fill() {
    let plan = ExecutionPlan {
        execution_id: ExecutionId::generate(),
        opportunity_id: oxide_arb_models::types::OpportunityId::new_v7(),
        market_id: MarketId::new("m1"),
        event_id: EventId::new("e1"),
        token_id: TokenId::new("t1"),
        side: oxide_arb_models::enums::common::Side::Buy,
        shares: Shares::new(dec!(100)),
        limit_price: Price::new(dec!(0.5)),
        estimated_cost: Usd::new(dec!(50)),
        estimated_fee: Usd::ZERO,
        neg_risk: false,
        reservation_id: ReservationId::new_id(),
        detected_at: Utc::now(),
        planned_at: Utc::now(),
    };

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
    );

    match outcome {
        ExecutionOutcome::Filled { filled_shares, .. } => {
            assert_eq!(filled_shares, Shares::new(dec!(40)));
        }
        other => panic!("expected filled partial, got {other:?}"),
    }
}

#[test]
fn filled_net_profit_scales_with_fill_ratio() {
    let opp = sample_opportunity();
    let scaled = filled_net_profit(&opp, Shares::new(dec!(50)), Shares::new(dec!(100)));
    assert_eq!(
        scaled,
        Usd::new(opp.expected_net_profit.inner() * dec!(0.5))
    );
}

#[tokio::test]
async fn paper_execution_fills_when_risk_and_books_pass() {
    let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();
    let (pipeline, _fsm, _metrics, scored) = build_pipeline(outcome_tx);
    let result = pipeline.execute(Arc::clone(&scored)).await;
    assert!(result.is_filled(), "expected paper fill, got {result:?}");
}

#[tokio::test]
async fn execution_rejects_when_fsm_emergency() {
    let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();
    let (pipeline, fsm, _metrics, scored) = build_pipeline(outcome_tx);
    fsm.enter_emergency("test halt");
    let result = pipeline.execute(scored).await;
    assert!(result.is_rejected());
    assert_eq!(result.rejection_stage.as_deref(), Some("halted"));
}

#[tokio::test]
async fn fill_enqueues_post_trade_job() {
    let (outcome_tx, outcome_rx) = ExecutionPipeline::outcome_channel();
    let (pipeline, _fsm, _metrics, scored) = build_pipeline(outcome_tx);
    let result = pipeline.execute(Arc::clone(&scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = outcome_rx
        .try_recv()
        .expect("post-trade job should be enqueued");
    assert!(matches!(job.outcome, ExecutionOutcome::Filled { .. }));
}

#[test]
fn post_trade_spill_does_not_halt() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let bp = BackpressurePolicy::new(Arc::clone(&metrics), None, Arc::clone(&spill));

    let job = PostTradeJob {
        trade_id: TradeId::new("trade-1"),
        market_id: MarketId::new("m1"),
        token_id: TokenId::new("t1"),
        entry_price: Price::new(dec!(0.5)),
        net_profit: Usd::new(dec!(1)),
        outcome: ExecutionOutcome::Miss {
            reason: "test".into(),
            execution_mode: ExecutionMode::Paper,
        },
    };

    assert_eq!(
        bp.on_post_trade_channel_full(job),
        BackpressureAction::Spilled
    );
    assert!(!fsm.is_emergency());
    assert_eq!(metrics.post_trade_spilled_total.get(), 1);
    assert_eq!(spill.pending_post_trade_count(), 1);
}

#[tokio::test]
async fn live_mode_wiremock_fill() {
    use wiremock::MockServer;

    const TOKEN: &str =
        "15871154585880608648532107628464183779895785213830018178010423617714102767076";

    let server = MockServer::start().await;
    mount_live_clob_wiremock(&server).await;
    let clob = live_clob_client(&server).await;
    let pipeline = live_execution_pipeline(clob);

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

async fn mount_live_clob_wiremock(server: &wiremock::MockServer) {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

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
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
                "success": true,
                "orderID": "0xfill",
                "status": "matched",
                "makingAmount": "100",
                "takingAmount": "92"
            }"#,
        ))
        .mount(server)
        .await;
}

async fn live_clob_client(server: &wiremock::MockServer) -> Arc<oxide_arb_api::clob::ClobClient> {
    use std::str::FromStr as _;

    use alloy::signers::Signer as _;
    use alloy::signers::local::LocalSigner;
    use oxide_arb_api::clob::ClobClient;
    use oxide_arb_api::keystore::OrderSigner;
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::clob::{Client as SdkClient, Config as SdkConfig};

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

fn live_execution_pipeline(clob: Arc<oxide_arb_api::clob::ClobClient>) -> LivePipelineHarness {
    use oxide_arb_core::execution::tiered_strategy::OrderStrategy;
    use oxide_arb_models::enums::common::ExecutionMode;
    let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
    ));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        ExposureReservationConfig::default(),
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
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        std::time::Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(metrics_state, exposure, ws_manager));

    let pipeline = ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(&book_store),
            StalenessClassifier::new(&MarketDataConfig::default()),
            dec!(50),
            5_000,
            Arc::clone(&metrics),
        ),
        plan_builder: PlanBuilder::new(Arc::new(FeeCalculator::default())),
        dispatcher: Dispatcher::new(ExecutionMode::Live, Arc::clone(&metrics)),
        order_strategy: OrderStrategy::new(ExecutionMode::Live, Some(clob), Arc::clone(&metrics)),
        capital_manager: capital,
        risk_engine,
        risk_metrics,
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics: Arc::clone(&metrics),
        execution_mode: ExecutionMode::Live,
        outcome_tx,
        backpressure,
    });

    LivePipelineHarness {
        pipeline,
        book_store,
    }
}

struct LivePipelineHarness {
    pipeline: ExecutionPipeline,
    book_store: Arc<BookStore>,
}

impl LivePipelineHarness {
    const fn book_store(&self) -> &Arc<BookStore> {
        &self.book_store
    }

    async fn execute(
        &self,
        scored: Arc<ScoredOpportunity>,
    ) -> oxide_arb_models::domain::execution::ExecutionResult {
        self.pipeline.execute(scored).await
    }
}
