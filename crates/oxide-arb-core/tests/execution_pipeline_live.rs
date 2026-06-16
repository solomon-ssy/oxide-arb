//! Live-mode execution pipeline tests backed by wiremock CLOB.

#[path = "common/mod.rs"]
mod common;

use alloy::signers::{Signer as _, local::LocalSigner};
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::{
    clob::ClobClient, fees::FeeCalculator, infra::retry::RetryPolicy, keystore::OrderSigner,
    ws::ClobWsManager,
};
use oxide_arb_core::{
    bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics},
    control::factor_snapshot::FactorSnapshotStore,
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::async_writer::{AsyncWriter, AsyncWriterConfig},
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
    post_trade::consumer::PostTradeConsumer,
    runtime_config::RuntimeConfigStore,
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::runtime_config::{NotificationConfig, RuntimeConfig};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    config::{PolymarketConfig, WebSocketConfig},
    domain::{
        CoreEventPublisher, PositionInfo, TradeInfo,
        book::BookLevel,
        calibration::{BucketKey, CalibrationSnapshot},
        latency::LatencyTrace,
        market::{MarketRegistryInfo, TokenInfo},
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{
            CategorySet, ExecutionMode, MarketCategory, Side, StalenessLevel, TickSize,
            TradeBusinessOutcome, TradeState,
        },
        execution::ExecutionOutcomeSummary,
        market::MarketStatus,
        opportunity::PayoutModel,
    },
    runtime_config::{ExecutionRuntimeConfig, MarketDataRuntimeConfig, RiskConfig},
    types::{
        Bps, EventId, MarketId, MicroProb, MicroScore, OpportunityId, Price, Shares, TokenId, Usd,
    },
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskMetrics,
};
use oxide_arb_test_support::mocks::{
    MockCalibrationRepository, MockPositionRepository, MockTradeRepository,
};
use polymarket_client_sdk_v2::{
    POLYGON,
    clob::{Client as SdkClient, Config as SdkConfig},
};
use rust_decimal_macros::dec;
use std::{str::FromStr as _, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const API_KEY: &str = "00000000-0000-0000-0000-000000000000";
const PASSPHRASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

struct LiveFixture {
    pipeline: ExecutionPipeline<MockTradeRepository>,
    scored: Arc<ScoredOpportunity>,
    trade_repo: Arc<MockTradeRepository>,
    position_repo: Arc<MockPositionRepository>,
    calibration_repo: Arc<MockCalibrationRepository>,
    market_registry: Arc<MarketRegistry>,
    exposure: Arc<InMemoryExposureReservation>,
    capital_manager: Arc<CapitalManager>,
    fsm: Arc<ExecutionFSM>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    audit_writer: Arc<ExecutionAuditWriter>,
    metrics_state: Arc<RiskMetricsState>,
    metrics: Arc<MetricsHub>,
}

fn token_yes() -> TokenId {
    TokenId::new("15871154585880608648532107628464183779895785213830018178010423617714102767076")
}

fn token_no() -> TokenId {
    TokenId::new("25871154585880608648532107628464183779895785213830018178010423617714102767076")
}

async fn mount_derive_api_key(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/auth/derive-api-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apiKey": API_KEY,
            "passphrase": PASSPHRASE,
            "secret": SECRET
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
        .mount(server)
        .await;
}

async fn mount_clob_requirements(server: &MockServer) {
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
        .and(query_param("token_id", token_yes().as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "minimum_tick_size": "0.01" })),
        )
        .mount(server)
        .await;
}

async fn mount_post_order(server: &MockServer, body: &str) {
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(server)
        .await;
}

async fn test_clob_client(server: &MockServer) -> Arc<ClobClient> {
    let signer = LocalSigner::from_str(PRIVATE_KEY)
        .expect("local signer")
        .with_chain_id(Some(POLYGON));
    let mut sdk = SdkClient::new(&server.uri(), SdkConfig::builder().build())
        .expect("sdk client")
        .authentication_builder(&signer)
        .authenticate()
        .await
        .expect("authenticate");
    sdk.stop_heartbeats()
        .await
        .expect("stop test heartbeat task");

    let bytes = hex::decode(PRIVATE_KEY.trim_start_matches("0x")).expect("test key hex");
    let order_signer = Arc::new(
        OrderSigner::from_bytes(&bytes)
            .expect("order signer")
            .with_chain_id(Some(POLYGON)),
    );
    let retry_policy = RetryPolicy {
        max_attempts: Some(2),
        initial_interval_ms: 1,
        max_interval_ms: 1,
        randomization_factor: 0.0,
        multiplier: 1.0,
        max_elapsed_time_ms: None,
    };
    Arc::new(
        ClobClient::from_sdk_for_test(Arc::new(sdk), order_signer)
            .with_order_retry_policy(retry_policy),
    )
}

fn live_pipeline_market_registry() -> Arc<MarketRegistry> {
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(MarketRegistryInfo {
        market_id: MarketId::new("0xlive-pipeline-market"),
        event_id: EventId::new("evt-live-pipeline"),
        token_yes: token_yes(),
        token_no: token_no(),
        question: "Live pipeline test?".into(),
        slug: "live-pipeline".into(),
        categories: CategorySet::from(MarketCategory::Politics),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: token_yes(),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: token_no(),
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

fn scored_opportunity() -> Arc<ScoredOpportunity> {
    Arc::new(ScoredOpportunity {
        opportunity: Arc::new(Opportunity {
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("0xlive-pipeline-market"),
            event_id: EventId::new("evt-live-pipeline"),
            token_id: token_yes(),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::new(dec!(100)),
                expected_payout: Usd::new(dec!(95)),
                predicted_side: Side::Buy,
            },
            shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.92)),
            total_cost: Usd::new(dec!(20)),
            total_fees: Usd::new(dec!(0.40)),
            net_profit: Usd::new(dec!(5)),
            expected_net_profit: Usd::new(dec!(4.5)),
            edge_bps: Bps::new(dec!(300)),
            resolution_adjust: dec!(0.95),
            depth_used_pct: dec!(10),
            staleness: StalenessLevel::Fresh,
            category: MarketCategory::Politics,
            meta: EndgameMeta {
                predicted_yes: true,
                confidence: dec!(0.95),
                convergence_duration_secs: 600,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
                settlement_deadline: None,
            },
            calibration: CalibrationSnapshot {
                bucket_key: BucketKey {
                    category: MarketCategory::Politics,
                    price_zone: PriceZone::Z97,
                    duration_bucket: DurationBucket::Medium,
                },
                posterior_mean: dec!(0.93),
                sample_size: 50,
                alpha_prior: dec!(2.0),
                beta_prior: dec!(1.0),
                fallback_tier: 1,
                fused_probability: dec!(0.99),
            },
            detected_at: Utc::now(),
        }),
        token_yes: token_yes(),
        token_no: token_no(),
        score: MicroScore::try_from_decimal(dec!(0.9)).expect("score"),
        fill_probability: MicroProb::ONE,
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        book_yes_version: 1,
        book_no_version: 1,
        applied_factors: Arc::from([]),
        trace: Arc::new(LatencyTrace::default()),
    })
}

fn seed_books(store: &BookStore, scored: &ScoredOpportunity) {
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.07)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.08)),
        Shares::new(dec!(1000)),
    )];
    let now_ms = u64::try_from(Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    store.apply_snapshot(&scored.token_yes, vec![], yes_asks, now_ms, None);
    store.apply_snapshot(&scored.token_no, no_bids, no_asks, now_ms, None);
}

fn risk_engine() -> Arc<RiskEngine> {
    Arc::new(
        RiskEngineBuilder::new()
            .config(RiskConfig {
                max_total_exposure_usd: dec!(5000),
                max_single_market_exposure_usd: dec!(500),
                max_single_bet_usd: dec!(25),
                max_open_positions: 5,
                max_daily_loss_usd: dec!(75),
                max_weekly_loss_usd: dec!(120),
                daily_budget_usd: dec!(200),
                min_balance_usd: dec!(50),
                reserve_balance_usd: dec!(100),
                min_trade_usd: dec!(1),
                max_consecutive_misses: 3,
                ..RiskConfig::default()
            })
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
        AsyncWriterConfig::new("test-execution-audit")
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
        ExecutionModeHandle::new(ExecutionMode::Live),
    ))
}

fn fixture(clob_client: Option<Arc<ClobClient>>, dispatcher_timeout_ms: u64) -> LiveFixture {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics), alerts));
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let reservation_config = RiskConfig::default().exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation_config.clone()));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        &reservation_config,
    ));
    let scored = scored_opportunity();
    seed_books(&book_store, &scored);
    let trade_repo = Arc::new(MockTradeRepository::default());
    let position_repo = Arc::new(MockPositionRepository::default());
    let calibration_repo = Arc::new(MockCalibrationRepository::default());
    let fee_calculator = Arc::new(FeeCalculator::default());
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    let risk_metrics = risk_metrics(Arc::clone(&exposure), Arc::clone(&metrics_state));
    let audit_writer = audit_writer(Arc::clone(&metrics));
    let risk_engine = risk_engine();
    let market_registry = live_pipeline_market_registry();
    let pipeline = ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Arc::new(Validator::new(
            Arc::clone(&book_store),
            StalenessClassifier::new(&MarketDataRuntimeConfig::default()),
            &ExecutionRuntimeConfig {
                endgame_latency: oxide_arb_models::runtime_config::EndgameLatencyConfig {
                    max_book_to_order_ms: 5_000,
                    ..Default::default()
                },
                ..Default::default()
            },
            Arc::clone(&metrics),
        )),
        plan_builder: PlanBuilder::new(Arc::clone(&fee_calculator), Arc::clone(&market_registry)),
        dispatcher: Dispatcher::new(
            ExecutionModeHandle::new(ExecutionMode::Live),
            Arc::clone(&book_store),
            Arc::clone(&fee_calculator),
            Arc::clone(&metrics),
        ),
        order_strategy: Arc::new(FokOrderStrategy::new(
            ExecutionModeHandle::new(ExecutionMode::Live),
            clob_client,
            fee_calculator,
            dispatcher_timeout_ms,
            Arc::clone(&metrics),
        )),
        capital_manager: Arc::clone(&capital),
        risk_engine: Arc::clone(&risk_engine),
        risk_metrics: Arc::clone(&risk_metrics),
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics: Arc::clone(&metrics),
        mode: ExecutionModeHandle::new(ExecutionMode::Live),
        trade_repo: Arc::clone(&trade_repo),
        audit_writer: Arc::clone(&audit_writer),
        relay_notify: Arc::new(Notify::new()),
        reconcile_notify: Arc::new(Notify::new()),
        metrics_state: Arc::clone(&metrics_state),
        runtime_config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        factors: Arc::new(FactorSnapshotStore::new(chrono::Utc::now())),
        shadow_writer: None,
    });

    LiveFixture {
        pipeline,
        scored,
        trade_repo,
        position_repo,
        calibration_repo,
        market_registry,
        exposure,
        capital_manager: capital,
        fsm,
        risk_engine,
        risk_metrics,
        audit_writer,
        metrics_state,
        metrics,
    }
}

fn only_trade(repo: &MockTradeRepository) -> TradeInfo {
    let trades = repo.trades_snapshot();
    assert_eq!(trades.len(), 1);
    trades.into_iter().next().expect("one trade")
}

#[tokio::test]
async fn live_pipeline_fill_observed_then_consumer_settles() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xfill",
            "status": "matched",
            "makingAmount": "100",
            "takingAmount": "92",
            "transactionHashes": ["0x0000000000000000000000000000000000000000000000000000000000000001"]
        }"#,
    )
    .await;

    let fixture = fixture(Some(test_clob_client(&server).await), 30_000);
    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert!(
        result.rejection_stage.is_none(),
        "unexpected rejection: {result:?}"
    );

    let observed = only_trade(&fixture.trade_repo);
    assert_eq!(observed.state, TradeState::FillObserved);
    assert_eq!(
        observed.business_outcome,
        Some(TradeBusinessOutcome::Success)
    );
    assert_eq!(
        fixture.exposure.active_count_sync(),
        1,
        "fill reservation must remain until position is durably created"
    );
    let duplicate = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert_eq!(
        duplicate.rejection_stage.as_deref(),
        Some("risk"),
        "same-market execution must be blocked while filled exposure is pending"
    );

    let metrics_refresh = common::disconnected_metrics_refresh(
        Arc::clone(&fixture.metrics_state),
        ExecutionMode::Live,
        Arc::clone(&fixture.metrics),
    );
    let consumer = PostTradeConsumer {
        risk_engine: fixture.risk_engine,
        risk_metrics: fixture.risk_metrics,
        fsm: fixture.fsm,
        capital_manager: fixture.capital_manager,
        trade_repo: fixture.trade_repo.clone(),
        position_repo: fixture.position_repo,
        calibration_repo: fixture.calibration_repo,
        audit_writer: fixture.audit_writer,
        metrics_state: fixture.metrics_state,
        metrics_refresh,
        metrics: fixture.metrics,
        events: CoreEventPublisher::bounded(1).0,
        market_registry: Arc::clone(&fixture.market_registry),
        runtime_config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
    };
    consumer.process(&observed).await;

    let terminal = only_trade(&fixture.trade_repo);
    assert_eq!(terminal.state, TradeState::Settled);
    assert_eq!(
        terminal.business_outcome,
        Some(TradeBusinessOutcome::Success)
    );
    assert_eq!(
        fixture.exposure.active_count_sync(),
        0,
        "post-trade terminal fill releases the reservation after position creation"
    );
}

#[tokio::test]
async fn live_pipeline_miss_releases_reservation() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xmiss",
            "status": "live",
            "makingAmount": "0",
            "takingAmount": "0"
        }"#,
    )
    .await;

    let fixture = fixture(Some(test_clob_client(&server).await), 30_000);
    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert!(
        result.rejection_stage.is_none(),
        "unexpected rejection: {result:?}"
    );
    assert!(matches!(
        result.outcome_summary,
        Some(ExecutionOutcomeSummary::Miss)
    ));
    assert_eq!(fixture.exposure.active_count_sync(), 0);

    let trade = only_trade(&fixture.trade_repo);
    assert_eq!(trade.state, TradeState::MissObserved);
    assert_eq!(trade.business_outcome, Some(TradeBusinessOutcome::Miss));
}

#[tokio::test]
async fn live_pipeline_clob_failure_records_fail_observed() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server).await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .expect(1)
        .mount(&server)
        .await;

    let fixture = fixture(Some(test_clob_client(&server).await), 30_000);
    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert!(
        result.rejection_stage.is_none(),
        "unexpected rejection: {result:?}"
    );
    assert_eq!(fixture.exposure.active_count_sync(), 0);

    let trade = only_trade(&fixture.trade_repo);
    assert_eq!(trade.state, TradeState::FailObserved);
    assert_eq!(trade.business_outcome, Some(TradeBusinessOutcome::Failed));
}

#[tokio::test]
async fn live_pipeline_timeout_marks_needs_reconcile_without_releasing_reservation() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server).await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_string(
                    r#"{
                        "success": true,
                        "orderID": "0xtimeout",
                        "status": "matched",
                        "makingAmount": "100",
                        "takingAmount": "92"
                    }"#,
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let fixture = fixture(Some(test_clob_client(&server).await), 1);
    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert!(
        result.rejection_stage.is_none(),
        "unexpected rejection: {result:?}"
    );
    assert!(matches!(
        result.outcome_summary,
        Some(ExecutionOutcomeSummary::Unknown)
    ));
    assert_eq!(
        fixture.exposure.active_count_sync(),
        1,
        "unknown venue outcome must keep capital reserved"
    );

    let trade = only_trade(&fixture.trade_repo);
    assert_eq!(trade.state, TradeState::Orphaned);
    assert!(trade.needs_reconcile);
}

#[tokio::test]
async fn live_pipeline_mark_submitted_failure_releases_and_enters_emergency() {
    let fixture = fixture(None, 30_000);
    fixture.trade_repo.fail_mark_submitted();

    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert_eq!(result.rejection_stage.as_deref(), Some("submit_persist"));
    assert_eq!(fixture.exposure.active_count_sync(), 0);
    assert!(fixture.fsm.is_emergency());
}

#[tokio::test]
async fn live_pipeline_mark_observed_failure_enters_emergency() {
    let server = MockServer::start().await;
    mount_derive_api_key(&server).await;
    mount_clob_requirements(&server).await;
    mount_post_order(
        &server,
        r#"{
            "success": true,
            "orderID": "0xfill",
            "status": "matched",
            "makingAmount": "100",
            "takingAmount": "92"
        }"#,
    )
    .await;

    let fixture = fixture(Some(test_clob_client(&server).await), 30_000);
    fixture.trade_repo.fail_mark_observed();
    let result = fixture.pipeline.execute(Arc::clone(&fixture.scored)).await;
    assert!(
        result.rejection_stage.is_none(),
        "unexpected rejection: {result:?}"
    );
    assert!(fixture.fsm.is_emergency());
    assert_eq!(
        fixture.exposure.active_count_sync(),
        1,
        "mark_observed failure after a fill must preserve reserved exposure"
    );

    let trade = only_trade(&fixture.trade_repo);
    assert_eq!(trade.state, TradeState::Submitted);
    assert_eq!(trade.business_outcome, None);
}
