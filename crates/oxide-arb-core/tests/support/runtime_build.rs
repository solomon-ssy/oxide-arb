//! Test-only runtime wiring — no Postgres/ClickHouse/Redis required.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use oxide_arb_algorithm::calibration::ResolutionCalibrator;
use oxide_arb_algorithm::cooldown::InMemoryEmissionCooldown;
use oxide_arb_algorithm::endgame::EndgameDetector;
use oxide_arb_algorithm::pipeline::OpportunityPipeline;
use oxide_arb_algorithm::scorer::EndgameScorer;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::{ExposureReservationConfig, Settings};
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::types::{MarketId, MicroScore, MicroUsd, TokenId, Usd};
use oxide_arb_risk::audit_sink::AuditSink;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_risk::traits::RiskPersistence;
use tokio_util::sync::CancellationToken;

use oxide_arb_core::bridge::CoreOpportunityPipeline;
use oxide_arb_core::bridge::fee_estimator::CoreFeeEstimator;
use oxide_arb_core::bridge::risk_audit_sink::new_audit_sink;
use oxide_arb_core::bridge::risk_metrics::CoreRiskMetrics;
use oxide_arb_core::detection::coalescer::Coalescer;
use oxide_arb_core::detection::funnel::Funnel;
use oxide_arb_core::detection::scanner::Scanner;
use oxide_arb_core::detection::scanner_task::{ScannerTask, ScannerTaskDeps};
use oxide_arb_core::execution::capital_manager::CapitalManager;
use oxide_arb_core::execution::dispatcher::Dispatcher;
use oxide_arb_core::execution::execution_pipeline::{
    ExecutionPipeline, ExecutionPipelineDeps, PostTradeJob,
};
use oxide_arb_core::execution::fsm::ExecutionFSM;
use oxide_arb_core::execution::market_inflight::MarketInFlightRegistry;
use oxide_arb_core::execution::plan_builder::PlanBuilder;
use oxide_arb_core::execution::port::ExecutionPort;
use oxide_arb_core::execution::runner::{ExecutionRunner, ExecutionRunnerPool};
use oxide_arb_core::execution::tiered_strategy::OrderStrategy;
use oxide_arb_core::execution::validator::Validator;
use oxide_arb_core::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_core::observability::alert_dispatcher::AlertDispatcher;
use oxide_arb_core::observability::backpressure::BackpressurePolicy;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::outbox::in_memory::InMemoryEventStore;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::data_pipeline::{DataPipeline, DataPipelineDeps};
use oxide_arb_core::pipeline::event_source::PipelineEventSource;
use oxide_arb_core::pipeline::market_cache::MarketCache;
use oxide_arb_core::pipeline::market_registry::MarketRegistry;
use oxide_arb_core::pipeline::staleness_classifier::StalenessClassifier;
use oxide_arb_core::service::risk_metrics::{ApiHealthTracker, RiskMetricsState};

use super::risk_config::test_risk_config;
use super::risk_metrics::TestRiskMetrics;

/// Injectable dependencies for [`assemble_test_runtime`].
pub struct TestBuildDeps {
    pub persistence: Arc<dyn RiskPersistence>,
    pub event_source: Arc<dyn PipelineEventSource>,
    pub clob: Option<Arc<ClobClient>>,
    pub execution_mode: ExecutionMode,
}

/// Wired runtime graph consumed by [`RuntimeHarness`](super::RuntimeHarness).
pub struct TestRuntime {
    pub metrics: Arc<MetricsHub>,
    pub fsm: Arc<ExecutionFSM>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub token_rx: flume::Receiver<TokenId>,
    pub market_rx_tap: flume::Receiver<MarketId>,
    pub post_trade_rx: flume::Receiver<PostTradeJob>,
    pub scanner_task: ScannerTask,
    pub execution_runners: Vec<ExecutionRunner>,
}

struct TestInfra {
    metrics: Arc<MetricsHub>,
    fsm: Arc<ExecutionFSM>,
    backpressure: Arc<BackpressurePolicy>,
    fee_calculator: Arc<FeeCalculator>,
    engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    exposure: Arc<InMemoryExposureReservation>,
}

fn build_test_infra(
    settings: &Settings,
    persistence: Arc<dyn RiskPersistence>,
    ws_manager: &Arc<ClobWsManager>,
) -> OxideResult<TestInfra> {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(None, None, None, 60));
    let (risk_decision_audit, _audit_rx) = new_audit_sink(4096);

    let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.polymarket.fees));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
    let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
    metrics_state.seed_test_snapshot(Usd::new(rust_decimal_macros::dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        Arc::clone(ws_manager),
    ));

    let audit_sink: Arc<dyn AuditSink> = risk_decision_audit;
    let engine: Arc<RiskEngine> = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .persistence(persistence)
            .initial_equity(Usd::new(rust_decimal_macros::dec!(5000)))
            .clock(utc_clock())
            .audit_sink(audit_sink)
            .build(&TestRiskMetrics)?,
    );

    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let post_trade_spill = Arc::new(InMemoryEventStore::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        Some(alerts),
        post_trade_spill,
        settings.execution.book_apply.shard_count,
    ));

    Ok(TestInfra {
        metrics,
        fsm,
        backpressure,
        fee_calculator,
        engine,
        risk_metrics,
        exposure,
    })
}

fn build_test_scanner(
    settings: &Settings,
    infra: &TestInfra,
    book_store: &Arc<BookStore>,
    market_cache: &Arc<MarketCache>,
) -> Arc<Scanner> {
    let calibrator = Arc::new(ResolutionCalibrator::empty(
        settings.detection.calibration.clone(),
    ));
    let detector = EndgameDetector::new(
        &settings.detection.endgame,
        &settings.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(Arc::clone(&infra.fee_calculator)),
    );
    let scorer = EndgameScorer::new(
        settings.detection.endgame.scorer.clone(),
        &settings.detection.endgame.fill_probability,
        settings.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&settings.detection.endgame.emission_cooldown);
    let min_profit = MicroUsd::try_from_decimal(settings.detection.min_profit_threshold_usd)
        .unwrap_or(MicroUsd::ZERO);
    let opportunity_pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        min_profit,
        &settings.detection.endgame.scorer,
    ));
    let staleness = StalenessClassifier::new(&settings.market_data);

    Arc::new(Scanner::new(
        opportunity_pipeline,
        Arc::clone(book_store),
        Arc::clone(market_cache),
        staleness,
        Arc::clone(&infra.metrics),
    ))
}

fn spawn_market_forwarder(
    coalescer_market_rx: flume::Receiver<MarketId>,
    scanner_market_tx: flume::Sender<MarketId>,
    market_tap_tx: flume::Sender<MarketId>,
) {
    tokio::spawn(async move {
        while let Ok(market_id) = coalescer_market_rx.recv_async().await {
            let _ = scanner_market_tx.send(market_id.clone());
            let _ = market_tap_tx.try_send(market_id);
        }
    });
}

struct TestExecutionGraph {
    pipeline: Arc<ExecutionPipeline>,
    market_inflight: Arc<MarketInFlightRegistry>,
    post_trade_rx: flume::Receiver<PostTradeJob>,
    funnel: Arc<Funnel>,
    execution_runners: Vec<ExecutionRunner>,
}

fn build_test_execution_graph(
    settings: &Settings,
    infra: &TestInfra,
    book_store: &Arc<BookStore>,
    mode: ExecutionMode,
    clob: Option<Arc<ClobClient>>,
    shutdown: &CancellationToken,
) -> TestExecutionGraph {
    let (outcome_tx, post_trade_rx) = ExecutionPipeline::outcome_channel();
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&infra.exposure),
        ExposureReservationConfig::default(),
    ));
    let market_inflight = Arc::new(MarketInFlightRegistry::new());
    let pipeline = Arc::new(ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(book_store),
            StalenessClassifier::new(&settings.market_data),
            settings.execution.timeout.max_validation_slippage_bps,
            settings.execution.endgame_latency.max_book_to_order_ms,
            Arc::clone(&infra.metrics),
        ),
        plan_builder: PlanBuilder::new(Arc::clone(&infra.fee_calculator)),
        dispatcher: Dispatcher::new(mode, Arc::clone(&infra.metrics)),
        order_strategy: OrderStrategy::new(mode, clob, Arc::clone(&infra.metrics)),
        capital_manager: capital,
        risk_engine: Arc::clone(&infra.engine),
        risk_metrics: Arc::clone(&infra.risk_metrics),
        fsm: Arc::clone(&infra.fsm),
        market_inflight: Arc::clone(&market_inflight),
        metrics: Arc::clone(&infra.metrics),
        execution_mode: mode,
        outcome_tx,
        backpressure: Arc::clone(&infra.backpressure),
    }));

    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline_port: Arc<dyn ExecutionPort> = pipeline.clone();
    let (runner_pool, execution_runners) = ExecutionRunnerPool::new(
        settings.execution.book_apply.shard_count,
        &pipeline_port,
        shutdown,
        &inflight,
        &infra.metrics,
    );
    let funnel = Arc::new(Funnel::with_backpressure(
        runner_pool.shard_senders().to_vec(),
        settings.execution.funnel.max_queue_size,
        Duration::from_millis(settings.execution.funnel.min_dispatch_interval_ms),
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&infra.backpressure)),
    ));

    TestExecutionGraph {
        pipeline,
        market_inflight,
        post_trade_rx,
        funnel,
        execution_runners,
    }
}

pub fn assemble_test_runtime(
    settings: &Arc<Settings>,
    deps: TestBuildDeps,
    shutdown: CancellationToken,
) -> OxideResult<TestRuntime> {
    let mode = deps.execution_mode;
    settings.ensure_valid_for_mode(mode)?;

    let ws_manager = Arc::new(ClobWsManager::new(
        &settings.polymarket,
        &settings.market_data.websocket,
        shutdown.clone(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();

    let infra = build_test_infra(settings, deps.persistence, &ws_manager)?;

    let book_store = Arc::new(BookStore::new(Arc::clone(&infra.metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&market_registry)));
    let scanner = build_test_scanner(settings, &infra, &book_store, &market_cache);

    let (token_tx, token_rx) = flume::bounded(8192);
    let (coalescer_market_tx, coalescer_market_rx) = flume::bounded::<MarketId>(512);
    let (scanner_market_tx, scanner_market_rx) = flume::bounded::<MarketId>(512);
    let (market_tap_tx, market_rx_tap) = flume::bounded::<MarketId>(512);
    spawn_market_forwarder(coalescer_market_rx, scanner_market_tx, market_tap_tx);

    let coalescer = Arc::new(Coalescer::new(
        Arc::clone(&market_registry),
        Duration::from_millis(settings.execution.coalescer.coalesce_window_ms),
        coalescer_market_tx,
        Arc::clone(&infra.metrics),
        shutdown.clone(),
    ));

    let execution =
        build_test_execution_graph(settings, &infra, &book_store, mode, deps.clob, &shutdown);

    let data_pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source: deps.event_source,
        book_store: Arc::clone(&book_store),
        market_registry: Arc::clone(&market_registry),
        coalescer_tx: token_tx,
        metrics: Arc::clone(&infra.metrics),
        backpressure: Arc::clone(&infra.backpressure),
        book_shard_count: settings.execution.book_apply.shard_count,
        book_channel_capacity: settings.execution.book_apply.channel_capacity,
        shutdown: shutdown.clone(),
    }));

    let scanner_task = ScannerTask::new(ScannerTaskDeps {
        rx: scanner_market_rx,
        scanner,
        market_cache: Arc::clone(&market_cache),
        funnel: Arc::clone(&execution.funnel),
        dispatch_immediate_threshold: MicroScore::try_from_decimal(
            settings
                .execution
                .endgame_latency
                .dispatch_immediate_threshold,
        )
        .unwrap_or(MicroScore::ZERO),
        shutdown,
        metrics: Arc::clone(&infra.metrics),
    });

    Ok(TestRuntime {
        metrics: infra.metrics,
        fsm: infra.fsm,
        book_store,
        market_registry,
        market_cache,
        data_pipeline,
        coalescer,
        funnel: execution.funnel,
        pipeline: execution.pipeline,
        market_inflight: execution.market_inflight,
        execution_runners: execution.execution_runners,
        token_rx,
        market_rx_tap,
        post_trade_rx: execution.post_trade_rx,
        scanner_task,
    })
}
