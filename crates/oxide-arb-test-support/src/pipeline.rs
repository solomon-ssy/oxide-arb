//! Shared execution pipeline test harness with in-memory persistence mocks.

use crate::mocks::MockTradeRepository;
use crate::{
    book::seed_book_store,
    fixtures::sample_scored,
    persistence::{TestPersistence, test_persistence},
    risk::{TestRiskMetrics, test_risk_config},
};
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::{fees::FeeCalculator, ws::ClobWsManager};
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        tiered_strategy::OrderStrategy,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::{backpressure::BackpressurePolicy, metrics_hub::MetricsHub},
    outbox::in_memory::InMemoryEventStore,
    pipeline::{
        book_store::BookStore, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    config::{ExposureReservationConfig, MarketDataConfig, PolymarketConfig, WebSocketConfig},
    domain::execution::PostTradeJob,
    enums::common::ExecutionMode,
    types::Usd,
};
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct PipelineHarness {
    pub pipeline: ExecutionPipeline<MockTradeRepository>,
    pub persistence: TestPersistence,
    pub fsm: Arc<ExecutionFSM>,
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub metrics: Arc<MetricsHub>,
    pub scored: Arc<ScoredOpportunity>,
    pub book_store: Arc<BookStore>,
    pub outcome_rx: flume::Receiver<PostTradeJob>,
}

pub fn build_pipeline() -> PipelineHarness {
    let shutdown = CancellationToken::new();
    let persistence = test_persistence(shutdown.clone());
    let (outcome_tx, outcome_rx) = flume::bounded(1024);
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::new(InMemoryEventStore::new()),
        1,
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
        shutdown,
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(metrics_state, exposure, ws_manager));

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
            ExecutionMode::Paper,
            Some(Arc::clone(&book_store)),
            Arc::clone(&fee_calculator),
            Arc::clone(&metrics),
        ),
        order_strategy: OrderStrategy::new(
            ExecutionMode::Paper,
            None,
            fee_calculator,
            30_000,
            Arc::clone(&metrics),
        ),
        capital_manager: capital,
        risk_engine: Arc::clone(&risk_engine),
        risk_metrics: Arc::clone(&risk_metrics),
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics: Arc::clone(&metrics),
        execution_mode: ExecutionMode::Paper,
        trade_repo: Arc::clone(&persistence.trade_repo),
        audit_writer: Arc::clone(&persistence.audit_writer),
        outcome_tx,
        backpressure,
    });

    let scored = sample_scored();
    seed_book_store(&book_store, scored.as_ref());

    PipelineHarness {
        pipeline,
        persistence,
        fsm,
        risk_engine,
        risk_metrics,
        metrics,
        scored,
        book_store,
        outcome_rx,
    }
}
