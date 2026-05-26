//! Execution pipeline integration tests.

mod support;

use std::sync::Arc;
use std::time::Instant;

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
use oxide_arb_core::observability::drop_halt::DropHaltGuard;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
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
    EventId, ExecutionId, MarketId, OrderId, Price, ReservationId, Shares, TokenId, Usd,
};
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use rust_decimal_macros::dec;
use support::{TestRiskMetrics, sample_scored, seed_book_store, test_risk_config};
use tokio_util::sync::CancellationToken;

fn build_pipeline(
    outcome_tx: flume::Sender<PostTradeJob>,
) -> (
    ExecutionPipeline,
    Arc<ExecutionFSM>,
    Arc<MetricsHub>,
    ScoredOpportunity,
) {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
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
        drop_halt: None,
    });

    let scored = sample_scored();
    seed_book_store(&book_store, &scored);

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
    let opp = support::sample_opportunity();
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
    let result = pipeline.execute(scored).await;
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
    let result = pipeline.execute(scored).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = outcome_rx
        .try_recv()
        .expect("post-trade job should be enqueued");
    assert!(matches!(job.outcome, ExecutionOutcome::Filled { .. }));
}

#[test]
fn post_trade_drop_guard_halts_execution() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let guard = DropHaltGuard::new(metrics, Arc::clone(&fsm));
    guard.on_post_trade_drop();
    assert!(fsm.is_emergency());
}
