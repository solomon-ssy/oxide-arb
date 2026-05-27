//! Emergency halt, recovery, and spill drain ordering under stress.

#[path = "support/test_util/book_store_seed.rs"]
mod book_store_seed;
#[path = "support/test_util/risk_config.rs"]
mod risk_config;
#[path = "support/test_util/risk_metrics.rs"]
mod risk_metrics;
#[path = "support/test_util/scored_opportunity.rs"]
mod scored_opportunity;

use std::sync::Arc;

use book_store_seed::seed_book_store;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::bridge::risk_metrics::CoreRiskMetrics;
use oxide_arb_core::bridge::trading_gate::{halt_trading, resume_trading};
use oxide_arb_core::execution::capital_manager::CapitalManager;
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
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::enums::execution::ExecutionOutcome;
use oxide_arb_models::types::{MarketId, Price, TokenId, TradeId, Usd};
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use risk_config::test_risk_config;
use risk_metrics::TestRiskMetrics;
use rust_decimal_macros::dec;
use scored_opportunity::sample_scored;
use tokio_util::sync::CancellationToken;

fn build_pipeline(
    outcome_tx: flume::Sender<PostTradeJob>,
) -> (
    ExecutionPipeline,
    Arc<ExecutionFSM>,
    Arc<oxide_arb_risk::engine::RiskEngine>,
    Arc<ScoredOpportunity>,
    Arc<InMemoryExposureReservation>,
) {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::clone(&spill),
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
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        std::time::Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        metrics_state,
        Arc::clone(&exposure),
        ws_manager,
    ));

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
        risk_engine: Arc::clone(&risk_engine),
        risk_metrics,
        fsm: Arc::clone(&fsm),
        market_inflight: Arc::new(MarketInFlightRegistry::new()),
        metrics,
        execution_mode: ExecutionMode::Paper,
        outcome_tx,
        backpressure,
    });

    let scored = sample_scored();
    seed_book_store(&book_store, scored.as_ref());

    (pipeline, fsm, risk_engine, scored, exposure)
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

#[tokio::test]
async fn emergency_rejects_execute_until_resume() {
    let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();
    let (pipeline, fsm, risk_engine, scored, _exposure) = build_pipeline(outcome_tx);

    halt_trading(&risk_engine, &fsm, "reservation confirm failed".into()).await;
    assert!(pipeline.execute(Arc::clone(&scored)).await.is_rejected());

    resume_trading(&risk_engine, &fsm, "operator cleared halt")
        .await
        .expect("resume");

    let result = pipeline.execute(scored).await;
    assert!(
        result.is_filled(),
        "expected fill after resume, got {result:?}"
    );
}

#[tokio::test]
async fn spill_fifo_survives_emergency_drain() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let spill = Arc::new(InMemoryEventStore::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&metrics),
        None,
        Arc::clone(&spill),
        1,
    ));

    fsm.enter_emergency("test halt");
    for id in ["job-a", "job-b", "job-c"] {
        assert_eq!(
            backpressure.on_post_trade_channel_full(spill_job(id)),
            BackpressureAction::Spilled
        );
    }
    assert!(fsm.is_emergency());

    let drained = spill.drain_post_trade_jobs();
    assert_eq!(
        drained
            .iter()
            .map(|j| j.trade_id.as_str())
            .collect::<Vec<_>>(),
        vec!["job-a", "job-b", "job-c"]
    );
}

#[tokio::test]
async fn reservation_confirm_failure_enters_emergency() {
    let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();
    let (pipeline, fsm, _risk_engine, scored, exposure) = build_pipeline(outcome_tx);

    let exposure_steal = Arc::clone(&exposure);
    let steal_thread = std::thread::spawn(move || {
        for _ in 0..1_000_000 {
            if let Some(id) = exposure_steal.test_snapshot_active_ids().into_iter().next() {
                let _ = exposure_steal.confirm_sync(&id);
                return;
            }
            std::hint::spin_loop();
        }
    });

    let result = pipeline.execute(scored).await;
    steal_thread.join().expect("steal thread");

    assert!(
        result.outcome_summary.is_some(),
        "paper fill should complete before emergency halt, got {result:?}"
    );
    assert!(
        fsm.is_emergency(),
        "confirm failure during settle_reservation should enter emergency"
    );
}
