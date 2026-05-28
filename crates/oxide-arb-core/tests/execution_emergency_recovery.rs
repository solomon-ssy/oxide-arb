//! Emergency halt, recovery, and spill drain ordering under stress.

use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_api::{fees::FeeCalculator, ws::ClobWsManager};
use oxide_arb_core::{
    bridge::{
        risk_metrics::CoreRiskMetrics,
        trading_gate::{halt_trading, resume_trading},
    },
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
    config::{ExposureReservationConfig, MarketDataConfig, PolymarketConfig, WebSocketConfig},
    domain::execution::PostTradeJob,
    enums::common::ExecutionMode,
    types::Usd,
};
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::mocks::MockTradeRepository;
use oxide_arb_test_support::{
    book::seed_book_store,
    fixtures::{minimal_post_trade_job, sample_scored},
    persistence::test_persistence,
    risk::{TestRiskMetrics, test_risk_config},
};
use rust_decimal_macros::dec;
use std::{hint::spin_loop, sync::Arc, thread::spawn, time::Duration};
use tokio_util::sync::CancellationToken;

type EmergencyPipelineTuple = (
    ExecutionPipeline<MockTradeRepository>,
    Arc<ExecutionFSM>,
    Arc<RiskEngine>,
    Arc<ScoredOpportunity>,
    Arc<InMemoryExposureReservation>,
);

fn build_pipeline() -> EmergencyPipelineTuple {
    let shutdown = CancellationToken::new();
    let persistence = test_persistence(shutdown);
    let (outcome_tx, _rx) = flume::bounded(1024);
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
        Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        metrics_state,
        Arc::clone(&exposure),
        ws_manager,
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
        risk_metrics,
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

    (pipeline, fsm, risk_engine, scored, exposure)
}

fn spill_job(id: &str) -> PostTradeJob {
    minimal_post_trade_job(id)
}

#[tokio::test]
async fn emergency_rejects_execute_until_resume() {
    let (pipeline, fsm, risk_engine, scored, _exposure) = build_pipeline();

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
    let (pipeline, fsm, _risk_engine, scored, exposure) = build_pipeline();

    let exposure_steal = Arc::clone(&exposure);
    let steal_thread = spawn(move || {
        for _ in 0..1_000_000 {
            if let Some(id) = exposure_steal.test_snapshot_active_ids().into_iter().next() {
                let _ = exposure_steal.confirm_sync(&id);
                return;
            }
            spin_loop();
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

#[tokio::test]
async fn emergency_auto_recovery_when_risk_allows() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );

    fsm.enter_emergency("reservation confirm failed");
    assert!(fsm.is_emergency());
    assert!(risk_engine.allows_trading());

    assert!(fsm.try_auto_recover(&risk_engine));
    assert!(!fsm.is_emergency());
}

#[tokio::test]
async fn emergency_auto_recovery_skips_when_risk_blocked() {
    let metrics = Arc::new(MetricsHub::new());
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .config(test_risk_config())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );

    fsm.enter_emergency("post-trade persist failed");
    risk_engine.halt("operator manual halt".into()).await;
    assert!(!risk_engine.allows_trading());

    assert!(!fsm.try_auto_recover(&risk_engine));
    assert!(fsm.is_emergency());
}
