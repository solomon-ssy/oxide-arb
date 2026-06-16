//! End-to-end runtime-config activation: `RuntimeConfigApplicator::apply`
//! propagates one config snapshot to every live subscriber.
//!
//! Builds the full subscriber set (risk engine, exposure/capital, detection
//! pipeline, calibration updater, staleness, validator, FOK strategy,
//! coalescer, funnel, settlement, oracle, alerts) without any network or
//! database I/O, applies a candidate config, and asserts the change is
//! observable on each component — including the simulated-bankroll rebase and
//! the fail-closed exposure preflight that must leave all state untouched.

#[path = "common/mod.rs"]
mod common;

use async_trait::async_trait;
use oxide_arb_algorithm::{
    calibration::{
        CalibrationDataSource, CalibrationUpdater, ResolutionCalibrator, UnresolvedOutcome,
    },
    cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector,
    pipeline::OpportunityPipeline,
    scorer::EndgameScorer,
};
use oxide_arb_api::{VotingOracle, fees::FeeCalculator, ws::ClobWsManager};
use oxide_arb_core::{
    bridge::{
        CoreOpportunityPipeline, execution_mode::ExecutionModeHandle,
        fee_estimator::CoreFeeEstimator, risk_metrics::CoreRiskMetrics,
    },
    control::factor_snapshot::FactorSnapshotStore,
    detection::{coalescer::Coalescer, funnel::Funnel},
    execution::{
        capital_manager::CapitalManager,
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        settlement::{
            dedup::SettlementDedup,
            service::{MarketSettlementService, MarketSettlementServiceDeps},
        },
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::async_writer::{AsyncWriter, AsyncWriterConfig},
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, market_cache::MarketCache, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier, universe_filter::MarketUniverseFilter,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_error::algorithm::AlgoError;
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    config::{PolymarketConfig, WebSocketConfig},
    domain::{
        CoreEventPublisher, RuntimeConfigPort, calibration::UpsertCalibration,
        control_factor::ControlFactorProvider,
    },
    enums::common::{ExecutionMode, StalenessLevel},
    runtime_config::RuntimeConfig,
    types::{MarketId, Usd},
};
use oxide_arb_repository::postgres::{
    PgPositionRepository, PgResolutionEventRepository, PgTradeRepository,
};
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::{mocks::MockPositionRepository, risk::TestRiskMetrics};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// No-op calibration data source: the applicator test only exercises
/// `reload`, which never touches the data source.
struct NoopCalibrationSource;

#[async_trait]
impl CalibrationDataSource for NoopCalibrationSource {
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError> {
        Ok(Vec::new())
    }

    async fn check_gamma_resolution(&self, _: &MarketId) -> Result<Option<bool>, AlgoError> {
        Ok(None)
    }

    async fn check_ctf_resolution(&self, _: &MarketId) -> Result<Option<bool>, AlgoError> {
        Ok(None)
    }

    async fn upsert_buckets(&self, _: &[UpsertCalibration]) -> Result<(), AlgoError> {
        Ok(())
    }

    async fn resolve_outcome(&self, _: i64, _: bool) -> Result<(), AlgoError> {
        Ok(())
    }
}

/// Everything a test needs to observe propagation after `apply`.
struct Fixture {
    applicator: RuntimeConfigApplicator,
    store: Arc<RuntimeConfigStore>,
    risk_engine: Arc<RiskEngine>,
    metrics_state: Arc<RiskMetricsState>,
    exposure: Arc<InMemoryExposureReservation>,
    staleness: StalenessClassifier,
    validator: Arc<Validator>,
    calibration_updater: Arc<CalibrationUpdater>,
}

fn audit_writer(metrics: Arc<MetricsHub>) -> Arc<ExecutionAuditWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("test-applicator-audit")
            .batch_size(16)
            .flush_interval(Duration::from_secs(3600)),
        move |_batch: Vec<OpportunityAuditRow>| Box::pin(async move { Ok(()) }),
        metrics,
        CancellationToken::new(),
    );
    Arc::new(ExecutionAuditWriter::new(Arc::new(writer)))
}

/// Detection-chain subscribers (D2–D7) over the boot config.
fn detection_chain(
    boot: &RuntimeConfig,
    fee_calculator: &Arc<FeeCalculator>,
) -> (Arc<CoreOpportunityPipeline>, Arc<CalibrationUpdater>) {
    let calibrator = Arc::new(ResolutionCalibrator::empty(
        boot.detection.calibration.clone(),
    ));
    let detector = EndgameDetector::new(
        &boot.detection.endgame,
        &boot.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(Arc::clone(fee_calculator)),
    );
    let scorer = EndgameScorer::new(
        &boot.detection.endgame.scorer,
        &boot.detection.endgame.fill_probability,
        boot.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&boot.detection.endgame.emission_cooldown);
    let factors: Arc<dyn ControlFactorProvider> =
        Arc::new(FactorSnapshotStore::new(chrono::Utc::now()));
    let pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        factors,
        &boot.detection,
    ));
    let updater = Arc::new(CalibrationUpdater::new(
        calibrator,
        Arc::new(NoopCalibrationSource),
        boot.detection.calibration.clone(),
    ));
    (pipeline, updater)
}

/// Settlement-chain subscribers (S1–S2) over the boot config.
///
/// The Postgres repositories are constructed over a disconnected handle:
/// `reload` never performs I/O, which is exactly what this test pins down.
struct SettlementChainDeps<'a> {
    boot: &'a RuntimeConfig,
    mode: ExecutionModeHandle,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    risk_engine: Arc<RiskEngine>,
    metrics_state: Arc<RiskMetricsState>,
    exposure: Arc<InMemoryExposureReservation>,
    voting_oracle: Arc<VotingOracle>,
}

fn settlement_chain(deps: SettlementChainDeps<'_>) -> Arc<MarketSettlementService> {
    let fsm = Arc::new(ExecutionFSM::new(
        Arc::clone(&deps.metrics),
        Arc::clone(&deps.alerts),
    ));
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    let metrics_refresh = common::disconnected_metrics_refresh(
        Arc::clone(&deps.metrics_state),
        ExecutionMode::Paper,
        Arc::clone(&deps.metrics),
    );
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        deps.metrics_state,
        deps.exposure,
        ws_manager,
        deps.mode,
    ));
    Arc::new(MarketSettlementService::new(MarketSettlementServiceDeps {
        position_repo: Arc::new(PgPositionRepository::new(DatabaseConnection::default())),
        resolution_event_repo: Arc::new(PgResolutionEventRepository::new(
            DatabaseConnection::default(),
        )),
        trade_repo: Arc::new(PgTradeRepository::new(DatabaseConnection::default())),
        risk_engine: deps.risk_engine,
        risk_metrics,
        fsm,
        ctf_redeem: None,
        market_registry: Arc::new(MarketRegistry::new()),
        voting_oracle: deps.voting_oracle,
        metrics: Arc::clone(&deps.metrics),
        alerts: deps.alerts,
        audit_writer: audit_writer(Arc::clone(&deps.metrics)),
        metrics_refresh,
        events: CoreEventPublisher::bounded(16).0,
        config: deps.boot.settlement.clone(),
    }))
}

/// Execution-chain subscribers (E1–E4) over the boot config.
fn execution_chain(
    boot: &RuntimeConfig,
    staleness: &StalenessClassifier,
    mode: ExecutionModeHandle,
    fee_calculator: Arc<FeeCalculator>,
    metrics: &Arc<MetricsHub>,
) -> (
    Arc<Validator>,
    Arc<FokOrderStrategy>,
    Arc<Coalescer>,
    Arc<Funnel>,
) {
    let book_store = Arc::new(BookStore::new(Arc::clone(metrics)));
    let validator = Arc::new(Validator::new(
        book_store,
        staleness.clone(),
        &boot.execution,
        Arc::clone(metrics),
    ));
    let order_strategy = Arc::new(FokOrderStrategy::new(
        mode,
        None,
        fee_calculator,
        boot.execution.timeout.dispatcher_timeout_ms,
        Arc::clone(metrics),
    ));
    let (token_tx, _token_rx) = flume::unbounded();
    let coalescer = Arc::new(Coalescer::new(
        Arc::new(MarketRegistry::new()),
        Duration::from_millis(boot.execution.coalescer.coalesce_window_ms),
        token_tx,
        Arc::clone(metrics),
        CancellationToken::new(),
    ));
    let funnel = Arc::new(Funnel::new(
        Vec::new(),
        boot.execution.funnel.max_queue_size,
        Duration::from_millis(boot.execution.funnel.min_dispatch_interval_ms),
        Arc::clone(metrics),
    ));
    (validator, order_strategy, coalescer, funnel)
}

fn fixture() -> Fixture {
    let boot = RuntimeConfig::default();
    let mode = ExecutionModeHandle::new(ExecutionMode::Paper);
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&boot.notification));

    // Risk chain.
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .config(boot.risk.clone())
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&TestRiskMetrics)
            .expect("risk engine build"),
    );
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    metrics_state.seed_simulated_snapshot(ExecutionMode::Paper, Usd::new(boot.risk.bankroll_usd));
    let reservation = boot.risk.exposure_reservation_config();
    let exposure = Arc::new(InMemoryExposureReservation::new(reservation.clone()));
    let capital = Arc::new(CapitalManager::new(Arc::clone(&exposure), &reservation));

    // Detection chain.
    let fee_calculator = Arc::new(FeeCalculator::default());
    let (opportunity_pipeline, calibration_updater) = detection_chain(&boot, &fee_calculator);
    let staleness = StalenessClassifier::new(&boot.market_data);

    // Execution chain.
    let (validator, order_strategy, coalescer, funnel) =
        execution_chain(&boot, &staleness, mode.clone(), fee_calculator, &metrics);

    // Settlement chain.
    let voting_oracle = Arc::new(VotingOracle::new(
        Vec::new(),
        usize::from(boot.settlement.oracle.voting_quorum),
        Duration::from_secs(boot.settlement.oracle.cross_check_delay_secs),
        boot.settlement.oracle.all_sources_down_strategy.clone(),
    ));
    let settlement_service = settlement_chain(SettlementChainDeps {
        boot: &boot,
        mode: mode.clone(),
        metrics: Arc::clone(&metrics),
        alerts: Arc::clone(&alerts),
        risk_engine: Arc::clone(&risk_engine),
        metrics_state: Arc::clone(&metrics_state),
        exposure: Arc::clone(&exposure),
        voting_oracle: Arc::clone(&voting_oracle),
    });
    let settlement_dedup = Arc::new(SettlementDedup::new(Duration::from_secs(
        boot.settlement.lifecycle.dedup_window_secs,
    )));

    let market_registry = Arc::new(MarketRegistry::new());
    let universe = Arc::new(MarketUniverseFilter::default());
    let market_cache = Arc::new(MarketCache::new(
        Arc::clone(&market_registry),
        Arc::clone(&universe),
    ));

    let store = Arc::new(RuntimeConfigStore::new(boot));
    let applicator = RuntimeConfigApplicator::new(
        Arc::clone(&store),
        mode,
        Arc::new(MockPositionRepository::default()),
        RuntimeConfigSubscribers {
            risk_engine: Arc::clone(&risk_engine),
            metrics_state: Arc::clone(&metrics_state),
            exposure: Arc::clone(&exposure),
            capital,
            opportunity_pipeline,
            calibration_updater: Arc::clone(&calibration_updater),
            staleness: staleness.clone(),
            universe,
            market_registry,
            market_cache,
            ws_subscription: None,
            validator: Arc::clone(&validator),
            order_strategy,
            coalescer,
            funnel,
            settlement_service,
            settlement_dedup,
            voting_oracle,
            ctf_redeem: None,
            alerts,
        },
    );

    Fixture {
        applicator,
        store,
        risk_engine,
        metrics_state,
        exposure,
        staleness,
        validator,
        calibration_updater,
    }
}

#[tokio::test]
async fn apply_propagates_one_snapshot_to_every_subscriber() {
    let fixture = fixture();

    let mut candidate = RuntimeConfig::default();
    candidate.market_data.staleness_fresh_ms = 1_000;
    candidate.market_data.staleness_acceptable_ms = 2_000;
    candidate.market_data.staleness_stale_ms = 3_000;
    candidate.market_data.staleness_expired_ms = 4_000;
    candidate.detection.calibration.refresh_interval_secs = 7_200;
    candidate.execution.timeout.max_validation_slippage_bps = dec!(75);
    candidate.risk.max_daily_loss_usd = dec!(33);
    candidate.risk.bankroll_usd = dec!(2000);

    fixture
        .applicator
        .apply(candidate.clone())
        .await
        .expect("activation applies cleanly");

    // Store readers observe the full new snapshot.
    assert_eq!(*fixture.store.current(), candidate);
    // R1 — risk engine swapped its config + derived pipeline atomically.
    assert_eq!(fixture.risk_engine.config().max_daily_loss_usd, dec!(33));
    // R1 — Paper-mode simulated cash rebased by the bankroll delta (1000→2000).
    assert_eq!(fixture.metrics_state.cash_balance(), Usd::new(dec!(2000)));
    // D1 — the shared staleness ladder reclassifies immediately.
    assert_eq!(fixture.staleness.classify(2_500), StalenessLevel::Stale);
    assert_eq!(
        fixture.staleness.classify(1_500),
        StalenessLevel::Acceptable
    );
    // D7 — the calibration cadence read by the periodic tick.
    assert_eq!(fixture.calibration_updater.refresh_interval_secs(), 7_200);
    // E1 — validation slippage budget.
    assert_eq!(fixture.validator.max_slippage_bps(), dec!(75));
}

#[tokio::test]
async fn apply_rejects_exposure_tightening_and_leaves_all_state_untouched() {
    let fixture = fixture();
    let boot_daily_loss = fixture.store.current().risk.max_daily_loss_usd;

    // Commit live capital across two markets (700 USD total).
    for (market, usd) in [("0xm1", dec!(400)), ("0xm2", dec!(300))] {
        fixture
            .exposure
            .try_reserve_sync(
                &MarketId::new(market),
                Usd::new(usd),
                Duration::from_secs(300),
            )
            .expect("reservation within boot limits");
    }

    let mut candidate = RuntimeConfig::default();
    candidate.risk.max_total_exposure_usd = dec!(500);
    candidate.risk.max_daily_loss_usd = dec!(99);

    let error = fixture
        .applicator
        .apply(candidate)
        .await
        .expect_err("tightening below reserved capital must fail closed");
    assert!(
        error.to_string().contains("max_total_exposure_usd"),
        "unexpected error: {error}"
    );

    // Nothing was applied: store and subscribers keep the previous snapshot.
    assert_eq!(
        fixture.store.current().risk.max_total_exposure_usd,
        RuntimeConfig::default().risk.max_total_exposure_usd
    );
    assert_eq!(
        fixture.risk_engine.config().max_daily_loss_usd,
        boot_daily_loss
    );
}
