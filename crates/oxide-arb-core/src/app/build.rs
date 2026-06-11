//! Application composition root — wires subsystems in dependency order.
//!
//! Infrastructure (connections, pools, channels, shards) is wired from the
//! [`DeployConfig`]; every trading parameter is wired from the **runtime
//! config snapshot** seeded out of Postgres (`runtime_config_version`), so the
//! audited activation history — not the TOML — is the single source of truth
//! for money-relevant behaviour.

use super::{
    AppContext, ControlFactorBundle, DataBundle, ExecutionBundle, InfraBundle, RiskBundle,
    RuntimeChannels, SettlementBundle, TradingBundle, task_registry::PendingTaskQueue,
};
use crate::{
    app::periodic_services::run_calibration_startup_tick,
    bridge::{
        CoreOpportunityPipeline, calibration_source::CoreCalibrationDataSource,
        execution_mode::ExecutionModeHandle, fee_estimator::CoreFeeEstimator,
        potential_loss_store::CorePotentialLossStore, risk_audit_sink::new_audit_sink,
        risk_metrics::CoreRiskMetrics, risk_persistence::CoreRiskPersistence,
    },
    control::{
        ControlFactorRegistry,
        factor_refresher::{FactorRefreshConfig, FactorRefresher},
        factor_shadow::{ShadowDecisionWriter, ShadowWriterTask},
        factor_snapshot::FactorSnapshotStore,
    },
    detection::{coalescer::Coalescer, funnel::Funnel, scanner::Scanner},
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        port::ExecutionPort,
        runner::{ExecutionRunner, ExecutionRunnerPool},
        settlement::{
            dedup::SettlementDedup,
            service::{MarketSettlementService, MarketSettlementServiceDeps},
        },
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::persistence_writers::{
        PersistenceBackgroundWorkers, PersistenceBundle, PersistenceWireInput,
    },
    infra::{
        oracle_health_tracker::OracleHealthTracker,
        risk_decision_audit_buffer::RiskDecisionAuditBuffer,
    },
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        balance_fact_writer::BalanceFactWriter, book_fact_writer::BookFactWriter,
        execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore,
        data_pipeline::{DataPipeline, DataPipelineDeps},
        market_cache::MarketCache,
        market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
        universe_filter::MarketUniverseFilter,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
    service::{
        equity_valuator::EquityValuator,
        gamma::{GammaService, GammaServiceDeps},
        risk_metrics::{ApiHealthTracker, RiskMetricsRefreshService, RiskMetricsState},
        ws_subscription::WsSubscriptionCoordinator,
    },
};
use oxide_arb_algorithm::{
    calibration::{CalibrationEntry, CalibrationUpdater, ResolutionCalibrator},
    cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector,
    pipeline::OpportunityPipeline,
    scorer::EndgameScorer,
};
use oxide_arb_api::{
    VotingOracle, build_voting_oracle,
    clob::ClobClient,
    ctf::client::CtfRedeemClient,
    fees::FeeCalculator,
    gamma::GammaClient,
    keystore::Keystore,
    ws::{ClobWsManager, WsEventDropHook},
};
use oxide_arb_error::{OxideError, OxideResult, config::ConfigError};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        CoreEvent, CoreEventPublisher, NewRuntimeConfigActivation, NewRuntimeConfigVersion,
        control_factor::ControlFactorProvider, risk::RiskEngineState, runtime_config_hash,
        settlement::MarketSettlementRequest,
    },
    enums::{
        common::ExecutionMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::{
        CalibrationConfig, RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig,
        validation::validate_runtime_for_mode,
    },
    types::{MarketId, RuntimeConfigActivationId, RuntimeConfigVersionId, TokenId, Usd},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgBlacklistPersistenceRepository, PgCalibrationRepository, PgControlFactorRepository,
        PgEmergencyRepository, PgEventRepository, PgFactDataRepository, PgMarketRepository,
        PgPositionRepository, PgPotentialLossRepository, PgReconciliationRepository,
        PgReportRepository, PgResolutionEventRepository, PgRiskAuditRepository,
        PgRiskStateRepository, PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository,
        PgTradeRepository, risk_fill::PgRiskFillRepository,
    },
    traits::{
        BlacklistPersistenceRepository, CalibrationRepository, ControlFactorRepository,
        ControlFactorShadowDecisionRepository, PotentialLossRepository, RiskStateRepository,
        RuntimeConfigVersionRepository, SystemRuntimeStateRepository,
    },
};
use oxide_arb_risk::{
    audit::RiskAuditEvent, audit_sink::AuditSink, builder::RiskEngineBuilder, clock::utc_clock,
    engine::RiskEngine,
};
use oxide_arb_storage::{
    cache::{MokaBackend, RedisBackend, TieredCache},
    clickhouse::{ChWriteManager, ClickHousePool},
    postgres::{
        PostgresPool,
        migration::{Migrator, MigratorTrait},
    },
};
use parking_lot::Mutex;
use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct BuildRepos {
    risk_state: Arc<PgRiskStateRepository>,
    blacklist: Arc<PgBlacklistPersistenceRepository>,
    audit: Arc<PgRiskAuditRepository>,
    risk_fill: Arc<PgRiskFillRepository>,
    emergency: Arc<PgEmergencyRepository>,
    reconciliation: Arc<PgReconciliationRepository>,
    resolution_event: Arc<PgResolutionEventRepository>,
    potential_loss: Arc<PgPotentialLossRepository>,
    calibration: Arc<PgCalibrationRepository>,
    fact_data: Arc<PgFactDataRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    market: Arc<PgMarketRepository>,
    event: Arc<PgEventRepository>,
    trade: Arc<PgTradeRepository>,
    report: Arc<PgReportRepository>,
    position: Arc<PgPositionRepository>,
    control_factor: Arc<dyn ControlFactorRepository>,
}

struct BuildInfra {
    /// Effective execution mode after restoring the persisted operational state
    /// (the `system_runtime_state` singleton, seeded to `DryRun`).
    execution_mode: ExecutionMode,
    /// In-process snapshot of the active runtime config (seeded from PG).
    runtime_store: Arc<RuntimeConfigStore>,
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    cache: Arc<TieredCache>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    risk_decision_audit_rx: Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    repos: BuildRepos,
    persistence: PersistenceBundle,
    balance_fact_writer: Arc<BalanceFactWriter>,
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer: ShadowDecisionWriter,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

struct BuildPersistence {
    trade_repo: Arc<PgTradeRepository>,
    timeseries: Arc<ChTimeseriesRepository>,
    audit_writer: Arc<ExecutionAuditWriter>,
    book_fact_writer: Arc<BookFactWriter>,
}

struct AssembledInfra {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    cache: Arc<TieredCache>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    risk_decision_audit_rx: Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    repos: BuildRepos,
    balance_fact_writer: Arc<BalanceFactWriter>,
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

struct AppContextAssembly {
    config: Arc<DeployConfig>,
    runtime_store: Arc<RuntimeConfigStore>,
    applicator: Arc<RuntimeConfigApplicator>,
    execution_mode: ExecutionModeHandle,
    events: CoreEventPublisher,
    event_rx: flume::Receiver<CoreEvent>,
    infra: AssembledInfra,
    clients: BuildClients,
    risk: BuildRisk,
    trading: BuildTrading,
    persistence: BuildPersistence,
    settlement_service: Arc<MarketSettlementService>,
    settlement_dedup: Arc<SettlementDedup>,
    shutdown: CancellationToken,
    pending_tasks: PendingTaskQueue,
}

struct BuildClients {
    ws_manager: Arc<ClobWsManager>,
    gamma_client: Arc<GammaClient>,
    fee_calculator: Arc<FeeCalculator>,
    voting_oracle: Arc<VotingOracle>,
    clob_client: Option<Arc<ClobClient>>,
    ctf_redeem: Option<Arc<CtfRedeemClient>>,
    holder_address: String,
}

struct BuildRisk {
    exposure: Arc<InMemoryExposureReservation>,
    metrics: Arc<CoreRiskMetrics>,
    metrics_state: Arc<RiskMetricsState>,
    engine: Arc<RiskEngine>,
    potential_loss_store: Arc<CorePotentialLossStore>,
    metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    fsm: Arc<ExecutionFSM>,
    backpressure: Arc<BackpressurePolicy>,
}

struct BuildTrading {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    universe: Arc<MarketUniverseFilter>,
    ws_subscription: Arc<WsSubscriptionCoordinator>,
    gamma_service: Arc<GammaService>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    staleness: StalenessClassifier,
    funnel: Arc<Funnel>,
    validator: Arc<Validator>,
    order_strategy: Arc<FokOrderStrategy>,
    data_pipeline: Arc<DataPipeline>,
    execution: ExecutionBundle,
    settlement_rx: flume::Receiver<MarketSettlementRequest>,
    token_rx: flume::Receiver<TokenId>,
    market_rx: flume::Receiver<MarketId>,
    execution_runners: Vec<ExecutionRunner>,
}

struct DetectionStack {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    universe: Arc<MarketUniverseFilter>,
    ws_subscription: Arc<WsSubscriptionCoordinator>,
    gamma_service: Arc<GammaService>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    staleness: StalenessClassifier,
    token_tx: flume::Sender<TokenId>,
    token_rx: flume::Receiver<TokenId>,
    market_rx: flume::Receiver<MarketId>,
}

struct ExecutionLoop {
    funnel: Arc<Funnel>,
    validator: Arc<Validator>,
    order_strategy: Arc<FokOrderStrategy>,
    data_pipeline: Arc<DataPipeline>,
    execution: ExecutionBundle,
    settlement_rx: flume::Receiver<MarketSettlementRequest>,
    execution_runners: Vec<ExecutionRunner>,
}

/// Bounded capacity of the real-time `CoreEvent` bus. Sized for short bursts of
/// events; a full channel drops (counted per kind) rather than blocking producers.
const CORE_EVENT_CHANNEL_CAPACITY: usize = 4096;

impl AppContext {
    /// Build all subsystems from the deploy config (PG/CH/Redis + trading loop).
    ///
    /// Trading parameters come from the runtime-config snapshot seeded out of
    /// the `runtime_config_version` table during [`connect_infra`].
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
    ) -> OxideResult<Self> {
        // Real-time event bus consumed by the WebSocket broadcaster. Bounded +
        // non-blocking: a full channel drops events, never stalling producers.
        let (events, event_rx) = CoreEventPublisher::bounded(CORE_EVENT_CHANNEL_CAPACITY);

        // `connect_infra` migrates the schema, seeds/loads the active runtime
        // config, and restores the persisted operational execution mode (the
        // seeded `system_runtime_state` singleton), so that mode — not config —
        // drives validation and wiring.
        let (infra, persistence_workers) = connect_infra(&deploy, shutdown.clone()).await?;
        // Attach the per-kind drop observer now that the metrics hub exists: a
        // full or disconnected bus increments `oxide_arb_ws_event_dropped_total`
        // labeled by `CoreEvent::kind`, never blocking the producer.
        let events = {
            let dropped = infra.metrics.register_ws_event_dropped();
            events.with_drop_hook(Arc::new(move |kind| {
                dropped.with_label_values(&[kind]).inc();
            }))
        };
        let mode = infra.execution_mode;
        deploy.ensure_valid_for_mode(mode)?;
        let runtime_store = Arc::clone(&infra.runtime_store);
        let runtime = runtime_store.current();
        ensure_runtime_valid_for_mode(&runtime, mode)?;
        // Single source of truth for the live execution mode; every hot-path
        // reader holds a clone and observes governed transitions atomically.
        let execution_mode = ExecutionModeHandle::new(mode);

        let clients = connect_clients(
            &deploy,
            &runtime,
            shutdown.clone(),
            Arc::clone(&infra.metrics),
        )
        .await?;
        let wiring = WiringConfig {
            deploy: &deploy,
            runtime: &runtime,
        };
        let (risk, trading) = wire_risk_and_trading(
            wiring,
            &execution_mode,
            &infra,
            &clients,
            &events,
            shutdown.clone(),
        )
        .await?;

        let (settlement_service, settlement_dedup) =
            wire_settlement_bundle(&runtime, &infra, &clients, &risk, &trading, &events);

        // Activation propagation: every live subscriber, risk-first.
        let applicator = wire_applicator(WireApplicatorInput {
            runtime_store: Arc::clone(&runtime_store),
            execution_mode: execution_mode.clone(),
            infra: &infra,
            clients: &clients,
            risk: &risk,
            trading: &trading,
            settlement_service: Arc::clone(&settlement_service),
            settlement_dedup: Arc::clone(&settlement_dedup),
        });

        let (assembled_infra, persistence_handles, pending_tasks) =
            finish_infra(infra, persistence_workers);

        Ok(assemble_app_context(AppContextAssembly {
            config: deploy,
            runtime_store,
            applicator,
            execution_mode,
            events,
            event_rx,
            infra: assembled_infra,
            clients,
            risk,
            trading,
            persistence: persistence_handles,
            settlement_service,
            settlement_dedup,
            shutdown,
            pending_tasks,
        }))
    }
}

/// Consume [`BuildInfra`], queue persistence background workers, and project
/// the handles needed by the final assembly.
fn finish_infra(
    infra: BuildInfra,
    persistence_workers: PersistenceBackgroundWorkers,
) -> (AssembledInfra, BuildPersistence, PendingTaskQueue) {
    let BuildInfra {
        execution_mode: _,
        runtime_store: _,
        pg_pool,
        ch_pool,
        cache,
        metrics,
        alerts,
        risk_decision_audit,
        risk_decision_audit_rx,
        repos,
        persistence,
        balance_fact_writer,
        factor_store,
        factor_refresher,
        factor_registry,
        shadow_writer: _shadow_writer,
        shadow_writer_task,
    } = infra;
    let persistence_handles = BuildPersistence {
        trade_repo: Arc::clone(&persistence.trade_repo),
        timeseries: Arc::clone(&persistence.timeseries),
        audit_writer: Arc::clone(&persistence.audit_writer),
        book_fact_writer: Arc::clone(&persistence.book_fact_writer),
    };
    let mut pending_tasks = PendingTaskQueue::default();
    persistence.queue_background_tasks(persistence_workers, &mut pending_tasks);

    let assembled = AssembledInfra {
        pg_pool,
        ch_pool,
        cache,
        metrics,
        alerts,
        risk_decision_audit,
        risk_decision_audit_rx,
        repos,
        balance_fact_writer,
        factor_store,
        factor_refresher,
        factor_registry,
        shadow_writer_task,
    };
    (assembled, persistence_handles, pending_tasks)
}

/// Deploy + runtime configuration views shared by the wiring functions.
#[derive(Clone, Copy)]
struct WiringConfig<'a> {
    deploy: &'a DeployConfig,
    runtime: &'a RuntimeConfig,
}

/// Inputs for [`wire_applicator`].
struct WireApplicatorInput<'a> {
    runtime_store: Arc<RuntimeConfigStore>,
    execution_mode: ExecutionModeHandle,
    infra: &'a BuildInfra,
    clients: &'a BuildClients,
    risk: &'a BuildRisk,
    trading: &'a BuildTrading,
    settlement_service: Arc<MarketSettlementService>,
    settlement_dedup: Arc<SettlementDedup>,
}

/// Assemble the activation applicator over every live subscriber handle.
fn wire_applicator(input: WireApplicatorInput<'_>) -> Arc<RuntimeConfigApplicator> {
    let WireApplicatorInput {
        runtime_store,
        execution_mode,
        infra,
        clients,
        risk,
        trading,
        settlement_service,
        settlement_dedup,
    } = input;
    Arc::new(RuntimeConfigApplicator::new(
        runtime_store,
        execution_mode,
        RuntimeConfigSubscribers {
            risk_engine: Arc::clone(&risk.engine),
            metrics_state: Arc::clone(&risk.metrics_state),
            exposure: Arc::clone(&risk.exposure),
            capital: Arc::clone(&trading.execution.capital_manager),
            opportunity_pipeline: Arc::clone(&trading.opportunity_pipeline),
            calibration_updater: Arc::clone(&trading.calibration_updater),
            staleness: trading.staleness.clone(),
            universe: Arc::clone(&trading.universe),
            market_registry: Arc::clone(&trading.market_registry),
            market_cache: Arc::clone(&trading.market_cache),
            ws_subscription: Some(Arc::clone(&trading.ws_subscription)),
            validator: Arc::clone(&trading.validator),
            order_strategy: Arc::clone(&trading.order_strategy),
            coalescer: Arc::clone(&trading.coalescer),
            funnel: Arc::clone(&trading.funnel),
            settlement_service,
            settlement_dedup,
            voting_oracle: Arc::clone(&clients.voting_oracle),
            ctf_redeem: clients.ctf_redeem.as_ref().map(Arc::clone),
            alerts: Arc::clone(&infra.alerts),
        },
    ))
}

/// Fail-closed gate: the persisted operational mode must be valid for the
/// active runtime config (e.g. Live with a disabled redeem route aborts boot).
fn ensure_runtime_valid_for_mode(runtime: &RuntimeConfig, mode: ExecutionMode) -> OxideResult<()> {
    let report = validate_runtime_for_mode(runtime, mode);
    for w in &report.warnings {
        tracing::warn!(mode = ?mode, "Runtime config warning: {w}");
    }
    if report.has_errors() {
        return Err(ConfigError::from(report).into());
    }
    Ok(())
}

fn wire_settlement_bundle(
    runtime: &RuntimeConfig,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    trading: &BuildTrading,
    events: &CoreEventPublisher,
) -> (Arc<MarketSettlementService>, Arc<SettlementDedup>) {
    let trade_repo = Arc::clone(&infra.persistence.trade_repo);
    let audit_writer = Arc::clone(&infra.persistence.audit_writer);
    let settlement_service = Arc::new(MarketSettlementService::new(MarketSettlementServiceDeps {
        position_repo: Arc::clone(&infra.repos.position),
        resolution_event_repo: Arc::clone(&infra.repos.resolution_event),
        trade_repo,
        risk_engine: Arc::clone(&risk.engine),
        risk_metrics: Arc::clone(&risk.metrics),
        fsm: Arc::clone(&risk.fsm),
        ctf_redeem: clients.ctf_redeem.as_ref().map(Arc::clone),
        market_registry: Arc::clone(&trading.market_registry),
        voting_oracle: Arc::clone(&clients.voting_oracle),
        metrics: Arc::clone(&infra.metrics),
        alerts: Arc::clone(&infra.alerts),
        audit_writer,
        metrics_refresh: risk.metrics_refresh.clone(),
        events: events.clone(),
        config: runtime.settlement.clone(),
    }));
    let settlement_dedup = Arc::new(SettlementDedup::new(Duration::from_secs(
        runtime.settlement.lifecycle.dedup_window_secs,
    )));
    (settlement_service, settlement_dedup)
}

fn assemble_app_context(parts: AppContextAssembly) -> AppContext {
    let AppContextAssembly {
        config,
        runtime_store,
        applicator,
        execution_mode,
        events,
        event_rx,
        infra,
        clients,
        risk,
        trading,
        persistence,
        settlement_service,
        settlement_dedup,
        shutdown,
        pending_tasks,
    } = parts;
    AppContext {
        config,
        runtime_config: runtime_store,
        applicator,
        execution_mode,
        events,
        event_rx: Mutex::new(Some(event_rx)),
        infra: InfraBundle {
            pg: infra.pg_pool,
            ch: infra.ch_pool,
            cache: infra.cache,
            metrics: infra.metrics,
            alerts: infra.alerts,
            risk_decision_audit: infra.risk_decision_audit,
            risk_decision_audit_rx: infra.risk_decision_audit_rx,
            trade_repo: persistence.trade_repo,
            position_repo: infra.repos.position,
            report_repo: infra.repos.report,
            fact_data_repo: infra.repos.fact_data,
            calibration_repo: infra.repos.calibration,
            risk_state_repo: infra.repos.risk_state,
            timeseries: persistence.timeseries,
            audit_writer: persistence.audit_writer,
            balance_fact_writer: infra.balance_fact_writer,
            book_fact_writer: persistence.book_fact_writer,
            holder_address: clients.holder_address,
        },
        data: DataBundle {
            book_store: trading.book_store,
            market_registry: trading.market_registry,
            market_cache: trading.market_cache,
            data_pipeline: trading.data_pipeline,
            gamma_service: trading.gamma_service,
        },
        risk: RiskBundle {
            engine: risk.engine,
            metrics: risk.metrics,
            metrics_state: risk.metrics_state,
            exposure: risk.exposure,
            potential_loss_store: risk.potential_loss_store,
            metrics_refresh: risk.metrics_refresh,
        },
        trading: TradingBundle {
            opportunity_pipeline: trading.opportunity_pipeline,
            calibrator: trading.calibrator,
            calibration_updater: trading.calibration_updater,
            scanner: trading.scanner,
            coalescer: trading.coalescer,
            funnel: trading.funnel,
            fsm: risk.fsm,
            execution: Some(trading.execution),
            clob_client: clients.clob_client,
            ws_manager: clients.ws_manager,
        },
        control: ControlFactorBundle {
            store: infra.factor_store,
            refresher: infra.factor_refresher,
            registry: infra.factor_registry,
            shadow_writer_task: infra.shadow_writer_task,
        },
        settlement: SettlementBundle {
            service: settlement_service,
            dedup: settlement_dedup,
            settlement_rx: Mutex::new(Some(trading.settlement_rx)),
        },
        runtime: RuntimeChannels {
            coalescer_token_rx: Mutex::new(Some(trading.token_rx)),
            scanner_market_rx: Mutex::new(Some(trading.market_rx)),
            execution_runners: Mutex::new(Some(trading.execution_runners)),
        },
        shutdown,
        pending_tasks,
    }
}

/// Construct every Postgres repository over a shared connection clone.
fn build_pg_repos(pg_pool: &PostgresPool) -> BuildRepos {
    let db = pg_pool.connection().clone();
    BuildRepos {
        risk_state: Arc::new(PgRiskStateRepository::new(db.clone())),
        blacklist: Arc::new(PgBlacklistPersistenceRepository::new(db.clone())),
        audit: Arc::new(PgRiskAuditRepository::new(db.clone())),
        risk_fill: Arc::new(PgRiskFillRepository::new(db.clone())),
        emergency: Arc::new(PgEmergencyRepository::new(db.clone())),
        reconciliation: Arc::new(PgReconciliationRepository::new(db.clone())),
        resolution_event: Arc::new(PgResolutionEventRepository::new(db.clone())),
        potential_loss: Arc::new(PgPotentialLossRepository::new(db.clone())),
        calibration: Arc::new(PgCalibrationRepository::new(db.clone())),
        fact_data: Arc::new(PgFactDataRepository::new(db.clone())),
        runtime_config: Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>,
        market: Arc::new(PgMarketRepository::new(db.clone())),
        event: Arc::new(PgEventRepository::new(db.clone())),
        trade: Arc::new(PgTradeRepository::new(db.clone())),
        report: Arc::new(PgReportRepository::new(db.clone())),
        position: Arc::new(PgPositionRepository::new(db.clone())),
        control_factor: Arc::new(PgControlFactorRepository::new(db)),
    }
}

async fn connect_infra(
    deploy: &DeployConfig,
    shutdown: CancellationToken,
) -> OxideResult<(BuildInfra, PersistenceBackgroundWorkers)> {
    let metrics = Arc::new(MetricsHub::new());

    let pg_pool = Arc::new(PostgresPool::connect(&deploy.db.postgres).await?);
    Migrator::up(pg_pool.connection(), None).await?;

    let repos = build_pg_repos(&pg_pool);
    // Seed / load the active runtime config before anything trading-relevant
    // is wired: the audited activation history is the source of truth.
    let runtime = ensure_runtime_config_activation(repos.runtime_config.as_ref()).await?;
    let alerts = Arc::new(AlertDispatcher::new(&runtime.notification));
    let runtime_store = Arc::new(RuntimeConfigStore::new(runtime));

    let (risk_decision_audit, risk_decision_audit_rx) = new_audit_sink(4096);

    let ch_pool = Arc::new(ClickHousePool::connect(&deploy.db.clickhouse)?);
    ch_pool.ensure_schema().await?;

    let cache = Arc::new(TieredCache::new(
        MokaBackend::new(deploy.cache.moka.max_capacity),
        RedisBackend::new(&deploy.cache.redis).await?,
    ));

    let write_manager = Arc::new(ChWriteManager::new(
        deploy.db.clickhouse.max_concurrent_inserts,
    ));
    let timeseries = Arc::new(ChTimeseriesRepository::new(
        ch_pool.client().clone(),
        &deploy.db.clickhouse,
        write_manager,
        shutdown.clone(),
    ));
    let balance_fact_writer = Arc::new(BalanceFactWriter::new(Arc::clone(&repos.fact_data)));

    // Restore the persisted operational execution mode. The singleton is seeded
    // to `DryRun` by the migration seed lane, so it is the single source of
    // truth: the operator's most recent deliberate `/system/mode` transition is
    // authoritative across restarts. Entering Live still passes the boot
    // preflight, so restore cannot escalate silently into an unsafe Live state.
    let execution_mode = restore_execution_mode(&PgSystemRuntimeStateRepository::new(
        pg_pool.connection().clone(),
    ))
    .await?;

    let control = wire_control_factors(&repos, &metrics, execution_mode).await?;
    let ControlFactorWiring {
        factor_store,
        factor_refresher,
        factor_registry,
        shadow_writer,
        shadow_writer_task,
    } = control;

    let (persistence, persistence_workers) = PersistenceBundle::wire(PersistenceWireInput {
        metrics: Arc::clone(&metrics),
        shutdown,
        trade_repo: Arc::clone(&repos.trade),
        timeseries,
    });

    Ok((
        BuildInfra {
            execution_mode,
            runtime_store,
            pg_pool,
            ch_pool,
            cache,
            metrics,
            alerts,
            risk_decision_audit,
            risk_decision_audit_rx,
            repos,
            persistence,
            balance_fact_writer,
            factor_store,
            factor_refresher,
            factor_registry,
            shadow_writer,
            shadow_writer_task,
        },
        persistence_workers,
    ))
}

/// Constructed live control-factor wiring returned by [`wire_control_factors`].
struct ControlFactorWiring {
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer: ShadowDecisionWriter,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

/// Restore the persisted operational execution mode (the single source of
/// truth).
///
/// The migration seed lane guarantees the singleton exists in `DryRun`, so a
/// normal boot just loads it. If the row is somehow absent (e.g. a truncated
/// table), fail closed: re-seed `DryRun` rather than guessing an escalated mode.
async fn restore_execution_mode(
    repo: &PgSystemRuntimeStateRepository,
) -> OxideResult<ExecutionMode> {
    if let Some(state) = repo.load().await? {
        return Ok(state.execution_mode);
    }
    tracing::warn!("system_runtime_state singleton missing; re-seeding DryRun (fail-closed)");
    let mode = ExecutionMode::DryRun;
    repo.upsert_execution_mode(mode, "bootstrap", "fail-closed re-seed (row missing)")
        .await?;
    Ok(mode)
}

async fn wire_control_factors(
    repos: &BuildRepos,
    metrics: &Arc<MetricsHub>,
    mode: ExecutionMode,
) -> OxideResult<ControlFactorWiring> {
    let factor_store = Arc::new(FactorSnapshotStore::new(chrono::Utc::now()));
    let shadow_repo_concrete = Arc::clone(&repos.fact_data);
    let shadow_repo: Arc<dyn ControlFactorShadowDecisionRepository> = shadow_repo_concrete;
    let (shadow_writer, shadow_writer_task) =
        ShadowDecisionWriter::new(shadow_repo, Arc::clone(metrics));
    let factor_refresher = Arc::new(FactorRefresher::new(
        Arc::clone(&repos.control_factor),
        Arc::clone(&factor_store),
        Arc::clone(metrics),
        FactorRefreshConfig::for_live(mode == ExecutionMode::Live),
    ));
    let factor_registry = Arc::new(
        ControlFactorRegistry::new(
            Arc::clone(&repos.control_factor),
            Arc::clone(&repos.runtime_config),
        )
        .with_snapshot_refresh_notify(factor_refresher.notify_handle()),
    );
    factor_refresher.startup().await?;
    Ok(ControlFactorWiring {
        factor_store,
        factor_refresher,
        factor_registry,
        shadow_writer,
        shadow_writer_task: Mutex::new(Some(shadow_writer_task)),
    })
}

/// Seed / load the active runtime configuration.
///
/// - No active version: create + activate [`RuntimeConfig::default`]
///   (`source = Bootstrap`, `Initial`).
/// - Active version parses as `schema_version = 1`: use it verbatim — TOML
///   never overrides the audited activation history.
/// - Active version fails the typed parse (legacy/corrupt document): reseed
///   defaults with a fresh `Bootstrap` version and a loud warning. The project
///   has exactly one schema version; no migration chain exists by design.
async fn ensure_runtime_config_activation(
    repo: &dyn RuntimeConfigVersionRepository,
) -> OxideResult<RuntimeConfig> {
    let current = repo.load_current().await?;
    if let Some(version) = &current {
        match RuntimeConfig::from_json(&version.config_json) {
            Ok(config) => return Ok(config),
            Err(error) => {
                tracing::warn!(
                    %error,
                    version_id = %version.runtime_config_version_id,
                    "active runtime config is not a valid schema_version=1 document — \
                     reseeding defaults"
                );
            }
        }
    }

    let config = RuntimeConfig::default();
    let config_json = config.to_json();
    let config_hash = runtime_config_hash(&config_json);
    let version = match repo.load_by_hash(&config_hash).await? {
        Some(version) => version,
        None => {
            repo.create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash: config_hash.clone(),
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                config_json,
                source: RuntimeConfigVersionSource::Bootstrap,
                created_by: "system".to_owned(),
                reason: "bootstrap default runtime config (schema_version=1)".to_owned(),
            })
            .await?
        }
    };

    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id.clone(),
        activated_at: chrono::Utc::now(),
        activated_by: "system".to_owned(),
        reason: "bootstrap runtime config activation".to_owned(),
        activation_kind: if current.is_some() {
            RuntimeConfigActivationKind::Promote
        } else {
            RuntimeConfigActivationKind::Initial
        },
        previous_runtime_config_version_id: current
            .map(|version| version.runtime_config_version_id),
        rollback_target_version_id: None,
        audit_event_id: None,
    })
    .await?;
    Ok(config)
}

async fn connect_clients(
    deploy: &DeployConfig,
    runtime: &RuntimeConfig,
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
) -> OxideResult<BuildClients> {
    let metrics_hook = Arc::clone(&metrics);
    let on_events_dropped: WsEventDropHook = Arc::new(move |n| {
        metrics_hook.ws_events_dropped.inc_by(n);
    });
    let reject_hook = {
        let metrics_hook = Arc::clone(&metrics);
        Arc::new(move || {
            metrics_hook
                .book_level_rejected
                .with_label_values(&["ws"])
                .inc();
        })
    };
    let rest_reject_hook = {
        let metrics_hook = Arc::clone(&metrics);
        Arc::new(move || {
            metrics_hook
                .book_level_rejected
                .with_label_values(&["rest"])
                .inc();
        })
    };

    let ws_manager = Arc::new(ClobWsManager::new(
        &deploy.polymarket,
        &deploy.market_data.websocket,
        shutdown,
        Some(on_events_dropped),
        Some(reject_hook),
    ));
    let gamma_client = Arc::new(GammaClient::new(deploy.market_data.gamma.clone()));
    let fee_calculator = Arc::new(FeeCalculator::from_config(&deploy.polymarket.fees));
    let voting_oracle = Arc::new(build_voting_oracle(
        &deploy.polymarket,
        &deploy.market_data.gamma,
        &runtime.settlement.oracle,
    )?);

    let (clob_client, ctf_redeem, holder_address) = match Keystore::from_config(&deploy.keys) {
        Ok(ks) => {
            let holder_address = ks.address_string();
            let signer = ks.signer_arc();
            let ctf_redeem = match CtfRedeemClient::new(
                Arc::clone(&signer),
                deploy.polymarket.onchain.rpc_url.clone(),
                runtime.settlement.redeem.clone(),
                deploy.polymarket.chain_id,
            ) {
                Ok(client) => Some(Arc::new(client)),
                Err(error) => {
                    tracing::warn!(%error, "CTF redeem client unavailable");
                    None
                }
            };
            let clob_client = match ClobClient::connect(signer, &deploy.polymarket).await {
                Ok(client) => Some(Arc::new(
                    client.with_book_level_reject_hook(Some(rest_reject_hook)),
                )),
                Err(error) => {
                    tracing::warn!(%error, "ClobClient connect failed — Live/paper CLOB disabled");
                    None
                }
            };
            (clob_client, ctf_redeem, holder_address)
        }
        Err(error) => {
            tracing::info!(%error, "Keystore unavailable — running without ClobClient");
            (None, None, "unavailable".to_owned())
        }
    };

    Ok(BuildClients {
        ws_manager,
        gamma_client,
        fee_calculator,
        voting_oracle,
        clob_client,
        ctf_redeem,
        holder_address,
    })
}

async fn wire_risk_and_trading(
    wiring: WiringConfig<'_>,
    execution_mode: &ExecutionModeHandle,
    infra: &BuildInfra,
    clients: &BuildClients,
    events: &CoreEventPublisher,
    shutdown: CancellationToken,
) -> OxideResult<(BuildRisk, BuildTrading)> {
    let detection = wire_detection(wiring, infra, clients, events, shutdown.clone()).await?;
    let risk = wire_risk(wiring, execution_mode, infra, clients, events, &detection).await?;
    let trading = wire_trading(
        wiring,
        execution_mode,
        infra,
        clients,
        &risk,
        detection,
        shutdown,
    );
    Ok((risk, trading))
}

async fn wire_risk(
    wiring: WiringConfig<'_>,
    execution_mode: &ExecutionModeHandle,
    infra: &BuildInfra,
    clients: &BuildClients,
    events: &CoreEventPublisher,
    detection: &DetectionStack,
) -> OxideResult<BuildRisk> {
    let WiringConfig { deploy, runtime } = wiring;
    let exposure = Arc::new(InMemoryExposureReservation::new(
        runtime.risk.exposure_reservation_config(),
    ));
    let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
    let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
    let mode = execution_mode.current();
    if mode != ExecutionMode::Live {
        metrics_state.seed_simulated_snapshot(mode, Usd::new(runtime.risk.bankroll_usd));
    }
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        Arc::clone(&clients.ws_manager),
        execution_mode.clone(),
    ));

    let risk_persistence = Arc::new(CoreRiskPersistence::new(
        infra.repos.risk_state.clone(),
        infra.repos.blacklist.clone(),
        infra.repos.audit.clone(),
        infra.repos.risk_fill.clone(),
        infra.repos.emergency.clone(),
        infra.repos.reconciliation.clone(),
    ));
    let risk_state_info = infra.repos.risk_state.load().await?;
    let blacklist = infra.repos.blacklist.load_active().await?;
    let potential_loss = infra
        .repos
        .potential_loss
        .find_active()
        .await
        .unwrap_or_default();
    let audit_sink = Arc::clone(&infra.risk_decision_audit);
    let audit_sink: Arc<dyn AuditSink> = audit_sink;
    let potential_loss_store = Arc::new(CorePotentialLossStore::new(
        infra.repos.potential_loss.clone(),
    ));

    let engine = Arc::new(
        RiskEngineBuilder::new()
            .config(runtime.risk.clone())
            .persistence(risk_persistence)
            .snapshot(RiskEngineState::from(&risk_state_info))
            .blacklist_entries(blacklist)
            .potential_loss_entries(potential_loss)
            .potential_loss_store(potential_loss_store.clone())
            .audit_sink(audit_sink)
            .event_publisher(events.clone())
            .clock(utc_clock())
            .build(risk_metrics.as_ref())?,
    );

    let fsm = Arc::new(ExecutionFSM::new(
        Arc::clone(&infra.metrics),
        Arc::clone(&infra.alerts),
    ));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&infra.metrics),
        deploy.execution.book_apply.shard_count.max(1),
    ));

    let metrics_refresh = clients.clob_client.as_ref().map(|clob_client| {
        let position_repo = Arc::clone(&infra.repos.position);
        let equity_valuator = Arc::new(EquityValuator::new(
            Arc::clone(&detection.market_registry),
            Arc::clone(&detection.book_store),
            Arc::clone(&detection.calibrator),
        ));
        Arc::new(RiskMetricsRefreshService::new(
            Arc::clone(&metrics_state),
            Arc::clone(clob_client),
            position_repo,
            equity_valuator,
            Arc::clone(&infra.metrics),
        ))
    });

    Ok(BuildRisk {
        exposure,
        metrics: risk_metrics,
        metrics_state,
        engine,
        potential_loss_store,
        metrics_refresh,
        fsm,
        backpressure,
    })
}

fn wire_trading(
    wiring: WiringConfig<'_>,
    execution_mode: &ExecutionModeHandle,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    detection: DetectionStack,
    shutdown: CancellationToken,
) -> BuildTrading {
    let execution = wire_execution_loop(
        wiring,
        execution_mode,
        infra,
        clients,
        risk,
        &detection,
        shutdown,
    );

    BuildTrading {
        book_store: detection.book_store,
        market_registry: detection.market_registry,
        market_cache: detection.market_cache,
        universe: detection.universe,
        ws_subscription: detection.ws_subscription,
        gamma_service: detection.gamma_service,
        opportunity_pipeline: detection.opportunity_pipeline,
        calibrator: detection.calibrator,
        calibration_updater: detection.calibration_updater,
        scanner: detection.scanner,
        coalescer: detection.coalescer,
        staleness: detection.staleness,
        funnel: execution.funnel,
        validator: execution.validator,
        order_strategy: execution.order_strategy,
        data_pipeline: execution.data_pipeline,
        execution: execution.execution,
        settlement_rx: execution.settlement_rx,
        token_rx: detection.token_rx,
        market_rx: detection.market_rx,
        execution_runners: execution.execution_runners,
    }
}

async fn wire_calibration_stack(
    runtime: &RuntimeConfig,
    infra: &BuildInfra,
    clients: &BuildClients,
) -> OxideResult<(Arc<ResolutionCalibrator>, Arc<CalibrationUpdater>)> {
    let calibrator = load_resolution_calibrator(
        Arc::clone(&infra.repos.calibration),
        runtime.detection.calibration.clone(),
    )
    .await?;
    infra
        .metrics
        .calibration_bucket_count
        .set(i64::try_from(calibrator.bucket_count()).unwrap_or(i64::MAX));
    let calibration_source = Arc::new(CoreCalibrationDataSource::new(
        infra.repos.calibration.clone(),
        clients.gamma_client.clone(),
        clients.voting_oracle.clone(),
        Arc::new(OracleHealthTracker::new()),
        Arc::clone(&infra.persistence.timeseries),
    ));
    let calibration_updater = Arc::new(CalibrationUpdater::new(
        Arc::clone(&calibrator),
        calibration_source,
        runtime.detection.calibration.clone(),
    ));
    run_calibration_startup_tick(
        calibration_updater.as_ref(),
        &infra.metrics,
        calibrator.as_ref(),
    )
    .await;
    Ok((calibrator, calibration_updater))
}

async fn wire_detection(
    wiring: WiringConfig<'_>,
    infra: &BuildInfra,
    clients: &BuildClients,
    events: &CoreEventPublisher,
    shutdown: CancellationToken,
) -> OxideResult<DetectionStack> {
    let WiringConfig { deploy, runtime } = wiring;
    let book_store = Arc::new(BookStore::new(Arc::clone(&infra.metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let universe = Arc::new(MarketUniverseFilter::new(
        &runtime.market_data.enabled_categories,
    ));
    let market_cache = Arc::new(MarketCache::new(
        Arc::clone(&market_registry),
        Arc::clone(&universe),
    ));

    let (calibrator, calibration_updater) = wire_calibration_stack(runtime, infra, clients).await?;

    let detector = EndgameDetector::new(
        &runtime.detection.endgame,
        &runtime.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(clients.fee_calculator.clone()),
    );
    let scorer = EndgameScorer::new(
        &runtime.detection.endgame.scorer,
        &runtime.detection.endgame.fill_probability,
        runtime.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&runtime.detection.endgame.emission_cooldown);
    let factor_store: Arc<FactorSnapshotStore> = Arc::clone(&infra.factor_store);
    let factor_provider: Arc<dyn ControlFactorProvider> = factor_store;
    let opportunity_pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        factor_provider,
        &runtime.detection,
    ));

    // One classifier, cloned into scanner + validator; the applicator reloads
    // it once and every consumer observes the new ladder.
    let staleness = StalenessClassifier::new(&runtime.market_data);
    let scanner = Arc::new(Scanner::new(
        Arc::clone(&opportunity_pipeline),
        Arc::clone(&book_store),
        Arc::clone(&market_cache),
        staleness.clone(),
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&infra.persistence.detection_writer)),
        events.clone(),
    ));

    let (token_tx, token_rx) = flume::bounded(8192);
    let (market_tx, market_rx) = flume::bounded(512);

    let coalescer = Arc::new(Coalescer::new(
        Arc::clone(&market_registry),
        Duration::from_millis(runtime.execution.coalescer.coalesce_window_ms),
        market_tx,
        Arc::clone(&infra.metrics),
        shutdown,
    ));

    let ws_subscription = Arc::new(WsSubscriptionCoordinator::new(Arc::clone(
        &clients.ws_manager,
    )));
    let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
        gamma_client: Arc::clone(&clients.gamma_client),
        market_registry: Arc::clone(&market_registry),
        market_cache: Arc::clone(&market_cache),
        universe: Arc::clone(&universe),
        fee_calculator: Arc::clone(&clients.fee_calculator),
        market_repo: Arc::clone(&infra.repos.market),
        event_repo: Arc::clone(&infra.repos.event),
        cache: Arc::clone(&infra.cache),
        metrics: Arc::clone(&infra.metrics),
        ws_subscription: Some(Arc::clone(&ws_subscription)),
        full_sync_interval_secs: deploy.market_data.gamma.full_sync_interval_secs,
    }));

    gamma_service.sync().await.map_err(|error| {
        tracing::error!(
            %error,
            "Gamma startup sync failed — cannot start without market catalog"
        );
        error
    })?;

    tracing::info!(
        markets = market_registry.market_count(),
        "Gamma startup sync complete"
    );

    Ok(DetectionStack {
        book_store,
        market_registry,
        market_cache,
        universe,
        ws_subscription,
        gamma_service,
        opportunity_pipeline,
        calibrator,
        calibration_updater,
        scanner,
        coalescer,
        staleness,
        token_tx,
        token_rx,
        market_rx,
    })
}

fn wire_execution_loop(
    wiring: WiringConfig<'_>,
    execution_mode: &ExecutionModeHandle,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    detection: &DetectionStack,
    shutdown: CancellationToken,
) -> ExecutionLoop {
    let WiringConfig { deploy, runtime } = wiring;
    let relay_notify = Arc::new(Notify::new());
    let (settlement_tx, settlement_rx) =
        flume::bounded(deploy.settlement.lifecycle.channel_capacity);
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&risk.exposure),
        &runtime.risk.exposure_reservation_config(),
    ));
    let market_inflight = Arc::new(MarketInFlightRegistry::new());
    let validator = Arc::new(Validator::new(
        Arc::clone(&detection.book_store),
        detection.staleness.clone(),
        &runtime.execution,
        Arc::clone(&infra.metrics),
    ));
    let order_strategy = Arc::new(FokOrderStrategy::new(
        execution_mode.clone(),
        clients.clob_client.clone(),
        clients.fee_calculator.clone(),
        runtime.execution.timeout.dispatcher_timeout_ms,
        Arc::clone(&infra.metrics),
    ));
    let execution_pipeline = Arc::new(ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Arc::clone(&validator),
        plan_builder: PlanBuilder::new(
            clients.fee_calculator.clone(),
            Arc::clone(&detection.market_registry),
        ),
        dispatcher: Dispatcher::new(
            execution_mode.clone(),
            Arc::clone(&detection.book_store),
            clients.fee_calculator.clone(),
            Arc::clone(&infra.metrics),
        ),
        order_strategy: Arc::clone(&order_strategy),
        capital_manager: Arc::clone(&capital),
        risk_engine: Arc::clone(&risk.engine),
        risk_metrics: Arc::clone(&risk.metrics),
        fsm: Arc::clone(&risk.fsm),
        market_inflight: Arc::clone(&market_inflight),
        metrics: Arc::clone(&infra.metrics),
        mode: execution_mode.clone(),
        trade_repo: Arc::clone(&infra.persistence.trade_repo),
        audit_writer: Arc::clone(&infra.persistence.audit_writer),
        relay_notify: Arc::clone(&relay_notify),
        metrics_state: Arc::clone(&risk.metrics_state),
        factors: Arc::clone(&infra.factor_store),
        shadow_writer: Some(infra.shadow_writer.clone()),
    }));

    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline_port = Arc::clone(&execution_pipeline);
    let pipeline_port: Arc<dyn ExecutionPort> = pipeline_port;
    let (runner_pool, execution_runners) = ExecutionRunnerPool::new(
        deploy.execution.book_apply.shard_count,
        &pipeline_port,
        &shutdown,
        &inflight,
        &infra.metrics,
    );
    let funnel = Arc::new(Funnel::with_backpressure(
        runner_pool.shard_senders().to_vec(),
        runtime.execution.funnel.max_queue_size,
        Duration::from_millis(runtime.execution.funnel.min_dispatch_interval_ms),
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&risk.backpressure)),
    ));

    let data_pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source: clients.ws_manager.clone(),
        book_store: Arc::clone(&detection.book_store),
        market_registry: Arc::clone(&detection.market_registry),
        coalescer_tx: detection.token_tx.clone(),
        settlement_tx,
        metrics: Arc::clone(&infra.metrics),
        alerts: Arc::clone(&infra.alerts),
        backpressure: Arc::clone(&risk.backpressure),
        book_fact_writer: Some(Arc::clone(&infra.persistence.book_fact_writer)),
        book_shard_count: deploy.execution.book_apply.shard_count,
        book_channel_capacity: deploy.execution.book_apply.channel_capacity,
        shutdown,
    }));

    let execution = ExecutionBundle::new(
        execution_pipeline,
        market_inflight,
        Arc::clone(&risk.engine),
        Arc::clone(&risk.fsm),
        Arc::clone(&capital),
        relay_notify,
    );

    ExecutionLoop {
        funnel,
        validator,
        order_strategy,
        data_pipeline,
        execution,
        settlement_rx,
        execution_runners,
    }
}

async fn load_resolution_calibrator(
    calibration_repo: Arc<PgCalibrationRepository>,
    config: CalibrationConfig,
) -> OxideResult<Arc<ResolutionCalibrator>> {
    let buckets = calibration_repo
        .get_all_buckets()
        .await
        .map_err(OxideError::from)?;
    let bucket_count = buckets.len();
    let entries: Vec<CalibrationEntry> = buckets.into_iter().map(CalibrationEntry::from).collect();
    let calibrator = Arc::new(if entries.is_empty() {
        ResolutionCalibrator::empty(config)
    } else {
        ResolutionCalibrator::from_entries(entries, config)
    });
    tracing::info!(bucket_count, "loaded calibration buckets from database");
    Ok(calibrator)
}
