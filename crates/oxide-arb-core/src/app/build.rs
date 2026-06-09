//! Application composition root — wires subsystems in dependency order.

use super::{
    AppContext, ControlFactorBundle, DataBundle, ExecutionBundle, InfraBundle, RiskBundle,
    RuntimeChannels, SettlementBundle, TradingBundle, task_registry::PendingTaskQueue,
};
use crate::{
    app::periodic_services::run_calibration_startup_tick,
    bridge::{
        CoreOpportunityPipeline, calibration_source::CoreCalibrationDataSource,
        fee_estimator::CoreFeeEstimator, potential_loss_store::CorePotentialLossStore,
        risk_audit_sink::new_audit_sink, risk_metrics::CoreRiskMetrics,
        risk_persistence::CoreRiskPersistence,
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
    },
    service::{
        equity_valuator::EquityValuator,
        gamma::{GammaService, GammaServiceDeps},
        risk_metrics::{ApiHealthTracker, RiskMetricsRefreshService, RiskMetricsState},
        ws_subscription::WsSubscriptionCoordinator,
    },
};
use num_traits::ToPrimitive;
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
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    config::{CalibrationConfig, Settings},
    domain::{
        DetectionRuntimeConfig, ExecutionRuntimeConfig, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, OperatorRuntimeConfig, RiskLimitRuntimeConfig,
        RuntimeConfigDocument, SizingRuntimeConfig, control_factor::ControlFactorProvider,
        risk::RiskEngineState, runtime_config_hash, settlement::MarketSettlementRequest,
    },
    enums::{
        common::ExecutionMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    types::{MarketId, MicroUsd, RuntimeConfigActivationId, RuntimeConfigVersionId, TokenId, Usd},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgBlacklistPersistenceRepository, PgCalibrationRepository, PgControlFactorRepository,
        PgEmergencyRepository, PgEventRepository, PgFactDataRepository, PgMarketRepository,
        PgPositionRepository, PgPotentialLossRepository, PgReconciliationRepository,
        PgReportRepository, PgResolutionEventRepository, PgRiskAuditRepository,
        PgRiskStateRepository, PgRuntimeConfigVersionRepository, PgTradeRepository,
        risk_fill::PgRiskFillRepository,
    },
    traits::{
        BlacklistPersistenceRepository, CalibrationRepository, ControlFactorRepository,
        ControlFactorShadowDecisionRepository, PotentialLossRepository, RiskStateRepository,
        RuntimeConfigVersionRepository,
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
    config: Arc<Settings>,
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
    gamma_service: Arc<GammaService>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    funnel: Arc<Funnel>,
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
    gamma_service: Arc<GammaService>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    token_tx: flume::Sender<TokenId>,
    token_rx: flume::Receiver<TokenId>,
    market_rx: flume::Receiver<MarketId>,
}

struct ExecutionLoop {
    funnel: Arc<Funnel>,
    data_pipeline: Arc<DataPipeline>,
    execution: ExecutionBundle,
    settlement_rx: flume::Receiver<MarketSettlementRequest>,
    execution_runners: Vec<ExecutionRunner>,
}

impl AppContext {
    /// Build all subsystems from loaded settings (PG/CH/Redis + trading loop).
    pub async fn build(settings: Arc<Settings>, shutdown: CancellationToken) -> OxideResult<Self> {
        let mode = settings.execution.execution_mode;
        settings.ensure_valid_for_mode(mode)?;

        let (infra, persistence_workers) = connect_infra(&settings, shutdown.clone()).await?;
        let clients =
            connect_clients(&settings, shutdown.clone(), Arc::clone(&infra.metrics)).await?;
        let (risk, trading) =
            wire_risk_and_trading(&settings, mode, &infra, &clients, shutdown.clone()).await?;

        let settlement = wire_settlement_bundle(&settings, mode, &infra, &clients, &risk, &trading);
        let BuildInfra {
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

        let (settlement_service, settlement_dedup) = settlement;
        Ok(assemble_app_context(AppContextAssembly {
            config: settings,
            infra: AssembledInfra {
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
            },
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

fn wire_settlement_bundle(
    settings: &Settings,
    mode: ExecutionMode,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    trading: &BuildTrading,
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
        ctf_redeem: clients.ctf_redeem.clone(),
        market_registry: Arc::clone(&trading.market_registry),
        voting_oracle: Arc::clone(&clients.voting_oracle),
        metrics: Arc::clone(&infra.metrics),
        alerts: Arc::clone(&infra.alerts),
        audit_writer,
        metrics_refresh: risk.metrics_refresh.clone(),
        config: Arc::new(settings.settlement.clone()),
        execution_mode: mode,
    }));
    let settlement_dedup = Arc::new(SettlementDedup::new(Duration::from_secs(
        settings.settlement.lifecycle.dedup_window_secs,
    )));
    (settlement_service, settlement_dedup)
}

fn assemble_app_context(parts: AppContextAssembly) -> AppContext {
    let AppContextAssembly {
        config,
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

async fn connect_infra(
    settings: &Settings,
    shutdown: CancellationToken,
) -> OxideResult<(BuildInfra, PersistenceBackgroundWorkers)> {
    let metrics = Arc::new(MetricsHub::new());
    let telegram_token = settings.notification.telegram.bot_token.trim();
    let telegram_chat = settings.notification.telegram.chat_id.trim();
    let webhook_url = settings.notification.webhook.url.trim();
    let alerts = Arc::new(AlertDispatcher::new(
        if settings.notification.telegram.enabled && !telegram_token.is_empty() {
            Some(telegram_token)
        } else {
            None
        },
        if settings.notification.telegram.enabled && !telegram_chat.is_empty() {
            telegram_chat.parse().ok()
        } else {
            None
        },
        if settings.notification.webhook.enabled && !webhook_url.is_empty() {
            Some(webhook_url)
        } else {
            None
        },
        60,
    ));
    let (risk_decision_audit, risk_decision_audit_rx) = new_audit_sink(4096);

    let pg_pool = Arc::new(PostgresPool::connect(&settings.db.postgres).await?);
    Migrator::up(pg_pool.connection(), None).await?;

    let ch_pool = Arc::new(ClickHousePool::connect(&settings.analytics)?);
    ch_pool.ensure_schema().await?;

    let cache = Arc::new(TieredCache::new(
        MokaBackend::new(settings.cache.moka.max_capacity),
        RedisBackend::new(&settings.cache.redis).await?,
    ));

    let db = pg_pool.connection().clone();
    let write_manager = Arc::new(ChWriteManager::new_without_probe(
        settings.analytics.max_concurrent_inserts,
    ));
    let timeseries = Arc::new(ChTimeseriesRepository::new(
        ch_pool.client().clone(),
        &settings.analytics,
        write_manager,
        shutdown.clone(),
    ));
    let repos = BuildRepos {
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
    };
    ensure_runtime_config_activation(settings, repos.runtime_config.as_ref()).await?;
    let balance_fact_writer = Arc::new(BalanceFactWriter::new(Arc::clone(&repos.fact_data)));

    let control = wire_control_factors(settings, &repos, &metrics).await?;
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

/// Build the live control-factor store, refresher, and shadow writer, then run
/// the startup snapshot load (fail closed in Live when a critical safety factor
/// is lost).
async fn wire_control_factors(
    settings: &Settings,
    repos: &BuildRepos,
    metrics: &Arc<MetricsHub>,
) -> OxideResult<ControlFactorWiring> {
    let factor_store = Arc::new(FactorSnapshotStore::new(chrono::Utc::now()));
    let shadow_repo_concrete = Arc::clone(&repos.fact_data);
    let shadow_repo: Arc<dyn ControlFactorShadowDecisionRepository> = shadow_repo_concrete;
    let (shadow_writer, shadow_writer_task) =
        ShadowDecisionWriter::new(shadow_repo, Arc::clone(metrics));
    let mode = settings.execution.execution_mode;
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

async fn ensure_runtime_config_activation(
    settings: &Settings,
    repo: &dyn RuntimeConfigVersionRepository,
) -> OxideResult<()> {
    let document = runtime_config_document(settings);
    let config_json = serde_json::to_value(&document)
        .map_err(|error| OxideError::Internal(format!("runtime config encode failed: {error}")))?;
    let config_hash = runtime_config_hash(&config_json);
    let current = repo.load_current().await?;
    if current
        .as_ref()
        .is_some_and(|version| version.config_hash == config_hash)
    {
        return Ok(());
    }

    let version = match repo.load_by_hash(&config_hash).await? {
        Some(version) => version,
        None => {
            repo.create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash: config_hash.clone(),
                schema_version: document.schema_version,
                config_json,
                source: RuntimeConfigVersionSource::Bootstrap,
                created_by: "system".to_owned(),
                reason: "startup canonical runtime config import".to_owned(),
            })
            .await?
        }
    };

    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id.clone(),
        activated_at: chrono::Utc::now(),
        activated_by: "system".to_owned(),
        reason: "startup runtime config activation".to_owned(),
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
    Ok(())
}

fn runtime_config_document(settings: &Settings) -> RuntimeConfigDocument {
    RuntimeConfigDocument {
        schema_version: 1,
        operator: OperatorRuntimeConfig {
            maintenance_mode: false,
            dry_run_mode: settings.execution.execution_mode == ExecutionMode::DryRun,
        },
        detection: DetectionRuntimeConfig {
            min_profit_threshold_usd: settings.detection.min_profit_threshold_usd,
            endgame_hours_before_close: u32::try_from(
                settings.detection.endgame.settlement_window_hours,
            )
            .unwrap_or(u32::MAX),
            convergence_threshold: settings.detection.endgame.high_threshold,
            endgame: Some(settings.detection.endgame.clone()),
            calibration: Some(settings.detection.calibration.clone()),
        },
        execution: ExecutionRuntimeConfig {
            max_slippage_bps: settings
                .execution
                .timeout
                .max_validation_slippage_bps
                .to_u32()
                .unwrap_or(u32::MAX),
            order_timeout_secs: u32::try_from(
                settings.execution.timeout.dispatcher_timeout_ms / 1000,
            )
            .unwrap_or(u32::MAX),
            cooldown_after_trade_secs: u32::try_from(settings.risk.base_cooldown_secs)
                .unwrap_or(u32::MAX),
        },
        sizing: SizingRuntimeConfig {
            kelly_fraction: settings.risk.kelly_fraction,
            max_position_fraction_of_book: settings.risk.max_depth_usage_pct,
        },
        risk_limits: RiskLimitRuntimeConfig {
            max_portfolio_exposure_usd: Usd::new(settings.risk.max_total_exposure_usd),
            max_single_position_usd: Usd::new(settings.risk.max_single_market_exposure_usd),
            max_daily_loss_usd: Usd::new(settings.risk.max_daily_loss_usd),
            circuit_breaker_threshold: settings.risk.max_consecutive_misses,
        },
    }
}

async fn connect_clients(
    settings: &Settings,
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
        &settings.polymarket,
        &settings.market_data.websocket,
        shutdown,
        Some(on_events_dropped),
        Some(reject_hook),
    ));
    let gamma_client = Arc::new(GammaClient::new(settings.market_data.gamma.clone()));
    let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.polymarket.fees));
    let voting_oracle = Arc::new(build_voting_oracle(
        &settings.polymarket,
        &settings.market_data.gamma,
        &settings.settlement.oracle,
        &settings.settlement.contracts,
    )?);

    let (clob_client, ctf_redeem, holder_address) = match Keystore::from_config(&settings.keys) {
        Ok(ks) => {
            let holder_address = ks.address_string();
            let signer = ks.signer_arc();
            let ctf_redeem = match CtfRedeemClient::new(
                Arc::clone(&signer),
                settings.polymarket.onchain.rpc_url.clone(),
                settings.settlement.contracts.clone(),
                settings.settlement.redeem.clone(),
                settings.polymarket.chain_id,
            ) {
                Ok(client) => Some(Arc::new(client)),
                Err(error) => {
                    tracing::warn!(%error, "CTF redeem client unavailable");
                    None
                }
            };
            let clob_client = match ClobClient::connect(signer, &settings.polymarket).await {
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
            (
                None,
                None,
                format!("unavailable:{}", settings.execution.execution_mode.as_str()),
            )
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
    settings: &Settings,
    mode: ExecutionMode,
    infra: &BuildInfra,
    clients: &BuildClients,
    shutdown: CancellationToken,
) -> OxideResult<(BuildRisk, BuildTrading)> {
    let detection = wire_detection(settings, infra, clients, shutdown.clone()).await?;
    let risk = wire_risk(settings, infra, clients, &detection).await?;
    let trading = wire_trading(settings, mode, infra, clients, &risk, detection, shutdown);
    Ok((risk, trading))
}

async fn wire_risk(
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
    detection: &DetectionStack,
) -> OxideResult<BuildRisk> {
    let exposure = Arc::new(InMemoryExposureReservation::new(
        settings.risk.exposure_reservation_config(),
    ));
    let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
    let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
    let mode = settings.execution.execution_mode;
    if mode != ExecutionMode::Live {
        metrics_state.seed_simulated_snapshot(mode, Usd::new(settings.risk.bankroll_usd));
    }
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        Arc::clone(&clients.ws_manager),
        mode,
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
            .config(settings.risk.clone())
            .persistence(risk_persistence)
            .snapshot(RiskEngineState::from(&risk_state_info))
            .blacklist_entries(blacklist)
            .potential_loss_entries(potential_loss)
            .potential_loss_store(potential_loss_store.clone())
            .audit_sink(audit_sink)
            .clock(utc_clock())
            .build(risk_metrics.as_ref())?,
    );

    let fsm = Arc::new(ExecutionFSM::new(
        Arc::clone(&infra.metrics),
        Arc::clone(&infra.alerts),
    ));
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&infra.metrics),
        settings.execution.book_apply.shard_count.max(1),
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
    settings: &Settings,
    mode: ExecutionMode,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    detection: DetectionStack,
    shutdown: CancellationToken,
) -> BuildTrading {
    let execution = wire_execution_loop(settings, mode, infra, clients, risk, &detection, shutdown);

    BuildTrading {
        book_store: detection.book_store,
        market_registry: detection.market_registry,
        market_cache: detection.market_cache,
        gamma_service: detection.gamma_service,
        opportunity_pipeline: detection.opportunity_pipeline,
        calibrator: detection.calibrator,
        calibration_updater: detection.calibration_updater,
        scanner: detection.scanner,
        coalescer: detection.coalescer,
        funnel: execution.funnel,
        data_pipeline: execution.data_pipeline,
        execution: execution.execution,
        settlement_rx: execution.settlement_rx,
        token_rx: detection.token_rx,
        market_rx: detection.market_rx,
        execution_runners: execution.execution_runners,
    }
}

async fn wire_calibration_stack(
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
) -> OxideResult<(Arc<ResolutionCalibrator>, Arc<CalibrationUpdater>)> {
    let calibrator = load_resolution_calibrator(
        Arc::clone(&infra.repos.calibration),
        settings.detection.calibration.clone(),
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
        settings.detection.calibration.clone(),
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
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
    shutdown: CancellationToken,
) -> OxideResult<DetectionStack> {
    let book_store = Arc::new(BookStore::new(Arc::clone(&infra.metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&market_registry)));

    let (calibrator, calibration_updater) =
        wire_calibration_stack(settings, infra, clients).await?;

    let detector = EndgameDetector::new(
        &settings.detection.endgame,
        &settings.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(clients.fee_calculator.clone()),
    );
    let scorer = EndgameScorer::new(
        settings.detection.endgame.scorer.clone(),
        &settings.detection.endgame.fill_probability,
        settings.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&settings.detection.endgame.emission_cooldown);
    let min_profit = MicroUsd::try_from_decimal(settings.detection.min_profit_threshold_usd)
        .unwrap_or(MicroUsd::ZERO);
    let factor_store: Arc<FactorSnapshotStore> = Arc::clone(&infra.factor_store);
    let factor_provider: Arc<dyn ControlFactorProvider> = factor_store;
    let opportunity_pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        factor_provider,
        min_profit,
        &settings.detection.endgame.scorer,
    ));

    let staleness = StalenessClassifier::new(&settings.market_data);
    let scanner = Arc::new(Scanner::new(
        Arc::clone(&opportunity_pipeline),
        Arc::clone(&book_store),
        Arc::clone(&market_cache),
        staleness,
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&infra.persistence.detection_writer)),
    ));

    let (token_tx, token_rx) = flume::bounded(8192);
    let (market_tx, market_rx) = flume::bounded(512);

    let coalescer = Arc::new(Coalescer::new(
        Arc::clone(&market_registry),
        Duration::from_millis(settings.execution.coalescer.coalesce_window_ms),
        market_tx,
        Arc::clone(&infra.metrics),
        shutdown,
    ));

    let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
        gamma_client: clients.gamma_client.clone(),
        market_registry: Arc::clone(&market_registry),
        market_cache: Arc::clone(&market_cache),
        fee_calculator: clients.fee_calculator.clone(),
        market_repo: infra.repos.market.clone(),
        event_repo: infra.repos.event.clone(),
        cache: infra.cache.clone(),
        metrics: infra.metrics.clone(),
        ws_subscription: Some(Arc::new(WsSubscriptionCoordinator::new(Arc::clone(
            &clients.ws_manager,
        )))),
        full_sync_interval_secs: settings.market_data.gamma.full_sync_interval_secs,
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
        gamma_service,
        opportunity_pipeline,
        calibrator,
        calibration_updater,
        scanner,
        coalescer,
        token_tx,
        token_rx,
        market_rx,
    })
}

fn wire_execution_loop(
    settings: &Settings,
    mode: ExecutionMode,
    infra: &BuildInfra,
    clients: &BuildClients,
    risk: &BuildRisk,
    detection: &DetectionStack,
    shutdown: CancellationToken,
) -> ExecutionLoop {
    let relay_notify = Arc::new(Notify::new());
    let (settlement_tx, settlement_rx) =
        flume::bounded(settings.settlement.lifecycle.channel_capacity);
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&risk.exposure),
        settings.risk.exposure_reservation_config(),
    ));
    let market_inflight = Arc::new(MarketInFlightRegistry::new());
    let execution_pipeline = Arc::new(ExecutionPipeline::new(ExecutionPipelineDeps {
        validator: Validator::new(
            Arc::clone(&detection.book_store),
            StalenessClassifier::new(&settings.market_data),
            settings.execution.timeout.max_validation_slippage_bps,
            settings.execution.endgame_latency.max_book_to_order_ms,
            Arc::clone(&infra.metrics),
        ),
        plan_builder: PlanBuilder::new(
            clients.fee_calculator.clone(),
            Arc::clone(&detection.market_registry),
        ),
        dispatcher: Dispatcher::new(
            mode,
            match mode {
                ExecutionMode::Paper => Some(Arc::clone(&detection.book_store)),
                ExecutionMode::DryRun | ExecutionMode::Live => None,
            },
            clients.fee_calculator.clone(),
            Arc::clone(&infra.metrics),
        ),
        order_strategy: FokOrderStrategy::new(
            mode,
            clients.clob_client.clone(),
            clients.fee_calculator.clone(),
            settings.execution.timeout.dispatcher_timeout_ms,
            Arc::clone(&infra.metrics),
        ),
        capital_manager: Arc::clone(&capital),
        risk_engine: Arc::clone(&risk.engine),
        risk_metrics: Arc::clone(&risk.metrics),
        fsm: Arc::clone(&risk.fsm),
        market_inflight: Arc::clone(&market_inflight),
        metrics: Arc::clone(&infra.metrics),
        execution_mode: mode,
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
        settings.execution.book_apply.shard_count,
        &pipeline_port,
        &shutdown,
        &inflight,
        &infra.metrics,
    );
    let funnel = Arc::new(Funnel::with_backpressure(
        runner_pool.shard_senders().to_vec(),
        settings.execution.funnel.max_queue_size,
        Duration::from_millis(settings.execution.funnel.min_dispatch_interval_ms),
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
        book_shard_count: settings.execution.book_apply.shard_count,
        book_channel_capacity: settings.execution.book_apply.channel_capacity,
        shutdown,
    }));

    let execution = ExecutionBundle::new(
        execution_pipeline,
        market_inflight,
        risk.engine.clone(),
        risk.fsm.clone(),
        Arc::clone(&capital),
        relay_notify,
    );

    ExecutionLoop {
        funnel,
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
