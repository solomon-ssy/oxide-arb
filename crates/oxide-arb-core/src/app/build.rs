//! Application composition root — wires subsystems in dependency order.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_algorithm::cooldown::InMemoryEmissionCooldown;
use oxide_arb_algorithm::endgame::EndgameDetector;
use oxide_arb_algorithm::pipeline::OpportunityPipeline;
use oxide_arb_algorithm::scorer::EndgameScorer;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_api::gamma::GammaClient;
use oxide_arb_api::keystore::Keystore;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_api::{VotingOracle, build_voting_oracle};
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::{ExposureReservationConfig, Settings};
use oxide_arb_models::domain::risk::RiskEngineState;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::types::{MarketId, MicroUsd, TokenId};
use oxide_arb_repository::postgres::{
    PgBlacklistPersistenceRepository, PgCalibrationRepository, PgEmergencyRepository,
    PgPotentialLossRepository, PgReconciliationRepository, PgRiskAuditRepository,
    PgRiskStateRepository,
};
use oxide_arb_repository::traits::{
    BlacklistPersistenceRepository, PotentialLossRepository, RiskStateRepository,
};
use oxide_arb_risk::audit::RiskAuditEvent;
use oxide_arb_risk::audit_sink::AuditSink;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_storage::cache::{MokaBackend, RedisBackend, TieredCache};
use oxide_arb_storage::clickhouse::ClickHousePool;
use oxide_arb_storage::postgres::PostgresPool;
use oxide_arb_storage::postgres::migration::{Migrator, MigratorTrait};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::task_registry::PendingTaskQueue;
use super::{
    AppContext, DataBundle, ExecutionBundle, InfraBundle, RiskBundle, RuntimeChannels,
    TradingBundle,
};
use crate::bridge::CoreOpportunityPipeline;
use crate::bridge::calibration_source::CoreCalibrationDataSource;
use crate::bridge::fee_estimator::CoreFeeEstimator;
use crate::bridge::potential_loss_store::CorePotentialLossStore;
use crate::bridge::risk_audit_sink::new_audit_sink;
use crate::bridge::risk_metrics::CoreRiskMetrics;
use crate::bridge::risk_persistence::CoreRiskPersistence;
use crate::detection::coalescer::Coalescer;
use crate::detection::funnel::Funnel;
use crate::detection::scanner::Scanner;
use crate::execution::capital_manager::CapitalManager;
use crate::execution::dispatcher::Dispatcher;
use crate::execution::execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps};
use crate::execution::fsm::ExecutionFSM;
use crate::execution::market_inflight::MarketInFlightRegistry;
use crate::execution::plan_builder::PlanBuilder;
use crate::execution::port::ExecutionPort;
use crate::execution::runner::{ExecutionRunner, ExecutionRunnerPool};
use crate::execution::tiered_strategy::OrderStrategy;
use crate::execution::validator::Validator;
use crate::exposure::in_memory::InMemoryExposureReservation;
use crate::infra::oracle_health_tracker::OracleHealthTracker;
use crate::infra::risk_decision_audit_buffer::RiskDecisionAuditBuffer;
use crate::observability::alert_dispatcher::AlertDispatcher;
use crate::observability::backpressure::BackpressurePolicy;
use crate::observability::metrics_hub::MetricsHub;
use crate::outbox::in_memory::InMemoryEventStore;
use crate::pipeline::book_store::BookStore;
use crate::pipeline::data_pipeline::{DataPipeline, DataPipelineDeps};
use crate::pipeline::market_cache::MarketCache;
use crate::pipeline::market_registry::MarketRegistry;
use crate::pipeline::staleness_classifier::StalenessClassifier;
use crate::service::risk_metrics::{ApiHealthTracker, RiskMetricsState};

struct BuildRepos {
    risk_state: Arc<PgRiskStateRepository>,
    blacklist: Arc<PgBlacklistPersistenceRepository>,
    audit: Arc<PgRiskAuditRepository>,
    emergency: Arc<PgEmergencyRepository>,
    reconciliation: Arc<PgReconciliationRepository>,
    potential_loss: Arc<PgPotentialLossRepository>,
    calibration: Arc<PgCalibrationRepository>,
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
}

struct BuildClients {
    ws_manager: Arc<ClobWsManager>,
    gamma_client: Arc<GammaClient>,
    fee_calculator: Arc<FeeCalculator>,
    voting_oracle: Arc<VotingOracle>,
    clob_client: Option<Arc<ClobClient>>,
}

struct BuildRisk {
    exposure: Arc<InMemoryExposureReservation>,
    metrics: Arc<CoreRiskMetrics>,
    metrics_state: Arc<RiskMetricsState>,
    engine: Arc<RiskEngine>,
    potential_loss_store: Arc<CorePotentialLossStore>,
    fsm: Arc<ExecutionFSM>,
    backpressure: Arc<BackpressurePolicy>,
}

struct BuildTrading {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    funnel: Arc<Funnel>,
    data_pipeline: Arc<DataPipeline>,
    execution: ExecutionBundle,
    token_rx: flume::Receiver<TokenId>,
    market_rx: flume::Receiver<MarketId>,
    execution_runners: Vec<ExecutionRunner>,
}

struct DetectionStack {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
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
    execution_runners: Vec<ExecutionRunner>,
}

impl AppContext {
    /// Build all subsystems from loaded settings (PG/CH/Redis + trading loop).
    pub async fn build(settings: Arc<Settings>, shutdown: CancellationToken) -> OxideResult<Self> {
        let mode = settings.execution.execution_mode;
        settings.ensure_valid_for_mode(mode)?;

        let infra = connect_infra(&settings).await?;
        let clients = connect_clients(&settings, shutdown.clone()).await?;
        let risk = wire_risk(&settings, &infra, &clients).await?;
        let trading = wire_trading(&settings, mode, &infra, &clients, &risk, shutdown.clone());

        Ok(Self {
            config: settings,
            infra: InfraBundle {
                pg: infra.pg_pool,
                ch: infra.ch_pool,
                cache: infra.cache,
                metrics: infra.metrics,
                alerts: infra.alerts,
                risk_decision_audit: infra.risk_decision_audit,
                risk_decision_audit_rx: infra.risk_decision_audit_rx,
            },
            data: DataBundle {
                book_store: trading.book_store,
                market_registry: trading.market_registry,
                market_cache: trading.market_cache,
                data_pipeline: trading.data_pipeline,
            },
            risk: RiskBundle {
                engine: risk.engine,
                metrics: risk.metrics,
                metrics_state: risk.metrics_state,
                exposure: risk.exposure,
                potential_loss_store: risk.potential_loss_store,
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
            runtime: RuntimeChannels {
                coalescer_token_rx: Mutex::new(Some(trading.token_rx)),
                scanner_market_rx: Mutex::new(Some(trading.market_rx)),
                execution_runners: Mutex::new(Some(trading.execution_runners)),
            },
            shutdown,
            pending_tasks: PendingTaskQueue::default(),
        })
    }
}

async fn connect_infra(settings: &Settings) -> OxideResult<BuildInfra> {
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
    let repos = BuildRepos {
        risk_state: Arc::new(PgRiskStateRepository::new(db.clone())),
        blacklist: Arc::new(PgBlacklistPersistenceRepository::new(db.clone())),
        audit: Arc::new(PgRiskAuditRepository::new(db.clone())),
        emergency: Arc::new(PgEmergencyRepository::new(db.clone())),
        reconciliation: Arc::new(PgReconciliationRepository::new(db.clone())),
        potential_loss: Arc::new(PgPotentialLossRepository::new(db.clone())),
        calibration: Arc::new(PgCalibrationRepository::new(db)),
    };

    Ok(BuildInfra {
        pg_pool,
        ch_pool,
        cache,
        metrics,
        alerts,
        risk_decision_audit,
        risk_decision_audit_rx,
        repos,
    })
}

async fn connect_clients(
    settings: &Settings,
    shutdown: CancellationToken,
) -> OxideResult<BuildClients> {
    let ws_manager = Arc::new(ClobWsManager::new(
        &settings.polymarket,
        &settings.market_data.websocket,
        shutdown,
    ));
    let gamma_client = Arc::new(GammaClient::new(settings.market_data.gamma.clone()));
    let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.polymarket.fees));
    let voting_oracle = Arc::new(build_voting_oracle(
        &settings.polymarket,
        &settings.market_data.gamma,
        &settings.settlement_oracle,
    )?);

    let clob_client = match Keystore::from_config(&settings.keys) {
        Ok(ks) => match ClobClient::connect(ks.signer_arc(), &settings.polymarket).await {
            Ok(client) => Some(Arc::new(client)),
            Err(error) => {
                tracing::warn!(%error, "ClobClient connect failed — Live/paper CLOB disabled");
                None
            }
        },
        Err(error) => {
            tracing::info!(%error, "Keystore unavailable — running without ClobClient");
            None
        }
    };

    Ok(BuildClients {
        ws_manager,
        gamma_client,
        fee_calculator,
        voting_oracle,
        clob_client,
    })
}

async fn wire_risk(
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
) -> OxideResult<BuildRisk> {
    let exposure = Arc::new(InMemoryExposureReservation::new(
        ExposureReservationConfig::default(),
    ));
    let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
    let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        Arc::clone(&clients.ws_manager),
    ));

    let risk_persistence = Arc::new(CoreRiskPersistence::new(
        infra.repos.risk_state.clone(),
        infra.repos.blacklist.clone(),
        infra.repos.audit.clone(),
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
    let audit_sink: Arc<dyn AuditSink> = infra.risk_decision_audit.clone();

    let engine = Arc::new(
        RiskEngineBuilder::new()
            .config(settings.risk.clone())
            .persistence(risk_persistence)
            .snapshot(RiskEngineState::from(&risk_state_info))
            .blacklist_entries(blacklist)
            .potential_loss_entries(potential_loss)
            .audit_sink(audit_sink)
            .clock(utc_clock())
            .build(risk_metrics.as_ref())?,
    );

    let potential_loss_store = Arc::new(CorePotentialLossStore::new(
        infra.repos.potential_loss.clone(),
    ));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&infra.metrics)));
    let post_trade_spill = Arc::new(InMemoryEventStore::new());
    let backpressure = Arc::new(BackpressurePolicy::new(
        Arc::clone(&infra.metrics),
        Some(Arc::clone(&infra.alerts)),
        Arc::clone(&post_trade_spill),
    ));

    Ok(BuildRisk {
        exposure,
        metrics: risk_metrics,
        metrics_state,
        engine,
        potential_loss_store,
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
    shutdown: CancellationToken,
) -> BuildTrading {
    let detection = wire_detection(settings, infra, clients, shutdown.clone());
    let execution = wire_execution_loop(settings, mode, infra, clients, risk, &detection, shutdown);

    BuildTrading {
        book_store: detection.book_store,
        market_registry: detection.market_registry,
        market_cache: detection.market_cache,
        opportunity_pipeline: detection.opportunity_pipeline,
        calibrator: detection.calibrator,
        calibration_updater: detection.calibration_updater,
        scanner: detection.scanner,
        coalescer: detection.coalescer,
        funnel: execution.funnel,
        data_pipeline: execution.data_pipeline,
        execution: execution.execution,
        token_rx: detection.token_rx,
        market_rx: detection.market_rx,
        execution_runners: execution.execution_runners,
    }
}

fn wire_detection(
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
    shutdown: CancellationToken,
) -> DetectionStack {
    let book_store = Arc::new(BookStore::new(Arc::clone(&infra.metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&market_registry)));

    let calibrator = Arc::new(ResolutionCalibrator::empty(
        settings.detection.calibration.clone(),
    ));
    let calibration_source = Arc::new(CoreCalibrationDataSource::new(
        infra.repos.calibration.clone(),
        clients.gamma_client.clone(),
        clients.voting_oracle.clone(),
        Arc::new(OracleHealthTracker::new()),
    ));
    let calibration_updater = Arc::new(CalibrationUpdater::new(
        Arc::clone(&calibrator),
        calibration_source,
        settings.detection.calibration.clone(),
    ));

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
    let opportunity_pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
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

    DetectionStack {
        book_store,
        market_registry,
        market_cache,
        opportunity_pipeline,
        calibrator,
        calibration_updater,
        scanner,
        coalescer,
        token_tx,
        token_rx,
        market_rx,
    }
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
    let (outcome_tx, outcome_rx) = ExecutionPipeline::outcome_channel();
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&risk.exposure),
        ExposureReservationConfig::default(),
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
        plan_builder: PlanBuilder::new(clients.fee_calculator.clone()),
        dispatcher: Dispatcher::new(mode, Arc::clone(&infra.metrics)),
        order_strategy: OrderStrategy::new(
            mode,
            clients.clob_client.clone(),
            Arc::clone(&infra.metrics),
        ),
        capital_manager: capital,
        risk_engine: Arc::clone(&risk.engine),
        risk_metrics: Arc::clone(&risk.metrics),
        fsm: Arc::clone(&risk.fsm),
        market_inflight: Arc::clone(&market_inflight),
        metrics: Arc::clone(&infra.metrics),
        execution_mode: mode,
        outcome_tx,
        backpressure: Arc::clone(&risk.backpressure),
    }));

    let inflight = Arc::new(AtomicU32::new(0));
    let pipeline_port: Arc<dyn ExecutionPort> = execution_pipeline.clone();
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
        metrics: Arc::clone(&infra.metrics),
        backpressure: Arc::clone(&risk.backpressure),
        book_shard_count: settings.execution.book_apply.shard_count,
        book_channel_capacity: settings.execution.book_apply.channel_capacity,
        shutdown,
    }));

    let execution = ExecutionBundle::new(
        execution_pipeline,
        market_inflight,
        risk.engine.clone(),
        risk.fsm.clone(),
        outcome_rx,
    );

    ExecutionLoop {
        funnel,
        data_pipeline,
        execution,
        execution_runners,
    }
}
