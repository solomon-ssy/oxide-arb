//! Application context — system composition root and lifecycle manager.
//!
//! `AppContext` owns all subsystem instances and orchestrates startup,
//! run, and graceful shutdown. The struct is decomposed into four bundles
//! to avoid a 40+ field god struct.

pub mod bootstrap;
pub mod build;
pub mod lifecycle;
pub mod periodic_services;
pub mod task_id;
pub mod task_registry;

use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    bridge::{
        CoreOpportunityPipeline, execution_mode::ExecutionModeHandle, market_data::CoreMarketData,
        metrics_scrape::CoreMetricsScrape, potential_loss_store::CorePotentialLossStore,
        risk_metrics::CoreRiskMetrics, trading_gate::TradingGate,
    },
    control::{
        ControlFactorRegistry,
        factor_refresher::FactorRefresher,
        factor_shadow::ShadowWriterTask,
        factor_snapshot::FactorSnapshotStore,
        mode_transition::{CoreRuntimeControl, CoreRuntimeControlDeps},
        replay::CoreReplay,
    },
    detection::{
        coalescer::Coalescer,
        funnel::Funnel,
        scanner::Scanner,
        scanner_task::{ScannerTask, ScannerTaskDeps},
    },
    execution::{
        capital_manager::CapitalManager,
        execution_pipeline::ExecutionPipeline,
        fsm::ExecutionFSM,
        heartbeat::HeartbeatTask,
        market_inflight::MarketInFlightRegistry,
        runner::ExecutionRunner,
        settlement::{
            dedup::SettlementDedup, service::MarketSettlementService, task::MarketSettlementTask,
        },
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::{
        health_checker::HealthChecker, periodic_task::PeriodicTask,
        risk_decision_audit_buffer::RiskDecisionAuditBuffer,
        risk_decision_audit_drain::spawn_risk_decision_audit_drain,
    },
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        balance_fact_writer::BalanceFactWriter,
        book_fact_writer::BookFactWriter,
        execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_registry::MarketRegistry,
    },
    post_trade::{
        consumer::PostTradeConsumer,
        relay::{PostTradeRelay, PostTradeRelayDeps},
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        book_update_coalescer::BookUpdateCoalescer,
        gamma::GammaService,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
    },
};
use chrono::Utc;
use flume::Receiver;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::{clob::ClobClient, ws::ClobWsManager};
use oxide_arb_control::{
    evidence::engine::EvidenceEngine,
    materialization::{
        MaterializationRunner, MaterializationRunnerDeps, PointInTimeResolver, ResolverRepositories,
    },
    scheduler::{MaterializationScheduler, ScheduleAlert, SchedulePolicy},
};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        CoreEvent, CoreEventPublisher, MarketDataPort, NewOperationLog, ReplayPort,
        RuntimeConfigPort, RuntimeConfigRef, RuntimeControlPort,
        settlement::MarketSettlementRequest,
    },
    enums::common::AlertLevel,
    types::{MarketId, TokenId},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgCalibrationRepository, PgControlFactorRepository, PgEventRepository,
        PgFactDataRepository, PgMarketRepository, PgMenuRepository, PgOperationLogRepository,
        PgPositionRepository, PgPotentialLossRepository, PgReconciliationRepository,
        PgReportRepository, PgResolutionEventRepository, PgRiskAuditRepository,
        PgRiskStateRepository, PgRoleMenuRepository, PgRolePermissionRepository, PgRoleRepository,
        PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository, PgTradeRepository,
        PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        ControlFactorRepository, ControlFactorShadowDecisionRepository,
        EvidenceTimeseriesRepository, MarketRepository, OperationLogRepository, PositionRepository,
        ReportRepository, RiskAuditRepository, RuntimeConfigVersionRepository, TradeRepository,
    },
};
use oxide_arb_risk::{audit::RiskAuditEvent, engine::RiskEngine};
use oxide_arb_storage::{cache::TieredCache, clickhouse::ClickHousePool, postgres::PostgresPool};
use oxide_arb_web::{
    AppState,
    audit::{OperationLogBuffer, spawn_operation_log_writer},
    auth::casbin::CasbinService,
    jwt::{JwtService, RedisTokenBlacklist, TokenBlacklist},
    readiness::PgRedisReadiness,
    routes, spawn_web_server,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, time::interval};
use tokio_util::sync::CancellationToken;

/// Max trades claimed per post-trade relay drain iteration.
const POST_TRADE_RELAY_BATCH_SIZE: u64 = 128;
/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    pub cache: Arc<TieredCache>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    pub risk_decision_audit_rx: Mutex<Option<Receiver<RiskAuditEvent>>>,
    pub trade_repo: Arc<PgTradeRepository>,
    pub position_repo: Arc<PgPositionRepository>,
    pub report_repo: Arc<PgReportRepository>,
    pub fact_data_repo: Arc<PgFactDataRepository>,
    pub calibration_repo: Arc<PgCalibrationRepository>,
    pub risk_state_repo: Arc<PgRiskStateRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub balance_fact_writer: Arc<BalanceFactWriter>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub holder_address: String,
}

/// Data pipeline subsystem: WS event loop, order books, market metadata.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
}

/// Risk management subsystem.
pub struct RiskBundle {
    pub engine: Arc<RiskEngine>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub potential_loss_store: Arc<CorePotentialLossStore>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
}

/// Execution subsystem wired after opportunity detection.
pub struct ExecutionBundle {
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub trading_gate: TradingGate,
    pub capital_manager: Arc<CapitalManager>,
    /// Shared with the pipeline; rung after each `*_observed` write to wake the relay.
    pub relay_notify: Arc<Notify>,
}

impl ExecutionBundle {
    pub fn new(
        pipeline: Arc<ExecutionPipeline>,
        market_inflight: Arc<MarketInFlightRegistry>,
        risk_engine: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        capital_manager: Arc<CapitalManager>,
        relay_notify: Arc<Notify>,
    ) -> Self {
        Self {
            pipeline,
            market_inflight,
            trading_gate: TradingGate::new(risk_engine, fsm),
            capital_manager,
            relay_notify,
        }
    }
}

/// Trading subsystem: detection, execution, algorithm.
pub struct TradingBundle {
    pub opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    pub calibrator: Arc<ResolutionCalibrator>,
    pub calibration_updater: Arc<CalibrationUpdater>,
    pub scanner: Arc<Scanner>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,
    pub fsm: Arc<ExecutionFSM>,
    pub execution: Option<ExecutionBundle>,
    pub clob_client: Option<Arc<ClobClient>>,
    pub ws_manager: Arc<ClobWsManager>,
}

/// Live control-factor subsystem (Phase 5.6): snapshot store, refresher, and
/// the shadow-decision writer drain task.
pub struct ControlFactorBundle {
    pub store: Arc<FactorSnapshotStore>,
    pub refresher: Arc<FactorRefresher>,
    /// Governance registry wired to the refresher notify handle (publish/rollback
    /// wake the snapshot reload without waiting for the periodic poll).
    pub registry: Arc<ControlFactorRegistry>,
    pub shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

/// Market settlement subsystem.
pub struct SettlementBundle {
    pub service: Arc<MarketSettlementService>,
    pub dedup: Arc<SettlementDedup>,
    settlement_rx: Mutex<Option<Receiver<MarketSettlementRequest>>>,
}

/// One-shot channel receivers consumed when registering runtime tasks.
pub struct RuntimeChannels {
    pub coalescer_token_rx: Mutex<Option<flume::Receiver<TokenId>>>,
    pub scanner_market_rx: Mutex<Option<flume::Receiver<MarketId>>>,
    pub execution_runners: Mutex<Option<Vec<ExecutionRunner>>>,
}

/// System composition root — owns all subsystems.
///
/// Decomposed into four bundles (`InfraBundle`, `DataBundle`,
/// `RiskBundle`, `TradingBundle`) to avoid a 40+ flat-field struct.
pub struct AppContext {
    /// Deploy configuration (restart to apply).
    pub config: Arc<DeployConfig>,
    /// Active runtime-config snapshot (hot-reloadable via the applicator).
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Activation propagation surface (also the web `RuntimeConfigPort`).
    pub applicator: Arc<RuntimeConfigApplicator>,
    /// Atomically swappable live execution mode shared by every hot-path reader.
    pub execution_mode: ExecutionModeHandle,
    /// Non-blocking producer handle for the real-time event bus.
    pub events: CoreEventPublisher,
    /// Receiver consumed once by the WebSocket broadcaster task.
    pub event_rx: Mutex<Option<Receiver<CoreEvent>>>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub risk: RiskBundle,
    pub trading: TradingBundle,
    pub control: ControlFactorBundle,
    pub settlement: SettlementBundle,
    pub runtime: RuntimeChannels,
    pub shutdown: CancellationToken,
    pub pending_tasks: PendingTaskQueue,
}

impl AppContext {
    /// Clone the non-blocking event publisher for a producer (e.g. the web
    /// governance handlers or a periodic service).
    #[must_use]
    pub fn event_publisher(&self) -> CoreEventPublisher {
        self.events.clone()
    }

    /// Queue the background task that drains pre-trade audit events to Postgres.
    ///
    /// Consumes `infra.risk_decision_audit_rx` — call at most once before `AppRunner::run`.
    pub fn queue_risk_decision_audit_drain(&self, repo: Arc<PgRiskAuditRepository>) {
        let Some(rx) = self.infra.risk_decision_audit_rx.lock().take() else {
            tracing::warn!("risk decision audit drain already registered or rx unavailable");
            return;
        };

        self.pending_tasks
            .push(TaskId::RiskAuditBatch, move |shutdown| async move {
                if let Err(error) = spawn_risk_decision_audit_drain(rx, repo, shutdown).await {
                    tracing::error!(%error, "risk decision audit drain exited with error");
                }
            });
    }

    /// Queue market settlement worker consuming resolved-market events.
    pub fn queue_market_settlement_task(&self) {
        let Some(rx) = self.settlement.settlement_rx.lock().take() else {
            tracing::warn!("market settlement task already registered or rx unavailable");
            return;
        };
        let service = Arc::clone(&self.settlement.service);
        let dedup = Arc::clone(&self.settlement.dedup);
        let metrics = Arc::clone(&self.infra.metrics);

        self.pending_tasks
            .push(TaskId::MarketSettlement, move |shutdown| async move {
                let task = MarketSettlementTask::new(rx, service, dedup, metrics, shutdown);
                if let Err(error) = task.run().await {
                    tracing::error!(%error, "market settlement task exited with error");
                }
            });
    }

    /// Queue the durable post-trade relay (notify-woken + periodic crash-recovery poll).
    pub fn queue_post_trade_relay(&self) {
        let Some(exec) = &self.trading.execution else {
            tracing::warn!("post-trade relay skipped — execution bundle not configured");
            return;
        };

        let trade_repo = Arc::clone(&self.infra.trade_repo);
        let trade_repo: Arc<dyn TradeRepository> = trade_repo;
        let position_repo = Arc::clone(&self.infra.position_repo);
        let position_repo: Arc<dyn PositionRepository> = position_repo;
        let calibration_repo = Arc::clone(&self.infra.calibration_repo);
        let consumer = PostTradeConsumer {
            risk_engine: Arc::clone(&self.risk.engine),
            risk_metrics: Arc::clone(&self.risk.metrics),
            fsm: Arc::clone(&self.trading.fsm),
            trade_repo: Arc::clone(&trade_repo),
            position_repo,
            calibration_repo,
            audit_writer: Arc::clone(&self.infra.audit_writer),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            metrics_refresh: self.risk.metrics_refresh.clone(),
            metrics: Arc::clone(&self.infra.metrics),
            events: self.event_publisher(),
        };
        // Relay timing (trade_confirm_*) is read from the runtime-config store
        // on every cycle, so activations apply on the next poll.
        let relay = PostTradeRelay::new(PostTradeRelayDeps {
            consumer,
            trade_repo,
            notify: Arc::clone(&exec.relay_notify),
            capital_manager: Arc::clone(&exec.capital_manager),
            batch_size: POST_TRADE_RELAY_BATCH_SIZE,
            runtime: Arc::clone(&self.runtime_config),
            metrics: Arc::clone(&self.infra.metrics),
        });

        self.pending_tasks
            .push(TaskId::PostTradeRelay, move |shutdown| async move {
                if let Err(error) = relay.run(shutdown).await {
                    tracing::error!(%error, "post-trade relay exited with error");
                }
            });
    }

    /// Queue venue heartbeat probe (self-gates on the live mode each tick).
    pub fn queue_execution_heartbeat(&self, interval_secs: u64) {
        let Some(clob_client) = self.trading.clob_client.as_ref() else {
            tracing::info!("execution heartbeat skipped — ClobClient unavailable");
            return;
        };
        let clob_client = Arc::clone(clob_client);
        let risk_engine = Arc::clone(&self.risk.engine);
        let fsm = Arc::clone(&self.trading.fsm);
        let mode = self.execution_mode.clone();

        self.pending_tasks
            .push(TaskId::ExecutionHeartbeat, move |shutdown| async move {
                let task = HeartbeatTask::new(
                    clob_client,
                    risk_engine,
                    fsm,
                    interval_secs,
                    shutdown,
                    mode,
                );
                if let Err(error) = task.run().await {
                    tracing::error!(%error, "execution heartbeat exited with error");
                }
            });
    }

    /// Queue one registered task per execution shard.
    pub fn queue_execution_runners(&self, runners: Vec<ExecutionRunner>) {
        if runners.is_empty() {
            tracing::warn!("no execution runners to queue");
            return;
        }

        for (index, runner) in runners.into_iter().enumerate() {
            let shard = u8::try_from(index).unwrap_or(u8::MAX);
            self.pending_tasks.push(
                TaskId::ExecutionRunner { shard },
                move |_token| async move {
                    if let Err(error) = runner.run().await {
                        tracing::error!(shard, %error, "execution runner exited with error");
                    }
                },
            );
        }
    }

    /// Register the core trading loop tasks (WS → books → detect → execute).
    pub fn queue_runtime_tasks(&self) {
        let data_pipeline = Arc::clone(&self.data.data_pipeline);
        self.pending_tasks
            .push(TaskId::DataPipeline, move |_token| async move {
                if let Err(error) = data_pipeline.run().await {
                    tracing::error!(%error, "data pipeline exited with error");
                }
            });

        let coalescer = Arc::clone(&self.trading.coalescer);
        let token_rx = self.runtime.coalescer_token_rx.lock().take();
        self.pending_tasks
            .push(TaskId::Coalescer, move |_token| async move {
                if let Err(error) = coalescer.run_with_ingress(token_rx).await {
                    tracing::error!(%error, "coalescer exited with error");
                }
            });

        let Some(market_rx) = self.runtime.scanner_market_rx.lock().take() else {
            tracing::warn!("scanner task skipped — market channel unavailable");
            return;
        };
        let scanner_task = ScannerTask::new(ScannerTaskDeps {
            rx: market_rx,
            scanner: Arc::clone(&self.trading.scanner),
            market_cache: Arc::clone(&self.data.market_cache),
            funnel: Arc::clone(&self.trading.funnel),
            runtime: Arc::clone(&self.runtime_config),
            shutdown: self.shutdown.clone(),
        });
        self.pending_tasks
            .push(TaskId::Scanner, move |_token| async move {
                if let Err(error) = scanner_task.run().await {
                    tracing::error!(%error, "scanner task exited with error");
                }
            });

        let funnel = Arc::clone(&self.trading.funnel);
        let shutdown = self.shutdown.clone();
        self.pending_tasks
            .push(TaskId::Funnel, move |_token| async move {
                if let Err(error) = funnel.run(shutdown).await {
                    tracing::error!(%error, "funnel exited with error");
                }
            });

        // Live control-factor refresher (periodic + notify) and shadow writer.
        let refresher = Arc::clone(&self.control.refresher);
        self.pending_tasks
            .push(TaskId::FactorRefresher, move |token| async move {
                refresher.run(token).await;
            });
        let shadow_task = self.control.shadow_writer_task.lock().take();
        if let Some(shadow_task) = shadow_task {
            self.pending_tasks
                .push(TaskId::ShadowDecisionWriter, move |token| async move {
                    shadow_task.run(token).await;
                });
        }

        if self.trading.execution.is_some() {
            let runners = self.runtime.execution_runners.lock().take();
            if let Some(runners) = runners {
                self.queue_execution_runners(runners);
            }
            self.queue_post_trade_relay();
            self.queue_execution_heartbeat(30);
        }
    }
}

// ── Web + governance process wiring ─────────────────────────────────────────
//
// Assembles the `oxide-arb-web` `AppState` from core subsystems (the
// dependency-inverted runtime control port, RBAC repositories, Casbin, JWT, and
// the operation-log + event pipelines) and registers the web-facing background
// tasks into the unified shutdown-staged task queue.

/// Operation-log writer flush threshold (rows) and cadence.
const OPERATION_LOG_BATCH_SIZE: usize = 64;
const OPERATION_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// Operation-log buffer capacity (non-blocking producer).
const OPERATION_LOG_BUFFER_CAPACITY: usize = 4096;
/// Control-factor scheduler tick cadence.
const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(300);
const SCHEDULER_JITTER_PCT: f64 = 0.1;
/// Materialization execute-worker poll cadence + per-tick claim budget.
const EXECUTE_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(30);
const EXECUTE_WORKER_CLAIM_LIMIT: u64 = 4;

impl AppContext {
    /// Assemble the web [`AppState`] and the operation-log writer's receiver.
    ///
    /// Constructs the RBAC repositories, the live Casbin enforcer (with its
    /// policy loaded), the JWT service (Redis-backed revocation), the
    /// dependency-inverted runtime control port, and the operation-log buffer.
    async fn assemble_web_state(&self) -> OxideResult<(AppState, Receiver<NewOperationLog>)> {
        let db = self.infra.pg.connection().clone();

        let jwt_blacklist = Arc::new(
            RedisTokenBlacklist::connect(&self.config.cache.redis)
                .await
                .map_err(|error| OxideError::Internal(format!("jwt blacklist: {error}")))?,
        );
        let blacklist: Arc<dyn TokenBlacklist> = jwt_blacklist.clone();
        let jwt = Arc::new(JwtService::new(
            &self.config.web.jwt,
            Arc::clone(&blacklist),
        ));
        let readiness = Arc::new(PgRedisReadiness::new(db.clone(), blacklist));
        let metrics = Arc::new(CoreMetricsScrape::new(Arc::clone(&self.infra.metrics)));

        let casbin = Arc::new(
            CasbinService::new(db.clone())
                .await
                .map_err(|error| OxideError::Internal(format!("casbin init: {error}")))?,
        );
        let perm_checker = Arc::new(routes::init_rbac_rules());

        let control_factors: Arc<dyn ControlFactorRepository> =
            Arc::new(PgControlFactorRepository::new(db.clone()));
        let runtime_config: Arc<dyn RuntimeConfigVersionRepository> =
            Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()));
        let shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository> =
            Arc::new(PgFactDataRepository::new(db.clone()));
        let operation_logs: Arc<dyn OperationLogRepository> =
            Arc::new(PgOperationLogRepository::new(db.clone()));
        let trades: Arc<dyn TradeRepository> = Arc::new(PgTradeRepository::new(db.clone()));
        let markets: Arc<dyn MarketRepository> = Arc::new(PgMarketRepository::new(db.clone()));
        let reports: Arc<dyn ReportRepository> = Arc::new(PgReportRepository::new(db.clone()));
        let risk_audit: Arc<dyn RiskAuditRepository> =
            Arc::new(PgRiskAuditRepository::new(db.clone()));
        let evidence: Arc<dyn EvidenceTimeseriesRepository> =
            Arc::clone(&self.infra.timeseries) as Arc<dyn EvidenceTimeseriesRepository>;

        let (operation_log, operation_log_rx) =
            OperationLogBuffer::new(OPERATION_LOG_BUFFER_CAPACITY);

        let control: Arc<dyn RuntimeControlPort> =
            Arc::new(CoreRuntimeControl::new(self.control_deps()));
        let market_data: Arc<dyn MarketDataPort> = Arc::new(CoreMarketData::new(
            Arc::clone(&self.data.book_store),
            Arc::clone(&self.trading.ws_manager),
        ));
        let replay: Arc<dyn ReplayPort> = Arc::new(CoreReplay::new(Arc::new(
            PgControlFactorRepository::new(db.clone()),
        )));
        let applicator = Arc::clone(&self.applicator);
        let runtime_config_apply: Arc<dyn RuntimeConfigPort> = applicator;

        let state = AppState {
            deploy: Arc::clone(&self.config),
            runtime_config_apply,
            jwt,
            jwt_blacklist,
            users: Arc::new(PgUserRepository::new(db.clone())),
            roles: Arc::new(PgRoleRepository::new(db.clone())),
            menus: Arc::new(PgMenuRepository::new(db.clone())),
            user_roles: Arc::new(PgUserRoleRepository::new(db.clone())),
            role_menus: Arc::new(PgRoleMenuRepository::new(db.clone())),
            role_permissions: Arc::new(PgRolePermissionRepository::new(db.clone())),
            positions: Arc::new(PgPositionRepository::new(db)),
            trades,
            markets,
            reports,
            evidence,
            risk_audit,
            casbin,
            perm_checker,
            registry: Arc::clone(&self.control.registry),
            control_factors,
            runtime_config,
            shadow_decisions,
            operation_logs,
            operation_log,
            control,
            market_data,
            replay,
            events: self.event_publisher(),
            ws_sessions: SessionRegistry::default(),
            metrics,
            readiness,
        };
        Ok((state, operation_log_rx))
    }

    /// Build the runtime control port's dependencies from the live subsystems.
    fn control_deps(&self) -> CoreRuntimeControlDeps {
        let health_checker = Arc::new(HealthChecker::new(
            Arc::clone(&self.infra.pg),
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.trading.ws_manager),
            self.trading.clob_client.clone(),
            self.execution_mode.clone(),
        ));
        CoreRuntimeControlDeps {
            execution_mode: self.execution_mode.clone(),
            risk_engine: Arc::clone(&self.risk.engine),
            fsm: Arc::clone(&self.trading.fsm),
            exposure: Arc::clone(&self.risk.exposure),
            metrics: Arc::clone(&self.risk.metrics),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            metrics_refresh: self.risk.metrics_refresh.clone(),
            clob_client: self.trading.clob_client.clone(),
            market_registry: Arc::clone(&self.data.market_registry),
            health_checker,
            deploy: Arc::clone(&self.config),
            runtime_config: Arc::clone(&self.runtime_config),
            system_runtime_state: Arc::new(PgSystemRuntimeStateRepository::new(
                self.infra.pg.connection().clone(),
            )),
        }
    }

    /// Assemble web state and queue the web server, operation-log writer, and the
    /// WebSocket broadcaster (all over the shared session registry).
    pub async fn queue_web_services(&self) -> OxideResult<()> {
        let (state, operation_log_rx) = self.assemble_web_state().await?;
        let operation_logs = Arc::clone(&state.operation_logs);
        self.queue_operation_log_writer(operation_log_rx, operation_logs);
        self.queue_ws_broadcaster(state.ws_sessions.clone());
        self.queue_book_update_coalescer(state.ws_sessions.clone());
        self.queue_web_server(state);
        Ok(())
    }

    /// Queue the book-update coalescer: throttled per-market book pushes to the
    /// `CoreEvent` bus for markets WebSocket sessions are actively watching.
    fn queue_book_update_coalescer(&self, sessions: SessionRegistry) {
        let coalescer = BookUpdateCoalescer::new(
            Arc::clone(&self.data.book_store),
            Arc::clone(&self.data.market_registry),
            sessions,
            self.events.clone(),
        );
        self.pending_tasks
            .push(TaskId::BookUpdateCoalescer, move |shutdown| async move {
                coalescer.run(shutdown).await;
            });
    }

    /// Queue the WebSocket broadcaster, consuming the `CoreEvent` bus and fanning
    /// out to the shared session registry. Consumes `event_rx` once.
    fn queue_ws_broadcaster(&self, registry: SessionRegistry) {
        let Some(event_rx) = self.event_rx.lock().take() else {
            tracing::warn!("ws broadcaster already registered or event_rx unavailable");
            return;
        };
        self.pending_tasks
            .push(TaskId::WsBroadcaster, move |shutdown| async move {
                spawn_ws_broadcaster(event_rx, registry, shutdown).await;
            });
    }

    /// Queue the HTTP/WebSocket server (drained first, stage 0).
    fn queue_web_server(&self, state: AppState) {
        let config = self.config.web.clone();
        self.pending_tasks
            .push(TaskId::WebServer, move |shutdown| async move {
                if let Err(error) = spawn_web_server(state, config, shutdown).await {
                    tracing::error!(%error, "web server exited with error");
                }
            });
    }

    /// Queue the operation-log writer (drains the audit buffer to Postgres).
    fn queue_operation_log_writer(
        &self,
        rx: Receiver<NewOperationLog>,
        operation_logs: Arc<dyn OperationLogRepository>,
    ) {
        self.pending_tasks
            .push(TaskId::OperationLogWriter, move |shutdown| async move {
                spawn_operation_log_writer(
                    rx,
                    operation_logs,
                    OPERATION_LOG_BATCH_SIZE,
                    OPERATION_LOG_FLUSH_INTERVAL,
                    shutdown,
                )
                .await;
            });
    }

    /// Queue the enqueue-only control-factor materialization scheduler.
    ///
    /// The scheduler **never publishes**; it only evaluates cadences and enqueues
    /// `Queued` runs, mapping overdue / stale cadences to alerts.
    pub fn queue_control_factor_scheduler(&self) {
        let repo: Arc<dyn ControlFactorRepository> = Arc::new(PgControlFactorRepository::new(
            self.infra.pg.connection().clone(),
        ));
        let policy = SchedulePolicy::production_default(
            RuntimeConfigRef::ActiveAt { at: Utc::now() },
            "scheduler",
            option_env!("GIT_SHA").unwrap_or("unknown"),
        );
        let scheduler = Arc::new(MaterializationScheduler::new(repo, policy));
        let alerts = Arc::clone(&self.infra.alerts);
        let events = self.event_publisher();

        self.pending_tasks
            .push(TaskId::ControlFactorScheduler, move |shutdown| async move {
                let result = PeriodicTask::run(
                    TaskId::ControlFactorScheduler.static_name(),
                    || SCHEDULER_TICK_INTERVAL,
                    SCHEDULER_JITTER_PCT,
                    true,
                    shutdown,
                    || {
                        let scheduler = Arc::clone(&scheduler);
                        let alerts = Arc::clone(&alerts);
                        let events = events.clone();
                        async move {
                            let report = scheduler
                                .tick(Utc::now())
                                .await
                                .map_err(|error| OxideError::Internal(error.to_string()))?;
                            for alert in report.alerts {
                                dispatch_schedule_alert(&alerts, &events, alert).await;
                            }
                            Ok(())
                        }
                    },
                )
                .await;
                if let Err(error) = result {
                    tracing::error!(%error, "control-factor scheduler exited with error");
                }
            });
    }

    /// Queue the materialization execute worker (claims `Queued` runs, runs them).
    pub fn queue_materialization_execute_worker(&self) {
        let db = self.infra.pg.connection().clone();
        let control_factors: Arc<dyn ControlFactorRepository> =
            Arc::new(PgControlFactorRepository::new(db.clone()));
        let timeseries: Arc<dyn EvidenceTimeseriesRepository> =
            Arc::clone(&self.infra.timeseries) as Arc<dyn EvidenceTimeseriesRepository>;
        let resolver = Arc::new(PointInTimeResolver::new(ResolverRepositories {
            runtime_config: Some(Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()))),
            timeseries: Some(Arc::clone(&timeseries)),
            markets: Some(Arc::new(PgMarketRepository::new(db.clone()))),
            events: Some(Arc::new(PgEventRepository::new(db.clone()))),
            balances: Some(Arc::new(PgFactDataRepository::new(db.clone()))),
            trades: Some(Arc::new(PgTradeRepository::new(db.clone()))),
            positions: Some(Arc::new(PgPositionRepository::new(db.clone()))),
            potential_loss: Some(Arc::new(PgPotentialLossRepository::new(db.clone()))),
            risk_audit: Some(Arc::new(PgRiskAuditRepository::new(db.clone()))),
            reconciliation: Some(Arc::new(PgReconciliationRepository::new(db.clone()))),
            resolution_events: Some(Arc::new(PgResolutionEventRepository::new(db))),
        }));
        let evidence_engine = Arc::new(EvidenceEngine::new(timeseries));
        let runner = Arc::new(MaterializationRunner::new(MaterializationRunnerDeps {
            control_factors: Arc::clone(&control_factors),
            pit_resolver: resolver,
            evidence_engine,
        }));

        self.pending_tasks.push(
            TaskId::MaterializationExecuteWorker,
            move |shutdown| async move {
                let mut ticker = interval(EXECUTE_WORKER_POLL_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            tracing::info!("materialization execute worker shutting down");
                            return;
                        }
                        _ = ticker.tick() => {
                            let run_ids = match control_factors
                                .list_queued_materialization_runs(EXECUTE_WORKER_CLAIM_LIMIT)
                                .await
                            {
                                Ok(ids) => ids,
                                Err(error) => {
                                    tracing::error!(%error, "list queued materialization runs failed");
                                    continue;
                                }
                            };
                            for run_id in run_ids {
                                if shutdown.is_cancelled() {
                                    break;
                                }
                                if let Err(error) =
                                    runner.execute_run(&run_id, shutdown.clone()).await
                                {
                                    tracing::error!(%error, %run_id, "materialization run failed");
                                }
                            }
                        }
                    }
                }
            },
        );
    }
}

/// Dispatch a scheduler cadence alert to the operator alert channel and the
/// real-time event bus.
async fn dispatch_schedule_alert(
    alerts: &Arc<AlertDispatcher>,
    events: &CoreEventPublisher,
    alert: ScheduleAlert,
) {
    let (title, body) = match &alert {
        ScheduleAlert::Overdue {
            schedule_id,
            last_run_at,
        } => (
            "Materialization cadence overdue".to_owned(),
            format!("schedule {schedule_id} last ran at {last_run_at}"),
        ),
        ScheduleAlert::Stale {
            schedule_id,
            last_success_at,
            threshold_secs,
        } => (
            "Materialization cadence stale".to_owned(),
            format!(
                "schedule {schedule_id} no success within {threshold_secs}s (last success: {last_success_at:?})"
            ),
        ),
    };
    alerts
        .dispatch(Alert {
            severity: AlertLevel::Warning,
            title: title.clone(),
            body: body.clone(),
            timestamp: Utc::now(),
        })
        .await;
    events.publish(CoreEvent::Alert {
        level: AlertLevel::Warning,
        message: format!("{title}: {body}"),
    });
}
