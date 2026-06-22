//! Web admin surface assembly and governance background tasks for [`super::AppContext`].

use super::AppContext;
use crate::{
    bridge::{market_data::CoreMarketData, metrics_scrape::CoreMetricsScrape},
    control::{
        mode_transition::{CoreRuntimeControl, CoreRuntimeControlDeps},
        replay::CoreReplay,
    },
    infra::periodic_task::PeriodicTask,
    service::{
        book_update_coalescer::BookUpdateCoalescer,
        system_status_broadcaster::SystemStatusBroadcaster,
        system_status_publisher::SystemStatusPublisher,
    },
};
use chrono::Utc;
use flume::Receiver;
use oxide_arb_control::{
    evidence::engine::EvidenceEngine,
    materialization::{
        MaterializationRunner, MaterializationRunnerDeps, PointInTimeResolver, ResolverRepositories,
    },
    scheduler::{MaterializationScheduler, ScheduleOutcome, SchedulePolicy},
};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::domain::{
    CatalogStatusPort, CoreEvent, MarketDataPort, NewOperationLog, ReplayPort, RuntimeConfigPort,
    RuntimeConfigRef, RuntimeControlPort,
};
use oxide_arb_repository::{
    pg_arc_repo,
    postgres::{
        PgControlFactorRepository, PgEventRepository, PgFactDataRepository, PgMarketRepository,
        PgMenuRepository, PgOperationLogRepository, PgPositionRepository,
        PgPotentialLossRepository, PgReconciliationRepository, PgReportRepository,
        PgResolutionEventRepository, PgRiskAuditRepository, PgRoleMenuRepository,
        PgRolePermissionRepository, PgRoleRepository, PgRuntimeConfigVersionRepository,
        PgSystemRuntimeStateRepository, PgTradeRepository, PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        ControlFactorRepository, ControlFactorShadowDecisionRepository,
        EvidenceTimeseriesRepository, MarketRepository, MenuRepository, OperationLogRepository,
        PositionRepository, ReportRepository, RiskAuditRepository, RoleMenuRepository,
        RolePermissionRepository, RoleRepository, RuntimeConfigVersionRepository, TradeRepository,
        UserRepository, UserRoleRepository,
    },
};
use oxide_arb_storage::postgres::PostgresPool;
use oxide_arb_web::{
    AppState,
    audit::{OperationLogBuffer, spawn_operation_log_writer},
    auth::casbin::CasbinService,
    jwt::{JwtService, TokenBlacklist},
    readiness::PgRedisReadiness,
    routes, spawn_web_server,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use std::{sync::Arc, time::Duration};
use tokio::time::interval;

use super::task_id::TaskId;

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

/// Postgres repositories wired into the web [`AppState`].
pub(super) struct WebStateRepos {
    control_factors: Arc<dyn ControlFactorRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository>,
    operation_logs: Arc<dyn OperationLogRepository>,
    users: Arc<dyn UserRepository>,
    roles: Arc<dyn RoleRepository>,
    menus: Arc<dyn MenuRepository>,
    user_roles: Arc<dyn UserRoleRepository>,
    role_menus: Arc<dyn RoleMenuRepository>,
    role_permissions: Arc<dyn RolePermissionRepository>,
    positions: Arc<dyn PositionRepository>,
    trades: Arc<dyn TradeRepository>,
    markets: Arc<dyn MarketRepository>,
    reports: Arc<dyn ReportRepository>,
    risk_audit: Arc<dyn RiskAuditRepository>,
}

impl WebStateRepos {
    /// Construct every web-layer repository over clones of the shared pool connection.
    fn from_pool(pg: &PostgresPool) -> Self {
        let db = pg.connection();
        Self {
            control_factors: pg_arc_repo!(db, PgControlFactorRepository),
            runtime_config: pg_arc_repo!(db, PgRuntimeConfigVersionRepository),
            shadow_decisions: pg_arc_repo!(db, PgFactDataRepository),
            operation_logs: pg_arc_repo!(db, PgOperationLogRepository),
            users: pg_arc_repo!(db, PgUserRepository),
            roles: pg_arc_repo!(db, PgRoleRepository),
            menus: pg_arc_repo!(db, PgMenuRepository),
            user_roles: pg_arc_repo!(db, PgUserRoleRepository),
            role_menus: pg_arc_repo!(db, PgRoleMenuRepository),
            role_permissions: pg_arc_repo!(db, PgRolePermissionRepository),
            positions: pg_arc_repo!(db, PgPositionRepository),
            trades: pg_arc_repo!(db, PgTradeRepository),
            markets: pg_arc_repo!(db, PgMarketRepository),
            reports: pg_arc_repo!(db, PgReportRepository),
            risk_audit: pg_arc_repo!(db, PgRiskAuditRepository),
        }
    }

    fn resolver_repositories(
        pg: &PostgresPool,
        timeseries: &Arc<dyn EvidenceTimeseriesRepository>,
    ) -> ResolverRepositories {
        let db = pg.connection();
        ResolverRepositories {
            runtime_config: Some(pg_arc_repo!(db, PgRuntimeConfigVersionRepository)),
            timeseries: Some(Arc::clone(timeseries)),
            markets: Some(pg_arc_repo!(db, PgMarketRepository)),
            events: Some(pg_arc_repo!(db, PgEventRepository)),
            balances: Some(pg_arc_repo!(db, PgFactDataRepository)),
            trades: Some(pg_arc_repo!(db, PgTradeRepository)),
            positions: Some(pg_arc_repo!(db, PgPositionRepository)),
            potential_loss: Some(pg_arc_repo!(db, PgPotentialLossRepository)),
            risk_audit: Some(pg_arc_repo!(db, PgRiskAuditRepository)),
            reconciliation: Some(pg_arc_repo!(db, PgReconciliationRepository)),
            resolution_events: Some(pg_arc_repo!(db, PgResolutionEventRepository)),
        }
    }
}

impl AppContext {
    /// Assemble the web [`AppState`] and the operation-log writer's receiver.
    async fn assemble_web_state(&self) -> OxideResult<(AppState, Receiver<NewOperationLog>)> {
        let db = self.infra.pg.connection().clone();
        let repos = WebStateRepos::from_pool(&self.infra.pg);

        let jwt_blacklist = Arc::clone(&self.infra.jwt_blacklist);
        let blacklist: Arc<dyn TokenBlacklist> = jwt_blacklist.clone();
        let jwt = Arc::new(JwtService::new(
            &self.config.web.jwt,
            Arc::clone(&blacklist),
        ));
        let catalog_status = Arc::clone(&self.data.catalog);
        let catalog_status: Arc<dyn CatalogStatusPort> = catalog_status;
        let readiness = Arc::new(PgRedisReadiness::new(
            db.clone(),
            blacklist,
            Some(catalog_status),
        ));
        let metrics = Arc::new(CoreMetricsScrape::new(Arc::clone(&self.infra.metrics)));

        let casbin = Arc::new(
            CasbinService::new(db.clone())
                .await
                .map_err(|error| OxideError::Internal(format!("casbin init: {error}")))?,
        );
        let perm_checker = Arc::new(routes::init_rbac_rules());

        let (operation_log, operation_log_rx) =
            OperationLogBuffer::new(OPERATION_LOG_BUFFER_CAPACITY);

        let mut control_deps = self.control_deps();
        let status_publisher = Arc::new(SystemStatusPublisher::new(
            control_deps.clone(),
            self.event_publisher(),
            self.started_at,
        ));
        control_deps.status_publisher = Some(Arc::clone(&status_publisher));

        let control: Arc<dyn RuntimeControlPort> =
            Arc::new(CoreRuntimeControl::new(control_deps, self.started_at));
        let market_data: Arc<dyn MarketDataPort> = Arc::new(CoreMarketData::new(
            Arc::clone(&self.data.book_store),
            Arc::clone(&self.trading.ws_manager),
        ));
        let replay: Arc<dyn ReplayPort> = Arc::new(CoreReplay::new(
            pg_arc_repo!(db, PgControlFactorRepository),
            self.event_publisher(),
        ));
        let runtime_config_apply: Arc<dyn RuntimeConfigPort> =
            Arc::clone(&self.applicator) as Arc<dyn RuntimeConfigPort>;
        let evidence: Arc<dyn EvidenceTimeseriesRepository> =
            Arc::clone(&self.infra.timeseries) as Arc<dyn EvidenceTimeseriesRepository>;

        let state = AppState {
            deploy: Arc::clone(&self.config),
            runtime_config_apply,
            jwt,
            jwt_blacklist,
            users: Arc::clone(&repos.users),
            roles: Arc::clone(&repos.roles),
            menus: Arc::clone(&repos.menus),
            user_roles: Arc::clone(&repos.user_roles),
            role_menus: Arc::clone(&repos.role_menus),
            role_permissions: Arc::clone(&repos.role_permissions),
            positions: Arc::clone(&repos.positions),
            trades: Arc::clone(&repos.trades),
            markets: Arc::clone(&repos.markets),
            reports: Arc::clone(&repos.reports),
            evidence,
            risk_audit: Arc::clone(&repos.risk_audit),
            casbin,
            perm_checker,
            registry: Arc::clone(&self.control.registry),
            control_factors: Arc::clone(&repos.control_factors),
            runtime_config: Arc::clone(&repos.runtime_config),
            shadow_decisions: Arc::clone(&repos.shadow_decisions),
            operation_logs: Arc::clone(&repos.operation_logs),
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
        CoreRuntimeControlDeps {
            execution_mode: self.execution_mode.clone(),
            risk_engine: Arc::clone(&self.risk.engine),
            catalog: Arc::clone(&self.data.catalog),
            fsm: Arc::clone(&self.trading.fsm),
            exposure: Arc::clone(&self.risk.exposure),
            metrics: Arc::clone(&self.risk.metrics),
            metrics_state: Arc::clone(&self.risk.metrics_state),
            metrics_refresh: Arc::clone(&self.risk.metrics_refresh),
            clob_client: self.trading.clob_client.clone(),
            ctf_redeem: self.trading.ctf_redeem.clone(),
            holder_address: self.infra.holder_address.clone(),
            market_registry: Arc::clone(&self.data.market_registry),
            ws_manager: Arc::clone(&self.trading.ws_manager),
            unhealthy_subsystems: Arc::clone(&self.unhealthy_subsystems),
            health_checker: Some(Arc::clone(&self.health_checker)),
            deploy: Arc::clone(&self.config),
            runtime_config: Arc::clone(&self.runtime_config),
            position_repo: {
                let repo = Arc::clone(&self.infra.position_repo);
                repo as Arc<dyn PositionRepository>
            },
            system_runtime_state: pg_arc_repo!(
                self.infra.pg.connection(),
                PgSystemRuntimeStateRepository
            ),
            trade_repo: {
                let repo = Arc::clone(&self.infra.trade_repo);
                repo as Arc<dyn TradeRepository>
            },
            capital_manager: Arc::clone(
                &self
                    .trading
                    .execution
                    .as_ref()
                    .expect("execution bundle")
                    .capital_manager,
            ),
            trade_integrity: Arc::clone(&self.trade_integrity),
            factor_store: Arc::clone(&self.control.store),
            alerts: Arc::clone(&self.infra.alerts),
            detection_readiness: Arc::clone(&self.detection_readiness),
            status_publisher: None,
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
        self.queue_system_status_broadcaster();
        self.queue_web_server(state);
        Ok(())
    }

    /// Queue the periodic system-status broadcaster (5s + nudge-driven pushes).
    fn queue_system_status_broadcaster(&self) {
        let mut deps = self.control_deps();
        let publisher = Arc::new(SystemStatusPublisher::new(
            deps.clone(),
            self.event_publisher(),
            self.started_at,
        ));
        deps.status_publisher = Some(Arc::clone(&publisher));
        let broadcaster = SystemStatusBroadcaster::new(publisher, self.system_status_nudge.clone());
        self.pending_tasks.push(
            TaskId::SystemStatusBroadcaster,
            move |shutdown| async move {
                broadcaster.run(shutdown).await;
            },
        );
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
    pub fn queue_control_factor_scheduler(&self) {
        let repo: Arc<dyn ControlFactorRepository> =
            pg_arc_repo!(self.infra.pg.connection(), PgControlFactorRepository);
        let alerts = Arc::clone(&self.infra.alerts);
        let execution_mode = self.execution_mode.clone();
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
                        let repo = Arc::clone(&repo);
                        let alerts = Arc::clone(&alerts);
                        let execution_mode = execution_mode.clone();
                        let events = events.clone();
                        async move {
                            let now = Utc::now();
                            let policy = SchedulePolicy::for_mode(
                                execution_mode.current(),
                                RuntimeConfigRef::ActiveAt { at: now },
                                "scheduler",
                                option_env!("GIT_SHA").unwrap_or("unknown"),
                            );
                            let scheduler =
                                MaterializationScheduler::new(Arc::clone(&repo), policy);
                            let report = scheduler
                                .tick(now)
                                .await
                                .map_err(|error| OxideError::Internal(error.to_string()))?;
                            for outcome in report.outcomes {
                                if let ScheduleOutcome::Enqueued { run_id, .. } = outcome {
                                    if let Ok(Some(run)) =
                                        repo.load_materialization_run(&run_id).await
                                    {
                                        events.publish(CoreEvent::MaterializationRunUpdated(
                                            run.clone(),
                                        ));
                                    }
                                }
                            }
                            for alert in report.alerts {
                                alerts.dispatch_schedule_alert(alert).await;
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
            pg_arc_repo!(db, PgControlFactorRepository);
        let timeseries: Arc<dyn EvidenceTimeseriesRepository> =
            Arc::clone(&self.infra.timeseries) as Arc<dyn EvidenceTimeseriesRepository>;
        let resolver = Arc::new(PointInTimeResolver::new(
            WebStateRepos::resolver_repositories(&self.infra.pg, &timeseries),
        ));
        let evidence_engine = Arc::new(EvidenceEngine::new(timeseries));
        let runner = Arc::new(MaterializationRunner::new(MaterializationRunnerDeps {
            control_factors: Arc::clone(&control_factors),
            pit_resolver: resolver,
            evidence_engine,
            events: self.event_publisher(),
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
