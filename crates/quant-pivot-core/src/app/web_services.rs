//! Web admin surface assembly for Phase 0.

use super::AppContext;
use crate::{
    app::{
        ports::{
            account_read::CoreAccountReadPort, backtest::CoreBacktestPort,
            execution_read::CoreExecutionReadPort, execution_recovery::CoreExecutionRecoveryPort,
            market_data::CoreMarketData, metrics_scrape::CoreMetricsScrape,
            model_training::CoreModelTrainingPort, quant_report::CoreQuantReportPort,
            reconciliation::CoreReconciliationPort, research_catalog::CoreResearchCatalogPort,
            structural_monitor::CoreStructuralMonitor, training_dataset::CoreTrainingDatasetPort,
        },
        task_id::TaskId,
        task_registry::AppRunner,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::model_calibration_fit::ModelCalibrationFitService,
};
use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_models::domain::{
    CatalogStatusPort, DataQualityPort, ExecutionReadPort, ExecutionRecoveryPort,
    MarketLinkageGovernancePort, ModelCalibrationFitPort, NewOperationLog, OrderIntentPort,
    ReconciliationPort, ResearchJobPort, RuntimeConfigPort,
};
use quant_pivot_repository::traits::{
    AccountSnapshotRepository, AttributionRepository, BasisAlertRepository,
    CalibrationArtifactRepository, DomainSourceCursorRepository, EquitySnapshotRepository,
    ExecutionOrderRepository, MenuRepository, OperationLogRepository, OrderIntentRepository,
    PositionRepository, RecommendationReportRepository, RecommendationRepository,
    ReconciliationRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository,
    RuntimeConfigVersionRepository, SettlementRedeemRepository, TradeTapeBlockCursorRepository,
    UserRepository, UserRoleRepository,
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterWorker};
use quant_pivot_web::{
    AppState,
    audit::OperationLogBuffer,
    auth::casbin::CasbinService,
    jwt::{JwtService, TokenBlacklist},
    readiness::PgRedisReadiness,
    routes, spawn_web_server,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use std::{sync::Arc, time::Duration};

const OPERATION_LOG_BATCH_SIZE: usize = 64;
const OPERATION_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const OPERATION_LOG_BUFFER_CAPACITY: usize = 4096;

impl AppContext {
    pub async fn register_web_services(
        &self,
        runner: &mut AppRunner,
        order_intents: Arc<dyn OrderIntentPort>,
        research_jobs: Arc<dyn ResearchJobPort>,
    ) -> QuantResult<()> {
        let (operation_log, op_log_worker) = build_operation_log_writer(self);
        let state = build_app_state(self, order_intents, research_jobs, operation_log).await?;

        let event_rx = self
            .event_rx
            .lock()
            .take()
            .ok_or_else(|| InfraError::Misconfigured {
                detail: "event_rx already taken".into(),
            })?;
        let ws_sessions = state.ws_sessions.clone();

        let web_config = self.config.web.clone();
        let shutdown = self.shutdown.clone();
        runner.spawn(TaskId::WebServer, move |token| async move {
            if let Err(error) = spawn_web_server(state, web_config, token).await {
                tracing::error!(%error, "web server exited");
            }
            shutdown.cancel();
        });

        self.register_book_update_coalescer(runner, ws_sessions.clone());

        self.register_system_status_broadcaster(runner);

        runner.spawn(TaskId::WsBroadcaster, move |token| async move {
            spawn_ws_broadcaster(event_rx, ws_sessions, token).await;
        });

        runner.spawn(TaskId::OperationLogWriter, move |token| {
            op_log_worker.run(token)
        });

        Ok(())
    }
}

async fn build_app_state(
    ctx: &AppContext,
    order_intents: Arc<dyn OrderIntentPort>,
    research_jobs: Arc<dyn ResearchJobPort>,
    operation_log: OperationLogBuffer,
) -> QuantResult<AppState> {
    let repos = &ctx.infra.repos;
    let auth = build_web_auth(ctx).await?;
    let execution = build_web_execution_ports(ctx);
    let research_ports = build_research_web_ports(ctx);

    Ok(AppState {
        deploy: Arc::clone(&ctx.config),
        runtime_config_apply: Arc::clone(&ctx.governance.applicator) as Arc<dyn RuntimeConfigPort>,
        jwt: auth.jwt,
        jwt_blacklist: Arc::clone(&ctx.infra.jwt_blacklist),
        users: Arc::clone(&repos.user) as Arc<dyn UserRepository>,
        roles: Arc::clone(&repos.role) as Arc<dyn RoleRepository>,
        menus: Arc::clone(&repos.menu) as Arc<dyn MenuRepository>,
        user_roles: Arc::clone(&repos.user_role) as Arc<dyn UserRoleRepository>,
        role_menus: Arc::clone(&repos.role_menu) as Arc<dyn RoleMenuRepository>,
        role_permissions: Arc::clone(&repos.role_permission) as Arc<dyn RolePermissionRepository>,
        casbin: auth.casbin,
        perm_checker: auth.perm_checker,
        runtime_config: Arc::clone(&repos.runtime_config)
            as Arc<dyn RuntimeConfigVersionRepository>,
        operation_logs: Arc::clone(&repos.operation_log) as Arc<dyn OperationLogRepository>,
        operation_log,
        control: Arc::clone(&ctx.governance.runtime_control),
        kill_switch: Arc::clone(&ctx.governance.kill_switch),
        market_data: Arc::new(CoreMarketData::new(
            Arc::clone(&ctx.data.book_store),
            Arc::clone(&ctx.data.ws_manager),
        )),
        catalog: Arc::clone(&ctx.data.catalog) as Arc<dyn CatalogStatusPort>,
        data_quality: Arc::clone(&ctx.data.data_quality) as Arc<dyn DataQualityPort>,
        events: ctx.events.clone(),
        markets: Arc::clone(&ctx.data.market_repo),
        quant_facts: Arc::clone(&ctx.infra.quant_fact_read),
        ws_sessions: SessionRegistry::default(),
        metrics: Arc::new(CoreMetricsScrape::new(ctx.infra.metrics.registry.clone())),
        readiness: Arc::new(PgRedisReadiness::new(
            ctx.infra.pg.connection().clone(),
            Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
            Some(Arc::clone(&ctx.data.catalog) as Arc<dyn CatalogStatusPort>),
        )),
        training_datasets: research_ports.training_datasets,
        model_training: research_ports.model_training,
        backtests: research_ports.backtests,
        model_governance: Arc::clone(&ctx.research.model_governance),
        factor_governance: Arc::clone(&ctx.research.factor_governance),
        model_spec: Arc::clone(&ctx.research.model_spec),
        research_catalog: Arc::new(CoreResearchCatalogPort::from_research(&ctx.research)),
        research_jobs,
        favorite_longshot: Arc::clone(&ctx.research.favorite_longshot_fit),
        model_calibration_fit: research_ports.model_calibration_fit
            as Arc<dyn ModelCalibrationFitPort>,
        market_linkages: Arc::clone(&ctx.research.market_linkage_repo),
        domain_source_cursors: Arc::clone(&ctx.infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        basis_alerts: Arc::clone(&ctx.infra.repos.basis_alert) as Arc<dyn BasisAlertRepository>,
        linkage_governance: Arc::clone(&ctx.data.linkage_resolver)
            as Arc<dyn MarketLinkageGovernancePort>,
        structural_monitor: Arc::new(CoreStructuralMonitor::new(
            Arc::clone(&ctx.data.market_registry),
            Arc::clone(&ctx.data.book_store),
            Arc::new(FeatureWindowProvider::new(Arc::clone(
                &ctx.infra.quant_fact_read,
            ))),
            Arc::clone(&repos.trade_tape_block_cursor) as Arc<dyn TradeTapeBlockCursorRepository>,
            Arc::clone(&ctx.governance.applicator) as Arc<dyn RuntimeConfigPort>,
            ctx.config.market_data.trade_tape_on_chain.clone(),
        )),
        quant_reports: Arc::new(CoreQuantReportPort::new(
            Arc::clone(&repos.recommendation_report) as Arc<dyn RecommendationReportRepository>,
            Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
            Arc::clone(&ctx.report.lifecycle),
            Arc::clone(&ctx.report.scheduler),
        )),
        order_intents,
        account_read: Arc::new(CoreAccountReadPort::new(
            Arc::clone(&repos.account_snapshot) as Arc<dyn AccountSnapshotRepository>,
            Arc::clone(&repos.equity_snapshot) as Arc<dyn EquitySnapshotRepository>,
            Arc::clone(&ctx.account.provider_factory),
            Arc::clone(&ctx.governance.applicator) as Arc<dyn RuntimeConfigPort>,
        )),
        execution_read: execution.execution_read,
        execution_submit: ctx.execution_dispatcher(),
        reconciliation: execution.reconciliation,
        execution_recovery: execution.execution_recovery,
    })
}

struct WebAuthServices {
    casbin: Arc<CasbinService>,
    jwt: Arc<JwtService>,
    perm_checker: Arc<quant_pivot_web::auth::casbin::PermChecker>,
}

async fn build_web_auth(ctx: &AppContext) -> QuantResult<WebAuthServices> {
    let perm_checker = Arc::new(routes::init_rbac_rules());
    let casbin = Arc::new(
        CasbinService::new(ctx.infra.pg.connection().clone())
            .await
            .map_err(|error| InfraError::Misconfigured {
                detail: error.to_string(),
            })?,
    );
    let jwt = Arc::new(JwtService::new(
        &ctx.config.web.jwt,
        Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
    ));
    Ok(WebAuthServices {
        casbin,
        jwt,
        perm_checker,
    })
}

struct WebExecutionPorts {
    execution_read: Arc<dyn ExecutionReadPort>,
    reconciliation: Arc<dyn ReconciliationPort>,
    execution_recovery: Arc<dyn ExecutionRecoveryPort>,
}

fn build_web_execution_ports(ctx: &AppContext) -> WebExecutionPorts {
    let repos = &ctx.infra.repos;
    let execution_read = Arc::new(CoreExecutionReadPort::new(
        Arc::clone(&repos.execution_order) as Arc<dyn ExecutionOrderRepository>,
        Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
        Arc::clone(&repos.attribution) as Arc<dyn AttributionRepository>,
        Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
        Arc::clone(&repos.settlement_redeem) as Arc<dyn SettlementRedeemRepository>,
    ));
    let reconciliation = Arc::new(CoreReconciliationPort::new(
        Arc::clone(&ctx.execution.reconciliation),
        Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
        Arc::clone(&ctx.governance.execution_recovery),
    )) as Arc<dyn ReconciliationPort>;
    let execution_recovery = Arc::new(CoreExecutionRecoveryPort::new(
        Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
        Arc::clone(&ctx.governance.kill_switch),
        ctx.governance.runtime_mode.clone(),
    )) as Arc<dyn ExecutionRecoveryPort>;

    WebExecutionPorts {
        execution_read: execution_read as Arc<dyn ExecutionReadPort>,
        reconciliation,
        execution_recovery,
    }
}

/// Wire the best-effort Postgres operation-log `AsyncWriter`.
fn build_operation_log_writer(
    ctx: &AppContext,
) -> (OperationLogBuffer, AsyncWriterWorker<NewOperationLog>) {
    let op_log_repo = Arc::clone(&ctx.infra.repos.operation_log) as Arc<dyn OperationLogRepository>;
    let op_log_drops = ctx
        .infra
        .metrics
        .async_writer_dropped
        .with_label_values(&["operation_log"]);
    let (op_log_writer, op_log_worker) = AsyncWriter::new(
        AsyncWriterConfig::new("operation_log")
            .capacity(OPERATION_LOG_BUFFER_CAPACITY)
            .batch_size(OPERATION_LOG_BATCH_SIZE)
            .flush_interval(OPERATION_LOG_FLUSH_INTERVAL),
        move |rows: Vec<NewOperationLog>| {
            let repo = Arc::clone(&op_log_repo);
            Box::pin(async move { repo.append_batch(rows).await })
        },
        op_log_drops,
        ctx.infra
            .metrics
            .async_writer_observability("operation_log"),
    );
    (
        OperationLogBuffer::new(Arc::new(op_log_writer)),
        op_log_worker,
    )
}

struct ResearchWebPorts {
    training_datasets: Arc<CoreTrainingDatasetPort>,
    model_training: Arc<CoreModelTrainingPort>,
    backtests: Arc<CoreBacktestPort>,
    model_calibration_fit: Arc<ModelCalibrationFitService>,
}

fn build_research_web_ports(ctx: &AppContext) -> ResearchWebPorts {
    let repos = &ctx.infra.repos;
    let runtime_config =
        Arc::clone(&repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>;
    let bias_table =
        Arc::clone(&repos.calibration_artifact) as Arc<dyn CalibrationArtifactRepository>;
    let backtests = Arc::new(CoreBacktestPort::from_research(
        &ctx.research,
        Arc::clone(&runtime_config),
        Arc::clone(&bias_table),
    ));
    let model_calibration_fit = Arc::new(ModelCalibrationFitService::new(
        Arc::clone(&backtests),
        Arc::clone(&ctx.research.model_registry_repo),
        Arc::clone(&ctx.research.training_dataset_repo),
        Arc::clone(&bias_table),
        Arc::clone(&runtime_config),
    ));
    ResearchWebPorts {
        training_datasets: Arc::new(CoreTrainingDatasetPort::from_research(
            &ctx.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_table),
            ctx.config.quant.research_jobs.max_spine_samples,
            ctx.config.quant.research_jobs.plan_sample_slices,
            ctx.config.quant.research_jobs.plan_sample_markets,
        )),
        model_training: Arc::new(CoreModelTrainingPort::from_research(
            &ctx.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_table),
        )),
        backtests,
        model_calibration_fit,
    }
}
