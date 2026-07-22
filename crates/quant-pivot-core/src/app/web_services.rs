//! Web administration surface assembly.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_migration::PostgresLifecycleLeaseProvider;
use quant_pivot_models::{
    config::CompiledBuildIdentity,
    domain::{
        governance::NewOperationLog,
        ports::{
            CatalogStatusPort, DataQualityPort, ExecutionReadPort, ExecutionRecoveryPort,
            FeatureIntegrityPort, MarketLinkageGovernancePort, ModelCalibrationFitPort,
            OrderIntentPort, PolicySnapshotPort, ReconciliationPort, ResearchJobPort,
            ResearchReadinessPort, TradePolicyPort, TrainingDatasetPort,
        },
    },
};
use quant_pivot_repository::{
    clickhouse::ChFeatureParityEventRepository,
    traits::{
        AccountSnapshotRepository, AttributionRepository, BasisAlertRepository,
        CalibrationArtifactRepository, CatalogLedgerRepository, DomainSourceCursorRepository,
        DomainSourceExpectationRepository, EntryConditionRepository, EquitySnapshotRepository,
        ExecutionOrderRepository, FeatureParityEventRepository, FeatureRepository, MenuRepository,
        OperationLogRepository, OrderIntentRepository, PolicyRepository, PositionRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        ReportRunRepository, ResearchReadinessEvidenceRepository, RoleMenuRepository,
        RolePermissionRepository, RoleRepository, ServingEvidenceRepository,
        SettlementRedeemRepository, TradePolicyRepository, TradeTapeBlockCursorRepository,
        UserRepository, UserRoleRepository,
    },
};
use quant_pivot_storage::{
    evidence::FileProductionEvidenceVerifier,
    write::{AsyncWriter, AsyncWriterConfig, AsyncWriterWorker},
};
use quant_pivot_web::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    jwt::{JwtService, TokenBlacklist},
    readiness::PgRedisReadiness,
    routes, spawn_web_server,
    state::{AppState, LiveSchemaVerifier},
    ws::{SessionHubMetrics, SessionRegistry, spawn_ws_broadcaster},
};

use super::AppContext;
use crate::{
    app::{
        ports::{
            account_read::CoreAccountReadPort,
            backtest::CoreBacktestPort,
            cpcv_backtest::CoreCpcvBacktestPort,
            execution_read::CoreExecutionReadPort,
            execution_recovery::CoreExecutionRecoveryPort,
            market_data::CoreMarketData,
            metrics_scrape::CoreMetricsScrape,
            model_training::CoreModelTrainingPort,
            quant_report::{CoreQuantReportPort, CoreQuantReportPortDeps},
            reconciliation::CoreReconciliationPort,
            research_catalog::CoreResearchCatalogPort,
            structural_monitor::CoreStructuralMonitor,
            training_dataset::CoreTrainingDatasetPort,
        },
        task_id::TaskId,
        task_registry::AppRunner,
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        feature_integrity::{CatalogFeatureIntegrityCoverage, FeatureIntegrityService},
        model_calibration_fit::ModelCalibrationFitService,
        research_readiness::{
            EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
        },
        trade_policy::{TradePolicyService, TradePolicyServiceDeps},
    },
};

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
        let (ws_sessions, session_hub) = SessionRegistry::new(SessionHubMetrics {
            best_effort_dropped: self.infra.metrics.ws_fanout_best_effort_dropped.clone(),
            best_effort_coalesced: self.infra.metrics.ws_fanout_best_effort_coalesced.clone(),
            reliable_disconnects: self.infra.metrics.ws_fanout_reliable_disconnects.clone(),
            control_timeouts: self.infra.metrics.ws_hub_control_timeouts.clone(),
            control_latency_seconds: self.infra.metrics.ws_hub_control_latency_seconds.clone(),
            queue_depth: self.infra.metrics.ws_hub_queue_depth.clone(),
            queue_oldest_age_seconds: self.infra.metrics.ws_hub_queue_oldest_age_seconds.clone(),
            frame_bytes: self.infra.metrics.ws_hub_frame_bytes.clone(),
        });
        let state = build_app_state(
            self,
            order_intents,
            research_jobs,
            operation_log,
            ws_sessions.clone(),
        )
        .await?;

        let event_rx = self
            .event_rx
            .lock()
            .take()
            .ok_or_else(|| InfraError::Misconfigured {
                detail: "event_rx already taken".into(),
            })?;
        let ws_sessions = state.ws_sessions.clone();

        runner.spawn(TaskId::SessionHub, move |token| session_hub.run(token));

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
    ws_sessions: SessionRegistry,
) -> QuantResult<AppState> {
    let repos = &ctx.infra.repos;
    let auth = build_web_auth(ctx).await?;
    let execution = build_web_execution_ports(ctx);
    let research_ports = build_research_web_ports(ctx);
    let trade_policy_dataset_builder =
        Arc::clone(&research_ports.training_datasets) as Arc<dyn TrainingDatasetPort>;
    let research_readiness = build_research_readiness(ctx)?;

    Ok(AppState {
        deploy: Arc::clone(&ctx.config),
        postgres_schema_fingerprint: ctx.infra.postgres_schema_fingerprint,
        clickhouse_schema_fingerprint: ctx.infra.clickhouse_schema_fingerprint,
        build_identity: CompiledBuildIdentity::compiled()?,
        schema_verification: Arc::new(LiveSchemaVerifier::new(
            Arc::clone(&ctx.infra.pg),
            Arc::clone(&ctx.infra.ch),
        )),
        lifecycle_leases: Arc::new(PostgresLifecycleLeaseProvider::new(
            ctx.config.db.postgres.clone(),
        )),
        production_evidence_verification: Arc::new(FileProductionEvidenceVerifier),
        runtime_config_apply: Arc::clone(&ctx.governance.applicator) as Arc<dyn PolicySnapshotPort>,
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
        runtime_config: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
        operation_logs: Arc::clone(&repos.operation_log) as Arc<dyn OperationLogRepository>,
        operation_log,
        control: Arc::clone(&ctx.governance.runtime_control),
        bootstrap: Arc::clone(&ctx.governance.bootstrap),
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
        ws_sessions,
        metrics: Arc::new(CoreMetricsScrape::new(ctx.infra.metrics.registry.clone())),
        readiness: Arc::new(PgRedisReadiness::new(
            ctx.infra.pg.connection().clone(),
            Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
            Some(Arc::clone(&ctx.data.catalog) as Arc<dyn CatalogStatusPort>),
        )),
        training_datasets: Arc::clone(&research_ports.training_datasets)
            as Arc<dyn TrainingDatasetPort>,
        model_training: research_ports.model_training,
        backtests: research_ports.backtests,
        cpcv_backtests: research_ports.cpcv_backtests,
        model_governance: Arc::clone(&ctx.research.model_governance),
        factor_governance: Arc::clone(&ctx.research.factor_governance),
        model_spec: Arc::clone(&ctx.research.model_spec),
        research_catalog: Arc::new(CoreResearchCatalogPort::from_research(&ctx.research)),
        research_jobs,
        research_readiness: Arc::clone(&research_readiness) as Arc<dyn ResearchReadinessPort>,
        feature_integrity: build_feature_integrity(ctx),
        calibration_artifacts: Arc::clone(&ctx.research.calibration_artifact_fit),
        model_calibration_fit: research_ports.model_calibration_fit
            as Arc<dyn ModelCalibrationFitPort>,
        trade_policies: build_trade_policy_port(
            ctx,
            trade_policy_dataset_builder,
            research_readiness,
        ),
        market_linkages: Arc::clone(&ctx.research.market_linkage_repo),
        domain_source_cursors: Arc::clone(&ctx.infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        domain_source_expectations: Arc::clone(&ctx.infra.repos.domain_source_expectation)
            as Arc<dyn DomainSourceExpectationRepository>,
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
            Arc::clone(&ctx.governance.applicator) as Arc<dyn PolicySnapshotPort>,
            ctx.config.market_data.trade_tape_on_chain.clone(),
        )),
        quant_reports: Arc::new(CoreQuantReportPort::new(CoreQuantReportPortDeps {
            report_repo: Arc::clone(&repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
            report_run_repo: Arc::clone(&repos.report_run) as Arc<dyn ReportRunRepository>,
            recommendation_repo: Arc::clone(&repos.recommendation)
                as Arc<dyn RecommendationRepository>,
            order_intent_repo: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
            lifecycle: Arc::clone(&ctx.report.lifecycle),
            serving_evidence: Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
                &ctx.infra.ch,
            ))) as Arc<dyn ServingEvidenceRepository>,
            feature_repo: Arc::clone(&repos.feature) as Arc<dyn FeatureRepository>,
            runtime_config_repo: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
            quant_fact_read: Arc::clone(&ctx.infra.quant_fact_read),
            operation_logs: Arc::clone(&repos.operation_log) as Arc<dyn OperationLogRepository>,
        })),
        order_intents,
        entry_conditions: Arc::clone(&repos.entry_condition) as Arc<dyn EntryConditionRepository>,
        account_read: Arc::new(CoreAccountReadPort::new(
            Arc::clone(&repos.account_snapshot) as Arc<dyn AccountSnapshotRepository>,
            Arc::clone(&repos.equity_snapshot) as Arc<dyn EquitySnapshotRepository>,
            Arc::clone(&ctx.account.provider_factory),
            Arc::clone(&ctx.governance.applicator) as Arc<dyn PolicySnapshotPort>,
        )),
        execution_read: execution.execution_read,
        reconciliation: execution.reconciliation,
        execution_recovery: execution.execution_recovery,
    })
}

fn build_research_readiness(
    ctx: &AppContext,
) -> QuantResult<Arc<ResearchReadinessEvidenceService>> {
    let attestor = EvidenceAttestor::from_config(&ctx.config.research.evidence_attestation)?;
    let evidence_scope = EvidenceScopeIdentity::from_config(
        &ctx.config.db.clickhouse,
        &ctx.config.research.artifact_store,
    )?;
    Ok(Arc::new(ResearchReadinessEvidenceService::new(
        Arc::clone(&ctx.infra.repos.research_readiness)
            as Arc<dyn ResearchReadinessEvidenceRepository>,
        Arc::clone(&ctx.research.artifact_store),
        attestor,
        &evidence_scope,
    )?))
}

fn build_trade_policy_port(
    ctx: &AppContext,
    dataset_builder: Arc<dyn TrainingDatasetPort>,
    readiness: Arc<ResearchReadinessEvidenceService>,
) -> Arc<dyn TradePolicyPort> {
    Arc::new(TradePolicyService::new(TradePolicyServiceDeps {
        compute: Arc::clone(&ctx.compute),
        datasets: Arc::clone(&ctx.research.training_dataset_repo),
        dataset_builder,
        artifacts: Arc::clone(&ctx.research.artifact_store),
        policies: Arc::clone(&ctx.infra.repos.trade_policy) as Arc<dyn TradePolicyRepository>,
        model_registry: Arc::clone(&ctx.research.model_registry_repo),
        runtime_configs: Arc::clone(&ctx.infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
        source_slices: Arc::clone(&ctx.research.source_slice_repo),
        readiness,
        model_runtime_factory_builder: Arc::clone(&ctx.research.model_runtime_factory_builder),
    }))
}

fn build_feature_integrity(ctx: &AppContext) -> Arc<dyn FeatureIntegrityPort> {
    Arc::new(FeatureIntegrityService::new(
        Arc::clone(&ctx.report.feature_parity),
        Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
            &ctx.infra.ch,
        ))) as Arc<dyn FeatureParityEventRepository>,
        Some(Arc::new(CatalogFeatureIntegrityCoverage::new(Arc::clone(
            &ctx.research.catalog_ledger_repo,
        )
            as Arc<dyn CatalogLedgerRepository>))),
        Arc::clone(&ctx.infra.metrics),
    ))
}

struct WebAuthServices {
    casbin: Arc<CasbinService>,
    jwt: Arc<JwtService>,
    perm_checker: Arc<PermChecker>,
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
    let jwt = Arc::new(
        JwtService::new(
            &ctx.config.web.jwt,
            Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
        )
        .map_err(|error| InfraError::Misconfigured {
            detail: error.to_string(),
        })?,
    );
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
    cpcv_backtests: Arc<CoreCpcvBacktestPort>,
    model_calibration_fit: Arc<ModelCalibrationFitService>,
}

fn build_research_web_ports(ctx: &AppContext) -> ResearchWebPorts {
    let repos = &ctx.infra.repos;
    let runtime_config = Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>;
    let bias_table =
        Arc::clone(&repos.calibration_artifact) as Arc<dyn CalibrationArtifactRepository>;
    let backtests = Arc::new(CoreBacktestPort::from_research(
        &ctx.research,
        Arc::clone(&runtime_config),
        Arc::clone(&bias_table),
    ));
    let cpcv_backtests = Arc::new(CoreCpcvBacktestPort::from_research(
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
        )),
        backtests,
        cpcv_backtests,
        model_calibration_fit,
    }
}
