//! Web administration surface assembly.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_models::domain::{
    governance::NewOperationLog,
    ports::{
        CatalogStatusPort, CommittedPolicyApplyPort, DataQualityPort, ExecutionReadPort,
        ExecutionRecoveryPort, FeatureIntegrityPort, FeedbackActivationReadPort,
        FeedbackMutationPort, FeedbackReadPort, MarketLinkageGovernancePort,
        ModelCalibrationFitPort, OrderIntentPort, PasswordCryptoPort, PolicySnapshotPort,
        ReconciliationPort, ResearchJobPort, ResearchReadinessPort, TradePolicyPort,
        TrainingDatasetPort, settlement_control::SettlementControlPort,
    },
};
use quant_pivot_repository::{
    clickhouse::ChFeatureParityEventRepository,
    traits::{
        AccountSnapshotRepository, BasisAlertRepository, CalibrationArtifactRepository,
        CatalogLedgerRepository, DomainSourceCursorRepository, DomainSourceExpectationRepository,
        EntryConditionRepository, EquitySnapshotRepository, ExecutionAttemptOutcomeRepository,
        ExecutionOrderRepository, FeatureParityEventRepository, FeatureRepository,
        FeedbackCycleRepository, FeedbackOutboxRepository, FeedbackSchedulerRepository,
        MenuRepository, ModelRouteShadowBindingRepository, OperationLogRepository,
        OrderIntentRepository, PolicyRepository, PortfolioPlanRepository, PositionRepository,
        PromotionPermitRepository, RecommendationExecutionRollupRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        ReportRunRepository, ResolutionObservationRepository, RoleMenuRepository,
        RolePermissionRepository, RoleRepository, ServingEvidenceRepository, TradePolicyRepository,
        TradeTapeBlockCursorRepository, UserRepository, UserRoleRepository,
        quant::settlement_redeem::SettlementRedeemRepository,
    },
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterWorker};
use quant_pivot_web::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    jwt::{JwtService, TokenBlacklist},
    readiness::PgRedisReadiness,
    spawn_web_server,
    state::AppState,
    ws::{
        SessionHubMetrics, SessionRegistry, feedback::FeedbackOutboxWorker, spawn_ws_broadcaster,
    },
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
            feedback_mutation::{CoreFeedbackMutationDeps, CoreFeedbackMutationPort},
            feedback_read::{CoreFeedbackReadDeps, CoreFeedbackReadPort},
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
        feedback_coordinator::FeedbackCoordinatorWake,
        model_calibration_fit::ModelCalibrationFitService,
        password_crypto::PasswordCryptoService,
        research_readiness::ResearchReadinessEvidenceService,
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
        feedback_wake: FeedbackCoordinatorWake,
    ) -> QuantResult<()> {
        let (operation_log, op_log_worker) = (self).build_operation_log_writer();
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
        let feedback_outbox = FeedbackOutboxWorker::try_new(
            Arc::clone(&self.infra.repos.feedback_cycle) as Arc<dyn FeedbackOutboxRepository>,
            ws_sessions.clone(),
            self.config.quant.research_jobs,
            self.infra.metrics.feedback_ws_recovery_total.clone(),
        )?;
        let state = build_app_state(
            self,
            order_intents,
            research_jobs,
            feedback_wake,
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
        runner.spawn(TaskId::FeedbackOutboxWorker, move |token| async move {
            if let Err(error) = feedback_outbox.run(token).await {
                tracing::error!(%error, "FeedbackOutboxWorker exited with error");
            }
        });

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
    feedback_wake: FeedbackCoordinatorWake,
    operation_log: OperationLogBuffer,
    ws_sessions: SessionRegistry,
) -> QuantResult<AppState> {
    let repos = &ctx.infra.repos;
    let auth = (ctx).build_web_auth().await?;
    let execution = (ctx).build_web_execution_ports();
    let research_ports = (ctx).build_research_web_ports();
    let trade_policy_dataset_builder =
        Arc::clone(&research_ports.training_datasets) as Arc<dyn TrainingDatasetPort>;
    let research_readiness = Arc::clone(&ctx.research.research_readiness);
    let feedback_mutation = build_feedback_mutation(
        ctx,
        Arc::clone(&research_ports.training_datasets),
        feedback_wake,
    );

    Ok(AppState {
        deploy: Arc::clone(&ctx.config),
        runtime_config_apply: Arc::clone(&ctx.governance.applicator) as Arc<dyn PolicySnapshotPort>,
        committed_policy_apply: Arc::clone(&ctx.governance.committed_policy)
            as Arc<dyn CommittedPolicyApplyPort>,
        jwt: auth.jwt,
        password_crypto: auth.password_crypto,
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
        capabilities: Arc::clone(&ctx.governance.capabilities),
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
            Arc::clone(&ctx.governance.committed_policy) as Arc<dyn CommittedPolicyApplyPort>,
        )),
        training_datasets: Arc::clone(&research_ports.training_datasets)
            as Arc<dyn TrainingDatasetPort>,
        model_training: research_ports.model_training,
        backtests: research_ports.backtests,
        cpcv_backtests: research_ports.cpcv_backtests,
        model_governance: Arc::clone(&ctx.research.model_governance),
        model_spec: Arc::clone(&ctx.research.model_spec),
        research_catalog: Arc::new(CoreResearchCatalogPort::from_research(
            &ctx.research,
            Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
        )),
        research_jobs,
        research_readiness: Arc::clone(&research_readiness) as Arc<dyn ResearchReadinessPort>,
        feedback_read: Arc::new(CoreFeedbackReadPort::new(CoreFeedbackReadDeps {
            cycles: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
            scheduler: Arc::clone(&repos.feedback_scheduler)
                as Arc<dyn FeedbackSchedulerRepository>,
            readiness: Arc::clone(&research_readiness) as Arc<dyn ResearchReadinessPort>,
            artifacts: Arc::clone(&ctx.research.artifact_store),
            resolutions: Arc::clone(&repos.resolution_observation)
                as Arc<dyn ResolutionObservationRepository>,
            attempts: Arc::clone(&repos.execution_attempt_outcome)
                as Arc<dyn ExecutionAttemptOutcomeRepository>,
            rollups: Arc::clone(&repos.recommendation_execution_rollup)
                as Arc<dyn RecommendationExecutionRollupRepository>,
            shadow_bindings: Arc::clone(&repos.model_route_shadow_binding)
                as Arc<dyn ModelRouteShadowBindingRepository>,
            decisions: Arc::clone(&ctx.research.feedback_decisions),
            activations: Arc::clone(&feedback_mutation) as Arc<dyn FeedbackActivationReadPort>,
        })) as Arc<dyn FeedbackReadPort>,
        feedback_outbox: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackOutboxRepository>,
        feedback_mutation: feedback_mutation as Arc<dyn FeedbackMutationPort>,
        feature_integrity: (ctx).build_feature_integrity(),
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
            portfolio_plan_repo: Arc::clone(&repos.portfolio_plan)
                as Arc<dyn PortfolioPlanRepository>,
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
        settlement_control: execution.settlement_control,
        reconciliation: execution.reconciliation,
        execution_recovery: execution.execution_recovery,
    })
}

fn build_feedback_mutation(
    ctx: &AppContext,
    training_datasets: Arc<CoreTrainingDatasetPort>,
    feedback_wake: FeedbackCoordinatorWake,
) -> Arc<CoreFeedbackMutationPort> {
    let repos = &ctx.infra.repos;
    Arc::new(CoreFeedbackMutationPort::new(CoreFeedbackMutationDeps {
        cycles: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
        scheduler: Arc::clone(&repos.feedback_scheduler) as Arc<dyn FeedbackSchedulerRepository>,
        permits: Arc::clone(&repos.promotion_permit) as Arc<dyn PromotionPermitRepository>,
        permit_service: Arc::clone(&ctx.governance.promotion_permits),
        promotion_preflight: Arc::clone(&ctx.research.promotion_preflight),
        serving_preimages: Arc::clone(&ctx.research.serving_preimages),
        serving_generations: Arc::clone(&ctx.research.serving_generations),
        route_governance: Arc::clone(&ctx.research.model_route_governance),
        resolutions: Arc::clone(&repos.resolution_observation)
            as Arc<dyn ResolutionObservationRepository>,
        training_datasets,
        feedback_wake,
        shutdown: ctx.shutdown.clone(),
        metrics: Arc::clone(&ctx.infra.metrics),
    }))
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
        serving_preimages: Arc::clone(&ctx.research.serving_preimages),
    }))
}

impl AppContext {
    fn build_feature_integrity(&self) -> Arc<dyn FeatureIntegrityPort> {
        Arc::new(FeatureIntegrityService::new(
            Arc::clone(&self.report.feature_parity),
            Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
                &self.infra.ch,
            ))) as Arc<dyn FeatureParityEventRepository>,
            Some(Arc::new(CatalogFeatureIntegrityCoverage::new(Arc::clone(
                &self.research.catalog_ledger_repo,
            )
                as Arc<dyn CatalogLedgerRepository>))),
            Arc::clone(&self.infra.metrics),
        ))
    }
}

struct WebAuthServices {
    casbin: Arc<CasbinService>,
    jwt: Arc<JwtService>,
    password_crypto: Arc<dyn PasswordCryptoPort>,
    perm_checker: Arc<PermChecker>,
}

impl AppContext {
    async fn build_web_auth(&self) -> QuantResult<WebAuthServices> {
        let perm_checker = Arc::new(PermChecker::route_rules());
        let casbin = Arc::new(
            CasbinService::new(self.infra.pg.connection().clone())
                .await
                .map_err(|error| InfraError::Misconfigured {
                    detail: error.to_string(),
                })?,
        );
        let jwt = Arc::new(
            JwtService::new(
                &self.config.web.jwt,
                Arc::clone(&self.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
            )
            .map_err(|error| InfraError::Misconfigured {
                detail: error.to_string(),
            })?,
        );
        let password_crypto = Arc::new(
            PasswordCryptoService::new(Arc::clone(&self.compute), &self.config.web.password_crypto)
                .await?,
        ) as Arc<dyn PasswordCryptoPort>;
        Ok(WebAuthServices {
            casbin,
            jwt,
            password_crypto,
            perm_checker,
        })
    }
}

struct WebExecutionPorts {
    execution_read: Arc<dyn ExecutionReadPort>,
    settlement_control: Arc<dyn SettlementControlPort>,
    reconciliation: Arc<dyn ReconciliationPort>,
    execution_recovery: Arc<dyn ExecutionRecoveryPort>,
}

impl AppContext {
    fn build_web_execution_ports(&self) -> WebExecutionPorts {
        let repos = &self.infra.repos;
        let execution_read = Arc::new(CoreExecutionReadPort::new(
            Arc::clone(&repos.execution_order) as Arc<dyn ExecutionOrderRepository>,
            Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
            Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
            Arc::clone(&repos.settlement_redeem) as Arc<dyn SettlementRedeemRepository>,
        ));
        let settlement_control = Arc::clone(&self.execution.settlement_control);
        let reconciliation = Arc::new(CoreReconciliationPort::new(
            Arc::clone(&self.execution.reconciliation),
            Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
            Arc::clone(&self.governance.execution_recovery),
        )) as Arc<dyn ReconciliationPort>;
        let execution_recovery = Arc::new(CoreExecutionRecoveryPort::new(
            Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
            Arc::clone(&self.governance.kill_switch),
            self.governance.runtime_controls.clone(),
        )) as Arc<dyn ExecutionRecoveryPort>;

        WebExecutionPorts {
            execution_read: execution_read as Arc<dyn ExecutionReadPort>,
            settlement_control,
            reconciliation,
            execution_recovery,
        }
    }
}

impl AppContext {
    /// Wire the best-effort Postgres operation-log `AsyncWriter`.
    fn build_operation_log_writer(
        &self,
    ) -> (OperationLogBuffer, AsyncWriterWorker<NewOperationLog>) {
        let op_log_repo =
            Arc::clone(&self.infra.repos.operation_log) as Arc<dyn OperationLogRepository>;
        let op_log_drops = self
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
            self.infra
                .metrics
                .async_writer_observability("operation_log"),
        );
        (
            OperationLogBuffer::new(Arc::new(op_log_writer)),
            op_log_worker,
        )
    }
}

struct ResearchWebPorts {
    training_datasets: Arc<CoreTrainingDatasetPort>,
    model_training: Arc<CoreModelTrainingPort>,
    backtests: Arc<CoreBacktestPort>,
    cpcv_backtests: Arc<CoreCpcvBacktestPort>,
    model_calibration_fit: Arc<ModelCalibrationFitService>,
}

impl AppContext {
    fn build_research_web_ports(&self) -> ResearchWebPorts {
        let repos = &self.infra.repos;
        let runtime_config = Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>;
        let bias_table =
            Arc::clone(&repos.calibration_artifact) as Arc<dyn CalibrationArtifactRepository>;
        let backtests = Arc::new(CoreBacktestPort::from_research(
            &self.research,
            Arc::clone(&runtime_config),
        ));
        let cpcv_backtests = Arc::new(CoreCpcvBacktestPort::from_research(&self.research));
        let model_calibration_fit = Arc::new(ModelCalibrationFitService::new(
            Arc::clone(&backtests),
            Arc::clone(&self.research.model_registry_repo),
            Arc::clone(&self.research.training_dataset_repo),
            Arc::clone(&bias_table),
            Arc::clone(&self.research.model_run_repo),
            Arc::clone(&runtime_config),
        ));
        ResearchWebPorts {
            training_datasets: Arc::new(CoreTrainingDatasetPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
                Arc::clone(&bias_table),
                self.config.quant.research_jobs.max_spine_samples,
                self.config.quant.research_jobs.plan_sample_slices,
                self.config.quant.research_jobs.plan_sample_markets,
            )),
            model_training: Arc::new(CoreModelTrainingPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
            )),
            backtests,
            cpcv_backtests,
            model_calibration_fit,
        }
    }
}
