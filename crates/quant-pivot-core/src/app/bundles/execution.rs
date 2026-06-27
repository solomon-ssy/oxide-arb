//! Entry-execution subsystem bundle (Phase 05.4 — real money).
//!
//! Owns the venue order client, the (stateless) admission engine + input
//! builder, the stateful execution breaker, and the dispatcher that bridges an
//! admitted intent to a signed venue order. Assembled at boot from the shared
//! authenticated CLOB client (single L1+L2 identity, shared with the account
//! bundle).

use std::sync::Arc;

use quant_pivot_api::clob::ClobClient;
use quant_pivot_models::domain::{DataQualityPort, ExecutionSubmitPort};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgExecutionOrderRepository, PgExecutionSubmissionRepository,
        PgModelRegistryRepository, PgOrderIntentRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReconciliationRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        CapitalAllocationRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
        ModelRegistryRepository, OperationLogRepository, OrderIntentRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        RuntimeConfigVersionRepository,
    },
};

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle};
use crate::execution::{
    AdmissionInputBuilder, AdmissionInputBuilderDeps, ClobOrderClient, CoreExecutionDispatcher,
    DefaultAdmissionEngine, DispatchWake, ExecutionBreaker, ExecutionDispatcherDeps,
    PolymarketOrderClient,
};

/// Dependencies for [`ExecutionBundle::assemble`].
pub struct ExecutionBundleDeps<'a> {
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub governance: &'a GovernanceBundle,
    pub account: &'a AccountBundle,
    /// Shared authenticated CLOB client (same identity as the account bundle).
    pub clob: Arc<ClobClient>,
}

/// Entry-execution subsystem: order client + admission + breaker + dispatcher.
pub struct ExecutionBundle {
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub dispatcher: Arc<dyn ExecutionSubmitPort>,
    pub breaker: Arc<ExecutionBreaker>,
    /// Cross-table submission transactions (also drives boot recovery).
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    /// Approve→submit wake signal (shared by the intent service and the
    /// dispatcher worker); the durable queue stays in Postgres.
    pub dispatch_wake: DispatchWake,
}

impl ExecutionBundle {
    /// Assemble the execution subsystem from the shared planes.
    #[must_use]
    pub fn assemble(deps: ExecutionBundleDeps<'_>) -> Self {
        let infra = deps.infra;
        let pg = infra.pg.connection();

        // Stateful venue-health breaker — auto-trips the kill-switch (latched).
        let breaker_config = deps
            .governance
            .runtime_config
            .current()
            .execution
            .breaker
            .clone();
        let operation_log: Arc<dyn OperationLogRepository> =
            Arc::clone(&infra.operation_log_repo) as Arc<dyn OperationLogRepository>;
        let breaker = Arc::new(ExecutionBreaker::new(
            breaker_config,
            Arc::clone(&deps.governance.kill_switch),
            operation_log,
            Arc::clone(&infra.metrics),
        ));

        // Stateless admission: input builder (with the breaker venue-health seam)
        // + the 20-check engine.
        let admission_builder = Arc::new(AdmissionInputBuilder::new(AdmissionInputBuilderDeps {
            recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
                as Arc<dyn RecommendationRepository>,
            reports: Arc::new(PgRecommendationReportRepository::new(pg.clone()))
                as Arc<dyn RecommendationReportRepository>,
            model_registry: Arc::new(PgModelRegistryRepository::new(pg.clone()))
                as Arc<dyn ModelRegistryRepository>,
            reconciliation: Arc::new(PgReconciliationRepository::new(pg.clone()))
                as Arc<dyn ReconciliationRepository>,
            execution_orders: Arc::new(PgExecutionOrderRepository::new(pg.clone()))
                as Arc<dyn ExecutionOrderRepository>,
            capital: Arc::new(PgCapitalAllocationRepository::new(pg.clone()))
                as Arc<dyn CapitalAllocationRepository>,
            config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(pg.clone()))
                as Arc<dyn RuntimeConfigVersionRepository>,
            account_factory: Arc::clone(&deps.account.provider_factory),
            book_store: Arc::clone(&deps.data.book_store),
            data_quality: Arc::clone(&deps.data.data_quality) as Arc<dyn DataQualityPort>,
            config: Arc::clone(&deps.governance.runtime_config),
            runtime_mode: deps.governance.runtime_mode.clone(),
            kill_switch: deps.governance.kill_switch_handle.clone(),
            venue_health: breaker.venue_health(),
        }));
        let admission = Arc::new(DefaultAdmissionEngine::new(Arc::clone(&infra.metrics)));

        let order_client: Arc<dyn PolymarketOrderClient> = Arc::new(ClobOrderClient::new(
            deps.clob,
            Arc::clone(&deps.data.fee_calculator),
        ));

        let submission: Arc<dyn ExecutionSubmissionRepository> =
            Arc::new(PgExecutionSubmissionRepository::new(pg.clone()));
        let intents: Arc<dyn OrderIntentRepository> =
            Arc::new(PgOrderIntentRepository::new(pg.clone()));

        let dispatcher: Arc<dyn ExecutionSubmitPort> =
            Arc::new(CoreExecutionDispatcher::new(ExecutionDispatcherDeps {
                intents,
                submission: Arc::clone(&submission),
                admission_builder,
                admission,
                order_client: Arc::clone(&order_client),
                breaker: Arc::clone(&breaker),
                metrics: Arc::clone(&infra.metrics),
            }));

        Self {
            order_client,
            dispatcher,
            breaker,
            submission,
            dispatch_wake: DispatchWake::new(),
        }
    }
}
