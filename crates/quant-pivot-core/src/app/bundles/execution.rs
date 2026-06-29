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
        PgAttributionRepository, PgCapitalAllocationRepository, PgExecutionOrderRepository,
        PgExecutionSubmissionRepository, PgMarketRepository, PgModelRegistryRepository,
        PgOrderIntentRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReconciliationRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        AttributionRepository, CapitalAllocationRepository, ExecutionOrderRepository,
        ExecutionSubmissionRepository, MarketRepository, ModelRegistryRepository,
        OperationLogRepository, OrderIntentRepository, PositionRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        RuntimeConfigVersionRepository,
    },
};

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::{
    execution::{
        AdmissionInputBuilder, AdmissionInputBuilderDeps, AttributionService,
        AttributionServiceDeps, ClobOrderClient, ClobReconciliationReader, CoreExecutionDispatcher,
        CoreExitDispatcher, DefaultAdmissionEngine, DispatchWake, EvidenceCollector,
        ExecutionBreaker, ExecutionDispatcherDeps, ExitDispatcherDeps, ExitMonitorHealthHandle,
        ExitMonitorService, ExitMonitorServiceDeps, ExitSignalEvaluator, PolymarketOrderClient,
        ReconciliationService, ReconciliationServiceDeps, VenueEvidenceCollector,
        VenueReconciliationReader,
    },
    pipeline::feature_window_provider::FeatureWindowProvider,
    service::{
        model_backed_reinferer::{
            ModelBackedExitSignalReinferer, ModelBackedExitSignalReinfererDeps,
        },
        signal_reinference::{ReinferenceSignalEvaluator, ReinferenceSignalEvaluatorDeps},
    },
};

/// Dependencies for [`ExecutionBundle::assemble`].
pub struct ExecutionBundleDeps<'a> {
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub governance: &'a GovernanceBundle,
    pub research: &'a ResearchBundle,
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
    /// Reconciliation engine (05.5): resolves in-flight orders to venue truth.
    pub reconciliation: Arc<ReconciliationService>,
    /// Exit-monitor engine (05.6): scans open lots and drives the exit ladder.
    pub exit_monitor: Arc<ExitMonitorService>,
    /// Final WORM recommendation-attribution builder (05.7).
    pub attribution: Arc<AttributionService>,
    /// Exit-monitor health hot read consumed by admission `#20`.
    pub exit_monitor_health: ExitMonitorHealthHandle,
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

        // Exit-monitor health seam (05.6): owned by the governance bundle so it
        // is shared with the mode preflight; published by the worker, read by
        // admission `#20`. Starts not-ready (fail-closed) until the first scan.
        let exit_monitor_health = deps.governance.exit_monitor_health.clone();

        // Stateless admission: input builder (breaker venue-health + exit-monitor
        // seams) + the 20-check engine.
        let admission_builder =
            build_admission_builder(&deps, &breaker, exit_monitor_health.clone());
        let admission = Arc::new(DefaultAdmissionEngine::new(Arc::clone(&infra.metrics)));

        let clob = deps.clob;
        let order_client: Arc<dyn PolymarketOrderClient> = Arc::new(ClobOrderClient::new(
            Arc::clone(&clob),
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

        // Reconciliation engine (05.5): venue reader + fixed-order evidence
        // collector + the service that resolves in-flight orders to venue truth.
        let reader: Arc<dyn VenueReconciliationReader> =
            Arc::new(ClobReconciliationReader::new(Arc::clone(&clob)));
        let collector: Arc<dyn EvidenceCollector> = Arc::new(VenueEvidenceCollector::new(
            reader,
            Arc::clone(&deps.data.book_store),
        ));
        let reconciliation = Arc::new(ReconciliationService::new(ReconciliationServiceDeps {
            collector,
            order_client: Arc::clone(&order_client),
            execution_orders: Arc::new(PgExecutionOrderRepository::new(pg.clone()))
                as Arc<dyn ExecutionOrderRepository>,
            intents: Arc::new(PgOrderIntentRepository::new(pg.clone()))
                as Arc<dyn OrderIntentRepository>,
            recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
                as Arc<dyn RecommendationRepository>,
            positions: Arc::new(PgPositionRepository::new(pg.clone()))
                as Arc<dyn PositionRepository>,
            reconciliation: Arc::new(PgReconciliationRepository::new(pg.clone()))
                as Arc<dyn ReconciliationRepository>,
            submission: Arc::clone(&submission),
            fees: Arc::clone(&deps.data.fee_calculator),
            breaker: Arc::clone(&breaker),
            metrics: Arc::clone(&infra.metrics),
            config: Arc::clone(&deps.governance.runtime_config),
        }));

        // Exit-monitor engine (05.6): model-driven signal seam + exit dispatcher
        // + per-lot sweep service.
        let exit_monitor = build_exit_monitor(
            ExitMonitorWiring {
                infra,
                data: deps.data,
                governance: deps.governance,
                research: deps.research,
            },
            &submission,
            &order_client,
            &breaker,
            exit_monitor_health.clone(),
        );
        let attribution = build_attribution_service(infra);

        Self {
            order_client,
            dispatcher,
            breaker,
            submission,
            reconciliation,
            exit_monitor,
            attribution,
            exit_monitor_health,
            dispatch_wake: DispatchWake::new(),
        }
    }
}

fn build_attribution_service(infra: &InfraBundle) -> Arc<AttributionService> {
    let pg = infra.pg.connection();
    Arc::new(AttributionService::new(AttributionServiceDeps {
        attribution: Arc::new(PgAttributionRepository::new(pg.clone()))
            as Arc<dyn AttributionRepository>,
        intents: Arc::new(PgOrderIntentRepository::new(pg.clone()))
            as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
            as Arc<dyn RecommendationRepository>,
        execution_orders: Arc::new(PgExecutionOrderRepository::new(pg.clone()))
            as Arc<dyn ExecutionOrderRepository>,
        positions: Arc::new(PgPositionRepository::new(pg.clone())) as Arc<dyn PositionRepository>,
        reconciliation: Arc::new(PgReconciliationRepository::new(pg.clone()))
            as Arc<dyn ReconciliationRepository>,
        attribution_events: Arc::clone(&infra.attribution_event_writer),
    }))
}

/// Assemble the stateless admission input builder (real venue-health seam from
/// the breaker and real exit-monitor readiness seam from the worker).
fn build_admission_builder(
    deps: &ExecutionBundleDeps<'_>,
    breaker: &Arc<ExecutionBreaker>,
    exit_monitor_health: ExitMonitorHealthHandle,
) -> Arc<AdmissionInputBuilder> {
    let pg = deps.infra.pg.connection();
    Arc::new(AdmissionInputBuilder::new(AdmissionInputBuilderDeps {
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
        markets: Arc::new(PgMarketRepository::new(pg.clone())) as Arc<dyn MarketRepository>,
        config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(pg.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>,
        account_factory: Arc::clone(&deps.account.provider_factory),
        book_store: Arc::clone(&deps.data.book_store),
        data_quality: Arc::clone(&deps.data.data_quality) as Arc<dyn DataQualityPort>,
        config: Arc::clone(&deps.governance.runtime_config),
        runtime_mode: deps.governance.runtime_mode.clone(),
        kill_switch: deps.governance.kill_switch_handle.clone(),
        venue_health: breaker.venue_health(),
        exit_monitor_health,
    }))
}

/// Borrowed planes the exit-monitor engine is assembled from (avoids touching
/// the partially-moved [`ExecutionBundleDeps`]).
#[derive(Clone, Copy)]
struct ExitMonitorWiring<'a> {
    infra: &'a InfraBundle,
    data: &'a DataBundle,
    governance: &'a GovernanceBundle,
    research: &'a ResearchBundle,
}

/// Assemble the 05.6 exit-monitor engine: model-backed signal re-inference (06.0)
/// behind the score-degradation evaluator, the exit dispatcher, and the per-lot sweep.
fn build_exit_monitor(
    wiring: ExitMonitorWiring<'_>,
    submission: &Arc<dyn ExecutionSubmissionRepository>,
    order_client: &Arc<dyn PolymarketOrderClient>,
    breaker: &Arc<ExecutionBreaker>,
    health: ExitMonitorHealthHandle,
) -> Arc<ExitMonitorService> {
    let pg = wiring.infra.pg.connection();
    let metrics = Arc::clone(&wiring.infra.metrics);
    let reinferer = ModelBackedExitSignalReinferer::new(ModelBackedExitSignalReinfererDeps {
        model_registry: Arc::clone(&wiring.research.model_registry_repo),
        factory_builder: Arc::clone(&wiring.research.model_runtime_factory_builder),
        weight_overlay: Arc::clone(&wiring.governance.weight_overlay),
        config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(pg.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>,
        live_config: Arc::clone(&wiring.governance.runtime_config),
        pit_source: Arc::clone(&wiring.data.pit_source),
        market_registry: Arc::clone(&wiring.data.market_registry),
        window_provider: FeatureWindowProvider::new(Arc::clone(&wiring.infra.quant_fact_read)),
    });
    let exit_signal: Arc<dyn ExitSignalEvaluator> = Arc::new(ReinferenceSignalEvaluator::new(
        ReinferenceSignalEvaluatorDeps {
            reinferer,
            config: Arc::clone(&wiring.governance.runtime_config),
            metrics: Arc::clone(&metrics),
        },
    ));
    let exit_dispatcher = Arc::new(CoreExitDispatcher::new(ExitDispatcherDeps {
        submission: Arc::clone(submission),
        order_client: Arc::clone(order_client),
        breaker: Arc::clone(breaker),
        metrics: Arc::clone(&metrics),
    }));
    Arc::new(ExitMonitorService::new(ExitMonitorServiceDeps {
        positions: Arc::new(PgPositionRepository::new(pg.clone())) as Arc<dyn PositionRepository>,
        intents: Arc::new(PgOrderIntentRepository::new(pg.clone()))
            as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
            as Arc<dyn RecommendationRepository>,
        submission: Arc::clone(submission),
        book_store: Arc::clone(&wiring.data.book_store),
        kill_switch: wiring.governance.kill_switch_handle.clone(),
        config: Arc::clone(&wiring.governance.runtime_config),
        signal: exit_signal,
        dispatcher: exit_dispatcher,
        health,
        metrics: Arc::clone(&metrics),
    }))
}
