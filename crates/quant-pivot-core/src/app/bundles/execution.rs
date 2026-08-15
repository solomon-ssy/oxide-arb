//! Money-critical entry-execution subsystem bundle.
//!
//! Owns the venue order client, the (stateless) admission engine + input
//! builder, the stateful execution breaker, and the dispatcher that bridges an
//! admitted intent to a signed venue order. Assembled at boot from the shared
//! authenticated CLOB client (single L1+L2 identity, shared with the account
//! bundle).

use std::sync::Arc;

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::{
    app::ports::settlement_control::{
        CoreSettlementControlPort, CoreSettlementControlPortDeps, settlement_credentials,
    },
    execution::{
        AdmissionInputBuilder, AdmissionInputBuilderDeps, ClobOrderClient,
        ClobReconciliationReader, CompositeExitSignalEvaluator, CoreExecutionDispatcher,
        CoreExitDispatcher, DefaultAdmissionEngine, DispatchWake, EvidenceCollector,
        ExecutionBreaker, ExecutionDispatcherDeps, ExecutionOrderLifecyclePublisher,
        ExitDispatcherDeps, ExitMonitorHealthHandle, ExitMonitorService, ExitMonitorServiceDeps,
        ExitSignalEvaluator, IntentLifecyclePublisher, OutcomeReconciliationService,
        OutcomeReconciliationServiceDeps, PolymarketOrderClient, ReconciliationService,
        ReconciliationServiceDeps, SettlementLifecyclePublisher, VenueEvidenceCollector,
        VenueReconciliationReader,
        settlement_discovery::SettlementDiscoveryService,
        settlement_discovery_wake::SettlementDiscoveryWake,
        settlement_executor::ProductionSettlementExecutor,
        settlement_external::{
            SettlementExternalObservationService, SettlementExternalObservationServiceDeps,
        },
        settlement_governed_action_service::{
            SettlementGovernedActionExecutor, SettlementGovernedActionService,
            SettlementGovernedActionServiceDeps,
        },
        settlement_preflight::{SettlementPreflightService, SettlementPreflightServiceDeps},
        settlement_recovery_admission::SettlementRecoveryAdmissionPort,
        settlement_service::{
            SettlementService, SettlementServiceDeps, SettlementSubmissionExecutor,
        },
    },
    prefetch::feature_window::FeatureWindowProvider,
    service::{
        feature_integrity::RepositoryFeatureParityGate,
        model_backed_reinferer::{
            ModelBackedExitSignalReinferer, ModelBackedExitSignalReinfererDeps,
        },
        opportunistic_sell::{
            ModelBackedOpportunisticSellScorer, ModelBackedOpportunisticSellScorerDeps,
            OpportunisticSellSignalEvaluator, OpportunisticSellSignalEvaluatorDeps,
        },
        signal_reinference::{ReinferenceSignalEvaluator, ReinferenceSignalEvaluatorDeps},
    },
};
use quant_pivot_api::{
    clob::ClobClient,
    keystore::OrderSigner,
    settlement::{
        adapter::AlloySettlementAdapterReader,
        contracts::{
            AlloySettlementChainReader, ContractDeploymentVerifier, SettlementDeploymentCatalog,
        },
        external::ExternalSettlementScanner,
        resolution::{AlloyFinalizedResolutionReader, ResolutionSourceReader},
    },
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::DeployConfig,
    domain::ports::{
        DataQualityPort, ExecutionSubmitPort, settlement_control::SettlementControlPort,
    },
    types::WorkerId,
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ClobMarketInfoRepository, DomainSourceCursorRepository,
    EntryConditionRepository, ExchangeHistoryRepository, ExecutionAttemptOutcomeRepository,
    ExecutionOrderRepository, ExecutionSubmissionRepository, FactorRepository,
    FeatureParityRepository, MarketRepository, ModelRegistryRepository, OperationLogRepository,
    OrderIntentRepository, PolicyRepository, PositionRepository,
    RecommendationExecutionRollupRepository, RecommendationReportRepository,
    RecommendationRepository, RecommendationResolutionOutcomeRepository, ReconciliationRepository,
    ResolutionObservationRepository, TradePolicyRepository,
    quant::{
        settlement_governance::{
            SettlementExternalCursorRepository, SettlementGovernanceRepository,
        },
        settlement_redeem::SettlementRedeemRepository,
    },
};

/// Dependencies for [`ExecutionBundle::assemble`].
pub struct ExecutionBundleDeps<'a> {
    pub deploy: &'a DeployConfig,
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub governance: &'a GovernanceBundle,
    pub research: &'a ResearchBundle,
    pub account: &'a AccountBundle,
    /// Shared `quant.intent` lifecycle fan-out (bootstrap singleton).
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Shared authenticated CLOB client (same identity as the account bundle).
    pub clob: Arc<ClobClient>,
    pub signer: Arc<OrderSigner>,
    pub wallet: WalletTopology,
    pub settlement_discovery_wake: SettlementDiscoveryWake,
}

/// Entry-execution subsystem: order client + admission + breaker + dispatcher.
pub struct ExecutionBundle {
    /// Shared authenticated venue client used by runtime market-info capture.
    pub clob: Arc<ClobClient>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub dispatcher: Arc<dyn ExecutionSubmitPort>,
    pub breaker: Arc<ExecutionBreaker>,
    /// Cross-table submission transactions (also drives boot recovery).
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    /// Reconciliation engine: resolves in-flight orders to venue truth.
    pub reconciliation: Arc<ReconciliationService>,
    /// Produces orthogonal resolution and execution outcome truth.
    pub outcome_reconciliation: Arc<OutcomeReconciliationService>,
    /// Exit-monitor engine: scans open lots and drives the exit ladder.
    pub exit_monitor: Arc<ExitMonitorService>,
    /// Exit-monitor health hot read consumed by admission `#20`.
    pub exit_monitor_health: ExitMonitorHealthHandle,
    /// Approve→submit wake signal (shared by the intent service and the
    /// dispatcher worker); the durable queue stays in Postgres.
    pub dispatch_wake: DispatchWake,
    pub settlement_discovery: Arc<SettlementDiscoveryService>,
    pub settlement_preflight: Arc<SettlementPreflightService>,
    pub settlement: Arc<SettlementService>,
    pub settlement_governed_actions: Arc<SettlementGovernedActionService>,
    pub settlement_external: Arc<SettlementExternalObservationService>,
    pub settlement_control: Arc<dyn SettlementControlPort>,
    pub settlement_recovery_admission: Arc<dyn SettlementRecoveryAdmissionPort>,
    pub settlement_discovery_wake: SettlementDiscoveryWake,
}

impl ExecutionBundle {
    /// Assemble the execution subsystem from the shared planes.
    pub fn assemble(deps: &ExecutionBundleDeps<'_>) -> QuantResult<Self> {
        let infra = deps.infra;
        let repos = &infra.repos;

        let breaker = build_execution_breaker(deps)?;
        let settlement = build_settlement_runtime(deps)?;
        let order_lifecycle = Arc::new(ExecutionOrderLifecyclePublisher::new(
            deps.intent_lifecycle.publisher(),
        ));

        // Exit-monitor health seam: owned by the governance bundle so it
        // is shared with the mode preflight; published by the worker, read by
        // admission `#20`. Starts not-ready (fail-closed) until the first scan.
        let exit_monitor_health = deps.governance.exit_monitor_health.clone();

        // Stateless admission: input builder (breaker venue-health + exit-monitor
        // seams) + the 25-check engine.
        let admission_builder = build_admission_builder(
            deps,
            &breaker,
            exit_monitor_health.clone(),
            Arc::clone(&settlement.recovery_admission),
        );
        let admission = Arc::new(DefaultAdmissionEngine::new(Arc::clone(&infra.metrics)));

        let clob = Arc::clone(&deps.clob);
        let order_client: Arc<dyn PolymarketOrderClient> =
            Arc::new(ClobOrderClient::new(Arc::clone(&clob)));

        let submission: Arc<dyn ExecutionSubmissionRepository> =
            Arc::clone(&repos.execution_submission) as Arc<dyn ExecutionSubmissionRepository>;
        let intents: Arc<dyn OrderIntentRepository> =
            Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>;

        let dispatcher: Arc<dyn ExecutionSubmitPort> =
            Arc::new(CoreExecutionDispatcher::new(ExecutionDispatcherDeps {
                intents,
                submission: Arc::clone(&submission),
                conditions: Arc::clone(&repos.entry_condition) as Arc<dyn EntryConditionRepository>,
                admission_builder,
                admission,
                order_client: Arc::clone(&order_client),
                breaker: Arc::clone(&breaker),
                metrics: Arc::clone(&infra.metrics),
                execution_events: Arc::clone(&infra.execution_event_writer),
                intent_lifecycle: Arc::clone(&deps.intent_lifecycle),
                order_lifecycle: Arc::clone(&order_lifecycle),
                feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(Arc::clone(
                    &repos.feature_parity,
                )
                    as Arc<dyn FeatureParityRepository>)),
            }));

        // Reconciliation engine: venue reader + fixed-order evidence
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
            execution_orders: Arc::clone(&repos.execution_order)
                as Arc<dyn ExecutionOrderRepository>,
            intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
            recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            positions: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
            reconciliation: Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
            submission: Arc::clone(&submission),
            breaker: Arc::clone(&breaker),
            metrics: Arc::clone(&infra.metrics),
            config: Arc::clone(&deps.governance.runtime_config),
            capital: Arc::clone(&repos.capital_allocation) as Arc<dyn CapitalAllocationRepository>,
            execution_events: Arc::clone(&infra.execution_event_writer),
            capital_events: Arc::clone(&infra.capital_allocation_event_writer),
            position_events: Arc::clone(&infra.position_event_writer),
            intent_lifecycle: Arc::clone(&deps.intent_lifecycle),
            order_lifecycle: Arc::clone(&order_lifecycle),
            events: deps.intent_lifecycle.publisher(),
        }));

        // Exit-monitor engine: model-driven signal seam + exit dispatcher
        // + per-lot sweep service.
        let exit_monitor = build_exit_monitor(
            ExitMonitorWiring {
                deploy: deps.deploy,
                infra,
                data: deps.data,
                governance: deps.governance,
                research: deps.research,
            },
            &submission,
            &order_client,
            &breaker,
            &order_lifecycle,
            exit_monitor_health.clone(),
        );
        let outcome_reconciliation = build_outcome_reconciliation_service(deps)?;

        Ok(Self {
            clob,
            order_client,
            dispatcher,
            breaker,
            submission,
            reconciliation,
            outcome_reconciliation,
            exit_monitor,
            exit_monitor_health,
            dispatch_wake: DispatchWake::new(),
            settlement_discovery: settlement.discovery,
            settlement_preflight: settlement.preflight,
            settlement: settlement.service,
            settlement_governed_actions: settlement.governed_actions,
            settlement_external: settlement.external,
            settlement_control: settlement.control,
            settlement_recovery_admission: settlement.recovery_admission,
            settlement_discovery_wake: deps.settlement_discovery_wake.clone(),
        })
    }
}

struct SettlementRuntime {
    discovery: Arc<SettlementDiscoveryService>,
    preflight: Arc<SettlementPreflightService>,
    service: Arc<SettlementService>,
    governed_actions: Arc<SettlementGovernedActionService>,
    external: Arc<SettlementExternalObservationService>,
    control: Arc<dyn SettlementControlPort>,
    recovery_admission: Arc<dyn SettlementRecoveryAdmissionPort>,
}

fn build_settlement_runtime(deps: &ExecutionBundleDeps<'_>) -> QuantResult<SettlementRuntime> {
    let repository =
        Arc::clone(&deps.infra.repos.settlement_redeem) as Arc<dyn SettlementRedeemRepository>;
    let lifecycle = Arc::new(SettlementLifecyclePublisher::new(
        deps.intent_lifecycle.publisher(),
    ));
    let catalog = SettlementDeploymentCatalog::official_current()
        .map_err(|source| QuantError::config(source.to_string()))?;
    let chain_reader = AlloySettlementChainReader::connect(&deps.deploy.polymarket.onchain)?;
    let verifier = Arc::new(ContractDeploymentVerifier::new(
        catalog.clone(),
        chain_reader,
    ));
    let credentials =
        settlement_credentials(deps.wallet.kind, deps.deploy.polymarket.relayer.is_ready());
    let production_executor = Arc::new(ProductionSettlementExecutor::connect(
        &deps.deploy.polymarket,
        Arc::clone(&verifier),
        Arc::clone(&deps.signer),
        deps.wallet,
        credentials,
    )?);
    let executor = Arc::clone(&production_executor) as Arc<dyn SettlementSubmissionExecutor>;
    let governed_action_executor = production_executor as Arc<dyn SettlementGovernedActionExecutor>;
    let governance_repository = Arc::clone(&deps.infra.repos.settlement_governance)
        as Arc<dyn SettlementGovernanceRepository>;
    let adapter_reader = AlloySettlementAdapterReader::connect(&deps.deploy.polymarket.onchain)
        .map_err(|source| QuantError::config(source.to_string()))?;
    let discovery = Arc::new(SettlementDiscoveryService::new(
        Arc::clone(&repository),
        Arc::clone(&lifecycle),
    ));
    let preflight = Arc::new(SettlementPreflightService::new(
        SettlementPreflightServiceDeps {
            repository: Arc::clone(&repository),
            verifier: Arc::clone(&verifier),
            adapter_reader,
            catalog: catalog.clone(),
            topology: deps.wallet,
            credentials,
            config: deps.deploy.polymarket.settlement.clone(),
            worker_id: WorkerId::from_v7(),
            lifecycle: Arc::clone(&lifecycle),
        },
    ));
    let service = Arc::new(SettlementService::new(SettlementServiceDeps {
        repository: Arc::clone(&repository),
        governance: Arc::clone(&governance_repository),
        positions: Arc::clone(&deps.infra.repos.position) as Arc<dyn PositionRepository>,
        executor,
        runtime_controls: deps.governance.runtime_controls.clone(),
        config: deps.deploy.polymarket.settlement.clone(),
        worker_id: WorkerId::from_v7(),
        metrics: Arc::clone(&deps.infra.metrics),
        lifecycle: Arc::clone(&lifecycle),
    }));
    let governed_actions = Arc::new(SettlementGovernedActionService::new(
        SettlementGovernedActionServiceDeps {
            repository: Arc::clone(&governance_repository),
            executor: governed_action_executor,
            runtime_controls: deps.governance.runtime_controls.clone(),
            config: deps.deploy.polymarket.settlement.clone(),
            execution_account_id: deps.account.execution_account.execution_account_id,
            worker_id: WorkerId::from_v7(),
            metrics: Arc::clone(&deps.infra.metrics),
        },
    ));
    let external_scanner = ExternalSettlementScanner::connect(&deps.deploy.polymarket.onchain)
        .map_err(|source| QuantError::config(source.to_string()))?;
    let external = Arc::new(SettlementExternalObservationService::new(
        SettlementExternalObservationServiceDeps {
            cases: Arc::clone(&repository),
            cursors: Arc::clone(&deps.infra.repos.settlement_governance)
                as Arc<dyn SettlementExternalCursorRepository>,
            verifier: Arc::clone(&verifier),
            scanner: external_scanner,
            topology: deps.wallet,
            execution_account_id: deps.account.execution_account.execution_account_id,
            config: deps.deploy.polymarket.settlement.clone(),
        },
    ));
    let control = Arc::new(CoreSettlementControlPort::new(
        CoreSettlementControlPortDeps {
            repository,
            governance: governance_repository,
            verifier,
            catalog,
            topology: deps.wallet,
            credentials,
            config: deps.deploy.polymarket.settlement.clone(),
            runtime_controls: deps.governance.runtime_controls.clone(),
            execution_account_id: deps.account.execution_account.execution_account_id,
            lifecycle,
        },
    ));
    let recovery_admission = Arc::clone(&control) as Arc<dyn SettlementRecoveryAdmissionPort>;
    let control = control as Arc<dyn SettlementControlPort>;
    Ok(SettlementRuntime {
        discovery,
        preflight,
        service,
        governed_actions,
        external,
        control,
        recovery_admission,
    })
}

fn build_execution_breaker(deps: &ExecutionBundleDeps<'_>) -> QuantResult<Arc<ExecutionBreaker>> {
    let breaker_config = deps
        .governance
        .runtime_config
        .current()
        .execution_risk
        .breaker
        .clone();
    let operation_log =
        Arc::clone(&deps.infra.repos.operation_log) as Arc<dyn OperationLogRepository>;
    Ok(Arc::new(ExecutionBreaker::new(
        breaker_config,
        Arc::clone(&deps.governance.kill_switch),
        operation_log,
        Arc::clone(&deps.infra.metrics),
    )?))
}

fn build_outcome_reconciliation_service(
    deps: &ExecutionBundleDeps<'_>,
) -> QuantResult<Arc<OutcomeReconciliationService>> {
    let repos = &deps.infra.repos;
    let resolution_source = Arc::new(
        AlloyFinalizedResolutionReader::connect(&deps.deploy.polymarket.onchain)
            .map_err(|source| QuantError::config(source.to_string()))?,
    ) as Arc<dyn ResolutionSourceReader>;
    Ok(Arc::new(OutcomeReconciliationService::new(
        OutcomeReconciliationServiceDeps {
            resolution_source,
            resolution_fact_writer: Arc::clone(&deps.infra.market_resolution_fact_writer),
            resolution_facts: Arc::clone(&deps.infra.quant_fact_read),
            cursors: Arc::clone(&repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            resolution_observations: Arc::clone(&repos.resolution_observation)
                as Arc<dyn ResolutionObservationRepository>,
            markets: Arc::clone(&repos.market) as Arc<dyn MarketRepository>,
            resolution_outcomes: Arc::clone(&repos.recommendation_resolution_outcome)
                as Arc<dyn RecommendationResolutionOutcomeRepository>,
            execution_outcomes: Arc::clone(&repos.execution_attempt_outcome)
                as Arc<dyn ExecutionAttemptOutcomeRepository>,
            execution_rollups: Arc::clone(&repos.recommendation_execution_rollup)
                as Arc<dyn RecommendationExecutionRollupRepository>,
        },
    )))
}

/// Assemble the stateless admission input builder (real venue-health seam from
/// the breaker and real exit-monitor readiness seam from the worker).
fn build_admission_builder(
    deps: &ExecutionBundleDeps<'_>,
    breaker: &Arc<ExecutionBreaker>,
    exit_monitor_health: ExitMonitorHealthHandle,
    settlement_recovery: Arc<dyn SettlementRecoveryAdmissionPort>,
) -> Arc<AdmissionInputBuilder> {
    let repos = &deps.infra.repos;
    Arc::new(AdmissionInputBuilder::new(AdmissionInputBuilderDeps {
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        reports: Arc::clone(&repos.recommendation_report)
            as Arc<dyn RecommendationReportRepository>,
        model_registry: Arc::clone(&repos.model_registry) as Arc<dyn ModelRegistryRepository>,
        trade_policies: Arc::clone(&repos.trade_policy) as Arc<dyn TradePolicyRepository>,
        artifact_store: Arc::clone(&deps.research.artifact_store),
        calibration_loader: Arc::clone(&deps.research.calibration_loader),
        reconciliation: Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
        execution_orders: Arc::clone(&repos.execution_order) as Arc<dyn ExecutionOrderRepository>,
        intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
        conditions: Arc::clone(&repos.entry_condition) as Arc<dyn EntryConditionRepository>,
        capital: Arc::clone(&repos.capital_allocation) as Arc<dyn CapitalAllocationRepository>,
        markets: Arc::clone(&deps.data.market_repo),
        clob_market_info: Arc::clone(&repos.clob_market_info) as Arc<dyn ClobMarketInfoRepository>,
        config_versions: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
        account_factory: Arc::clone(&deps.account.provider_factory),
        book_store: Arc::clone(&deps.data.book_store),
        clob: Arc::clone(&deps.clob),
        data_quality: Arc::clone(&deps.data.data_quality) as Arc<dyn DataQualityPort>,
        config: Arc::clone(&deps.governance.runtime_config),
        runtime_controls: deps.governance.runtime_controls.clone(),
        venue_health: breaker.venue_health(),
        exit_monitor_health,
        settlement_recovery,
    }))
}

/// Borrowed planes the exit-monitor engine is assembled from (avoids touching
/// the partially-moved [`ExecutionBundleDeps`]).
#[derive(Clone, Copy)]
struct ExitMonitorWiring<'a> {
    deploy: &'a DeployConfig,
    infra: &'a InfraBundle,
    data: &'a DataBundle,
    governance: &'a GovernanceBundle,
    research: &'a ResearchBundle,
}

/// Assemble the exit-monitor engine: model-backed signal re-inference
/// behind the score-degradation evaluator, the exit dispatcher, and the per-lot sweep.
fn build_exit_monitor(
    wiring: ExitMonitorWiring<'_>,
    submission: &Arc<dyn ExecutionSubmissionRepository>,
    order_client: &Arc<dyn PolymarketOrderClient>,
    breaker: &Arc<ExecutionBreaker>,
    order_lifecycle: &Arc<ExecutionOrderLifecyclePublisher>,
    health: ExitMonitorHealthHandle,
) -> Arc<ExitMonitorService> {
    let repos = &wiring.infra.repos;
    let metrics = Arc::clone(&wiring.infra.metrics);
    let audit = Arc::clone(&wiring.infra.exit_signal_evaluation_event_writer);
    let reinferer = ModelBackedExitSignalReinferer::new(ModelBackedExitSignalReinfererDeps {
        model_registry: Arc::clone(&wiring.research.model_registry_repo),
        serving_preimages: Arc::clone(&wiring.research.serving_preimages),
        config_versions: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        factors: Arc::clone(&wiring.research.factor_repo) as Arc<dyn FactorRepository>,
        pit_source: Arc::clone(&wiring.data.pit_source),
        window_provider: FeatureWindowProvider::new(Arc::clone(&wiring.infra.quant_fact_read)),
        exchange_history_repo: Arc::clone(&repos.exchange_history)
            as Arc<dyn ExchangeHistoryRepository>,
        finalized_exchange_history: wiring.deploy.market_data.finalized_exchange_history.clone(),
    });
    // thesis-invalidation re-inference (invalidation-first).
    let reinference: Arc<dyn ExitSignalEvaluator> = Arc::new(ReinferenceSignalEvaluator::new(
        ReinferenceSignalEvaluatorDeps {
            reinferer,
            config: Arc::clone(&wiring.governance.runtime_config),
            metrics: Arc::clone(&metrics),
            audit: Arc::clone(&audit),
        },
    ));
    // opportunistic Sell scorer (advisory scale-out; runs only when the
    // thesis holds — composed behind re-inference).
    let opportunistic_scorer =
        ModelBackedOpportunisticSellScorer::new(ModelBackedOpportunisticSellScorerDeps {
            model_registry: Arc::clone(&wiring.research.model_registry_repo),
            serving_preimages: Arc::clone(&wiring.research.serving_preimages),
            config: Arc::clone(&wiring.governance.runtime_config),
            recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            factors: Arc::clone(&wiring.research.factor_repo) as Arc<dyn FactorRepository>,
            pit_source: Arc::clone(&wiring.data.pit_source),
            window_provider: FeatureWindowProvider::new(Arc::clone(&wiring.infra.quant_fact_read)),
            exchange_history_repo: Arc::clone(&repos.exchange_history)
                as Arc<dyn ExchangeHistoryRepository>,
            finalized_exchange_history: wiring
                .deploy
                .market_data
                .finalized_exchange_history
                .clone(),
        });
    let opportunistic: Arc<dyn ExitSignalEvaluator> = Arc::new(
        OpportunisticSellSignalEvaluator::new(OpportunisticSellSignalEvaluatorDeps {
            scorer: opportunistic_scorer,
            config: Arc::clone(&wiring.governance.runtime_config),
            metrics: Arc::clone(&metrics),
            audit,
        }),
    );
    let exit_signal: Arc<dyn ExitSignalEvaluator> = Arc::new(CompositeExitSignalEvaluator::new(
        reinference,
        opportunistic,
    ));
    let exit_dispatcher = Arc::new(CoreExitDispatcher::new(ExitDispatcherDeps {
        submission: Arc::clone(submission),
        order_client: Arc::clone(order_client),
        breaker: Arc::clone(breaker),
        metrics: Arc::clone(&metrics),
        execution_events: Arc::clone(&wiring.infra.execution_event_writer),
        intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
        clob_market_info: Arc::clone(&repos.clob_market_info) as Arc<dyn ClobMarketInfoRepository>,
        book_store: Arc::clone(&wiring.data.book_store),
        order_lifecycle: Arc::clone(order_lifecycle),
    }));
    Arc::new(ExitMonitorService::new(ExitMonitorServiceDeps {
        positions: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
        intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        submission: Arc::clone(submission),
        book_store: Arc::clone(&wiring.data.book_store),
        runtime_controls: wiring.governance.runtime_controls.clone(),
        config: Arc::clone(&wiring.governance.runtime_config),
        signal: exit_signal,
        dispatcher: exit_dispatcher,
        health,
        metrics: Arc::clone(&metrics),
        alerts: Arc::clone(&wiring.governance.alerts),
    }))
}
