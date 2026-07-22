//! Money-critical entry-execution subsystem bundle.
//!
//! Owns the venue order client, the (stateless) admission engine + input
//! builder, the stateful execution breaker, and the dispatcher that bridges an
//! admitted intent to a signed venue order. Assembled at boot from the shared
//! authenticated CLOB client (single L1+L2 identity, shared with the account
//! bundle).

use std::sync::Arc;

use quant_pivot_api::{
    clob::ClobClient, ctf::CtfClient, keystore::OrderSigner, wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::DeployConfig,
    domain::ports::{DataQualityPort, ExecutionSubmitPort},
    types::EvmAddress,
};
use quant_pivot_repository::traits::{
    AttributionRepository, CapitalAllocationRepository, ClobMarketInfoRepository,
    EntryConditionRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    FactorRepository, FeatureParityRepository, MarketRepository, ModelRegistryRepository,
    OperationLogRepository, OrderIntentRepository, PolicyRepository, PositionRepository,
    RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
    SettlementRedeemRepository, TradePolicyRepository,
};

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::{
    execution::{
        AdmissionInputBuilder, AdmissionInputBuilderDeps, AttributionService,
        AttributionServiceDeps, ClobOrderClient, ClobReconciliationReader,
        CompositeExitSignalEvaluator, CoreExecutionDispatcher, CoreExitDispatcher,
        DefaultAdmissionEngine, DispatchWake, EvidenceCollector, ExecutionBreaker,
        ExecutionDispatcherDeps, ExitDispatcherDeps, ExitMonitorHealthHandle, ExitMonitorService,
        ExitMonitorServiceDeps, ExitSignalEvaluator, IntentLifecyclePublisher,
        PolymarketOrderClient, ReconciliationService, ReconciliationServiceDeps,
        RelayerConnectParams, RelayerSettlementClient, SettlementCtfClient,
        SettlementRedeemService, SettlementRedeemServiceDeps, VenueEvidenceCollector,
        VenueReconciliationReader,
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
    /// Shared EOA signer loaded once at boot.
    pub signer: Arc<OrderSigner>,
    /// Resolved + validated venue wallet topology (drives settlement routing).
    pub wallet: WalletTopology,
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
    /// Exit-monitor engine: scans open lots and drives the exit ladder.
    pub exit_monitor: Arc<ExitMonitorService>,
    /// On-chain settlement redeem engine.
    pub settlement_redeem: Arc<SettlementRedeemService>,
    /// Final WORM recommendation-attribution builder.
    pub attribution: Arc<AttributionService>,
    /// Exit-monitor health hot read consumed by admission `#20`.
    pub exit_monitor_health: ExitMonitorHealthHandle,
    /// Approve→submit wake signal (shared by the intent service and the
    /// dispatcher worker); the durable queue stays in Postgres.
    pub dispatch_wake: DispatchWake,
}

impl ExecutionBundle {
    /// Assemble the execution subsystem from the shared planes.
    pub fn assemble(deps: &ExecutionBundleDeps<'_>) -> QuantResult<Self> {
        let infra = deps.infra;
        let repos = &infra.repos;

        let breaker = build_execution_breaker(deps)?;

        // Exit-monitor health seam: owned by the governance bundle so it
        // is shared with the mode preflight; published by the worker, read by
        // admission `#20`. Starts not-ready (fail-closed) until the first scan.
        let exit_monitor_health = deps.governance.exit_monitor_health.clone();

        // Stateless admission: input builder (breaker venue-health + exit-monitor
        // seams) + the 24-check engine.
        let admission_builder =
            build_admission_builder(deps, &breaker, exit_monitor_health.clone());
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
            events: deps.intent_lifecycle.publisher(),
        }));

        // Exit-monitor engine: model-driven signal seam + exit dispatcher
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
        let settlement_redeem = build_settlement_redeem_service(deps)?;

        Ok(Self {
            clob,
            order_client,
            dispatcher,
            breaker,
            submission,
            reconciliation,
            exit_monitor,
            settlement_redeem,
            attribution,
            exit_monitor_health,
            dispatch_wake: DispatchWake::new(),
        })
    }
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

fn build_settlement_redeem_service(
    deps: &ExecutionBundleDeps<'_>,
) -> QuantResult<Arc<SettlementRedeemService>> {
    let repos = &deps.infra.repos;
    let funder_address = deps.deploy.quant.account.funder.clone().ok_or_else(|| {
        QuantError::config("quant.account.funder is required for settlement redeem")
    })?;
    // The wallet topology (signer/funder equality for EOA, CREATE2 match for
    // proxy/Safe) is validated at boot in `resolve_wallet_topology`; here it only
    // selects the on-chain settlement route.
    let ctf = build_settlement_ctf_client(deps, &funder_address)?;
    let funder_address =
        EvmAddress::parse(funder_address).map_err(|error| QuantError::config(error.to_string()))?;

    Ok(Arc::new(SettlementRedeemService::new(
        SettlementRedeemServiceDeps {
            positions: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
            intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
            markets: Arc::clone(&repos.market) as Arc<dyn MarketRepository>,
            settlement_redeems: Arc::clone(&repos.settlement_redeem)
                as Arc<dyn SettlementRedeemRepository>,
            capital: Arc::clone(&repos.capital_allocation) as Arc<dyn CapitalAllocationRepository>,
            ctf,
            runtime_mode: deps.governance.runtime_mode.clone(),
            kill_switch: deps.governance.kill_switch_handle.clone(),
            config: Arc::clone(&deps.governance.runtime_config),
            funder_address,
            wallet_kind: deps.deploy.quant.account.wallet_kind,
            capital_events: Arc::clone(&deps.infra.capital_allocation_event_writer),
            position_events: Arc::clone(&deps.infra.position_event_writer),
            events: deps.intent_lifecycle.publisher(),
        },
    )))
}

/// Select the settlement client by wallet topology: EOA signs + pays gas directly
/// via the CTF contract; Proxy / Gnosis Safe read on-chain state via RPC but
/// broadcast the redeem gaslessly through the Polymarket relayer.
fn build_settlement_ctf_client(
    deps: &ExecutionBundleDeps<'_>,
    funder_address: &str,
) -> QuantResult<Arc<dyn SettlementCtfClient>> {
    let ctf = CtfClient::connect(deps.signer.as_ref(), &deps.deploy.polymarket)?;
    if deps.wallet.is_eoa() {
        return Ok(Arc::new(ctf) as Arc<dyn SettlementCtfClient>);
    }
    // Proxy / Gnosis Safe: on-chain reads work immediately, but the gasless
    // relayer (which requires API credentials) is connected lazily on the first
    // redeem. `report_only` never redeems, so it boots without relayer creds;
    // `semi_auto` / `auto_execution` fail closed at redeem time if creds are
    // absent (relayer credential gating lives in mode preflight / validation).
    let connect = RelayerConnectParams {
        signer: Arc::clone(&deps.signer),
        relayer_config: deps.deploy.polymarket.relayer.clone(),
        wallet: deps.wallet,
        chain_id: deps.deploy.polymarket.chain_id,
    };
    Ok(Arc::new(RelayerSettlementClient::deferred(
        ctf,
        funder_address.to_owned(),
        connect,
    )) as Arc<dyn SettlementCtfClient>)
}

fn build_attribution_service(infra: &InfraBundle) -> Arc<AttributionService> {
    let repos = &infra.repos;
    Arc::new(AttributionService::new(AttributionServiceDeps {
        attribution: Arc::clone(&repos.attribution) as Arc<dyn AttributionRepository>,
        intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        execution_orders: Arc::clone(&repos.execution_order) as Arc<dyn ExecutionOrderRepository>,
        positions: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
        reconciliation: Arc::clone(&repos.reconciliation) as Arc<dyn ReconciliationRepository>,
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

/// Assemble the exit-monitor engine: model-backed signal re-inference
/// behind the score-degradation evaluator, the exit dispatcher, and the per-lot sweep.
fn build_exit_monitor(
    wiring: ExitMonitorWiring<'_>,
    submission: &Arc<dyn ExecutionSubmissionRepository>,
    order_client: &Arc<dyn PolymarketOrderClient>,
    breaker: &Arc<ExecutionBreaker>,
    health: ExitMonitorHealthHandle,
) -> Arc<ExitMonitorService> {
    let repos = &wiring.infra.repos;
    let metrics = Arc::clone(&wiring.infra.metrics);
    let audit = Arc::clone(&wiring.infra.exit_signal_evaluation_event_writer);
    let reinferer = ModelBackedExitSignalReinferer::new(ModelBackedExitSignalReinfererDeps {
        model_registry: Arc::clone(&wiring.research.model_registry_repo),
        factory_builder: Arc::clone(&wiring.research.model_runtime_factory_builder),
        weight_overlay: Arc::clone(&wiring.governance.weight_overlay),
        config_versions: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        factors: Arc::clone(&wiring.research.factor_repo) as Arc<dyn FactorRepository>,
        pit_source: Arc::clone(&wiring.data.pit_source),
        window_provider: FeatureWindowProvider::new(Arc::clone(&wiring.infra.quant_fact_read)),
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
            factory_builder: Arc::clone(&wiring.research.model_runtime_factory_builder),
            config: Arc::clone(&wiring.governance.runtime_config),
            recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            factors: Arc::clone(&wiring.research.factor_repo) as Arc<dyn FactorRepository>,
            pit_source: Arc::clone(&wiring.data.pit_source),
            window_provider: FeatureWindowProvider::new(Arc::clone(&wiring.infra.quant_fact_read)),
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
    }));
    Arc::new(ExitMonitorService::new(ExitMonitorServiceDeps {
        positions: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
        intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
        submission: Arc::clone(submission),
        book_store: Arc::clone(&wiring.data.book_store),
        kill_switch: wiring.governance.kill_switch_handle.clone(),
        config: Arc::clone(&wiring.governance.runtime_config),
        signal: exit_signal,
        dispatcher: exit_dispatcher,
        health,
        metrics: Arc::clone(&metrics),
        alerts: Arc::clone(&wiring.governance.alerts),
    }))
}
