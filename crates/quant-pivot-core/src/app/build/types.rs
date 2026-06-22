//! Shared wiring types and channel capacity constants for the composition root.

use super::super::{ExecutionBundle, task_registry::PendingTaskQueue};
use crate::{
    bridge::{
        CoreOpportunityPipeline, execution_mode::ExecutionModeHandle,
        potential_loss_store::CorePotentialLossStore, risk_metrics::CoreRiskMetrics,
    },
    control::{
        ControlFactorRegistry,
        factor_refresher::FactorRefresher,
        factor_shadow::{ShadowDecisionWriter, ShadowWriterTask},
        factor_snapshot::FactorSnapshotStore,
    },
    detection::{coalescer::Coalescer, funnel::Funnel, scanner::Scanner},
    execution::{
        fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM,
        runner::ExecutionRunner,
        settlement::{dedup::SettlementDedup, service::MarketSettlementService},
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::{health_checker::HealthChecker, risk_decision_audit_buffer::RiskDecisionAuditBuffer},
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        balance_fact_writer::BalanceFactWriter,
        book_decision_context_writer::BookDecisionContextWriter, book_fact_writer::BookFactWriter,
        execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_registry::MarketRegistry, staleness_classifier::StalenessClassifier,
        universe_filter::MarketUniverseFilter,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        catalog_readiness::CatalogReadiness,
        detection_readiness::DetectionReadiness,
        gamma::GammaService,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
        runtime_lifecycle::LatestUnhealthySubsystems,
        ws_subscription::WsSubscriptionCoordinator,
    },
    trade_integrity::TradeIntegrityStore,
};
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::{
    VotingOracle, clob::ClobClient, ctf::client::CtfRedeemClient, fees::FeeCalculator,
    gamma::GammaClient, ws::ClobWsManager,
};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{CoreEvent, CoreEventPublisher, settlement::MarketSettlementRequest},
    enums::common::ExecutionMode,
    runtime_config::RuntimeConfig,
    types::{MarketId, TokenId},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgBlacklistPersistenceRepository, PgCalibrationRepository, PgEmergencyRepository,
        PgEventRepository, PgFactDataRepository, PgMarketRepository, PgPositionRepository,
        PgPotentialLossRepository, PgReconciliationRepository, PgReportRepository,
        PgResolutionEventRepository, PgRiskAuditRepository, PgRiskStateRepository,
        PgTradeRepository, risk_fill::PgRiskFillRepository,
    },
    traits::{ControlFactorRepository, RuntimeConfigVersionRepository},
};
use oxide_arb_risk::{audit::RiskAuditEvent, engine::RiskEngine};
use oxide_arb_storage::{
    cache::{CacheManager, RedisPool},
    clickhouse::ClickHousePool,
    postgres::PostgresPool,
};
use oxide_arb_web::jwt::RedisTokenBlacklist;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::control::status::SystemStatusNudge;
use crate::infra::persistence_writers::PersistenceBundle;

type PostBootstrapInfraParts = (
    Arc<PostgresPool>,
    Arc<ClickHousePool>,
    RedisPool,
    Arc<CacheManager>,
    Arc<RedisTokenBlacklist>,
    Arc<CatalogReadiness>,
    Arc<MetricsHub>,
    Arc<AlertDispatcher>,
    Arc<RiskDecisionAuditBuffer>,
    Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    BuildRepos,
    Arc<BalanceFactWriter>,
    Arc<FactorSnapshotStore>,
    Arc<FactorRefresher>,
    Arc<ControlFactorRegistry>,
    Mutex<Option<ShadowWriterTask>>,
    PersistenceBundle,
);

type InfraCorePackParts = (
    Arc<PostgresPool>,
    Arc<ClickHousePool>,
    RedisPool,
    Arc<CacheManager>,
    Arc<RedisTokenBlacklist>,
    Arc<MetricsHub>,
    Arc<AlertDispatcher>,
    Arc<RiskDecisionAuditBuffer>,
    Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    BuildRepos,
    Arc<BalanceFactWriter>,
    Arc<FactorSnapshotStore>,
    Arc<FactorRefresher>,
    Arc<ControlFactorRegistry>,
    Mutex<Option<ShadowWriterTask>>,
);

type RiskBundleParts = (
    Arc<RiskEngine>,
    Arc<CoreRiskMetrics>,
    Arc<RiskMetricsState>,
    Arc<InMemoryExposureReservation>,
    Arc<CorePotentialLossStore>,
    Arc<RiskMetricsRefreshService>,
    Arc<ExecutionFSM>,
);

type DetectionAppDataParts = (
    Arc<BookStore>,
    Arc<MarketRegistry>,
    Arc<MarketCache>,
    Arc<GammaService>,
    flume::Receiver<TokenId>,
    flume::Receiver<MarketId>,
    Arc<CoreOpportunityPipeline>,
    Arc<ResolutionCalibrator>,
    Arc<CalibrationUpdater>,
    Arc<Scanner>,
    Arc<Coalescer>,
);

type ExecutionAppParts = (
    Arc<Funnel>,
    Arc<DataPipeline>,
    ExecutionBundle,
    flume::Receiver<MarketSettlementRequest>,
    Vec<ExecutionRunner>,
);

type ControlFactorBootstrapParts = (
    Arc<FactorSnapshotStore>,
    Arc<FactorRefresher>,
    Arc<ControlFactorRegistry>,
    ShadowDecisionWriter,
    Mutex<Option<ShadowWriterTask>>,
);

type AppContextAssemblyIntoParts = (
    Arc<DeployConfig>,
    Arc<RuntimeConfigStore>,
    Arc<RuntimeConfigApplicator>,
    ExecutionModeHandle,
    CoreEventPublisher,
    flume::Receiver<CoreEvent>,
    BuildInfraCore,
    BuildClients,
    BuildRisk,
    BuildTrading,
    Arc<TradeIntegrityStore>,
    BuildPersistence,
    Arc<MarketSettlementService>,
    Arc<SettlementDedup>,
    CancellationToken,
    PendingTaskQueue,
    TradingLifecycleWiring,
);

/// Inputs for [`DetectionStack::assembled`].
pub(super) struct DetectionStackParts {
    pub(super) book_store: Arc<BookStore>,
    pub(super) market_registry: Arc<MarketRegistry>,
    pub(super) market_cache: Arc<MarketCache>,
    pub(super) universe: Arc<MarketUniverseFilter>,
    pub(super) ws_subscription: Arc<WsSubscriptionCoordinator>,
    pub(super) gamma_service: Arc<GammaService>,
    pub(super) opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    pub(super) calibrator: Arc<ResolutionCalibrator>,
    pub(super) calibration_updater: Arc<CalibrationUpdater>,
    pub(super) scanner: Arc<Scanner>,
    pub(super) coalescer: Arc<Coalescer>,
    pub(super) staleness: StalenessClassifier,
    pub(super) token_tx: flume::Sender<TokenId>,
    pub(super) token_rx: flume::Receiver<TokenId>,
    pub(super) market_rx: flume::Receiver<MarketId>,
}

/// Inputs for [`BuildRisk::assembled`].
pub(super) struct BuildRiskParts {
    pub(super) exposure: Arc<InMemoryExposureReservation>,
    pub(super) metrics: Arc<CoreRiskMetrics>,
    pub(super) metrics_state: Arc<RiskMetricsState>,
    pub(super) engine: Arc<RiskEngine>,
    pub(super) potential_loss_store: Arc<CorePotentialLossStore>,
    pub(super) metrics_refresh: Arc<RiskMetricsRefreshService>,
    pub(super) fsm: Arc<ExecutionFSM>,
    pub(super) backpressure: Arc<BackpressurePolicy>,
}

/// Inputs for [`ExecutionLoop::assembled`].
pub(super) struct ExecutionLoopParts {
    pub(super) funnel: Arc<Funnel>,
    pub(super) validator: Arc<Validator>,
    pub(super) order_strategy: Arc<FokOrderStrategy>,
    pub(super) data_pipeline: Arc<DataPipeline>,
    pub(super) execution: ExecutionBundle,
    pub(super) settlement_rx: flume::Receiver<MarketSettlementRequest>,
    pub(super) execution_runners: Vec<ExecutionRunner>,
    pub(super) trade_integrity: Arc<TradeIntegrityStore>,
}

/// Inputs for [`BuildInfraCore::assembled`].
pub(super) struct BuildInfraCoreParts {
    pub(super) pg_pool: Arc<PostgresPool>,
    pub(super) ch_pool: Arc<ClickHousePool>,
    pub(super) redis_pool: RedisPool,
    pub(super) cache: Arc<CacheManager>,
    pub(super) jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub(super) catalog: Arc<CatalogReadiness>,
    pub(super) metrics: Arc<MetricsHub>,
    pub(super) alerts: Arc<AlertDispatcher>,
    pub(super) risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    pub(super) risk_decision_audit_rx: Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    pub(super) repos: BuildRepos,
    pub(super) balance_fact_writer: Arc<BalanceFactWriter>,
    pub(super) factor_store: Arc<FactorSnapshotStore>,
    pub(super) factor_refresher: Arc<FactorRefresher>,
    pub(super) factor_registry: Arc<ControlFactorRegistry>,
    pub(super) shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

/// Inputs for [`BuildInfra::assembled`].
pub(super) struct BuildInfraParts {
    pub(super) execution_mode: ExecutionMode,
    pub(super) runtime_store: Arc<RuntimeConfigStore>,
    pub(super) core: BuildInfraCoreParts,
    pub(super) persistence: PersistenceBundle,
    pub(super) shadow_writer: ShadowDecisionWriter,
}

/// Inputs for [`BuildPersistence::assembled`].
pub(super) struct BuildPersistenceParts {
    pub(super) trade_repo: Arc<PgTradeRepository>,
    pub(super) timeseries: Arc<ChTimeseriesRepository>,
    pub(super) audit_writer: Arc<ExecutionAuditWriter>,
    pub(super) book_decision_context_writer: Arc<BookDecisionContextWriter>,
    pub(super) book_fact_writer: Arc<BookFactWriter>,
}

/// Inputs for [`BuildClients::assembled`].
pub(super) struct BuildClientsParts {
    pub(super) ws_manager: Arc<ClobWsManager>,
    pub(super) gamma_client: Arc<GammaClient>,
    pub(super) fee_calculator: Arc<FeeCalculator>,
    pub(super) voting_oracle: Arc<VotingOracle>,
    pub(super) clob_client: Option<Arc<ClobClient>>,
    pub(super) ctf_redeem: Option<Arc<CtfRedeemClient>>,
    pub(super) holder_address: String,
}

/// Inputs for [`ControlFactorWiring::assembled`].
pub(super) struct ControlFactorWiringParts {
    pub(super) factor_store: Arc<FactorSnapshotStore>,
    pub(super) factor_refresher: Arc<FactorRefresher>,
    pub(super) factor_registry: Arc<ControlFactorRegistry>,
    pub(super) shadow_writer: ShadowDecisionWriter,
    pub(super) shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

/// Inputs for [`AppContextAssembly::assembled`].
pub(super) struct AppContextAssemblyParts {
    pub(super) config: Arc<DeployConfig>,
    pub(super) runtime_store: Arc<RuntimeConfigStore>,
    pub(super) applicator: Arc<RuntimeConfigApplicator>,
    pub(super) execution_mode: ExecutionModeHandle,
    pub(super) events: CoreEventPublisher,
    pub(super) event_rx: flume::Receiver<CoreEvent>,
    pub(super) infra: BuildInfraCore,
    pub(super) clients: BuildClients,
    pub(super) risk: BuildRisk,
    pub(super) trading: BuildTrading,
    pub(super) trade_integrity: Arc<TradeIntegrityStore>,
    pub(super) persistence: BuildPersistence,
    pub(super) settlement_service: Arc<MarketSettlementService>,
    pub(super) settlement_dedup: Arc<SettlementDedup>,
    pub(super) shutdown: CancellationToken,
    pub(super) pending_tasks: PendingTaskQueue,
    pub(super) lifecycle: TradingLifecycleWiring,
}

/// Bounded capacity of the real-time `CoreEvent` bus. Sized for short bursts of
/// events; a full channel drops (counted per kind) rather than blocking producers.
pub const CORE_EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Risk decision audit sink — matches hot-path evaluation burst sizing.
pub const RISK_DECISION_AUDIT_CHANNEL_CAPACITY: usize = 4096;

/// Token-id channel from the data pipeline into the coalescer.
pub const COALESCER_TOKEN_CHANNEL_CAPACITY: usize = 8192;

/// Market-id channel from the coalescer into the scanner.
pub const SCANNER_MARKET_CHANNEL_CAPACITY: usize = 512;

pub struct BuildRepos {
    risk_state: Arc<PgRiskStateRepository>,
    blacklist: Arc<PgBlacklistPersistenceRepository>,
    audit: Arc<PgRiskAuditRepository>,
    risk_fill: Arc<PgRiskFillRepository>,
    emergency: Arc<PgEmergencyRepository>,
    reconciliation: Arc<PgReconciliationRepository>,
    resolution_event: Arc<PgResolutionEventRepository>,
    potential_loss: Arc<PgPotentialLossRepository>,
    calibration: Arc<PgCalibrationRepository>,
    fact_data: Arc<PgFactDataRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    market: Arc<PgMarketRepository>,
    event: Arc<PgEventRepository>,
    trade: Arc<PgTradeRepository>,
    report: Arc<PgReportRepository>,
    position: Arc<PgPositionRepository>,
    control_factor: Arc<dyn ControlFactorRepository>,
}

impl BuildRepos {
    pub(super) const fn risk_state(&self) -> &Arc<PgRiskStateRepository> {
        &self.risk_state
    }

    pub(super) const fn blacklist(&self) -> &Arc<PgBlacklistPersistenceRepository> {
        &self.blacklist
    }

    pub(super) const fn audit(&self) -> &Arc<PgRiskAuditRepository> {
        &self.audit
    }

    pub(super) const fn risk_fill(&self) -> &Arc<PgRiskFillRepository> {
        &self.risk_fill
    }

    pub(super) const fn emergency(&self) -> &Arc<PgEmergencyRepository> {
        &self.emergency
    }

    pub(super) const fn reconciliation(&self) -> &Arc<PgReconciliationRepository> {
        &self.reconciliation
    }

    pub(super) const fn resolution_event(&self) -> &Arc<PgResolutionEventRepository> {
        &self.resolution_event
    }

    pub(super) const fn potential_loss(&self) -> &Arc<PgPotentialLossRepository> {
        &self.potential_loss
    }

    pub(super) const fn calibration(&self) -> &Arc<PgCalibrationRepository> {
        &self.calibration
    }

    pub(super) const fn fact_data(&self) -> &Arc<PgFactDataRepository> {
        &self.fact_data
    }

    pub(super) const fn runtime_config(&self) -> &Arc<dyn RuntimeConfigVersionRepository> {
        &self.runtime_config
    }

    pub(super) const fn market(&self) -> &Arc<PgMarketRepository> {
        &self.market
    }

    pub(super) const fn event(&self) -> &Arc<PgEventRepository> {
        &self.event
    }

    pub(super) const fn trade(&self) -> &Arc<PgTradeRepository> {
        &self.trade
    }

    pub(super) const fn report(&self) -> &Arc<PgReportRepository> {
        &self.report
    }

    pub(super) const fn position(&self) -> &Arc<PgPositionRepository> {
        &self.position
    }

    pub(super) const fn control_factor(&self) -> &Arc<dyn ControlFactorRepository> {
        &self.control_factor
    }
}

/// Infrastructure wired during boot and consumed through trading assembly.
///
/// Bootstrap-only fields (`execution_mode`, `runtime_store`, `shadow_writer`) are
/// stripped by [`BuildInfra::finalize`] once execution wiring has cloned
/// `shadow_writer` into the pipeline.
pub struct BuildInfra {
    execution_mode: ExecutionMode,
    runtime_store: Arc<RuntimeConfigStore>,
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    redis_pool: RedisPool,
    cache: Arc<CacheManager>,
    jwt_blacklist: Arc<RedisTokenBlacklist>,
    catalog: Arc<CatalogReadiness>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    risk_decision_audit_rx: Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    repos: BuildRepos,
    persistence: PersistenceBundle,
    balance_fact_writer: Arc<BalanceFactWriter>,
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer: ShadowDecisionWriter,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

impl BuildInfra {
    pub(super) const fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    pub(super) const fn runtime_store(&self) -> &Arc<RuntimeConfigStore> {
        &self.runtime_store
    }

    pub(super) const fn cache(&self) -> &Arc<CacheManager> {
        &self.cache
    }

    pub(super) const fn catalog(&self) -> &Arc<CatalogReadiness> {
        &self.catalog
    }

    pub(super) const fn metrics(&self) -> &Arc<MetricsHub> {
        &self.metrics
    }

    pub(super) const fn alerts(&self) -> &Arc<AlertDispatcher> {
        &self.alerts
    }

    pub(super) const fn risk_decision_audit(&self) -> &Arc<RiskDecisionAuditBuffer> {
        &self.risk_decision_audit
    }

    pub(super) const fn repos(&self) -> &BuildRepos {
        &self.repos
    }

    pub(super) const fn persistence(&self) -> &PersistenceBundle {
        &self.persistence
    }

    pub(super) const fn factor_store(&self) -> &Arc<FactorSnapshotStore> {
        &self.factor_store
    }

    pub(super) const fn shadow_writer(&self) -> &ShadowDecisionWriter {
        &self.shadow_writer
    }

    /// Consume bootstrap-only fields and return the durable infra handles.
    pub(super) fn into_post_bootstrap(self) -> PostBootstrapInfraParts {
        (
            self.pg_pool,
            self.ch_pool,
            self.redis_pool,
            self.cache,
            self.jwt_blacklist,
            self.catalog,
            self.metrics,
            self.alerts,
            self.risk_decision_audit,
            self.risk_decision_audit_rx,
            self.repos,
            self.balance_fact_writer,
            self.factor_store,
            self.factor_refresher,
            self.factor_registry,
            self.shadow_writer_task,
            self.persistence,
        )
    }
}

/// Infra handles retained on [`super::super::AppContext`] after bootstrap fields
/// are stripped from [`BuildInfra`].
pub struct BuildInfraCore {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    redis_pool: RedisPool,
    cache: Arc<CacheManager>,
    jwt_blacklist: Arc<RedisTokenBlacklist>,
    catalog: Arc<CatalogReadiness>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    risk_decision_audit: Arc<RiskDecisionAuditBuffer>,
    risk_decision_audit_rx: Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
    repos: BuildRepos,
    balance_fact_writer: Arc<BalanceFactWriter>,
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

impl BuildInfraCore {
    pub(super) const fn pg_pool(&self) -> &Arc<PostgresPool> {
        &self.pg_pool
    }

    pub(super) const fn ch_pool(&self) -> &Arc<ClickHousePool> {
        &self.ch_pool
    }

    pub(super) const fn catalog(&self) -> &Arc<CatalogReadiness> {
        &self.catalog
    }

    pub(super) const fn factor_store(&self) -> &Arc<FactorSnapshotStore> {
        &self.factor_store
    }
}

pub struct BuildPersistence {
    trade_repo: Arc<PgTradeRepository>,
    timeseries: Arc<ChTimeseriesRepository>,
    audit_writer: Arc<ExecutionAuditWriter>,
    book_decision_context_writer: Arc<BookDecisionContextWriter>,
    book_fact_writer: Arc<BookFactWriter>,
}

impl BuildPersistence {
    pub(super) const fn trade_repo(&self) -> &Arc<PgTradeRepository> {
        &self.trade_repo
    }

    pub(super) const fn timeseries(&self) -> &Arc<ChTimeseriesRepository> {
        &self.timeseries
    }

    pub(super) const fn audit_writer(&self) -> &Arc<ExecutionAuditWriter> {
        &self.audit_writer
    }

    pub(super) const fn book_decision_context_writer(&self) -> &Arc<BookDecisionContextWriter> {
        &self.book_decision_context_writer
    }

    pub(super) const fn book_fact_writer(&self) -> &Arc<BookFactWriter> {
        &self.book_fact_writer
    }
}

pub struct TradingLifecycleWiring {
    system_status_nudge: SystemStatusNudge,
    detection_readiness: Arc<DetectionReadiness>,
}

impl TradingLifecycleWiring {
    pub(super) const fn system_status_nudge(&self) -> &SystemStatusNudge {
        &self.system_status_nudge
    }

    pub(super) const fn detection_readiness(&self) -> &Arc<DetectionReadiness> {
        &self.detection_readiness
    }

    pub(super) fn into_lifecycle_handles(self) -> (SystemStatusNudge, Arc<DetectionReadiness>) {
        (self.system_status_nudge, self.detection_readiness)
    }
}

pub struct TradingBuildInput<'a> {
    wiring: WiringConfig<'a>,
    execution_mode: &'a ExecutionModeHandle,
    infra: &'a BuildInfra,
    clients: &'a BuildClients,
    lifecycle: &'a TradingLifecycleWiring,
}

impl<'a> TradingBuildInput<'a> {
    pub(super) const fn new(
        wiring: WiringConfig<'a>,
        execution_mode: &'a ExecutionModeHandle,
        infra: &'a BuildInfra,
        clients: &'a BuildClients,
        lifecycle: &'a TradingLifecycleWiring,
    ) -> Self {
        Self {
            wiring,
            execution_mode,
            infra,
            clients,
            lifecycle,
        }
    }

    pub(super) const fn wiring(&self) -> WiringConfig<'a> {
        self.wiring
    }

    pub(super) const fn execution_mode(&self) -> &ExecutionModeHandle {
        self.execution_mode
    }

    pub(super) const fn infra(&self) -> &BuildInfra {
        self.infra
    }

    pub(super) const fn clients(&self) -> &BuildClients {
        self.clients
    }

    pub(super) const fn lifecycle(&self) -> &TradingLifecycleWiring {
        self.lifecycle
    }
}

pub struct AppContextAssembly {
    config: Arc<DeployConfig>,
    runtime_store: Arc<RuntimeConfigStore>,
    applicator: Arc<RuntimeConfigApplicator>,
    execution_mode: ExecutionModeHandle,
    events: CoreEventPublisher,
    event_rx: flume::Receiver<CoreEvent>,
    infra: BuildInfraCore,
    clients: BuildClients,
    risk: BuildRisk,
    trading: BuildTrading,
    trade_integrity: Arc<TradeIntegrityStore>,
    persistence: BuildPersistence,
    settlement_service: Arc<MarketSettlementService>,
    settlement_dedup: Arc<SettlementDedup>,
    shutdown: CancellationToken,
    pending_tasks: PendingTaskQueue,
    lifecycle: TradingLifecycleWiring,
}

pub struct BuildClients {
    ws_manager: Arc<ClobWsManager>,
    gamma_client: Arc<GammaClient>,
    fee_calculator: Arc<FeeCalculator>,
    voting_oracle: Arc<VotingOracle>,
    clob_client: Option<Arc<ClobClient>>,
    ctf_redeem: Option<Arc<CtfRedeemClient>>,
    holder_address: String,
}

impl BuildClients {
    pub(super) const fn ws_manager(&self) -> &Arc<ClobWsManager> {
        &self.ws_manager
    }

    pub(super) const fn gamma_client(&self) -> &Arc<GammaClient> {
        &self.gamma_client
    }

    pub(super) const fn fee_calculator(&self) -> &Arc<FeeCalculator> {
        &self.fee_calculator
    }

    pub(super) const fn voting_oracle(&self) -> &Arc<VotingOracle> {
        &self.voting_oracle
    }

    pub(super) const fn clob_client(&self) -> Option<&Arc<ClobClient>> {
        self.clob_client.as_ref()
    }

    pub(super) const fn ctf_redeem(&self) -> Option<&Arc<CtfRedeemClient>> {
        self.ctf_redeem.as_ref()
    }

    pub(super) fn holder_address(&self) -> &str {
        &self.holder_address
    }

    pub(super) fn into_trading_clients(
        self,
    ) -> (
        Option<Arc<ClobClient>>,
        Option<Arc<CtfRedeemClient>>,
        Arc<ClobWsManager>,
    ) {
        (self.clob_client, self.ctf_redeem, self.ws_manager)
    }
}

pub struct BuildRisk {
    exposure: Arc<InMemoryExposureReservation>,
    metrics: Arc<CoreRiskMetrics>,
    metrics_state: Arc<RiskMetricsState>,
    engine: Arc<RiskEngine>,
    potential_loss_store: Arc<CorePotentialLossStore>,
    metrics_refresh: Arc<RiskMetricsRefreshService>,
    fsm: Arc<ExecutionFSM>,
    backpressure: Arc<BackpressurePolicy>,
}

impl BuildRisk {
    pub(super) const fn exposure(&self) -> &Arc<InMemoryExposureReservation> {
        &self.exposure
    }

    pub(super) const fn metrics(&self) -> &Arc<CoreRiskMetrics> {
        &self.metrics
    }

    pub(super) const fn metrics_state(&self) -> &Arc<RiskMetricsState> {
        &self.metrics_state
    }

    pub(super) const fn engine(&self) -> &Arc<RiskEngine> {
        &self.engine
    }

    pub(super) const fn metrics_refresh(&self) -> &Arc<RiskMetricsRefreshService> {
        &self.metrics_refresh
    }

    pub(super) const fn fsm(&self) -> &Arc<ExecutionFSM> {
        &self.fsm
    }

    pub(super) const fn backpressure(&self) -> &Arc<BackpressurePolicy> {
        &self.backpressure
    }

    pub(super) fn into_risk_bundle(self) -> RiskBundleParts {
        (
            self.engine,
            self.metrics,
            self.metrics_state,
            self.exposure,
            self.potential_loss_store,
            self.metrics_refresh,
            self.fsm,
        )
    }
}

/// Detection + execution stacks composed without flattening intermediate fields.
pub struct BuildTrading {
    detection: DetectionStack,
    execution: ExecutionLoop,
}

impl BuildTrading {
    pub(super) const fn detection(&self) -> &DetectionStack {
        &self.detection
    }

    pub(super) const fn execution(&self) -> &ExecutionLoop {
        &self.execution
    }
}

pub struct DetectionStack {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    universe: Arc<MarketUniverseFilter>,
    ws_subscription: Arc<WsSubscriptionCoordinator>,
    gamma_service: Arc<GammaService>,
    opportunity_pipeline: Arc<CoreOpportunityPipeline>,
    calibrator: Arc<ResolutionCalibrator>,
    calibration_updater: Arc<CalibrationUpdater>,
    scanner: Arc<Scanner>,
    coalescer: Arc<Coalescer>,
    staleness: StalenessClassifier,
    token_tx: flume::Sender<TokenId>,
    token_rx: flume::Receiver<TokenId>,
    market_rx: flume::Receiver<MarketId>,
}

impl DetectionStack {
    pub(super) const fn book_store(&self) -> &Arc<BookStore> {
        &self.book_store
    }

    pub(super) const fn market_registry(&self) -> &Arc<MarketRegistry> {
        &self.market_registry
    }

    pub(super) const fn market_cache(&self) -> &Arc<MarketCache> {
        &self.market_cache
    }

    pub(super) const fn universe(&self) -> &Arc<MarketUniverseFilter> {
        &self.universe
    }

    pub(super) const fn ws_subscription(&self) -> &Arc<WsSubscriptionCoordinator> {
        &self.ws_subscription
    }

    pub(super) const fn opportunity_pipeline(&self) -> &Arc<CoreOpportunityPipeline> {
        &self.opportunity_pipeline
    }

    pub(super) const fn calibrator(&self) -> &Arc<ResolutionCalibrator> {
        &self.calibrator
    }

    pub(super) const fn calibration_updater(&self) -> &Arc<CalibrationUpdater> {
        &self.calibration_updater
    }

    pub(super) const fn coalescer(&self) -> &Arc<Coalescer> {
        &self.coalescer
    }

    pub(super) const fn staleness(&self) -> &StalenessClassifier {
        &self.staleness
    }

    pub(super) const fn token_tx(&self) -> &flume::Sender<TokenId> {
        &self.token_tx
    }

    pub(super) fn into_app_data(self) -> DetectionAppDataParts {
        (
            self.book_store,
            self.market_registry,
            self.market_cache,
            self.gamma_service,
            self.token_rx,
            self.market_rx,
            self.opportunity_pipeline,
            self.calibrator,
            self.calibration_updater,
            self.scanner,
            self.coalescer,
        )
    }
}

pub struct ExecutionLoop {
    funnel: Arc<Funnel>,
    validator: Arc<Validator>,
    order_strategy: Arc<FokOrderStrategy>,
    data_pipeline: Arc<DataPipeline>,
    execution: ExecutionBundle,
    settlement_rx: flume::Receiver<MarketSettlementRequest>,
    execution_runners: Vec<ExecutionRunner>,
    trade_integrity: Arc<TradeIntegrityStore>,
}

impl ExecutionLoop {
    pub(super) const fn funnel(&self) -> &Arc<Funnel> {
        &self.funnel
    }

    pub(super) const fn validator(&self) -> &Arc<Validator> {
        &self.validator
    }

    pub(super) const fn order_strategy(&self) -> &Arc<FokOrderStrategy> {
        &self.order_strategy
    }

    pub(super) const fn execution(&self) -> &ExecutionBundle {
        &self.execution
    }

    pub(super) const fn trade_integrity(&self) -> &Arc<TradeIntegrityStore> {
        &self.trade_integrity
    }

    pub(super) fn into_app_execution(self) -> ExecutionAppParts {
        (
            self.funnel,
            self.data_pipeline,
            self.execution,
            self.settlement_rx,
            self.execution_runners,
        )
    }
}

/// Deploy + runtime configuration views shared by the wiring functions.
#[derive(Clone, Copy)]
pub struct WiringConfig<'a> {
    deploy: &'a DeployConfig,
    runtime: &'a RuntimeConfig,
}

impl<'a> WiringConfig<'a> {
    pub(super) const fn new(deploy: &'a DeployConfig, runtime: &'a RuntimeConfig) -> Self {
        Self { deploy, runtime }
    }

    pub(super) const fn deploy(&self) -> &'a DeployConfig {
        self.deploy
    }

    pub(super) const fn runtime(&self) -> &'a RuntimeConfig {
        self.runtime
    }
}

pub struct HealthCheckerBundle {
    checker: Arc<HealthChecker>,
    unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
}

impl HealthCheckerBundle {
    pub(super) const fn checker(&self) -> &Arc<HealthChecker> {
        &self.checker
    }

    pub(super) const fn unhealthy_subsystems(&self) -> &Arc<LatestUnhealthySubsystems> {
        &self.unhealthy_subsystems
    }
}

pub struct ControlFactorWiring {
    factor_store: Arc<FactorSnapshotStore>,
    factor_refresher: Arc<FactorRefresher>,
    factor_registry: Arc<ControlFactorRegistry>,
    shadow_writer: ShadowDecisionWriter,
    shadow_writer_task: Mutex<Option<ShadowWriterTask>>,
}

impl ControlFactorWiring {
    pub(super) fn into_bootstrap_parts(self) -> ControlFactorBootstrapParts {
        (
            self.factor_store,
            self.factor_refresher,
            self.factor_registry,
            self.shadow_writer,
            self.shadow_writer_task,
        )
    }
}

impl BuildRepos {
    /// Construct every Postgres repository over a shared connection clone.
    pub(super) fn from_pool(pg_pool: &PostgresPool) -> Self {
        use oxide_arb_repository::pg_arc_repo;
        use oxide_arb_repository::postgres::{
            PgBlacklistPersistenceRepository, PgCalibrationRepository, PgControlFactorRepository,
            PgEmergencyRepository, PgEventRepository, PgFactDataRepository, PgMarketRepository,
            PgPositionRepository, PgPotentialLossRepository, PgReconciliationRepository,
            PgReportRepository, PgResolutionEventRepository, PgRiskAuditRepository,
            PgRiskStateRepository, PgRuntimeConfigVersionRepository, PgTradeRepository,
            risk_fill::PgRiskFillRepository,
        };

        let db = pg_pool.connection().clone();

        Self {
            risk_state: pg_arc_repo!(db, PgRiskStateRepository),
            blacklist: pg_arc_repo!(db, PgBlacklistPersistenceRepository),
            audit: pg_arc_repo!(db, PgRiskAuditRepository),
            risk_fill: pg_arc_repo!(db, PgRiskFillRepository),
            emergency: pg_arc_repo!(db, PgEmergencyRepository),
            reconciliation: pg_arc_repo!(db, PgReconciliationRepository),
            resolution_event: pg_arc_repo!(db, PgResolutionEventRepository),
            potential_loss: pg_arc_repo!(db, PgPotentialLossRepository),
            calibration: pg_arc_repo!(db, PgCalibrationRepository),
            fact_data: pg_arc_repo!(db, PgFactDataRepository),
            runtime_config: pg_arc_repo!(db, PgRuntimeConfigVersionRepository),
            market: pg_arc_repo!(db, PgMarketRepository),
            event: pg_arc_repo!(db, PgEventRepository),
            trade: pg_arc_repo!(db, PgTradeRepository),
            report: pg_arc_repo!(db, PgReportRepository),
            position: pg_arc_repo!(db, PgPositionRepository),
            control_factor: pg_arc_repo!(db, PgControlFactorRepository),
        }
    }
}

impl DetectionStack {
    pub(super) fn assembled(parts: DetectionStackParts) -> Self {
        Self {
            book_store: parts.book_store,
            market_registry: parts.market_registry,
            market_cache: parts.market_cache,
            universe: parts.universe,
            ws_subscription: parts.ws_subscription,
            gamma_service: parts.gamma_service,
            opportunity_pipeline: parts.opportunity_pipeline,
            calibrator: parts.calibrator,
            calibration_updater: parts.calibration_updater,
            scanner: parts.scanner,
            coalescer: parts.coalescer,
            staleness: parts.staleness,
            token_tx: parts.token_tx,
            token_rx: parts.token_rx,
            market_rx: parts.market_rx,
        }
    }
}

impl BuildRisk {
    pub(super) fn assembled(parts: BuildRiskParts) -> Self {
        Self {
            exposure: parts.exposure,
            metrics: parts.metrics,
            metrics_state: parts.metrics_state,
            engine: parts.engine,
            potential_loss_store: parts.potential_loss_store,
            metrics_refresh: parts.metrics_refresh,
            fsm: parts.fsm,
            backpressure: parts.backpressure,
        }
    }
}

impl ExecutionLoop {
    pub(super) fn assembled(parts: ExecutionLoopParts) -> Self {
        Self {
            funnel: parts.funnel,
            validator: parts.validator,
            order_strategy: parts.order_strategy,
            data_pipeline: parts.data_pipeline,
            execution: parts.execution,
            settlement_rx: parts.settlement_rx,
            execution_runners: parts.execution_runners,
            trade_integrity: parts.trade_integrity,
        }
    }
}

impl BuildTrading {
    pub(super) const fn assembled(detection: DetectionStack, execution: ExecutionLoop) -> Self {
        Self {
            detection,
            execution,
        }
    }

    pub(super) fn into_parts(self) -> (DetectionStack, ExecutionLoop) {
        (self.detection, self.execution)
    }
}

impl AppContextAssembly {
    pub(super) fn into_parts(self) -> AppContextAssemblyIntoParts {
        (
            self.config,
            self.runtime_store,
            self.applicator,
            self.execution_mode,
            self.events,
            self.event_rx,
            self.infra,
            self.clients,
            self.risk,
            self.trading,
            self.trade_integrity,
            self.persistence,
            self.settlement_service,
            self.settlement_dedup,
            self.shutdown,
            self.pending_tasks,
            self.lifecycle,
        )
    }
}

impl BuildInfraCore {
    pub(super) fn into_pack_parts(self) -> InfraCorePackParts {
        (
            self.pg_pool,
            self.ch_pool,
            self.redis_pool,
            self.cache,
            self.jwt_blacklist,
            self.metrics,
            self.alerts,
            self.risk_decision_audit,
            self.risk_decision_audit_rx,
            self.repos,
            self.balance_fact_writer,
            self.factor_store,
            self.factor_refresher,
            self.factor_registry,
            self.shadow_writer_task,
        )
    }

    pub(super) fn assembled(parts: BuildInfraCoreParts) -> Self {
        Self {
            pg_pool: parts.pg_pool,
            ch_pool: parts.ch_pool,
            redis_pool: parts.redis_pool,
            cache: parts.cache,
            jwt_blacklist: parts.jwt_blacklist,
            catalog: parts.catalog,
            metrics: parts.metrics,
            alerts: parts.alerts,
            risk_decision_audit: parts.risk_decision_audit,
            risk_decision_audit_rx: parts.risk_decision_audit_rx,
            repos: parts.repos,
            balance_fact_writer: parts.balance_fact_writer,
            factor_store: parts.factor_store,
            factor_refresher: parts.factor_refresher,
            factor_registry: parts.factor_registry,
            shadow_writer_task: parts.shadow_writer_task,
        }
    }
}

impl BuildInfra {
    pub(super) fn assembled(parts: BuildInfraParts) -> Self {
        Self {
            execution_mode: parts.execution_mode,
            runtime_store: parts.runtime_store,
            pg_pool: parts.core.pg_pool,
            ch_pool: parts.core.ch_pool,
            redis_pool: parts.core.redis_pool,
            cache: parts.core.cache,
            jwt_blacklist: parts.core.jwt_blacklist,
            catalog: parts.core.catalog,
            metrics: parts.core.metrics,
            alerts: parts.core.alerts,
            risk_decision_audit: parts.core.risk_decision_audit,
            risk_decision_audit_rx: parts.core.risk_decision_audit_rx,
            repos: parts.core.repos,
            persistence: parts.persistence,
            balance_fact_writer: parts.core.balance_fact_writer,
            factor_store: parts.core.factor_store,
            factor_refresher: parts.core.factor_refresher,
            factor_registry: parts.core.factor_registry,
            shadow_writer: parts.shadow_writer,
            shadow_writer_task: parts.core.shadow_writer_task,
        }
    }
}

impl BuildPersistence {
    pub(super) fn assembled(parts: BuildPersistenceParts) -> Self {
        Self {
            trade_repo: parts.trade_repo,
            timeseries: parts.timeseries,
            audit_writer: parts.audit_writer,
            book_decision_context_writer: parts.book_decision_context_writer,
            book_fact_writer: parts.book_fact_writer,
        }
    }
}

impl TradingLifecycleWiring {
    pub(super) const fn assembled(
        system_status_nudge: SystemStatusNudge,
        detection_readiness: Arc<DetectionReadiness>,
    ) -> Self {
        Self {
            system_status_nudge,
            detection_readiness,
        }
    }
}

impl AppContextAssembly {
    pub(super) fn assembled(parts: AppContextAssemblyParts) -> Self {
        Self {
            config: parts.config,
            runtime_store: parts.runtime_store,
            applicator: parts.applicator,
            execution_mode: parts.execution_mode,
            events: parts.events,
            event_rx: parts.event_rx,
            infra: parts.infra,
            clients: parts.clients,
            risk: parts.risk,
            trading: parts.trading,
            trade_integrity: parts.trade_integrity,
            persistence: parts.persistence,
            settlement_service: parts.settlement_service,
            settlement_dedup: parts.settlement_dedup,
            shutdown: parts.shutdown,
            pending_tasks: parts.pending_tasks,
            lifecycle: parts.lifecycle,
        }
    }
}

impl ControlFactorWiring {
    pub(super) fn assembled(parts: ControlFactorWiringParts) -> Self {
        Self {
            factor_store: parts.factor_store,
            factor_refresher: parts.factor_refresher,
            factor_registry: parts.factor_registry,
            shadow_writer: parts.shadow_writer,
            shadow_writer_task: parts.shadow_writer_task,
        }
    }
}

impl BuildClients {
    pub(super) fn assembled(parts: BuildClientsParts) -> Self {
        Self {
            ws_manager: parts.ws_manager,
            gamma_client: parts.gamma_client,
            fee_calculator: parts.fee_calculator,
            voting_oracle: parts.voting_oracle,
            clob_client: parts.clob_client,
            ctf_redeem: parts.ctf_redeem,
            holder_address: parts.holder_address,
        }
    }
}

impl HealthCheckerBundle {
    pub(super) const fn assembled(
        checker: Arc<HealthChecker>,
        unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
    ) -> Self {
        Self {
            checker,
            unhealthy_subsystems,
        }
    }
}
