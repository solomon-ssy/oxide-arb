//! Runtime subsystem bundles owned by [`super::AppContext`].

use crate::{
    bridge::{
        CoreOpportunityPipeline, potential_loss_store::CorePotentialLossStore,
        risk_metrics::CoreRiskMetrics,
    },
    control::{
        ControlFactorRegistry, factor_refresher::FactorRefresher, factor_shadow::ShadowWriterTask,
        factor_snapshot::FactorSnapshotStore,
    },
    detection::{coalescer::Coalescer, funnel::Funnel, scanner::Scanner},
    execution::{
        capital_manager::CapitalManager,
        execution_pipeline::ExecutionPipeline,
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        runner::ExecutionRunner,
        settlement::{dedup::SettlementDedup, service::MarketSettlementService},
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::risk_decision_audit_buffer::RiskDecisionAuditBuffer,
    observability::{
        balance_fact_writer::BalanceFactWriter,
        book_decision_context_writer::BookDecisionContextWriter, book_fact_writer::BookFactWriter,
        execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_registry::MarketRegistry,
    },
    service::{
        catalog_readiness::CatalogReadiness,
        gamma::GammaService,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
    },
};
use flume::Receiver;
use oxide_arb_algorithm::calibration::{CalibrationUpdater, ResolutionCalibrator};
use oxide_arb_api::{
    clob::ClobClient, ctf::client::CtfRedeemClient, fees::FeeCalculator, ws::ClobWsManager,
};
use oxide_arb_models::domain::settlement::MarketSettlementRequest;
use oxide_arb_models::types::{MarketId, TokenId};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{
        PgCalibrationRepository, PgFactDataRepository, PgPositionRepository, PgReportRepository,
        PgRiskStateRepository, PgTradeRepository,
    },
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
use tokio::sync::Notify;

/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    /// Shared Redis pool (cache L2 + JWT revocation), connected fail-fast at
    /// boot and closed once by the composition root after shutdown drains.
    pub redis: RedisPool,
    /// Policy-gated cache facade (fail-open, per-domain routing, noop mode).
    pub cache: Arc<CacheManager>,
    /// JWT revocation store over the shared Redis pool (fail-closed).
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<crate::observability::alert_dispatcher::AlertDispatcher>,
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
    pub book_decision_context_writer: Arc<BookDecisionContextWriter>,
    pub balance_fact_writer: Arc<BalanceFactWriter>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub holder_address: String,
    pub fee_calculator: Arc<FeeCalculator>,
}

/// Data pipeline subsystem: WS event loop, order books, market metadata.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
    /// Catalog warmup gate — `Warming` until the first successful Gamma sync.
    pub catalog: Arc<CatalogReadiness>,
}

/// Risk management subsystem.
pub struct RiskBundle {
    pub engine: Arc<RiskEngine>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub potential_loss_store: Arc<CorePotentialLossStore>,
    pub metrics_refresh: Arc<RiskMetricsRefreshService>,
}

/// Execution subsystem wired after opportunity detection.
pub struct ExecutionBundle {
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub capital_manager: Arc<CapitalManager>,
    /// Shared with the pipeline; rung after each `*_observed` write to wake the relay.
    pub relay_notify: Arc<Notify>,
    /// Shared with the pipeline; rung after Unknown outcomes to wake reconciliation.
    pub reconcile_notify: Arc<Notify>,
}

/// Construction inputs for [`ExecutionBundle`].
pub struct ExecutionBundleDeps {
    pub pipeline: Arc<ExecutionPipeline>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub capital_manager: Arc<CapitalManager>,
    pub relay_notify: Arc<Notify>,
    pub reconcile_notify: Arc<Notify>,
}

impl ExecutionBundle {
    #[must_use]
    pub fn new(deps: ExecutionBundleDeps) -> Self {
        Self {
            pipeline: deps.pipeline,
            market_inflight: deps.market_inflight,
            capital_manager: deps.capital_manager,
            relay_notify: deps.relay_notify,
            reconcile_notify: deps.reconcile_notify,
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
    pub ctf_redeem: Option<Arc<CtfRedeemClient>>,
    pub ws_manager: Arc<ClobWsManager>,
}

/// Live control-factor subsystem: snapshot store, refresher, and shadow writer drain.
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
    pub(crate) settlement_rx: Mutex<Option<Receiver<MarketSettlementRequest>>>,
}

/// One-shot channel receivers consumed when registering runtime tasks.
pub struct RuntimeChannels {
    pub coalescer_token_rx: Mutex<Option<flume::Receiver<TokenId>>>,
    pub scanner_market_rx: Mutex<Option<flume::Receiver<MarketId>>>,
    pub execution_runners: Mutex<Option<Vec<ExecutionRunner>>>,
}
