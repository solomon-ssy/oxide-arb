//! Runtime subsystem bundles owned by [`super::AppContext`].

use crate::{
    app::task_registry::PendingTaskQueue,
    governance::RuntimeModeHandle,
    observability::{
        alert_dispatcher::AlertDispatcher, book_fact_writer::BookFactWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        catalog_readiness::CatalogReadiness, gamma::GammaService,
        ws_subscription::WsSubscriptionCoordinator,
    },
};
use quant_pivot_api::{gamma::GammaClient, ws::ClobWsManager};
use quant_pivot_models::config::DeployConfig;
use quant_pivot_repository::{
    postgres::PgOperationLogRepository,
    traits::{MarketRepository, QuantFactRepository},
};
use quant_pivot_research::artifact::{ArtifactStore, LocalArtifactStore};
use quant_pivot_storage::{
    cache::{CacheManager, RedisPool},
    clickhouse::{ChWriteManager, ClickHousePool},
    postgres::PostgresPool,
};
use quant_pivot_web::jwt::RedisTokenBlacklist;
use std::sync::Arc;

/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    pub ch_write_manager: Arc<ChWriteManager>,
    pub quant_fact_repo: Arc<dyn QuantFactRepository>,
    pub redis: RedisPool,
    pub cache: Arc<CacheManager>,
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub operation_log_repo: Arc<PgOperationLogRepository>,
}

/// Polymarket data ingest subsystem.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub market_filter: Arc<MarketFilter>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
    pub ws_manager: Arc<ClobWsManager>,
    pub ws_subscription: Arc<WsSubscriptionCoordinator>,
    pub book_fact_writer: Arc<BookFactWriter>,
    /// Flush workers for each book fact stream, registered on the runner at boot.
    pub(crate) fact_writer_queue: PendingTaskQueue,
    pub catalog: Arc<CatalogReadiness>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub gamma_client: Arc<GammaClient>,
}

/// Governance: runtime config, quant runtime mode.
pub struct GovernanceBundle {
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub applicator: Arc<RuntimeConfigApplicator>,
    pub runtime_mode: RuntimeModeHandle,
}

/// Research plane: artifact store and compute contracts (Phase 3+).
pub struct ResearchBundle {
    /// Local (or future object-store) backend for dataset / model artifact bytes.
    pub artifact_store: Arc<dyn ArtifactStore>,
}

impl ResearchBundle {
    /// Build the research bundle from deploy config (`research.artifact_root`).
    pub fn from_deploy(deploy: &DeployConfig) -> Self {
        let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            deploy.research.artifact_root.clone(),
        ));
        Self {
            artifact_store: store,
        }
    }
}

/// Recommendation report bundle (Phase 4+).
pub struct ReportBundle;

/// Portfolio planning bundle (Phase 4+).
pub struct PortfolioBundle;

/// Execution intent bundle (Phase 5+).
pub struct ExecutionIntentBundle;

/// TODO: Cross-bundle runtime channels (Phase 2+).
pub struct RuntimeChannels;
