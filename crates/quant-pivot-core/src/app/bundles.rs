//! Runtime subsystem bundles owned by [`super::AppContext`].

use crate::{
    governance::RuntimeModeHandle,
    observability::{
        alert_dispatcher::AlertDispatcher, book_fact_writer::BookFactWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore, data_pipeline::DataPipeline, market_cache::MarketCache,
        market_registry::MarketRegistry, universe_filter::MarketUniverseFilter,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        catalog_readiness::CatalogReadiness, gamma::GammaService,
        ws_subscription::WsSubscriptionCoordinator,
    },
};
use quant_pivot_api::{gamma::GammaClient, ws::ClobWsManager};
use quant_pivot_repository::postgres::{PgMarketRepository, PgOperationLogRepository};
use quant_pivot_storage::{
    cache::{CacheManager, RedisPool},
    clickhouse::ClickHousePool,
    postgres::PostgresPool,
};
use quant_pivot_web::jwt::RedisTokenBlacklist;
use std::sync::Arc;

/// Infrastructure subsystem: storage, metrics, alerts.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
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
    pub universe: Arc<MarketUniverseFilter>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
    pub ws_manager: Arc<ClobWsManager>,
    pub ws_subscription: Arc<WsSubscriptionCoordinator>,
    pub book_fact_writer: Option<Arc<BookFactWriter>>,
    pub catalog: Arc<CatalogReadiness>,
    pub market_repo: Arc<PgMarketRepository>,
    pub gamma_client: Arc<GammaClient>,
}

/// Governance: runtime config, quant runtime mode.
pub struct GovernanceBundle {
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub applicator: Arc<RuntimeConfigApplicator>,
    pub runtime_mode: RuntimeModeHandle,
}
