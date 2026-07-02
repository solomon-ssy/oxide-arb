//! Polymarket data ingest bundle: live books, catalog sync, pipeline, quality.

use super::InfraBundle;
use crate::{
    observability::{
        backpressure::BackpressurePolicy, book_fact_writer::BookFactWriter, metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore,
        data_pipeline::{self, DataPipeline, DataPipelineDeps},
        data_quality::BookDataQualityService,
        event_source::PipelineEventSource,
        market_cache::MarketCache,
        market_filter::MarketFilter,
        market_registry::MarketRegistry,
        point_in_time::LiveBookDataSource,
    },
    service::{
        catalog_readiness::CatalogReadiness,
        gamma::{GammaService, GammaServiceDeps},
        system_status_nudge::SystemStatusNudge,
        ws_subscription::{MarketDataSubscriptionPolicy, WsSubscriptionCoordinator},
    },
};
use quant_pivot_api::{
    fees::FeeCalculator,
    gamma::GammaClient,
    ws::{ClobWsManager, WsEventDropHook, WsShardHealthPort},
};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{CoreEventPublisher, PointInTimeDataSource},
    runtime_config::RuntimeConfig,
};
use quant_pivot_repository::{
    cached::{CachedEventRepository, CachedMarketRepository},
    postgres::{PgEventRepository, PgMarketRepository},
    traits::MarketRepository,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Dependencies required to assemble the live data plane.
pub struct DataBundleDeps<'a> {
    pub deploy: &'a DeployConfig,
    pub shutdown: &'a CancellationToken,
    pub metrics: &'a Arc<MetricsHub>,
    pub infra: &'a InfraBundle,
    pub runtime: &'a RuntimeConfig,
    pub events: &'a CoreEventPublisher,
}

/// Polymarket data ingest subsystem: books, catalog, pipeline, and quality gates.
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub market_filter: Arc<MarketFilter>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,
    pub ws_manager: Arc<ClobWsManager>,
    pub ws_subscription: Arc<WsSubscriptionCoordinator>,
    pub catalog: Arc<CatalogReadiness>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub gamma_client: Arc<GammaClient>,
    /// Token freshness snapshots and fact-lag aggregation for governance and web.
    pub data_quality: Arc<BookDataQualityService>,
    /// Live point-in-time source for Phase 3 feature/report builders.
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    /// Best-effort nudge when pipeline status changes (web readiness).
    pub status_nudge: SystemStatusNudge,
    /// Polymarket taker fee calculator (category schedules from Gamma sync).
    pub fee_calculator: Arc<FeeCalculator>,
}

impl DataBundle {
    /// Wire the full Polymarket ingest stack from deploy config and infra handles.
    pub fn assemble(deps: &DataBundleDeps<'_>) -> Self {
        let drop_metrics = Arc::clone(deps.metrics);
        let on_events_dropped: WsEventDropHook =
            Arc::new(move |n| drop_metrics.ws_events_dropped.inc_by(n));
        let ws_manager = Arc::new(ClobWsManager::new(
            &deps.deploy.polymarket,
            &deps.deploy.market_data.websocket,
            deps.shutdown.clone(),
            Some(on_events_dropped),
            None,
        ));
        let gamma_client = Arc::new(GammaClient::new(deps.deploy.market_data.gamma.clone()));
        let fee_calculator = Arc::new(FeeCalculator::from_config(&deps.deploy.polymarket.fees));
        let book_store = Arc::new(BookStore::new(Arc::clone(deps.metrics)));
        let market_registry = Arc::new(MarketRegistry::new());
        let market_filter = Arc::new(MarketFilter::new(
            &deps.runtime.selection.enabled_categories,
        ));
        let market_cache = Arc::new(MarketCache::new(
            Arc::clone(&market_registry),
            Arc::clone(&market_filter),
        ));
        let catalog = Arc::new(CatalogReadiness::new());
        let market_repo: Arc<dyn MarketRepository> = Arc::new(CachedMarketRepository::new(
            PgMarketRepository::new(deps.infra.pg.connection().clone()),
            Arc::clone(&deps.infra.cache),
        ));
        let ws_subscription = Arc::new(WsSubscriptionCoordinator::new(
            Arc::clone(&ws_manager),
            MarketDataSubscriptionPolicy::new(
                deps.deploy
                    .market_data
                    .websocket
                    .engine_max_subscription_tokens,
                deps.deploy
                    .market_data
                    .websocket
                    .engine_subscription_window_hours,
            ),
        ));
        let status_nudge = SystemStatusNudge::default();
        let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
            gamma_client: Arc::clone(&gamma_client),
            market_registry: Arc::clone(&market_registry),
            market_cache: Arc::clone(&market_cache),
            market_filter: Arc::clone(&market_filter),
            fee_calculator: Arc::clone(&fee_calculator),
            market_repo: Arc::clone(&market_repo),
            event_repo: Arc::new(CachedEventRepository::new(
                PgEventRepository::new(deps.infra.pg.connection().clone()),
                Arc::clone(&deps.infra.cache),
            )),
            cache: Arc::clone(&deps.infra.cache),
            metrics: Arc::clone(deps.metrics),
            catalog: Arc::clone(&catalog),
            ws_subscription: Some(Arc::clone(&ws_subscription)),
            events: deps.events.clone(),
            status_nudge: status_nudge.clone(),
            subscription_window_hours: deps
                .deploy
                .market_data
                .websocket
                .engine_subscription_window_hours,
            full_sync_interval_secs: deps.deploy.market_data.gamma.full_sync_interval_secs,
        }));

        let data_quality = Arc::new(BookDataQualityService::new(
            Arc::clone(&book_store),
            Arc::clone(&ws_manager) as Arc<dyn WsShardHealthPort>,
            &deps.runtime.data_quality,
            Arc::clone(&deps.infra.ingest_lag_tracker),
        ));
        let pit_source: Arc<dyn PointInTimeDataSource> = Arc::new(LiveBookDataSource::new(
            Arc::clone(&book_store),
            Arc::clone(&market_registry),
        ));
        let data_pipeline = build_data_pipeline(
            &book_store,
            &market_registry,
            &ws_manager,
            deps.metrics,
            deps.shutdown,
            status_nudge.clone(),
            Arc::clone(&deps.infra.book_fact_writer),
        );

        Self {
            book_store,
            market_registry,
            market_cache,
            market_filter,
            data_pipeline,
            gamma_service,
            ws_manager,
            ws_subscription,
            catalog,
            market_repo,
            gamma_client,
            data_quality,
            pit_source,
            status_nudge,
            fee_calculator,
        }
    }
}

fn build_data_pipeline(
    book_store: &Arc<BookStore>,
    market_registry: &Arc<MarketRegistry>,
    ws_manager: &Arc<ClobWsManager>,
    metrics: &Arc<MetricsHub>,
    shutdown: &CancellationToken,
    status_nudge: SystemStatusNudge,
    book_fact_writer: Arc<BookFactWriter>,
) -> Arc<DataPipeline> {
    let event_source: Arc<dyn PipelineEventSource> =
        Arc::clone(ws_manager) as Arc<dyn PipelineEventSource>;
    Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(book_store),
        market_registry: Arc::clone(market_registry),
        metrics: Arc::clone(metrics),
        backpressure: Arc::new(BackpressurePolicy::new(
            Arc::clone(metrics),
            data_pipeline::DEFAULT_BOOK_SHARD_COUNT,
        )),
        book_fact_writer,
        book_shard_count: data_pipeline::DEFAULT_BOOK_SHARD_COUNT,
        book_channel_capacity: data_pipeline::DEFAULT_BOOK_CHANNEL_CAPACITY,
        shutdown: shutdown.clone(),
        status_nudge,
    }))
}
