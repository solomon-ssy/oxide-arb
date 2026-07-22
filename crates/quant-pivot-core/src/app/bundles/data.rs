//! Polymarket data ingest bundle: live books, catalog sync, pipeline, quality.

use std::{sync::Arc, time::Duration};

use flume::Receiver;
use quant_pivot_api::{
    gamma::GammaClient,
    ws::{
        ClobWsManager, ClobWsManagerHooks, TokenKeyResolver, TransportRetirement,
        TransportRetirementHook, WsSessionInvalidationHook, WsShardHealthPort,
    },
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig, domain::runtime::CoreEventPublisher,
    runtime_config::DecisionPolicySnapshot,
};
use quant_pivot_repository::{
    cached::{CachedEventRepository, CachedMarketRepository},
    postgres::{PgEventRepository, PgMarketRepository},
    traits::{
        CatalogLedgerRepository, ClobMarketInfoRepository, EventRepository,
        MarketLinkageRepository, MarketRepository,
    },
};
use quant_pivot_research::pit::PointInTimeSnapshotSource;
use tokio_util::sync::CancellationToken;

use super::InfraBundle;
use crate::{
    governance::{LinkageResolverDeps, LinkageResolverService},
    ingest::{
        book_store::BookStore,
        data_pipeline::{DataPipeline, DataPipelineDeps},
        data_plane_index::DataPlane,
        data_quality::BookDataQualityService,
        event_source::PipelineEventSource,
        market_cache::MarketCache,
        market_filter::MarketFilter,
        market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
    pit::platform::ch_historical::DurablePitSource,
    service::{
        catalog_readiness::CatalogReadiness,
        gamma::{GammaService, GammaServiceDeps},
        system_status_nudge::SystemStatusNudge,
        ws_subscription::{MarketDataSubscriptionPolicy, WsSubscriptionCoordinator},
    },
};

/// Dependencies required to assemble the live data plane.
pub struct DataBundleDeps<'a> {
    pub deploy: &'a DeployConfig,
    pub shutdown: &'a CancellationToken,
    pub metrics: &'a Arc<MetricsHub>,
    pub infra: &'a InfraBundle,
    pub runtime: &'a DecisionPolicySnapshot,
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
    /// Offline market-linkage resolver.
    pub linkage_resolver: Arc<LinkageResolverService>,
    pub ws_manager: Arc<ClobWsManager>,
    pub ws_subscription: Arc<WsSubscriptionCoordinator>,
    pub catalog: Arc<CatalogReadiness>,
    pub market_repo: Arc<dyn MarketRepository>,
    /// Immutable catalog ledger used by every historical/replay resolver.
    pub catalog_ledger_repo: Arc<dyn CatalogLedgerRepository>,
    pub gamma_client: Arc<GammaClient>,
    /// Token freshness snapshots and fact-lag aggregation for governance and web.
    pub data_quality: Arc<BookDataQualityService>,
    /// Durable point-in-time source shared by serving and replay.
    pub pit_source: Arc<dyn PointInTimeSnapshotSource>,
    /// Best-effort nudge when pipeline status changes (web readiness).
    pub status_nudge: SystemStatusNudge,
}

impl DataBundle {
    /// Wire the full Polymarket ingest stack from deploy config and infra handles.
    pub fn assemble(deps: &DataBundleDeps<'_>) -> QuantResult<Self> {
        let data_plane = Arc::new(DataPlane::new());
        let book_store = Arc::new(BookStore::new(
            Arc::clone(&data_plane),
            Arc::clone(deps.metrics),
        ));
        let invalidation_metrics = Arc::clone(deps.metrics);
        let invalidation_books = Arc::clone(&book_store);
        let on_session_invalidated: WsSessionInvalidationHook = Arc::new(move |token_ids| {
            let invalidated = invalidation_books.invalidate_ids(token_ids);
            invalidation_metrics
                .ws_session_backpressure_invalidations
                .inc_by(u64::try_from(invalidated).unwrap_or(u64::MAX));
        });
        let (retirement_tx, retirement_rx) = flume::bounded(1_024);
        let retirement_shutdown = deps.shutdown.clone();
        let on_transport_retired: TransportRetirementHook = Arc::new(move |retirement| {
            if retirement_tx
                .send_timeout(retirement, Duration::from_millis(100))
                .is_err()
            {
                tracing::error!(
                    "token retirement control queue unavailable; cancelling data plane"
                );
                retirement_shutdown.cancel();
            }
        });
        let ws_manager = Arc::new(ClobWsManager::new(
            &deps.deploy.polymarket,
            &deps.deploy.market_data.websocket,
            deps.shutdown.clone(),
            Arc::clone(&data_plane) as Arc<dyn TokenKeyResolver>,
            ClobWsManagerHooks {
                on_session_invalidated: Some(on_session_invalidated),
                on_transport_retired: Some(on_transport_retired),
                ..ClobWsManagerHooks::default()
            },
        ));
        let gamma_client = Arc::new(GammaClient::new(deps.deploy.market_data.gamma.clone()));
        let market_registry = Arc::new(MarketRegistry::new(data_plane));
        let market_filter = Arc::new(MarketFilter::new(
            &deps.runtime.recommendation.selection.enabled_categories,
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
        let event_repo: Arc<dyn EventRepository> = cached_event_repo(deps);
        let catalog_ledger_repo: Arc<dyn CatalogLedgerRepository> =
            Arc::clone(&deps.infra.repos.catalog_ledger) as Arc<dyn CatalogLedgerRepository>;
        let linkage_resolver = Arc::new(LinkageResolverService::new(
            LinkageResolverDeps {
                linkage_repo: Arc::clone(&deps.infra.repos.market_linkage)
                    as Arc<dyn MarketLinkageRepository>,
                market_repo: Arc::clone(&market_repo),
                event_repo: Arc::clone(&event_repo),
            },
            deps.deploy
                .domain_sources
                .weather_stations
                .clone()
                .into_iter()
                .collect(),
            &deps.deploy.domain_sources.weather_vertical_bindings,
        )?);
        let status_nudge = SystemStatusNudge::default();
        let (gamma_service, ws_subscription) = assemble_gamma_service(GammaServiceAssembly {
            deps,
            gamma_client: &gamma_client,
            ws_manager: &ws_manager,
            market_repo: &market_repo,
            catalog_ledger_repo: &catalog_ledger_repo,
            market_registry: &market_registry,
            market_cache: &market_cache,
            market_filter: &market_filter,
            catalog: &catalog,
            linkage_resolver: &linkage_resolver,
            status_nudge: status_nudge.clone(),
        });

        let data_quality = Arc::new(BookDataQualityService::new(
            Arc::clone(&book_store),
            Arc::clone(&ws_manager) as Arc<dyn WsShardHealthPort>,
            &deps.runtime.recommendation.data_quality,
            Arc::clone(&deps.infra.ingest_lag_tracker),
        ));
        let pit_source: Arc<dyn PointInTimeSnapshotSource> = Arc::new(DurablePitSource::new(
            Arc::clone(&deps.infra.quant_fact_read),
            Arc::clone(&catalog_ledger_repo),
            Arc::clone(&deps.infra.repos.clob_market_info) as Arc<dyn ClobMarketInfoRepository>,
        ));
        let data_pipeline = build_data_pipeline(DataPipelineAssembly {
            deps,
            book_store: &book_store,
            market_registry: &market_registry,
            ws_manager: &ws_manager,
            retirement_rx,
            status_nudge: status_nudge.clone(),
        });

        Ok(Self {
            book_store,
            market_registry,
            market_cache,
            market_filter,
            data_pipeline,
            gamma_service,
            linkage_resolver,
            ws_manager,
            ws_subscription,
            catalog,
            market_repo,
            catalog_ledger_repo,
            gamma_client,
            data_quality,
            pit_source,
            status_nudge,
        })
    }
}

fn cached_event_repo(deps: &DataBundleDeps<'_>) -> Arc<dyn EventRepository> {
    Arc::new(CachedEventRepository::new(
        PgEventRepository::new(deps.infra.pg.connection().clone()),
        Arc::clone(&deps.infra.cache),
    ))
}

struct GammaServiceAssembly<'a> {
    deps: &'a DataBundleDeps<'a>,
    gamma_client: &'a Arc<GammaClient>,
    ws_manager: &'a Arc<ClobWsManager>,
    market_repo: &'a Arc<dyn MarketRepository>,
    catalog_ledger_repo: &'a Arc<dyn CatalogLedgerRepository>,
    market_registry: &'a Arc<MarketRegistry>,
    market_cache: &'a Arc<MarketCache>,
    market_filter: &'a Arc<MarketFilter>,
    catalog: &'a Arc<CatalogReadiness>,
    linkage_resolver: &'a Arc<LinkageResolverService>,
    status_nudge: SystemStatusNudge,
}

fn assemble_gamma_service(
    inputs: GammaServiceAssembly<'_>,
) -> (Arc<GammaService>, Arc<WsSubscriptionCoordinator>) {
    let GammaServiceAssembly {
        deps,
        gamma_client,
        ws_manager,
        market_repo,
        catalog_ledger_repo,
        market_registry,
        market_cache,
        market_filter,
        catalog,
        linkage_resolver,
        status_nudge,
    } = inputs;
    let ws_subscription = Arc::new(WsSubscriptionCoordinator::new(
        Arc::clone(ws_manager),
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
    let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
        gamma_client: Arc::clone(gamma_client),
        market_registry: Arc::clone(market_registry),
        market_cache: Arc::clone(market_cache),
        market_filter: Arc::clone(market_filter),
        market_repo: Arc::clone(market_repo),
        catalog_ledger_repo: Arc::clone(catalog_ledger_repo),
        cache: Arc::clone(&deps.infra.cache),
        metrics: Arc::clone(deps.metrics),
        catalog: Arc::clone(catalog),
        ws_subscription: Some(Arc::clone(&ws_subscription)),
        events: deps.events.clone(),
        status_nudge,
        subscription_window_hours: deps
            .deploy
            .market_data
            .websocket
            .engine_subscription_window_hours,
        linkage_resolver: Some(Arc::clone(linkage_resolver)),
    }));
    (gamma_service, ws_subscription)
}

struct DataPipelineAssembly<'a> {
    deps: &'a DataBundleDeps<'a>,
    book_store: &'a Arc<BookStore>,
    market_registry: &'a Arc<MarketRegistry>,
    ws_manager: &'a Arc<ClobWsManager>,
    retirement_rx: Receiver<TransportRetirement>,
    status_nudge: SystemStatusNudge,
}

fn build_data_pipeline(inputs: DataPipelineAssembly<'_>) -> Arc<DataPipeline> {
    let DataPipelineAssembly {
        deps,
        book_store,
        market_registry,
        ws_manager,
        retirement_rx,
        status_nudge,
    } = inputs;
    let event_source: Arc<dyn PipelineEventSource> =
        Arc::clone(ws_manager) as Arc<dyn PipelineEventSource>;
    Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(book_store),
        market_registry: Arc::clone(market_registry),
        metrics: Arc::clone(deps.metrics),
        book_fact_writer: Arc::clone(&deps.infra.book_fact_writer),
        shutdown: deps.shutdown.clone(),
        status_nudge,
        retirement_rx,
        durable_publish_observer: None,
    }))
}
