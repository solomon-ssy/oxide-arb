//! Application composition and bootstrap wiring (Phase 0).

use super::{
    AppContext,
    bundles::{DataBundle, GovernanceBundle, InfraBundle},
};
use crate::{
    governance::{RuntimeModeHandle, runtime_control::QuantRuntimeControl},
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        metrics_hub::MetricsHub,
    },
    pipeline::{
        book_store::BookStore,
        data_pipeline::{self, DataPipeline, DataPipelineDeps},
        event_source::PipelineEventSource,
        market_cache::MarketCache,
        market_registry::MarketRegistry,
        universe_filter::MarketUniverseFilter,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
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
    ws::{ClobWsManager, WsEventDropHook},
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        CoreEventPublisher, NewRuntimeConfigActivation, NewRuntimeConfigVersion,
        runtime_config_hash,
    },
    enums::{
        quant::QuantRuntimeMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    postgres::{
        PgEventRepository, PgMarketRepository, PgOperationLogRepository,
        PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository,
    },
    traits::{RuntimeConfigVersionRepository, SystemRuntimeStateRepository},
};
use quant_pivot_storage::{
    cache::{CacheManager, MokaBackend, RedisBackend, RedisPool, TieredCache, connect_pool},
    clickhouse::ClickHousePool,
    postgres::{
        PostgresPool,
        migration::{Migrator, MigratorTrait},
    },
};
use quant_pivot_web::jwt::RedisTokenBlacklist;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl AppContext {
    /// Build all subsystems from deploy config.
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
    ) -> QuantResult<Self> {
        let metrics = Arc::new(MetricsHub::new());
        let pg_pool = connect_postgres(&deploy).await?;
        let (runtime, runtime_store, alerts) = bootstrap_runtime_config(&pg_pool).await?;
        let (ch_pool, redis_pool, cache, jwt_blacklist) =
            connect_analytics_and_cache(&deploy, &metrics).await?;
        let runtime_mode = RuntimeModeHandle::new(
            restore_quant_runtime_mode(&PgSystemRuntimeStateRepository::new(
                pg_pool.connection().clone(),
            ))
            .await?,
        );

        let market_stack =
            build_market_stack(&deploy, &shutdown, &metrics, &runtime, &pg_pool, &cache);

        let status_nudge = SystemStatusNudge::default();
        let data_pipeline =
            build_data_pipeline(&market_stack, &metrics, &shutdown, status_nudge.clone());
        let applicator = build_runtime_applicator(
            &deploy,
            &runtime_store,
            &market_stack.universe,
            &market_stack.market_registry,
            &market_stack.market_cache,
            &market_stack.ws_subscription,
            &metrics,
        );
        let catalog = Arc::clone(&market_stack.catalog);
        let health_checker = Arc::new(HealthChecker::new(HealthCheckerDeps {
            pg_pool: Arc::clone(&pg_pool),
            ch_pool: Arc::clone(&ch_pool),
            ws_manager: Arc::clone(&market_stack.ws_manager),
            catalog: Arc::clone(&catalog),
            runtime_mode: runtime_mode.clone(),
        }));
        let (events, event_rx) = CoreEventPublisher::bounded(4096);
        let runtime_control = Arc::new(QuantRuntimeControl::new(
            runtime_mode.clone(),
            Arc::clone(&health_checker),
            PgSystemRuntimeStateRepository::new(pg_pool.connection().clone()),
        ));

        Ok(Self {
            config: deploy,
            shutdown,
            events,
            event_rx: parking_lot::Mutex::new(Some(event_rx)),
            infra: InfraBundle {
                pg: pg_pool,
                ch: ch_pool,
                redis: redis_pool,
                cache,
                jwt_blacklist,
                metrics,
                alerts,
                operation_log_repo: market_stack.operation_log_repo,
            },
            data: DataBundle {
                book_store: market_stack.book_store,
                market_registry: market_stack.market_registry,
                market_cache: market_stack.market_cache,
                universe: market_stack.universe,
                data_pipeline,
                gamma_service: market_stack.gamma_service,
                ws_manager: market_stack.ws_manager,
                ws_subscription: market_stack.ws_subscription,
                book_fact_writer: None,
                catalog: Arc::clone(&catalog),
                market_repo: market_stack.market_repo,
                gamma_client: market_stack.gamma_client,
            },
            governance: GovernanceBundle {
                runtime_config: runtime_store,
                applicator,
                runtime_mode,
            },
            health_checker,
            runtime_control,
            catalog,
            status_nudge,
        })
    }
}

struct MarketStack {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    universe: Arc<MarketUniverseFilter>,
    ws_manager: Arc<ClobWsManager>,
    ws_subscription: Arc<WsSubscriptionCoordinator>,
    gamma_client: Arc<GammaClient>,
    gamma_service: Arc<GammaService>,
    market_repo: Arc<PgMarketRepository>,
    operation_log_repo: Arc<PgOperationLogRepository>,
    catalog: Arc<CatalogReadiness>,
}

async fn connect_postgres(deploy: &DeployConfig) -> QuantResult<Arc<PostgresPool>> {
    let pg_pool = Arc::new(PostgresPool::connect(&deploy.db.postgres).await?);
    Migrator::up(pg_pool.connection(), None).await?;
    Ok(pg_pool)
}

async fn bootstrap_runtime_config(
    pg_pool: &PostgresPool,
) -> QuantResult<(RuntimeConfig, Arc<RuntimeConfigStore>, Arc<AlertDispatcher>)> {
    let runtime_config_repo = Arc::new(PgRuntimeConfigVersionRepository::new(
        pg_pool.connection().clone(),
    ));
    let runtime = ensure_runtime_config_activation(runtime_config_repo.as_ref()).await?;
    let alerts = Arc::new(AlertDispatcher::new(&runtime.notification));
    let runtime_store = Arc::new(RuntimeConfigStore::new(runtime.clone()));
    Ok((runtime, runtime_store, alerts))
}

async fn connect_analytics_and_cache(
    deploy: &DeployConfig,
    metrics: &MetricsHub,
) -> QuantResult<(
    Arc<ClickHousePool>,
    RedisPool,
    Arc<CacheManager>,
    Arc<RedisTokenBlacklist>,
)> {
    let ch_pool = Arc::new(ClickHousePool::connect(&deploy.db.clickhouse).await?);
    ch_pool.ensure_schema().await?;

    let redis_pool = connect_pool(&deploy.cache.redis).await?;
    let cache = Arc::new(CacheManager::new(
        TieredCache::new(
            MokaBackend::new(deploy.cache.moka.max_capacity),
            RedisBackend::new(redis_pool.clone(), &deploy.cache.redis.key_prefix),
        ),
        &deploy.cache,
    ));
    cache
        .register_metrics(&metrics.registry)
        .map_err(|error| QuantError::Internal(format!("cache metrics registration: {error}")))?;

    let jwt_blacklist = Arc::new(RedisTokenBlacklist::new(
        redis_pool.clone(),
        &deploy.cache.redis.key_prefix,
    ));
    Ok((ch_pool, redis_pool, cache, jwt_blacklist))
}

fn build_market_stack(
    deploy: &DeployConfig,
    shutdown: &CancellationToken,
    metrics: &Arc<MetricsHub>,
    runtime: &RuntimeConfig,
    pg_pool: &PostgresPool,
    cache: &Arc<CacheManager>,
) -> MarketStack {
    let on_events_dropped: WsEventDropHook = {
        let metrics = Arc::clone(metrics);
        Arc::new(move |n| metrics.ws_events_dropped.inc_by(n))
    };
    let ws_manager = Arc::new(ClobWsManager::new(
        &deploy.polymarket,
        &deploy.market_data.websocket,
        shutdown.clone(),
        Some(on_events_dropped),
        None,
    ));
    let gamma_client = Arc::new(GammaClient::new(deploy.market_data.gamma.clone()));
    let fee_calculator = Arc::new(FeeCalculator::from_config(&deploy.polymarket.fees));
    let book_store = Arc::new(BookStore::new(Arc::clone(metrics)));
    let market_registry = Arc::new(MarketRegistry::new());
    let universe = Arc::new(MarketUniverseFilter::new(
        &runtime.universe.enabled_categories,
    ));
    let market_cache = Arc::new(MarketCache::new(
        Arc::clone(&market_registry),
        Arc::clone(&universe),
    ));
    let catalog = Arc::new(CatalogReadiness::new());
    let market_repo = Arc::new(PgMarketRepository::new(pg_pool.connection().clone()));
    let operation_log_repo = Arc::new(PgOperationLogRepository::new(pg_pool.connection().clone()));
    let ws_subscription = Arc::new(WsSubscriptionCoordinator::new(
        Arc::clone(&ws_manager),
        MarketDataSubscriptionPolicy::new(
            deploy.market_data.websocket.engine_max_subscription_tokens,
            deploy
                .market_data
                .websocket
                .engine_subscription_window_hours,
        ),
    ));
    let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
        gamma_client: Arc::clone(&gamma_client),
        market_registry: Arc::clone(&market_registry),
        market_cache: Arc::clone(&market_cache),
        universe: Arc::clone(&universe),
        fee_calculator: Arc::clone(&fee_calculator),
        market_repo: Arc::clone(&market_repo),
        event_repo: Arc::new(PgEventRepository::new(pg_pool.connection().clone())),
        cache: Arc::clone(cache),
        metrics: Arc::clone(metrics),
        catalog: Arc::clone(&catalog),
        ws_subscription: Some(Arc::clone(&ws_subscription)),
        subscription_window_hours: deploy
            .market_data
            .websocket
            .engine_subscription_window_hours,
        full_sync_interval_secs: deploy.market_data.gamma.full_sync_interval_secs,
    }));

    MarketStack {
        book_store,
        market_registry,
        market_cache,
        universe,
        ws_manager,
        ws_subscription,
        gamma_client,
        gamma_service,
        market_repo,
        operation_log_repo,
        catalog,
    }
}

fn build_data_pipeline(
    market_stack: &MarketStack,
    metrics: &Arc<MetricsHub>,
    shutdown: &CancellationToken,
    status_nudge: SystemStatusNudge,
) -> Arc<DataPipeline> {
    let event_source: Arc<dyn PipelineEventSource> =
        Arc::clone(&market_stack.ws_manager) as Arc<dyn PipelineEventSource>;
    Arc::new(DataPipeline::new(DataPipelineDeps {
        event_source,
        book_store: Arc::clone(&market_stack.book_store),
        market_registry: Arc::clone(&market_stack.market_registry),
        metrics: Arc::clone(metrics),
        backpressure: Arc::new(BackpressurePolicy::new(
            Arc::clone(metrics),
            data_pipeline::DEFAULT_BOOK_SHARD_COUNT,
        )),
        book_fact_writer: None,
        book_shard_count: data_pipeline::DEFAULT_BOOK_SHARD_COUNT,
        book_channel_capacity: data_pipeline::DEFAULT_BOOK_CHANNEL_CAPACITY,
        shutdown: shutdown.clone(),
        status_nudge,
    }))
}

fn build_runtime_applicator(
    deploy: &DeployConfig,
    runtime_store: &Arc<RuntimeConfigStore>,
    universe: &Arc<MarketUniverseFilter>,
    market_registry: &Arc<MarketRegistry>,
    market_cache: &Arc<MarketCache>,
    ws_subscription: &Arc<WsSubscriptionCoordinator>,
    metrics: &Arc<MetricsHub>,
) -> Arc<RuntimeConfigApplicator> {
    Arc::new(RuntimeConfigApplicator::new(
        Arc::clone(runtime_store),
        RuntimeConfigSubscribers {
            universe: Arc::clone(universe),
            market_registry: Arc::clone(market_registry),
            market_cache: Arc::clone(market_cache),
            ws_subscription: Some(Arc::clone(ws_subscription)),
            metrics: Arc::clone(metrics),
            subscription_window_hours: deploy
                .market_data
                .websocket
                .engine_subscription_window_hours,
        },
    ))
}

async fn restore_quant_runtime_mode(
    repo: &PgSystemRuntimeStateRepository,
) -> QuantResult<QuantRuntimeMode> {
    if let Some(state) = repo.load().await? {
        return Ok(state.quant_runtime_mode);
    }
    tracing::warn!("system_runtime_state singleton missing; re-seeding ReportOnly");
    let mode = QuantRuntimeMode::ReportOnly;
    repo.upsert_quant_runtime_mode(mode, "bootstrap", "fail-closed re-seed (row missing)")
        .await?;
    Ok(mode)
}

async fn ensure_runtime_config_activation(
    repo: &dyn RuntimeConfigVersionRepository,
) -> QuantResult<RuntimeConfig> {
    let current = repo.load_current().await?;
    if let Some(version) = &current {
        if let Ok(config) = RuntimeConfig::from_json(&version.config_json) {
            return Ok(config);
        }
        tracing::warn!("active runtime config invalid — reseeding defaults");
    }

    let config = RuntimeConfig::default();
    let config_json = config.to_json();
    let config_hash = runtime_config_hash(&config_json);
    let version = match repo.load_by_hash(&config_hash).await? {
        Some(version) => version,
        None => {
            repo.create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash,
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                config_json,
                source: RuntimeConfigVersionSource::Bootstrap,
                created_by: "system".to_owned(),
                reason: format!(
                    "bootstrap default runtime config (schema_version={RUNTIME_CONFIG_SCHEMA_VERSION})"
                ),
            })
            .await?
        }
    };

    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id.clone(),
        activated_at: chrono::Utc::now(),
        activated_by: "system".to_owned(),
        reason: "bootstrap runtime config activation".to_owned(),
        activation_kind: if current.is_some() {
            RuntimeConfigActivationKind::Promote
        } else {
            RuntimeConfigActivationKind::Initial
        },
        previous_runtime_config_version_id: current.map(|v| v.runtime_config_version_id),
        rollback_target_version_id: None,
        audit_event_id: None,
    })
    .await?;
    Ok(config)
}
