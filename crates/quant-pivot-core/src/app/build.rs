//! Application composition and bootstrap wiring (Phase 0).

use super::{
    AppContext,
    bundles::{DataBundle, GovernanceBundle, InfraBundle},
};
use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    governance::{RuntimeModeHandle, runtime_control::QuantRuntimeControl},
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        book_fact_writer::BookFactWriter, fact_lag::FactLagTracker, metrics_hub::MetricsHub,
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
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
    service::{
        catalog_readiness::CatalogReadiness,
        gamma::{GammaService, GammaServiceDeps},
        system_status_nudge::SystemStatusNudge,
        ws_subscription::{MarketDataSubscriptionPolicy, WsSubscriptionCoordinator},
    },
};
use prometheus::IntCounter;
use quant_pivot_api::{
    fees::FeeCalculator,
    gamma::GammaClient,
    ws::{ClobWsManager, WsEventDropHook},
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{BookL2ReplayRow, BookMicrostructureRow, BookSnapshotRow, TickEventRow},
    config::DeployConfig,
    domain::{
        CoreEventPublisher, NewRuntimeConfigActivation, NewRuntimeConfigVersion,
        PointInTimeDataSource, runtime_config_hash,
    },
    enums::{
        quant::QuantRuntimeMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    cached::{CachedEventRepository, CachedMarketRepository},
    clickhouse::{ChFactWriter, ChQuantFactRepository},
    postgres::{
        PgEventRepository, PgMarketRepository, PgOperationLogRepository,
        PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository,
    },
    traits::{
        FactWriter, MarketRepository, QuantFactRepository, RuntimeConfigVersionRepository,
        SystemRuntimeStateRepository,
    },
};
use quant_pivot_storage::{
    cache::{CacheManager, MokaBackend, RedisBackend, RedisPool, TieredCache, connect_pool},
    clickhouse::{ChWriteManager, ClickHousePool},
    postgres::{
        PostgresPool,
        migration::{Migrator, MigratorTrait},
    },
    write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability},
};
use quant_pivot_web::jwt::RedisTokenBlacklist;
use std::{sync::Arc, time::Duration};
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

        let fact_plane = build_fact_plane(&ch_pool, &metrics, &deploy)?;

        let DataPlaneBundle {
            data_quality,
            pit_source,
            data_pipeline,
            applicator,
            status_nudge,
        } = build_data_plane_bundle(
            &deploy,
            &runtime,
            &runtime_store,
            &market_stack,
            &fact_plane,
            &metrics,
            &shutdown,
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
                ch_write_manager: fact_plane.ch_write_manager,
                quant_fact_repo: fact_plane.quant_fact_repo,
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
                market_filter: market_stack.market_filter,
                data_pipeline,
                gamma_service: market_stack.gamma_service,
                ws_manager: market_stack.ws_manager,
                ws_subscription: market_stack.ws_subscription,
                book_fact_writer: fact_plane.book_fact_writer,
                fact_writer_queue: fact_plane.fact_writer_queue,
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
            data_quality,
            pit_source,
            status_nudge,
        })
    }
}

struct MarketStack {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    market_filter: Arc<MarketFilter>,
    ws_manager: Arc<ClobWsManager>,
    ws_subscription: Arc<WsSubscriptionCoordinator>,
    gamma_client: Arc<GammaClient>,
    gamma_service: Arc<GammaService>,
    market_repo: Arc<dyn MarketRepository>,
    operation_log_repo: Arc<PgOperationLogRepository>,
    catalog: Arc<CatalogReadiness>,
}

/// Shared `ClickHouse` write manager, quant fact repo, and book fact writers.
struct FactPlane {
    fact_lag_tracker: Arc<FactLagTracker>,
    ch_write_manager: Arc<ChWriteManager>,
    quant_fact_repo: Arc<dyn QuantFactRepository>,
    book_fact_writer: Arc<BookFactWriter>,
    fact_writer_queue: PendingTaskQueue,
}

struct DataPlaneBundle {
    data_quality: Arc<BookDataQualityService>,
    pit_source: Arc<dyn PointInTimeDataSource>,
    data_pipeline: Arc<DataPipeline>,
    applicator: Arc<RuntimeConfigApplicator>,
    status_nudge: SystemStatusNudge,
}

fn build_data_plane_bundle(
    deploy: &DeployConfig,
    runtime: &RuntimeConfig,
    runtime_store: &Arc<RuntimeConfigStore>,
    market_stack: &MarketStack,
    fact_plane: &FactPlane,
    metrics: &Arc<MetricsHub>,
    shutdown: &CancellationToken,
) -> DataPlaneBundle {
    let data_quality = Arc::new(BookDataQualityService::new(
        Arc::clone(&market_stack.book_store),
        &runtime.data_quality,
        Arc::clone(&fact_plane.fact_lag_tracker),
    ));
    let pit_source: Arc<dyn PointInTimeDataSource> = Arc::new(LiveBookDataSource::new(
        Arc::clone(&market_stack.book_store),
        Arc::clone(&market_stack.market_registry),
    ));
    let status_nudge = SystemStatusNudge::default();
    let data_pipeline = build_data_pipeline(
        market_stack,
        metrics,
        shutdown,
        status_nudge.clone(),
        Arc::clone(&fact_plane.book_fact_writer),
    );
    let applicator = Arc::new(RuntimeConfigApplicator::new(
        Arc::clone(runtime_store),
        RuntimeConfigSubscribers {
            market_filter: Arc::clone(&market_stack.market_filter),
            market_registry: Arc::clone(&market_stack.market_registry),
            market_cache: Arc::clone(&market_stack.market_cache),
            ws_subscription: Some(Arc::clone(&market_stack.ws_subscription)),
            data_quality: Arc::clone(&data_quality),
            metrics: Arc::clone(metrics),
            subscription_window_hours: deploy
                .market_data
                .websocket
                .engine_subscription_window_hours,
        },
    ));
    DataPlaneBundle {
        data_quality,
        pit_source,
        data_pipeline,
        applicator,
        status_nudge,
    }
}

fn build_fact_plane(
    ch_pool: &Arc<ClickHousePool>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
) -> QuantResult<FactPlane> {
    let fact_lag_tracker = Arc::new(FactLagTracker::new());
    let ch_write_manager = Arc::new(ChWriteManager::new(
        deploy.db.clickhouse.max_concurrent_inserts,
    ));
    ch_write_manager
        .metrics()
        .register(&metrics.registry)
        .map_err(|error| {
            QuantError::Internal(format!("clickhouse write metrics registration: {error}"))
        })?;
    let quant_fact_repo: Arc<dyn QuantFactRepository> = Arc::new(ChQuantFactRepository::new(
        Arc::clone(ch_pool),
        Arc::clone(&ch_write_manager),
    ));
    let (book_fact_writer, fact_writer_queue) = build_book_fact_writer(
        ch_pool,
        &ch_write_manager,
        &fact_lag_tracker,
        metrics,
        deploy,
    );
    Ok(FactPlane {
        fact_lag_tracker,
        ch_write_manager,
        quant_fact_repo,
        book_fact_writer,
        fact_writer_queue,
    })
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
    let market_filter = Arc::new(MarketFilter::new(&runtime.selection.enabled_categories));
    let market_cache = Arc::new(MarketCache::new(
        Arc::clone(&market_registry),
        Arc::clone(&market_filter),
    ));
    let catalog = Arc::new(CatalogReadiness::new());
    // L1(Moka)+L2(Redis) cache-aside decorator over the Postgres market repo:
    // catalog writes invalidate, catalog/report reads hit the tiered cache.
    let market_repo: Arc<dyn MarketRepository> = Arc::new(CachedMarketRepository::new(
        PgMarketRepository::new(pg_pool.connection().clone()),
        Arc::clone(cache),
    ));
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
        market_filter: Arc::clone(&market_filter),
        fee_calculator: Arc::clone(&fee_calculator),
        market_repo: Arc::clone(&market_repo),
        event_repo: Arc::new(CachedEventRepository::new(
            PgEventRepository::new(pg_pool.connection().clone()),
            Arc::clone(cache),
        )),
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
        market_filter,
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
    book_fact_writer: Arc<BookFactWriter>,
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
        book_fact_writer,
        book_shard_count: data_pipeline::DEFAULT_BOOK_SHARD_COUNT,
        book_channel_capacity: data_pipeline::DEFAULT_BOOK_CHANNEL_CAPACITY,
        shutdown: shutdown.clone(),
        status_nudge,
    }))
}

/// Assemble the book fact-writer plane: one `AsyncWriter` per `ClickHouse` fact
/// table, each flushing through the shared `ChWriteManager` (permit + retry +
/// metrics). Returns the producer-facing writer plus a queue of flush workers
/// to register on the runner (shutdown stage `Analytics`).
fn build_book_fact_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    fact_lag: &Arc<FactLagTracker>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
) -> (Arc<BookFactWriter>, PendingTaskQueue) {
    let ch = &deploy.db.clickhouse;

    let queue = PendingTaskQueue::default();
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = |name: &'static str| {
        AsyncWriterConfig::new(name)
            .capacity(capacity)
            .batch_size(ch.batch_size)
            .flush_interval(flush_interval)
    };
    let drops = |name: &'static str| metrics.async_writer_dropped.with_label_values(&[name]);

    let ticks = spawn_fact_stream::<TickEventRow>(
        &queue,
        TaskId::TickEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "tick_events",
        )),
        drops("tick_events"),
        metrics.async_writer_observability("tick_events"),
        config("tick_events"),
    );
    let l2 = spawn_fact_stream::<BookL2ReplayRow>(
        &queue,
        TaskId::BookL2ReplayWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "book_l2_replay_hot",
        )),
        drops("book_l2_replay_hot"),
        metrics.async_writer_observability("book_l2_replay_hot"),
        config("book_l2_replay_hot"),
    );
    let snapshots = spawn_fact_stream::<BookSnapshotRow>(
        &queue,
        TaskId::BookSnapshotWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "book_snapshots",
        )),
        drops("book_snapshots"),
        metrics.async_writer_observability("book_snapshots"),
        config("book_snapshots"),
    );
    let microstructure = spawn_fact_stream::<BookMicrostructureRow>(
        &queue,
        TaskId::BookMicrostructure1sWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "book_microstructure_1s",
        )),
        drops("book_microstructure_1s"),
        metrics.async_writer_observability("book_microstructure_1s"),
        config("book_microstructure_1s"),
    );

    let writer = Arc::new(BookFactWriter::new(
        ticks,
        l2,
        snapshots,
        microstructure,
        Arc::clone(fact_lag),
        Arc::clone(metrics),
    ));
    (writer, queue)
}

/// Build one fact stream: wire an `AsyncWriter` to a `ChFactWriter` sink and
/// queue its flush worker. Returns the producer handle for the writer facade.
fn spawn_fact_stream<T>(
    queue: &PendingTaskQueue,
    task: TaskId,
    sink: Arc<dyn FactWriter<T>>,
    drops: IntCounter,
    observability: AsyncWriterObservability,
    config: AsyncWriterConfig,
) -> Arc<AsyncWriter<T>>
where
    T: Send + 'static,
{
    let (writer, worker) = AsyncWriter::new(
        config,
        move |rows| {
            let sink = Arc::clone(&sink);
            Box::pin(async move { sink.write_batch(rows).await })
        },
        drops,
        observability,
    );
    queue.push(task, move |token| worker.run(token));
    Arc::new(writer)
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
