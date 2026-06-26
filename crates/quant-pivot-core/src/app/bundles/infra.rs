//! Infrastructure bundle: persistence, analytics write plane, metrics.

use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    observability::{
        book_fact_writer::BookFactWriter, fact_lag::FactLagTracker,
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub, recommendation_fact_writer::RecommendationEventWriter,
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
};
use prometheus::IntCounter;
use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_models::{
    clickhouse::{
        BookL2ReplayRow, BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow,
        QuantFactorEventRow, QuantFeatureEventRow, QuantRecommendationEventRow,
        QuantSignalCandidateEventRow, TickEventRow,
    },
    config::DeployConfig,
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository, ChQuantFactRepository},
    postgres::PgOperationLogRepository,
    traits::{FactWriter, QuantFactReadRepository, QuantFactRepository},
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

/// Persistence connections, `ClickHouse` fact writers, and shared observability.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    pub ch: Arc<ClickHousePool>,
    pub ch_write_manager: Arc<ChWriteManager>,
    pub quant_fact_repo: Arc<dyn QuantFactRepository>,
    pub redis: RedisPool,
    pub cache: Arc<CacheManager>,
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub metrics: Arc<MetricsHub>,
    pub operation_log_repo: Arc<PgOperationLogRepository>,
    /// Shared lag tracker for book facts and data-quality gates.
    pub fact_lag_tracker: Arc<FactLagTracker>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub feature_event_writer: Arc<FeatureEventWriter>,
    /// Long-format factor-event sink (`quant_factor_event`).
    pub factor_event_writer: Arc<FactorEventWriter>,
    /// Pre-portfolio signal-candidate sink (`quant_signal_candidate_event`).
    pub signal_candidate_event_writer: Arc<SignalCandidateEventWriter>,
    /// Published recommendation sink (`quant_recommendation_event`).
    pub recommendation_event_writer: Arc<RecommendationEventWriter>,
    /// Point-in-time read port over quant `ClickHouse` facts (feature windows).
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    /// Flush workers for each book fact stream, registered on the runner at boot.
    pub(crate) fact_writer_queue: PendingTaskQueue,
}

impl InfraBundle {
    /// Connect storage backends and wire the `ClickHouse` book-fact write plane.
    pub async fn assemble(deploy: &DeployConfig, metrics: Arc<MetricsHub>) -> QuantResult<Self> {
        let pg = Arc::new(PostgresPool::connect(&deploy.db.postgres).await?);
        Migrator::up(pg.connection(), None).await?;

        let ch = Arc::new(ClickHousePool::connect(&deploy.db.clickhouse).await?);
        ch.ensure_schema().await?;

        let redis = connect_pool(&deploy.cache.redis).await?;
        let cache = Arc::new(CacheManager::new(
            TieredCache::new(
                MokaBackend::new(deploy.cache.moka.max_capacity),
                RedisBackend::new(redis.clone(), &deploy.cache.redis.key_prefix),
            ),
            &deploy.cache,
        ));
        cache.register_metrics(&metrics.registry).map_err(|error| {
            InfraError::MetricsRegistration {
                subsystem: "cache",
                detail: error.to_string(),
            }
        })?;

        let jwt_blacklist = Arc::new(RedisTokenBlacklist::new(
            redis.clone(),
            &deploy.cache.redis.key_prefix,
        ));

        let operation_log_repo = Arc::new(PgOperationLogRepository::new(pg.connection().clone()));

        let fact_lag_tracker = Arc::new(FactLagTracker::new());
        let ch_write_manager = Arc::new(ChWriteManager::new(
            deploy.db.clickhouse.max_concurrent_inserts,
        ));
        ch_write_manager
            .metrics()
            .register(&metrics.registry)
            .map_err(|error| InfraError::MetricsRegistration {
                subsystem: "clickhouse_write",
                detail: error.to_string(),
            })?;

        let quant_fact_repo: Arc<dyn QuantFactRepository> = Arc::new(ChQuantFactRepository::new(
            Arc::clone(&ch),
            Arc::clone(&ch_write_manager),
        ));
        let quant_fact_read: Arc<dyn QuantFactReadRepository> =
            Arc::new(ChQuantFactReadRepository::new(Arc::clone(&ch)));

        let (book_fact_writer, fact_writer_queue) =
            build_book_fact_writer(&ch, &ch_write_manager, &fact_lag_tracker, &metrics, deploy);
        let feature_event_writer = build_feature_event_writer(
            &ch,
            &ch_write_manager,
            &metrics,
            deploy,
            &fact_writer_queue,
        );
        let factor_event_writer =
            build_factor_event_writer(&ch, &ch_write_manager, &metrics, deploy, &fact_writer_queue);
        let signal_candidate_event_writer = build_signal_candidate_event_writer(
            &ch,
            &ch_write_manager,
            &metrics,
            deploy,
            &fact_writer_queue,
        );
        let recommendation_event_writer = build_recommendation_event_writer(
            &ch,
            &ch_write_manager,
            &metrics,
            deploy,
            &fact_writer_queue,
        );

        Ok(Self {
            pg,
            ch,
            ch_write_manager,
            quant_fact_repo,
            redis,
            cache,
            jwt_blacklist,
            metrics,
            operation_log_repo,
            fact_lag_tracker,
            book_fact_writer,
            feature_event_writer,
            factor_event_writer,
            signal_candidate_event_writer,
            recommendation_event_writer,
            quant_fact_read,
            fact_writer_queue,
        })
    }
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
    let resolutions = spawn_fact_stream::<MarketResolutionRow>(
        &queue,
        TaskId::MarketResolutionWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "market_resolution_event",
        )),
        drops("market_resolution_event"),
        metrics.async_writer_observability("market_resolution_event"),
        config("market_resolution_event"),
    );

    let writer = Arc::new(BookFactWriter::new(
        ticks,
        l2,
        snapshots,
        microstructure,
        resolutions,
        Arc::clone(fact_lag),
        Arc::clone(metrics),
    ));
    (writer, queue)
}

/// Wire the long-format feature-event async writer (`quant_feature_event`).
fn build_feature_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<FeatureEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_feature_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_feature_event"]);
    let stream = spawn_fact_stream::<QuantFeatureEventRow>(
        queue,
        TaskId::FeatureEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_feature_event",
        )),
        drops,
        metrics.async_writer_observability("quant_feature_event"),
        config,
    );
    Arc::new(FeatureEventWriter::new(stream))
}

/// Wire the long-format factor-event async writer (`quant_factor_event`).
fn build_factor_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<FactorEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_factor_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_factor_event"]);
    let stream = spawn_fact_stream::<QuantFactorEventRow>(
        queue,
        TaskId::FactorEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_factor_event",
        )),
        drops,
        metrics.async_writer_observability("quant_factor_event"),
        config,
    );
    Arc::new(FactorEventWriter::new(stream))
}

/// Wire the pre-portfolio signal-candidate async writer
/// (`quant_signal_candidate_event`).
fn build_signal_candidate_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<SignalCandidateEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_signal_candidate_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_signal_candidate_event"]);
    let stream = spawn_fact_stream::<QuantSignalCandidateEventRow>(
        queue,
        TaskId::SignalCandidateEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_signal_candidate_event",
        )),
        drops,
        metrics.async_writer_observability("quant_signal_candidate_event"),
        config,
    );
    Arc::new(SignalCandidateEventWriter::new(stream))
}

/// Wire the published recommendation async writer (`quant_recommendation_event`).
fn build_recommendation_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<RecommendationEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_recommendation_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_recommendation_event"]);
    let stream = spawn_fact_stream::<QuantRecommendationEventRow>(
        queue,
        TaskId::RecommendationEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_recommendation_event",
        )),
        drops,
        metrics.async_writer_observability("quant_recommendation_event"),
        config,
    );
    Arc::new(RecommendationEventWriter::new(stream))
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
