//! Infrastructure bundle: persistence, analytics write plane, metrics.

use std::{sync::Arc, time::Duration};

use prometheus::IntCounter;
use quant_pivot_error::{QuantResult, infra::InfraError, storage::entity::MARKET_RESOLUTION_EVENT};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, MarketResolutionRow,
        QuantCapitalAllocationEventRow, QuantExecutionEventRow, QuantExitSignalEvaluationEventRow,
        QuantFactorEventRow, QuantPositionEventRow, QuantServingEvidenceCompletionRow,
        QuantSignalCandidateEventRow,
    },
    config::DeployConfig,
    types::ContentHash,
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    traits::{FactWriter, QuantFactReadRepository},
};
use quant_pivot_storage::{
    cache::{CacheManager, MokaBackend, RedisBackend, RedisPool, TieredCache, connect_pool},
    clickhouse::{ChWriteManager, ClickHousePool, verify_schema as verify_clickhouse_schema},
    postgres::{PostgresPool, migration::verify_schema as verify_postgres_schema},
    write::{
        AsyncWriter, AsyncWriterConfig, AsyncWriterObservability, DurableWriter,
        DurableWriterConfig,
    },
};
use quant_pivot_web::jwt::RedisTokenBlacklist;

use super::pg_repos::PgRepositories;
use crate::{
    app::{
        task_id::TaskId,
        task_registry::{AppRunner, PendingTaskQueue},
    },
    observability::{
        book_fact_writer::BookFactWriter,
        capital_allocation_fact_writer::CapitalAllocationEventWriter,
        execution_fact_writer::ExecutionEventWriter,
        exit_signal_fact_writer::ExitSignalEvaluationEventWriter,
        fact_lag::IngestPipelineLagTracker, factor_fact_writer::FactorEventWriter,
        feature_fact_writer::FeatureEventWriter, ledger_persistence::LedgerPersistenceCoordinator,
        metrics_hub::MetricsHub, model_input_fact_writer::ModelInputEventWriter,
        position_fact_writer::PositionEventWriter,
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
};

/// Persistence connections, `ClickHouse` fact writers, and shared observability.
pub struct InfraBundle {
    pub pg: Arc<PostgresPool>,
    /// Shared Postgres repositories (wired once at boot).
    pub repos: PgRepositories,
    pub ch: Arc<ClickHousePool>,
    pub ch_write_manager: Arc<ChWriteManager>,
    pub redis: RedisPool,
    pub cache: Arc<CacheManager>,
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    pub metrics: Arc<MetricsHub>,
    /// Verified semantic schema fingerprints captured once at startup.
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    /// Shared ingest-pipeline-lag tracker (enqueue→flush) for data-quality gates.
    pub ingest_lag_tracker: Arc<IngestPipelineLagTracker>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub feature_event_writer: Arc<FeatureEventWriter>,
    /// Long-format factor-event sink (`quant_factor_event`).
    pub factor_event_writer: Arc<FactorEventWriter>,
    /// Exact serving input evidence sink (`quant_model_input_event`).
    pub model_input_event_writer: Arc<ModelInputEventWriter>,
    /// Pre-portfolio signal-candidate sink (`quant_signal_candidate_event`).
    pub signal_candidate_event_writer: Arc<SignalCandidateEventWriter>,
    /// Execution-order lifecycle sink (`quant_execution_event`).
    pub execution_event_writer: Arc<ExecutionEventWriter>,
    /// Capital-allocation ledger sink (`quant_capital_allocation_event`).
    pub capital_allocation_event_writer: Arc<CapitalAllocationEventWriter>,
    /// Position-lot ledger sink (`quant_position_event`).
    pub position_event_writer: Arc<PositionEventWriter>,
    /// Exit-signal evaluation audit sink (`quant_exit_signal_evaluation_event`).
    pub exit_signal_evaluation_event_writer: Arc<ExitSignalEvaluationEventWriter>,
    /// Point-in-time read port over quant `ClickHouse` facts (feature windows).
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    /// Direct, durably acknowledged canonical market-resolution sink.
    pub market_resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>>,
    /// Flush workers for each book fact stream, registered on the runner at boot.
    pub(crate) fact_writer_queue: PendingTaskQueue,
}

impl InfraBundle {
    /// Connect storage backends and wire the `ClickHouse` book-fact write plane.
    pub async fn assemble(deploy: &DeployConfig, metrics: Arc<MetricsHub>) -> QuantResult<Self> {
        let persistence = connect_persistence(deploy, &metrics).await?;
        let analytics = build_analytics_writers(
            &persistence.ch,
            &persistence.ch_write_manager,
            &persistence.ingest_lag_tracker,
            &metrics,
            deploy,
        );
        let market_resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>> =
            Arc::new(ChFactWriter::new(
                Arc::clone(&persistence.ch),
                Arc::clone(&persistence.ch_write_manager),
                MARKET_RESOLUTION_EVENT,
            ));

        Ok(Self {
            pg: persistence.pg,
            repos: persistence.repos,
            ch: persistence.ch,
            ch_write_manager: persistence.ch_write_manager,
            redis: persistence.redis,
            cache: persistence.cache,
            jwt_blacklist: persistence.jwt_blacklist,
            metrics,
            postgres_schema_fingerprint: persistence.postgres_schema_fingerprint,
            clickhouse_schema_fingerprint: persistence.clickhouse_schema_fingerprint,
            ingest_lag_tracker: persistence.ingest_lag_tracker,
            quant_fact_read: persistence.quant_fact_read,
            market_resolution_fact_writer,
            book_fact_writer: analytics.book_fact_writer,
            feature_event_writer: analytics.feature_event_writer,
            factor_event_writer: analytics.factor_event_writer,
            model_input_event_writer: analytics.model_input_event_writer,
            signal_candidate_event_writer: analytics.signal_candidate_event_writer,
            execution_event_writer: analytics.execution_event_writer,
            capital_allocation_event_writer: analytics.capital_allocation_event_writer,
            position_event_writer: analytics.position_event_writer,
            exit_signal_evaluation_event_writer: analytics.exit_signal_evaluation_event_writer,
            fact_writer_queue: analytics.fact_writer_queue,
        })
    }

    /// Register every analytics writer built with this bundle on the owning
    /// application runner. System-level harnesses use the same lifecycle entry
    /// point without exposing or cloning the one-shot pending task queue.
    pub fn register_fact_writer_tasks(&self, runner: &mut AppRunner) {
        runner.absorb_pending_queue(&self.fact_writer_queue);
    }
}

struct PersistenceConnections {
    pg: Arc<PostgresPool>,
    repos: PgRepositories,
    ch: Arc<ClickHousePool>,
    redis: RedisPool,
    cache: Arc<CacheManager>,
    jwt_blacklist: Arc<RedisTokenBlacklist>,
    ingest_lag_tracker: Arc<IngestPipelineLagTracker>,
    ch_write_manager: Arc<ChWriteManager>,
    quant_fact_read: Arc<dyn QuantFactReadRepository>,
    postgres_schema_fingerprint: ContentHash,
    clickhouse_schema_fingerprint: ContentHash,
}

async fn connect_persistence(
    deploy: &DeployConfig,
    metrics: &MetricsHub,
) -> QuantResult<PersistenceConnections> {
    let pg = Arc::new(PostgresPool::connect_existing(&deploy.db.postgres).await?);
    let pg_schema = verify_postgres_schema(pg.connection()).await?;
    tracing::info!(
        schema_version = pg_schema.current_version,
        migrations = pg_schema.migration_count,
        required_tables = pg_schema.required_table_count,
        required_indexes = pg_schema.required_index_count,
        "PostgreSQL runtime schema verified"
    );

    let clickhouse_schema = verify_clickhouse_schema(&deploy.db.clickhouse).await?;
    tracing::info!(
        schema_version = clickhouse_schema.current_version,
        required_objects = clickhouse_schema.required_object_count,
        "ClickHouse runtime schema verified before pool initialization"
    );

    let ch = Arc::new(ClickHousePool::connect(&deploy.db.clickhouse).await?);
    let pooled_clickhouse_schema = ch.verify_schema().await?;
    tracing::info!(
        schema_version = pooled_clickhouse_schema.current_version,
        required_objects = pooled_clickhouse_schema.required_object_count,
        "ClickHouse runtime schema verified"
    );
    if pooled_clickhouse_schema.schema_fingerprint != clickhouse_schema.schema_fingerprint {
        return Err(InfraError::Misconfigured {
            detail: "ClickHouse schema fingerprint changed while initializing the runtime pool"
                .into(),
        }
        .into());
    }

    let redis = connect_pool(&deploy.cache.redis).await?;
    let cache = Arc::new(CacheManager::new(
        TieredCache::new(
            MokaBackend::new(deploy.cache.moka.max_capacity),
            RedisBackend::new(redis.clone(), &deploy.cache.redis.key_prefix),
        ),
        &deploy.cache,
    ));
    cache
        .register_metrics(&metrics.registry)
        .map_err(|error| InfraError::MetricsRegistration {
            subsystem: "cache",
            detail: error.to_string(),
        })?;

    let jwt_blacklist = Arc::new(RedisTokenBlacklist::new(
        redis.clone(),
        &deploy.cache.redis.key_prefix,
    ));
    let repos = PgRepositories::wire(&pg);
    let ingest_lag_tracker = Arc::new(IngestPipelineLagTracker::new());
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
    let quant_fact_read: Arc<dyn QuantFactReadRepository> =
        Arc::new(ChQuantFactReadRepository::new(Arc::clone(&ch)));

    Ok(PersistenceConnections {
        pg,
        repos,
        ch,
        redis,
        cache,
        jwt_blacklist,
        ingest_lag_tracker,
        ch_write_manager,
        quant_fact_read,
        postgres_schema_fingerprint: pg_schema.schema_fingerprint,
        clickhouse_schema_fingerprint: clickhouse_schema.schema_fingerprint,
    })
}

struct AnalyticsWriters {
    book_fact_writer: Arc<BookFactWriter>,
    feature_event_writer: Arc<FeatureEventWriter>,
    factor_event_writer: Arc<FactorEventWriter>,
    model_input_event_writer: Arc<ModelInputEventWriter>,
    signal_candidate_event_writer: Arc<SignalCandidateEventWriter>,
    execution_event_writer: Arc<ExecutionEventWriter>,
    capital_allocation_event_writer: Arc<CapitalAllocationEventWriter>,
    position_event_writer: Arc<PositionEventWriter>,
    exit_signal_evaluation_event_writer: Arc<ExitSignalEvaluationEventWriter>,
    fact_writer_queue: PendingTaskQueue,
}

fn build_analytics_writers(
    ch: &Arc<ClickHousePool>,
    ch_write_manager: &Arc<ChWriteManager>,
    ingest_lag_tracker: &Arc<IngestPipelineLagTracker>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
) -> AnalyticsWriters {
    let (book_fact_writer, fact_writer_queue) =
        build_book_fact_writer(ch, ch_write_manager, ingest_lag_tracker, metrics, deploy);
    let feature_event_writer = build_feature_event_writer(ch, ch_write_manager);
    let factor_event_writer =
        build_factor_event_writer(ch, ch_write_manager, metrics, deploy, &fact_writer_queue);
    let model_input_event_writer = build_model_input_event_writer(ch, ch_write_manager);
    let signal_candidate_event_writer = build_signal_candidate_event_writer(
        ch,
        ch_write_manager,
        metrics,
        deploy,
        &fact_writer_queue,
    );
    let exit_signal_evaluation_event_writer = build_exit_signal_evaluation_event_writer(
        ch,
        ch_write_manager,
        metrics,
        deploy,
        &fact_writer_queue,
    );
    let LedgerEventWriters {
        execution: execution_event_writer,
        capital: capital_allocation_event_writer,
        position: position_event_writer,
    } = build_ledger_event_writers(ch, ch_write_manager, metrics, deploy, &fact_writer_queue);

    AnalyticsWriters {
        book_fact_writer,
        feature_event_writer,
        factor_event_writer,
        model_input_event_writer,
        signal_candidate_event_writer,
        execution_event_writer,
        capital_allocation_event_writer,
        position_event_writer,
        exit_signal_evaluation_event_writer,
        fact_writer_queue,
    }
}

/// Assemble the book fact-writer plane: one `AsyncWriter` per `ClickHouse` fact
/// table, each flushing through the shared `ChWriteManager` (permit + retry +
/// metrics). Returns the producer-facing writer plus a queue of flush workers
/// to register on the runner (shutdown stage `Analytics`).
fn build_book_fact_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    ingest_lag: &Arc<IngestPipelineLagTracker>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
) -> (Arc<BookFactWriter>, PendingTaskQueue) {
    let ch = &deploy.db.clickhouse;

    let queue = PendingTaskQueue::default();
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let async_config = |name: &'static str| {
        AsyncWriterConfig::new(name)
            .capacity(capacity)
            .batch_size(ch.batch_size)
            .flush_interval(flush_interval)
    };
    let durable_config = |name: &'static str| {
        DurableWriterConfig::new(name)
            .capacity(capacity)
            .max_batch_size(ch.batch_size.min(256))
            .max_batch_delay(Duration::from_millis(5))
    };
    let drops = |name: &'static str| metrics.async_writer_dropped.with_label_values(&[name]);
    // Observability that also feeds the enqueue→flush pipeline lag into the
    // shared tracker (data-quality plane) and the Prometheus histogram.
    let lag_obs = |name: &'static str| {
        let mut obs = metrics.async_writer_observability(name);
        let tracker = Arc::clone(ingest_lag);
        let metrics = Arc::clone(metrics);
        obs.flush_lag_ms = Some(Arc::new(move |lag_ms| {
            tracker.record_ms(lag_ms);
            metrics.observe_ingest_pipeline_lag_ms(name, lag_ms);
        }));
        obs
    };

    let sessions = spawn_durable_fact_stream::<BookStreamSessionRow>(
        &queue,
        TaskId::BookStreamSessionWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_book_stream_session",
        )),
        lag_obs("quant_book_stream_session"),
        durable_config("quant_book_stream_session"),
    );
    let (ledger, ledger_coordinator) = LedgerPersistenceCoordinator::new(
        Arc::new(ChFactWriter::<BookL2LedgerRow>::new_async_insert(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_book_l2_ledger",
        )),
        lag_obs("quant_book_l2_ledger"),
    );
    queue.push(TaskId::BookL2LedgerWriter, move |token| {
        ledger_coordinator.run(token)
    });
    let microstructure = spawn_fact_stream::<BookMicrostructureRow>(
        &queue,
        TaskId::BookMicrostructure1sWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "book_microstructure_1s",
        )),
        drops("book_microstructure_1s"),
        lag_obs("book_microstructure_1s"),
        async_config("book_microstructure_1s"),
    );
    let writer = Arc::new(BookFactWriter::new(ledger, sessions, microstructure));
    (writer, queue)
}

/// Wire the acknowledged long-format feature-evidence sink.
fn build_feature_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
) -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(ChFactWriter::new(
        Arc::clone(ch_pool),
        Arc::clone(write_manager),
        "quant_feature_event",
    ))))
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

/// Wire model-input evidence and its run-scoped completion barrier.
fn build_model_input_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
) -> Arc<ModelInputEventWriter> {
    Arc::new(ModelInputEventWriter::new(
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_model_input_event",
        )),
        Arc::new(ChFactWriter::<QuantServingEvidenceCompletionRow>::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_serving_evidence_completion",
        )),
    ))
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

fn build_exit_signal_evaluation_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<ExitSignalEvaluationEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_exit_signal_evaluation_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_exit_signal_evaluation_event"]);
    let stream = spawn_fact_stream::<QuantExitSignalEvaluationEventRow>(
        queue,
        TaskId::ExitSignalEvaluationEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_exit_signal_evaluation_event",
        )),
        drops,
        metrics.async_writer_observability("quant_exit_signal_evaluation_event"),
        config,
    );
    Arc::new(ExitSignalEvaluationEventWriter::new(stream))
}

struct LedgerEventWriters {
    execution: Arc<ExecutionEventWriter>,
    capital: Arc<CapitalAllocationEventWriter>,
    position: Arc<PositionEventWriter>,
}

fn build_ledger_event_writers(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> LedgerEventWriters {
    LedgerEventWriters {
        execution: build_execution_event_writer(ch_pool, write_manager, metrics, deploy, queue),
        capital: build_capital_allocation_event_writer(
            ch_pool,
            write_manager,
            metrics,
            deploy,
            queue,
        ),
        position: build_position_event_writer(ch_pool, write_manager, metrics, deploy, queue),
    }
}

fn build_execution_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<ExecutionEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_execution_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_execution_event"]);
    let stream = spawn_fact_stream::<QuantExecutionEventRow>(
        queue,
        TaskId::ExecutionEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_execution_event",
        )),
        drops,
        metrics.async_writer_observability("quant_execution_event"),
        config,
    );
    Arc::new(ExecutionEventWriter::new(stream))
}

fn build_capital_allocation_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<CapitalAllocationEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_capital_allocation_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_capital_allocation_event"]);
    let stream = spawn_fact_stream::<QuantCapitalAllocationEventRow>(
        queue,
        TaskId::CapitalAllocationEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_capital_allocation_event",
        )),
        drops,
        metrics.async_writer_observability("quant_capital_allocation_event"),
        config,
    );
    Arc::new(CapitalAllocationEventWriter::new(stream))
}

fn build_position_event_writer(
    ch_pool: &Arc<ClickHousePool>,
    write_manager: &Arc<ChWriteManager>,
    metrics: &Arc<MetricsHub>,
    deploy: &DeployConfig,
    queue: &PendingTaskQueue,
) -> Arc<PositionEventWriter> {
    let ch = &deploy.db.clickhouse;
    let capacity = ch.batch_size.saturating_mul(4).max(8_192);
    let flush_interval = Duration::from_secs(ch.flush_interval_secs.max(1));
    let config = AsyncWriterConfig::new("quant_position_event")
        .capacity(capacity)
        .batch_size(ch.batch_size)
        .flush_interval(flush_interval);
    let drops = metrics
        .async_writer_dropped
        .with_label_values(&["quant_position_event"]);
    let stream = spawn_fact_stream::<QuantPositionEventRow>(
        queue,
        TaskId::PositionEventsWriter,
        Arc::new(ChFactWriter::new(
            Arc::clone(ch_pool),
            Arc::clone(write_manager),
            "quant_position_event",
        )),
        drops,
        metrics.async_writer_observability("quant_position_event"),
        config,
    );
    Arc::new(PositionEventWriter::new(stream))
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
    T: Send + Sync + 'static,
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

fn spawn_durable_fact_stream<T>(
    queue: &PendingTaskQueue,
    task: TaskId,
    sink: Arc<dyn FactWriter<T>>,
    observability: AsyncWriterObservability,
    config: DurableWriterConfig,
) -> Arc<DurableWriter<T>>
where
    T: Send + Sync + 'static,
{
    let (writer, worker) = DurableWriter::new(
        config,
        move |rows| {
            let sink = Arc::clone(&sink);
            Box::pin(async move { sink.write_batch(rows).await })
        },
        observability,
    );
    queue.push(task, move |token| worker.run(token));
    Arc::new(writer)
}
