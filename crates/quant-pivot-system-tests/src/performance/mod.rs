//! Reproducible production-stack performance verification.

mod evidence;
mod upstream;

use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Error, Result, bail};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use hdrhistogram::Histogram;
use num_traits::ToPrimitive;
use quant_pivot_api::ws::{
    ClobWsManager, ClobWsManagerHooks, IngressEnqueueObserver, SubscriptionSource,
    TokenKeyResolver, TransportRetirementHook, WsSessionInvalidationHook,
};
use quant_pivot_core::{
    app::{InfraBundle, task_id::TaskId, task_registry::AppRunner},
    ingest::{
        book_store::BookStore,
        data_pipeline::{
            DataPipeline, DataPipelineDeps, DurableBookPublishKind, DurableBookPublishObserver,
            DurableBookPublishSample, PARTITION_COUNT,
        },
        data_plane_index::DataPlane,
        event_source::PipelineEventSource,
        market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
    service::system_status_nudge::SystemStatusNudge,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig,
    domain::data_plane::pipeline::StreamSessionTicket,
    types::{TokenId, TokenKey},
};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;

use self::{
    evidence::{
        HDR_HIGHEST_US, HDR_LOWEST_US, HDR_SIGNIFICANT_FIGURES, HistogramSummaryV1,
        PERFORMANCE_EVIDENCE_SCHEMA_VERSION, PerformanceCorrectnessV1, PerformanceEnvironmentV1,
        PerformanceEvidenceV1, PerformanceMeasurementsV1, PerformanceWorkloadV1,
        collect_environment, jemalloc_allocated_bytes, peak_resident_memory_bytes, process_cpu_ns,
        resident_memory_bytes, write_evidence, write_histogram_artifact,
    },
    upstream::{DeliveryStats, DeterministicCatalog, DeterministicClobServer, measure_http_rtt},
};
use crate::stack::SystemStack;

const REQUIRED_RUNNER: &str = "quant-pivot-perf-8c16g";
const RUNNER_ATTESTATION_ENV: &str = "QUANT_PIVOT_PERF_RUNNER";
const ACTIVE_TOKENS: usize = 2_000;
const MARKET_COUNT: usize = ACTIVE_TOKENS / 2;
const LOAD_TICK: Duration = Duration::from_millis(20);
const INITIAL_SNAPSHOT_TIMEOUT: Duration = Duration::from_mins(1);
const PIPELINE_RECOVERY_TIMEOUT: Duration = Duration::from_mins(1);
const FULL_RUN_COUNT: u16 = 3;
const MAX_RUNNER_VARIATION_PERCENT: f64 = 3.0;
const MAX_ENQUEUE_P99_US: u64 = 250;
const MAX_DURABLE_PUBLISH_P99_US: u64 = 250_000;
const MAX_ONLINE_RSS_BYTES: u64 = 3_758_096_384;
const FIXTURE_SEED: u64 = 0x5150_5045_5246_3231;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PerformanceProfile {
    Smoke,
    Full,
    Soak,
}

impl PerformanceProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
            Self::Soak => "soak",
        }
    }

    const fn run_count(self) -> u16 {
        match self {
            Self::Full => FULL_RUN_COUNT,
            Self::Smoke | Self::Soak => 1,
        }
    }

    const fn workload(self) -> WorkloadProfile {
        match self {
            Self::Smoke => WorkloadProfile {
                warmup: Duration::from_secs(1),
                sustained: Duration::from_secs(3),
                sustained_rate: 1_000,
                burst: Duration::from_secs(1),
                burst_rate: 5_000,
                recovery: Duration::from_secs(1),
                churn_interval: None,
            },
            Self::Full => WorkloadProfile {
                warmup: Duration::from_mins(5),
                sustained: Duration::from_mins(30),
                sustained_rate: 10_000,
                burst: Duration::from_secs(10),
                burst_rate: 50_000,
                recovery: Duration::from_mins(5),
                churn_interval: None,
            },
            Self::Soak => WorkloadProfile {
                warmup: Duration::from_mins(5),
                sustained: Duration::from_hours(2),
                sustained_rate: 10_000,
                burst: Duration::ZERO,
                burst_rate: 0,
                recovery: Duration::from_mins(5),
                churn_interval: Some(Duration::from_mins(5)),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkloadProfile {
    warmup: Duration,
    sustained: Duration,
    sustained_rate: u64,
    burst: Duration,
    burst_rate: u64,
    recovery: Duration,
    churn_interval: Option<Duration>,
}

struct MeasurementRecorder {
    enabled: AtomicBool,
    expected_interval_us: AtomicU64,
    all_durable_publications: AtomicU64,
    durable_publications: AtomicU64,
    observer_errors: AtomicU64,
    gaps: AtomicU64,
    duplicates: AtomicU64,
    out_of_order: AtomicU64,
    enqueue: Mutex<Histogram<u64>>,
    durable: Vec<Mutex<Histogram<u64>>>,
    last_sequence: Mutex<HashMap<(TokenKey, StreamSessionTicket), u64>>,
}

impl MeasurementRecorder {
    fn new() -> Result<Self> {
        let mut durable = Vec::with_capacity(PARTITION_COUNT);
        for _ in 0..PARTITION_COUNT {
            durable.push(Mutex::new(new_histogram()?));
        }
        Ok(Self {
            enabled: AtomicBool::new(false),
            expected_interval_us: AtomicU64::new(1),
            all_durable_publications: AtomicU64::new(0),
            durable_publications: AtomicU64::new(0),
            observer_errors: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            out_of_order: AtomicU64::new(0),
            enqueue: Mutex::new(new_histogram()?),
            durable,
            last_sequence: Mutex::new(HashMap::new()),
        })
    }

    fn enable(&self, rate: u64) {
        self.set_rate(rate);
        self.enabled.store(true, Ordering::Release);
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    fn set_rate(&self, rate: u64) {
        self.expected_interval_us.store(
            1_000_000_u64.checked_div(rate.max(1)).unwrap_or(1).max(1),
            Ordering::Release,
        );
    }

    fn observe_enqueue(&self, elapsed: Duration) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let value = duration_us(elapsed);
        let expected = self.expected_interval_us.load(Ordering::Acquire);
        match self.enqueue.lock() {
            Ok(mut histogram) => {
                if histogram.record_correct(value, expected).is_err() {
                    self.observer_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                self.observer_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn observe_durable(&self, sample: DurableBookPublishSample) {
        self.all_durable_publications
            .fetch_add(1, Ordering::Release);
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        self.observe_sequence(sample);
        if sample.kind != DurableBookPublishKind::Delta {
            return;
        }
        let value = duration_us(
            sample
                .published_at
                .saturating_duration_since(sample.ws_ingress),
        );
        let expected = self.expected_interval_us.load(Ordering::Acquire);
        let shard = sample.token.index() % self.durable.len();
        match self.durable[shard].lock() {
            Ok(mut histogram) => {
                if histogram.record_correct(value, expected).is_err() {
                    self.observer_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                self.observer_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.durable_publications.fetch_add(1, Ordering::Release);
    }

    fn observe_sequence(&self, sample: DurableBookPublishSample) {
        let Ok(mut sequences) = self.last_sequence.lock() else {
            self.observer_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if let Some(previous) =
            sequences.insert((sample.token, sample.session), sample.token_sequence)
        {
            if sample.token_sequence == previous {
                self.duplicates.fetch_add(1, Ordering::Relaxed);
            } else if sample.token_sequence < previous {
                self.out_of_order.fetch_add(1, Ordering::Relaxed);
            } else if sample.token_sequence != previous.saturating_add(1) {
                self.gaps.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn enqueue_histogram(&self) -> Result<Histogram<u64>> {
        self.enqueue
            .lock()
            .map(|histogram| histogram.clone())
            .map_err(|_| anyhow::anyhow!("enqueue HDR histogram lock poisoned"))
    }

    fn durable_histogram(&self) -> Result<Histogram<u64>> {
        let mut merged = new_histogram()?;
        for histogram in &self.durable {
            let histogram = histogram
                .lock()
                .map_err(|_| anyhow::anyhow!("durable HDR histogram lock poisoned"))?;
            merged
                .add(&*histogram)
                .context("merge durable HDR histogram shard")?;
        }
        Ok(merged)
    }
}

fn new_histogram() -> Result<Histogram<u64>> {
    Histogram::new_with_bounds(HDR_LOWEST_US, HDR_HIGHEST_US, HDR_SIGNIFICANT_FIGURES)
        .context("create performance HDR histogram")
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros())
        .unwrap_or(u64::MAX)
        .max(1)
}

#[derive(Default)]
struct PhaseResult {
    events: u64,
    encoded_bytes: u64,
    elapsed: Duration,
}

#[derive(Clone, Copy)]
struct OpenLoopSpec {
    rate: u64,
    duration: Duration,
    sequence_base: u64,
}

struct CompletedLoad {
    source_events: u64,
    durable_publications: u64,
    invalid_fresh_reads: u64,
    measured_elapsed: Duration,
    encoded_bytes: u64,
    cpu_before: Option<u64>,
    cpu_after: Option<u64>,
    allocated_before: usize,
    allocated_after: usize,
    enqueue_histogram: Histogram<u64>,
    durable_histogram: Histogram<u64>,
}

struct PerformanceRuntime {
    stack: SystemStack,
    clob: DeterministicClobServer,
    catalog: DeterministicCatalog,
    recorder: Arc<MeasurementRecorder>,
    root_shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
    infra: InfraBundle,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    manager: Arc<ClobWsManager>,
    runner_task: JoinHandle<QuantResult<()>>,
}

pub async fn run_profile(profile: PerformanceProfile, output_dir: &Path) -> Result<Vec<PathBuf>> {
    (profile).validate_runner()?;
    let mut evidence_paths = Vec::with_capacity(usize::from(profile.run_count()));
    let mut measurements = Vec::with_capacity(usize::from(profile.run_count()));
    for run_index in 1..=profile.run_count() {
        let (evidence, path) = Box::pin(run_once(profile, run_index, output_dir)).await?;
        if !evidence.passed {
            bail!(
                "performance {} run {run_index} failed; evidence={}",
                profile.name(),
                path.display()
            );
        }
        measurements.push(evidence.measurements);
        evidence_paths.push(path);
    }
    if profile == PerformanceProfile::Full {
        enforce_runner_variation(&measurements)?;
    }
    Ok(evidence_paths)
}

impl PerformanceProfile {
    fn validate_runner(self) -> Result<()> {
        if self == Self::Smoke {
            return Ok(());
        }
        if !cfg!(target_os = "linux") {
            bail!(
                "{} performance profile requires the fixed Linux runner",
                self.name()
            );
        }
        let attested = env::var(RUNNER_ATTESTATION_ENV).unwrap_or_default();
        if attested != REQUIRED_RUNNER {
            bail!(
                "{} performance profile requires {RUNNER_ATTESTATION_ENV}={REQUIRED_RUNNER}",
                self.name()
            );
        }
        Ok(())
    }
}

async fn run_once(
    profile: PerformanceProfile,
    run_index: u16,
    output_root: &Path,
) -> Result<(PerformanceEvidenceV1, PathBuf)> {
    let started_at = Utc::now();
    let workload = profile.workload();
    let runtime = Box::pin(PerformanceRuntime::start()).await?;
    let run_result = Box::pin(execute_and_write_evidence(
        &runtime,
        profile,
        run_index,
        output_root,
        started_at,
        workload,
    ))
    .await;
    Box::pin(runtime.finish(run_result)).await
}

impl PerformanceRuntime {
    async fn start() -> Result<Self> {
        let stack = Box::pin(SystemStack::start())
            .await
            .context("start performance infrastructure")?;
        let clob = DeterministicClobServer::start().await?;
        let catalog = DeterministicCatalog::load(MARKET_COUNT).await?;
        let recorder = Arc::new(MeasurementRecorder::new()?);
        let root_shutdown = CancellationToken::new();
        let metrics = Arc::new(MetricsHub::new());
        let mut deploy = DeployConfig::default();
        deploy.db.postgres = stack.postgres_config.clone();
        deploy.db.clickhouse = stack.clickhouse_config.clone();
        deploy.cache.redis = stack.redis_config.clone();
        deploy.polymarket.clob_ws_url = clob.base_url();
        deploy
            .market_data
            .websocket
            .max_subscriptions_per_connection = 200;
        deploy.market_data.websocket.engine_max_subscription_tokens = ACTIVE_TOKENS;

        let infra = InfraBundle::assemble(&deploy, Arc::clone(&metrics))
            .await
            .context("assemble production analytics write plane")?;
        let data_plane = Arc::new(DataPlane::new());
        let book_store = Arc::new(BookStore::new(
            Arc::clone(&data_plane),
            Arc::clone(&metrics),
        ));
        let market_registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
        market_registry.register_events(catalog.events.clone());
        market_registry.register_markets(catalog.markets.clone());

        let invalidation_books = Arc::clone(&book_store);
        let invalidation_metrics = Arc::clone(&metrics);
        let on_session_invalidated: WsSessionInvalidationHook = Arc::new(move |token_ids| {
            let invalidated = invalidation_books.invalidate_ids(token_ids);
            invalidation_metrics
                .ws_session_backpressure_invalidations
                .inc_by(u64::try_from(invalidated).unwrap_or(u64::MAX));
        });
        let (retirement_tx, retirement_rx) = flume::bounded(1_024);
        let retirement_shutdown = root_shutdown.clone();
        let on_transport_retired: TransportRetirementHook = Arc::new(move |retirement| {
            if retirement_tx
                .send_timeout(retirement, Duration::from_millis(100))
                .is_err()
            {
                retirement_shutdown.cancel();
            }
        });
        let enqueue_recorder = Arc::clone(&recorder);
        let enqueue_observer: IngressEnqueueObserver = Arc::new(move |elapsed, _event_count| {
            enqueue_recorder.observe_enqueue(elapsed);
        });
        let manager = Arc::new(ClobWsManager::new(
            &deploy.polymarket,
            &deploy.market_data.websocket,
            root_shutdown.clone(),
            Arc::clone(&data_plane) as Arc<dyn TokenKeyResolver>,
            ClobWsManagerHooks {
                on_session_invalidated: Some(on_session_invalidated),
                on_transport_retired: Some(on_transport_retired),
                ingress_enqueue_observer: Some(enqueue_observer),
                ..ClobWsManagerHooks::default()
            },
        ));
        let publish_recorder = Arc::clone(&recorder);
        let publish_observer: DurableBookPublishObserver = Arc::new(move |sample| {
            publish_recorder.observe_durable(sample);
        });
        let event_source: Arc<dyn PipelineEventSource> =
            Arc::clone(&manager) as Arc<dyn PipelineEventSource>;
        let pipeline = Arc::new(DataPipeline::new(DataPipelineDeps {
            event_source,
            book_store: Arc::clone(&book_store),
            market_registry: Arc::clone(&market_registry),
            metrics: Arc::clone(&metrics),
            book_fact_writer: Arc::clone(&infra.book_fact_writer),
            shutdown: root_shutdown.clone(),
            status_nudge: SystemStatusNudge::default(),
            retirement_rx,
            durable_publish_observer: Some(publish_observer),
        }));
        let mut runner = AppRunner::new(root_shutdown.clone());
        infra.register_fact_writer_tasks(&mut runner);
        runner.spawn_critical(TaskId::DataPipeline, move |_token| async move {
            pipeline.run().await
        });
        let runner_task = tokio::spawn(runner.run());

        manager.subscribe_tokens(SubscriptionSource::Engine, &catalog.tokens);
        clob.wait_for_subscriptions(ACTIVE_TOKENS, INITIAL_SNAPSHOT_TIMEOUT)
            .await?;
        wait_for_all_fresh(&book_store, &catalog.tokens, INITIAL_SNAPSHOT_TIMEOUT).await?;
        Ok(Self {
            stack,
            clob,
            catalog,
            recorder,
            root_shutdown,
            metrics,
            infra,
            book_store,
            market_registry,
            manager,
            runner_task,
        })
    }

    async fn shutdown(self) -> Result<()> {
        let Self {
            stack,
            clob,
            root_shutdown,
            infra,
            book_store,
            market_registry,
            manager,
            runner_task,
            ..
        } = self;
        root_shutdown.cancel();
        let runner_result = async {
            timeout(Duration::from_mins(1), runner_task)
                .await
                .context("performance runner shutdown timed out")?
                .context("performance runner task panicked")?
                .context("performance runner reported a critical failure")
        }
        .await;
        clob.shutdown().await;
        drop((manager, book_store, market_registry, infra));
        let stack_result = Box::pin(stack.shutdown())
            .await
            .context("shutdown performance infrastructure");
        runner_result.and(stack_result)
    }

    async fn finish(
        self,
        run_result: Result<(PerformanceEvidenceV1, PathBuf)>,
    ) -> Result<(PerformanceEvidenceV1, PathBuf)> {
        let shutdown_result = Box::pin(self.shutdown()).await;
        merge_run_and_shutdown(run_result, shutdown_result)
    }
}

async fn execute_and_write_evidence(
    runtime: &PerformanceRuntime,
    profile: PerformanceProfile,
    run_index: u16,
    output_root: &Path,
    started_at: DateTime<Utc>,
    workload: WorkloadProfile,
) -> Result<(PerformanceEvidenceV1, PathBuf)> {
    let load = Box::pin(execute_workload(runtime, workload)).await?;
    Box::pin(write_run_evidence(
        runtime,
        &load,
        profile,
        run_index,
        output_root,
        started_at,
        workload,
    ))
    .await
}

async fn execute_workload(
    runtime: &PerformanceRuntime,
    workload: WorkloadProfile,
) -> Result<CompletedLoad> {
    let recorder = &runtime.recorder;
    let warmup_baseline = recorder.all_durable_publications.load(Ordering::Acquire);
    let warmup = run_open_loop(
        &runtime.clob,
        &runtime.catalog.tokens,
        OpenLoopSpec {
            rate: workload.sustained_rate,
            duration: workload.warmup,
            sequence_base: 0,
        },
    )
    .await?;
    wait_for_counter(
        &recorder.all_durable_publications,
        warmup_baseline.saturating_add(warmup.events),
        PIPELINE_RECOVERY_TIMEOUT,
        "warm-up durable publications",
    )
    .await?;

    let cpu_before = process_cpu_ns()?;
    let allocated_before = jemalloc_allocated_bytes()?;
    recorder.enable(workload.sustained_rate);
    let sustained = if let Some(churn_interval) = workload.churn_interval {
        run_churn_load(
            &runtime.manager,
            &runtime.clob,
            &runtime.book_store,
            &runtime.catalog.tokens,
            OpenLoopSpec {
                rate: workload.sustained_rate,
                duration: workload.sustained,
                sequence_base: 1_000_000,
            },
            churn_interval,
        )
        .await?
    } else {
        run_open_loop(
            &runtime.clob,
            &runtime.catalog.tokens,
            OpenLoopSpec {
                rate: workload.sustained_rate,
                duration: workload.sustained,
                sequence_base: 1_000_000,
            },
        )
        .await?
    };
    let burst = if workload.burst.is_zero() {
        PhaseResult::default()
    } else {
        recorder.set_rate(workload.burst_rate);
        run_open_loop(
            &runtime.clob,
            &runtime.catalog.tokens,
            OpenLoopSpec {
                rate: workload.burst_rate,
                duration: workload.burst,
                sequence_base: sustained.events.saturating_add(1_000_000),
            },
        )
        .await?
    };
    let source_events = sustained.events.saturating_add(burst.events);
    wait_for_durable_publications(recorder, source_events, PIPELINE_RECOVERY_TIMEOUT).await?;
    let cpu_after = process_cpu_ns()?;
    let allocated_after = jemalloc_allocated_bytes()?;
    sleep_until(Instant::now() + workload.recovery).await;
    recorder.disable();
    Ok(CompletedLoad {
        source_events,
        durable_publications: recorder.durable_publications.load(Ordering::Acquire),
        invalid_fresh_reads: count_invalid_fresh_reads(
            &runtime.book_store,
            &runtime.catalog.tokens,
        ),
        measured_elapsed: sustained.elapsed.saturating_add(burst.elapsed),
        encoded_bytes: sustained.encoded_bytes.saturating_add(burst.encoded_bytes),
        cpu_before,
        cpu_after,
        allocated_before,
        allocated_after,
        enqueue_histogram: recorder.enqueue_histogram()?,
        durable_histogram: recorder.durable_histogram()?,
    })
}

async fn write_run_evidence(
    runtime: &PerformanceRuntime,
    load: &CompletedLoad,
    profile: PerformanceProfile,
    run_index: u16,
    output_root: &Path,
    started_at: DateTime<Utc>,
    workload: WorkloadProfile,
) -> Result<(PerformanceEvidenceV1, PathBuf)> {
    let run_output = output_root.join(format!("{}-run-{run_index}", profile.name()));
    let fastest_rate = workload.sustained_rate.max(workload.burst_rate).max(1);
    let expected_interval_us = (1_000_000 / fastest_rate).max(1);
    let enqueue_artifact = write_histogram_artifact(
        &run_output,
        "enqueue-latency",
        &load.enqueue_histogram,
        expected_interval_us,
    )?;
    let durable_artifact = write_histogram_artifact(
        &run_output,
        "ingress-to-durable-publish-latency",
        &load.durable_histogram,
        expected_interval_us,
    )?;
    let environment = (runtime).collect_runtime_environment().await?;
    let correctness = collect_correctness(runtime, load)?;
    let measurements = (load).collect_measurements()?;
    let hard_slo_passed = measurements.enqueue_latency.p99_us <= MAX_ENQUEUE_P99_US
        && measurements.ingress_to_durable_publish_latency.p99_us <= MAX_DURABLE_PUBLISH_P99_US
        && measurements
            .online_rss_bytes
            .is_some_and(|rss| rss <= MAX_ONLINE_RSS_BYTES);
    let passed =
        correctness.total() == 0 && (profile == PerformanceProfile::Smoke || hard_slo_passed);
    let evidence = PerformanceEvidenceV1 {
        schema_version: PERFORMANCE_EVIDENCE_SCHEMA_VERSION,
        profile: profile.name().to_owned(),
        run_index,
        started_at,
        finished_at: Utc::now(),
        fixture_seed: FIXTURE_SEED,
        fixture_hash: runtime.catalog.fixture_hash.clone(),
        environment,
        workload: PerformanceWorkloadV1 {
            active_tokens: ACTIVE_TOKENS,
            warmup_seconds: workload.warmup.as_secs(),
            sustained_seconds: workload.sustained.as_secs(),
            sustained_events_per_second: workload.sustained_rate,
            burst_seconds: workload.burst.as_secs(),
            burst_events_per_second: workload.burst_rate,
            recovery_seconds: workload.recovery.as_secs(),
            source_events: load.source_events,
            durable_publications: load.durable_publications,
        },
        correctness,
        measurements,
        artifacts: vec![enqueue_artifact, durable_artifact],
        passed,
    };
    let evidence_path = write_evidence(&run_output, &evidence)?;
    Ok((evidence, evidence_path))
}

impl PerformanceRuntime {
    async fn collect_runtime_environment(&self) -> Result<PerformanceEnvironmentV1> {
        let clickhouse_version = self
            .infra
            .ch
            .client()
            .query("SELECT version()")
            .fetch_one::<String>()
            .await
            .context("read ClickHouse performance version")?;
        let clickhouse_settings = self
            .infra
            .ch
            .client()
            .query(
                "SELECT concat(name, '=', value) FROM system.settings \
             WHERE name IN ('async_insert', 'wait_for_async_insert', \
             'async_insert_deduplicate', 'max_threads') ORDER BY name",
            )
            .fetch_all::<String>()
            .await
            .context("read ClickHouse performance settings")?;
        let network_rtt_p50_us = measure_http_rtt(&self.stack.clickhouse_config.url, 20).await?;
        collect_environment(clickhouse_version, clickhouse_settings, network_rtt_p50_us)
    }
}

fn collect_correctness(
    runtime: &PerformanceRuntime,
    load: &CompletedLoad,
) -> Result<PerformanceCorrectnessV1> {
    let prometheus = runtime
        .metrics
        .gather_prometheus_text()
        .map_err(Error::msg)?
        .1;
    let prometheus = String::from_utf8(prometheus).context("decode performance metrics")?;
    let recorder = &runtime.recorder;
    let publication_excess = load.durable_publications.saturating_sub(load.source_events);
    Ok(PerformanceCorrectnessV1 {
        source_errors: recorder.observer_errors.load(Ordering::Acquire),
        dropped: load.source_events.saturating_sub(load.durable_publications),
        gaps: recorder.gaps.load(Ordering::Acquire),
        duplicates: recorder
            .duplicates
            .load(Ordering::Acquire)
            .saturating_add(publication_excess),
        out_of_order: recorder.out_of_order.load(Ordering::Acquire),
        invalid_fresh_reads: load.invalid_fresh_reads,
        ws_session_invalidations: runtime.metrics.ws_session_backpressure_invalidations.get(),
        book_apply_invalidations: runtime.metrics.book_apply_backpressure_invalidations.get(),
        writer_drops: prometheus_counter_sum(
            &prometheus,
            "quant_pivot_system_async_writer_dropped_total",
        ),
        writer_flush_failures: prometheus_counter_sum(
            &prometheus,
            "quant_pivot_system_async_writer_flush_failed_total",
        ),
    })
}

impl CompletedLoad {
    fn collect_measurements(&self) -> Result<PerformanceMeasurementsV1> {
        let event_count = self.source_events.max(1);
        Ok(PerformanceMeasurementsV1 {
            enqueue_latency: HistogramSummaryV1::from_histogram(&self.enqueue_histogram),
            ingress_to_durable_publish_latency: HistogramSummaryV1::from_histogram(
                &self.durable_histogram,
            ),
            throughput_events_per_second: u64_to_f64(self.source_events, "source events")?
                / self.measured_elapsed.as_secs_f64(),
            cpu_ns_per_event: optional_delta(self.cpu_before, self.cpu_after)
                .map(|delta| ratio_u64(delta, event_count, "CPU nanoseconds per event"))
                .transpose()?,
            encoded_bytes_per_event: ratio_u64(
                self.encoded_bytes,
                event_count,
                "encoded bytes per event",
            )?,
            net_allocated_bytes_per_event: Some(ratio_usize_u64(
                self.allocated_after.saturating_sub(self.allocated_before),
                event_count,
                "allocated bytes per event",
            )?),
            online_rss_bytes: resident_memory_bytes()?,
            peak_rss_bytes: peak_resident_memory_bytes()?,
        })
    }
}

async fn run_open_loop(
    upstream: &DeterministicClobServer,
    tokens: &[TokenId],
    spec: OpenLoopSpec,
) -> Result<PhaseResult> {
    if spec.duration.is_zero() {
        return Ok(PhaseResult::default());
    }
    let ticks = spec.duration.as_millis() / LOAD_TICK.as_millis();
    let events_per_tick = spec
        .rate
        .checked_mul(u64::try_from(LOAD_TICK.as_micros()).unwrap_or(u64::MAX))
        .context("performance rate overflow")?
        / 1_000_000;
    let token_count = u64::try_from(tokens.len()).unwrap_or(u64::MAX);
    if events_per_tick == 0 || events_per_tick > token_count {
        bail!("open-loop tick shape cannot emit {events_per_tick} unique token events");
    }
    let started = Instant::now();
    let mut result = PhaseResult::default();
    let mut cursor = usize::try_from(spec.sequence_base % token_count).unwrap_or(0);
    for tick in 0..ticks {
        let mut batch = Vec::with_capacity(usize::try_from(events_per_tick).unwrap_or(0));
        for offset in 0..events_per_tick {
            let offset = usize::try_from(offset).unwrap_or(0);
            batch.push(&tokens[(cursor + offset) % tokens.len()]);
        }
        cursor = (cursor + batch.len()) % tokens.len();
        let tick = u64::try_from(tick).unwrap_or(u64::MAX);
        let sequence = spec
            .sequence_base
            .saturating_add(tick.saturating_mul(events_per_tick));
        let DeliveryStats {
            events,
            encoded_bytes,
        } = upstream
            .send_delta_batch(
                &batch,
                1_767_225_600_000_u64.saturating_add(sequence),
                sequence,
            )
            .await?;
        if events != events_per_tick {
            bail!("deterministic CLOB delivered {events}/{events_per_tick} scheduled delta events");
        }
        result.events = result.events.saturating_add(events);
        result.encoded_bytes = result.encoded_bytes.saturating_add(encoded_bytes);
        let completed_ticks = u32::try_from(tick.saturating_add(1)).unwrap_or(u32::MAX);
        sleep_until(started + LOAD_TICK * completed_ticks).await;
    }
    result.elapsed = started.elapsed();
    Ok(result)
}

async fn run_churn_load(
    manager: &ClobWsManager,
    upstream: &DeterministicClobServer,
    books: &BookStore,
    tokens: &[TokenId],
    spec: OpenLoopSpec,
    churn_interval: Duration,
) -> Result<PhaseResult> {
    if churn_interval.is_zero() {
        bail!("performance churn interval must be positive");
    }
    let mut result = PhaseResult::default();
    let mut remaining = spec.duration;
    let mut next_sequence = spec.sequence_base;
    while !remaining.is_zero() {
        let chunk_duration = remaining.min(churn_interval);
        let chunk = run_open_loop(
            upstream,
            tokens,
            OpenLoopSpec {
                rate: spec.rate,
                duration: chunk_duration,
                sequence_base: next_sequence,
            },
        )
        .await?;
        next_sequence = next_sequence.saturating_add(chunk.events);
        result.events = result.events.saturating_add(chunk.events);
        result.encoded_bytes = result.encoded_bytes.saturating_add(chunk.encoded_bytes);
        result.elapsed = result.elapsed.saturating_add(chunk.elapsed);
        remaining = remaining.saturating_sub(chunk_duration);
        if remaining.is_zero() {
            break;
        }

        let invalidated = books.invalidate_ids(tokens);
        if invalidated != tokens.len() {
            bail!(
                "catalog churn invalidated {invalidated}/{} token books",
                tokens.len()
            );
        }
        manager.invalidate_tokens(tokens);
        wait_for_all_fresh(books, tokens, INITIAL_SNAPSHOT_TIMEOUT).await?;
        upstream
            .wait_for_subscriptions(tokens.len(), INITIAL_SNAPSHOT_TIMEOUT)
            .await?;
    }
    Ok(result)
}

async fn wait_for_all_fresh(books: &BookStore, tokens: &[TokenId], wait: Duration) -> Result<()> {
    timeout(wait, async {
        loop {
            if count_invalid_fresh_reads(books, tokens) == 0 {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("wait for every deterministic token to become fresh")?;
    Ok(())
}

fn count_invalid_fresh_reads(books: &BookStore, tokens: &[TokenId]) -> u64 {
    tokens
        .iter()
        .filter(|token| books.load_fresh_by_id(token).is_err())
        .count()
        .try_into()
        .unwrap_or(u64::MAX)
}

async fn wait_for_durable_publications(
    recorder: &MeasurementRecorder,
    expected: u64,
    wait: Duration,
) -> Result<()> {
    wait_for_counter(
        &recorder.durable_publications,
        expected,
        wait,
        "measured durable publications",
    )
    .await
}

async fn wait_for_counter(
    counter: &AtomicU64,
    expected: u64,
    wait: Duration,
    label: &str,
) -> Result<()> {
    timeout(wait, async {
        loop {
            if counter.load(Ordering::Acquire) >= expected {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("wait for {expected} {label}"))?;
    Ok(())
}

fn optional_delta(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    Some(after?.saturating_sub(before?))
}

fn merge_run_and_shutdown(
    run_result: Result<(PerformanceEvidenceV1, PathBuf)>,
    shutdown_result: Result<()>,
) -> Result<(PerformanceEvidenceV1, PathBuf)> {
    match (run_result, shutdown_result) {
        (Ok(evidence), Ok(())) => Ok(evidence),
        (Err(run_error), Ok(())) => Err(run_error),
        (Ok(_), Err(shutdown_error)) => Err(shutdown_error),
        (Err(run_error), Err(shutdown_error)) => Err(run_error.context(format!(
            "performance cleanup also failed: {shutdown_error:#}"
        ))),
    }
}

fn u64_to_f64(value: u64, label: &str) -> Result<f64> {
    value
        .to_f64()
        .with_context(|| format!("convert {label} to f64"))
}

fn usize_to_f64(value: usize, label: &str) -> Result<f64> {
    value
        .to_f64()
        .with_context(|| format!("convert {label} to f64"))
}

fn ratio_u64(numerator: u64, denominator: u64, label: &str) -> Result<f64> {
    if denominator == 0 {
        bail!("{label} denominator must be non-zero");
    }
    Ok(u64_to_f64(numerator, label)? / u64_to_f64(denominator, "event count")?)
}

fn ratio_usize_u64(numerator: usize, denominator: u64, label: &str) -> Result<f64> {
    if denominator == 0 {
        bail!("{label} denominator must be non-zero");
    }
    Ok(usize_to_f64(numerator, label)? / u64_to_f64(denominator, "event count")?)
}

fn prometheus_counter_sum(text: &str, name: &str) -> u64 {
    text.lines()
        .filter(|line| line.starts_with(name))
        .filter_map(|line| line.split_whitespace().last())
        .filter_map(|value| value.parse::<u64>().ok())
        .fold(0_u64, u64::saturating_add)
}

fn enforce_runner_variation(measurements: &[PerformanceMeasurementsV1]) -> Result<()> {
    enforce_variation(
        "throughput_events_per_second",
        measurements
            .iter()
            .map(|sample| sample.throughput_events_per_second),
    )?;
    let enqueue_p99 = measurements
        .iter()
        .map(|sample| u64_to_f64(sample.enqueue_latency.p99_us, "enqueue p99"))
        .collect::<Result<Vec<_>>>()?;
    enforce_variation("enqueue_p99_us", enqueue_p99.into_iter())?;
    let durable_p99 = measurements
        .iter()
        .map(|sample| {
            u64_to_f64(
                sample.ingress_to_durable_publish_latency.p99_us,
                "durable publication p99",
            )
        })
        .collect::<Result<Vec<_>>>()?;
    enforce_variation("durable_publish_p99_us", durable_p99.into_iter())?;
    let online_rss = measurements
        .iter()
        .filter_map(|sample| sample.online_rss_bytes)
        .map(|value| u64_to_f64(value, "online RSS"))
        .collect::<Result<Vec<_>>>()?;
    enforce_variation("online_rss_bytes", online_rss.into_iter())
}

fn enforce_variation(name: &str, values: impl Iterator<Item = f64>) -> Result<()> {
    let values = values.collect::<Vec<_>>();
    if values.len() < usize::from(FULL_RUN_COUNT) {
        bail!("runner variation metric {name} is incomplete");
    }
    let minimum = values.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !minimum.is_finite() || minimum <= 0.0 || !maximum.is_finite() {
        bail!("runner variation metric {name} is invalid: {values:?}");
    }
    let variation = (maximum - minimum) / minimum * 100.0;
    if variation > MAX_RUNNER_VARIATION_PERCENT {
        bail!(
            "runner variation for {name} is {variation:.3}% (> {MAX_RUNNER_VARIATION_PERCENT:.1}%): {values:?}"
        );
    }
    Ok(())
}
