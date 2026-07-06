//! Durable research-job worker: leases queued jobs, executes them off the HTTP
//! hot path, streams progress, and recovers orphaned runs on boot.
//!
//! # Lifecycle & recovery
//!
//! On start the worker runs a **boot recovery sweep** ([`ResearchJobRepository::reclaim_orphaned`]):
//! any `running` row whose lease is owned by a dead epoch or has expired is
//! re-queued (bounded by `recovery_attempt`) or quarantined to `failed`. During
//! steady state a per-job heartbeat renews the lease and doubles as a cooperative
//! stop signal (a job that is no longer `running` under this owner is dropped).
//! A graceful shutdown stops leasing, cooperatively drains in-flight runs
//! (bounded by `shutdown_drain_secs`), and then explicitly re-queues this
//! owner's still-`running` rows ([`ResearchJobRepository::requeue_inflight`]) so
//! the next epoch re-leases them immediately rather than after a lease-expiry
//! wait. Combined with pre-assigned result ids + idempotent result writes, this
//! makes execution effectively-once across restarts.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinSet, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    config::ResearchJobsConfig,
    domain::{
        BacktestJobParams, BacktestPort, BiasTableFitJobParams, BuildTrainingDatasetRequest,
        FavoriteLongshotFitPort, JobProgressSink, ModelTrainingPort, ResearchJobError,
        ResearchJobInfo, ResearchJobProgress, TrainModelRequest, TrainingDatasetPort,
    },
    enums::quant::{ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus},
    types::{DatasetCoverage, ResearchJobId},
};
use quant_pivot_repository::traits::{
    FavoriteLongshotBiasTableRepository, RuntimeConfigVersionRepository,
};

use super::{
    AppContext,
    ports::{
        backtest::CoreBacktestPort, model_training::CoreModelTrainingPort,
        training_dataset::CoreTrainingDatasetPort,
    },
    research_job::ResearchJobEngine,
    task_id::TaskId,
    task_registry::AppRunner,
};

const ALL_KINDS: [ResearchJobKind; 4] = [
    ResearchJobKind::DatasetBuild,
    ResearchJobKind::ModelTrain,
    ResearchJobKind::Backtest,
    ResearchJobKind::BiasTableFit,
];

fn lease_deadline(lease_ttl_secs: i64) -> DateTime<Utc> {
    Utc::now() + chrono::Duration::seconds(lease_ttl_secs)
}

/// Terminal outcome of one job execution.
struct JobOutcome {
    result_ref: Option<Uuid>,
    coverage: Option<DatasetCoverage>,
}

/// Dispatches a leased job to the matching offline service.
#[derive(Clone)]
struct ResearchJobExecutor {
    datasets: Arc<dyn TrainingDatasetPort>,
    training: Arc<dyn ModelTrainingPort>,
    backtests: Arc<dyn BacktestPort>,
    bias_tables: Arc<dyn FavoriteLongshotFitPort>,
}

/// Synchronous progress sink handed to the offline service: a lock-free channel
/// push, safe to call from CPU-bound `spawn_blocking` code (which cannot
/// `.await`). The async supervisor ([`run_one`]) drains the channel, throttles,
/// persists a heartbeat (renewing the lease), and pushes a WebSocket update.
struct ChannelProgressSink {
    tx: mpsc::UnboundedSender<ResearchJobProgress>,
}

impl JobProgressSink for ChannelProgressSink {
    fn report(&self, progress: ResearchJobProgress) {
        // Best-effort, non-blocking: once the supervisor has stopped receiving
        // (job finalizing), the send just drops — progress is advisory.
        let _ = self.tx.send(progress);
    }
}

/// Coalesces high-frequency progress reports (e.g. per cross-section) to at most
/// one durable write + WebSocket push per `min_interval`, so a fine-grained
/// build loop cannot hammer Postgres / the event bus.
struct ProgressThrottle {
    min_interval: Duration,
    last_emit: Option<Instant>,
}

impl ProgressThrottle {
    const fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_emit: None,
        }
    }

    /// Whether enough time has elapsed since the last emitted report to send
    /// another (coalescing intermediate reports within `min_interval`).
    fn should_emit(&mut self) -> bool {
        let now = Instant::now();
        match self.last_emit {
            Some(previous) if now.duration_since(previous) < self.min_interval => false,
            _ => {
                self.last_emit = Some(now);
                true
            }
        }
    }
}

impl ResearchJobExecutor {
    async fn execute(
        &self,
        job: &ResearchJobInfo,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        match job.kind {
            ResearchJobKind::DatasetBuild => {
                let request: BuildTrainingDatasetRequest = from_params(&job.params_json)?;
                let view = self.datasets.build(request, progress, cancel).await?;
                Ok(JobOutcome {
                    result_ref: Some(view.training_dataset_id.as_uuid()),
                    coverage: Some(view.coverage_json),
                })
            }
            ResearchJobKind::ModelTrain => {
                let request: TrainModelRequest = from_params(&job.params_json)?;
                let view = self.training.train(request, progress, cancel).await?;
                Ok(JobOutcome {
                    result_ref: Some(view.model_version_id.as_uuid()),
                    coverage: None,
                })
            }
            ResearchJobKind::Backtest => {
                let params: BacktestJobParams = from_params(&job.params_json)?;
                let view = self
                    .backtests
                    .run(params.model_version_id, params.request, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result_ref: Some(view.backtest_report_id.as_uuid()),
                    coverage: None,
                })
            }
            ResearchJobKind::BiasTableFit => {
                let params: BiasTableFitJobParams = from_params(&job.params_json)?;
                let outcome = self.bias_tables.fit(params, progress, cancel).await?;
                // Fail-closed fits succeed with no artifact (result_ref = None).
                Ok(JobOutcome {
                    result_ref: outcome.bias_table_id.map(|id| id.as_uuid()),
                    coverage: None,
                })
            }
        }
    }
}

fn from_params<T: serde::de::DeserializeOwned>(params: &Value) -> QuantResult<T> {
    serde_json::from_value(params.clone()).map_err(|error| {
        QuantError::from(ResearchError::Serialization {
            detail: format!("research job params deserialization failed: {error}"),
        })
    })
}

impl AppContext {
    /// Register the durable research-job worker (`TaskId::ResearchJobWorker`).
    pub fn register_research_job_worker(&self, runner: &mut AppRunner, engine: ResearchJobEngine) {
        let runtime_config =
            Arc::clone(&self.infra.repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>;
        let bias_table_repo = Arc::clone(&self.infra.repos.favorite_longshot_bias_table)
            as Arc<dyn FavoriteLongshotBiasTableRepository>;
        let config = self.config.quant.research_jobs;
        let executor = ResearchJobExecutor {
            datasets: Arc::new(CoreTrainingDatasetPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
                Arc::clone(&bias_table_repo),
                config.max_spine_samples,
                config.plan_sample_slices,
                config.plan_sample_markets,
            )),
            training: Arc::new(CoreModelTrainingPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
                Arc::clone(&bias_table_repo),
            )),
            backtests: Arc::new(CoreBacktestPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
                Arc::clone(&bias_table_repo),
            )),
            bias_tables: Arc::clone(&self.research.favorite_longshot_fit),
        };
        runner.spawn(TaskId::ResearchJobWorker, move |token| async move {
            run_worker(engine, executor, config, token).await;
        });
    }
}

async fn run_worker(
    engine: ResearchJobEngine,
    executor: ResearchJobExecutor,
    config: ResearchJobsConfig,
    token: CancellationToken,
) {
    let poll = Duration::from_secs(config.poll_secs);
    // Boot recovery: reclaim orphaned `running` rows before leasing anything new.
    match engine
        .repo()
        .reclaim_orphaned(engine.instance_id(), Utc::now())
        .await
    {
        Ok(outcome) if outcome.requeued > 0 || outcome.quarantined > 0 => info!(
            requeued = outcome.requeued,
            quarantined = outcome.quarantined,
            "research-job boot recovery reclaimed orphaned runs",
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "research-job boot recovery sweep failed"),
    }

    let mut tasks: JoinSet<ResearchJobKind> = JoinSet::new();
    let mut inflight: HashMap<ResearchJobKind, usize> = HashMap::new();

    loop {
        if token.is_cancelled() {
            break;
        }
        drain_finished(&mut tasks, &mut inflight);

        let eligible = eligible_kinds(&inflight, &config);
        if eligible.is_empty() {
            wait_for_slot(&token, &mut tasks, &mut inflight, poll).await;
            continue;
        }

        match engine
            .repo()
            .lease_next(
                &eligible,
                engine.instance_id(),
                lease_deadline(config.lease_ttl_secs),
            )
            .await
        {
            Ok(Some(job)) => {
                *inflight.entry(job.kind).or_insert(0) += 1;
                let engine = engine.clone();
                let executor = executor.clone();
                let shutdown = token.clone();
                tasks.spawn(async move {
                    let kind = job.kind;
                    run_one(engine, executor, job, config, shutdown).await;
                    kind
                });
            }
            Ok(None) => wait_for_slot(&token, &mut tasks, &mut inflight, poll).await,
            Err(error) => {
                warn!(%error, "research-job lease failed; backing off");
                sleep_or_cancel(&token, poll).await;
            }
        }
    }

    // Graceful shutdown: leasing already stopped (the loop broke on `token`).
    // Cooperatively **drain** in-flight runs rather than aborting them — each
    // `run_one` observes `shutdown.cancelled()` and unwinds at its next section
    // boundary, deliberately leaving its row `running`. Bound the wait so a
    // stuck build cannot stall the deploy; then explicitly re-queue this owner's
    // still-`running` rows so the next epoch re-leases them immediately, instead
    // of waiting a full `lease_ttl_secs` for the boot sweep to reclaim them.
    let drain = Duration::from_secs(config.shutdown_drain_secs);
    if tokio::time::timeout(drain, async { while tasks.join_next().await.is_some() {} })
        .await
        .is_err()
    {
        warn!(
            drain_secs = config.shutdown_drain_secs,
            "research-job graceful drain timed out; re-queueing in-flight runs anyway"
        );
    }
    match engine.repo().requeue_inflight(engine.instance_id()).await {
        Ok(outcome) if outcome.requeued > 0 || outcome.quarantined > 0 => info!(
            requeued = outcome.requeued,
            quarantined = outcome.quarantined,
            "research-job graceful shutdown re-queued in-flight runs",
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "research-job shutdown requeue_inflight failed"),
    }
    info!("research-job worker stopped");
}

fn eligible_kinds(
    inflight: &HashMap<ResearchJobKind, usize>,
    config: &ResearchJobsConfig,
) -> Vec<ResearchJobKind> {
    let total: usize = inflight.values().sum();
    if total >= config.global_concurrency {
        return Vec::new();
    }
    ALL_KINDS
        .into_iter()
        .filter(|kind| inflight.get(kind).copied().unwrap_or(0) < config.kind_concurrency(*kind))
        .collect()
}

fn drain_finished(
    tasks: &mut JoinSet<ResearchJobKind>,
    inflight: &mut HashMap<ResearchJobKind, usize>,
) {
    while let Some(joined) = tasks.try_join_next() {
        decrement(inflight, joined.ok());
    }
}

async fn wait_for_slot(
    token: &CancellationToken,
    tasks: &mut JoinSet<ResearchJobKind>,
    inflight: &mut HashMap<ResearchJobKind, usize>,
    poll: Duration,
) {
    if tasks.is_empty() {
        sleep_or_cancel(token, poll).await;
        return;
    }
    tokio::select! {
        () = token.cancelled() => {}
        joined = tasks.join_next() => {
            if let Some(joined) = joined {
                decrement(inflight, joined.ok());
            }
        }
        () = tokio::time::sleep(poll) => {}
    }
}

fn decrement(inflight: &mut HashMap<ResearchJobKind, usize>, kind: Option<ResearchJobKind>) {
    if let Some(kind) = kind
        && let Some(count) = inflight.get_mut(&kind)
    {
        *count = count.saturating_sub(1);
    }
}

async fn sleep_or_cancel(token: &CancellationToken, duration: Duration) {
    tokio::select! {
        () = token.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

/// Supervise one leased job: spawn its execution (which offloads CPU-bound work
/// to `spawn_blocking` and polls `cancel`), then `select!` over completion,
/// throttled progress-heartbeats drained from the sink channel, periodic lease
/// renewal, and graceful shutdown.
async fn run_one(
    engine: ResearchJobEngine,
    executor: ResearchJobExecutor,
    job: ResearchJobInfo,
    config: ResearchJobsConfig,
    shutdown: CancellationToken,
) {
    let job_id = job.job_id.clone();
    let cancel = CancellationToken::new();
    engine.register_cancel(&job_id, cancel.clone());
    engine.publish_progress(
        &job_id,
        job.kind,
        None,
        ResearchJobStatus::Running,
        Some("start".to_owned()),
        None,
    );

    // Synchronous progress channel: the (possibly blocking) execution pushes
    // snapshots; the supervisor drains + throttles them here.
    let (tx, mut progress_rx) = mpsc::unbounded_channel::<ResearchJobProgress>();
    let sink: Arc<dyn JobProgressSink> = Arc::new(ChannelProgressSink { tx });
    let execution = executor.execute(&job, sink, cancel.clone());
    tokio::pin!(execution);

    let mut throttle =
        ProgressThrottle::new(Duration::from_millis(config.progress_min_interval_ms));
    let mut heartbeat = interval(Duration::from_secs(config.heartbeat_secs));
    heartbeat.tick().await; // consume the immediate first tick

    let terminal = loop {
        tokio::select! {
            result = &mut execution => break Terminal::from_result(result),
            Some(progress) = progress_rx.recv() => {
                // Coalesce bursty per-section reports; each surfaced report also
                // renews the lease (doubles as a liveness heartbeat).
                if throttle.should_emit() {
                    let pct = progress.pct();
                    let phase = progress.phase.clone();
                    let alive = engine
                        .repo()
                        .heartbeat(&job_id, engine.instance_id(), lease_deadline(config.lease_ttl_secs), Some(progress))
                        .await
                        .unwrap_or(false);
                    engine.publish_progress(&job_id, job.kind, None, ResearchJobStatus::Running, Some(phase), pct);
                    if !alive {
                        cancel.cancel();
                    }
                }
            }
            _ = heartbeat.tick() => {
                let alive = engine
                    .repo()
                    .heartbeat(&job_id, engine.instance_id(), lease_deadline(config.lease_ttl_secs), None)
                    .await
                    .unwrap_or(false);
                if !alive {
                    cancel.cancel();
                }
            }
            () = cancel.cancelled() => break Terminal::Cancelled,
            () = shutdown.cancelled() => {
                // Cooperative graceful stop: signal the build, bounded-await it to
                // unwind at its next section boundary, and leave the row `running`
                // (never finalize) so `requeue_inflight` re-queues it for the next
                // epoch instead of waiting for lease expiry.
                cancel.cancel();
                let _ = tokio::time::timeout(
                    Duration::from_secs(config.shutdown_drain_secs),
                    &mut execution,
                )
                .await;
                engine.clear_cancel(&job_id);
                return;
            }
        }
    };

    finalize(&engine, &job_id, engine.instance_id(), job.kind, terminal).await;
    engine.clear_cancel(&job_id);
}

enum Terminal {
    Succeeded(Box<JobOutcome>),
    Failed(String),
    Cancelled,
}

impl Terminal {
    fn from_result(result: QuantResult<JobOutcome>) -> Self {
        match result {
            Ok(outcome) => Self::Succeeded(Box::new(outcome)),
            // A cooperative cancel funnels through the build token and surfaces
            // as a terminal `Cancelled` (not a failure).
            Err(QuantError::Research(ResearchError::Cancelled { .. })) => Self::Cancelled,
            Err(error) => Self::Failed(error.to_string()),
        }
    }
}

async fn finalize(
    engine: &ResearchJobEngine,
    job_id: &ResearchJobId,
    owner: &str,
    kind: ResearchJobKind,
    terminal: Terminal,
) {
    let (status, result_ref, error, coverage) = match terminal {
        Terminal::Succeeded(outcome) => (
            ResearchJobStatus::Succeeded,
            outcome.result_ref,
            None,
            outcome.coverage,
        ),
        Terminal::Failed(message) => (
            ResearchJobStatus::Failed,
            None,
            Some(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionFailed,
                &message,
            )),
            None,
        ),
        Terminal::Cancelled => (
            ResearchJobStatus::Cancelled,
            None,
            Some(ResearchJobError::new(
                ResearchJobErrorCode::Cancelled,
                "cancelled by operator",
            )),
            None,
        ),
    };
    match engine
        .repo()
        .finalize(job_id, owner, status, result_ref, error, coverage)
        .await
    {
        Ok(info) => {
            let pct = if status == ResearchJobStatus::Succeeded {
                Some(1.0)
            } else {
                None
            };
            engine.publish(&info, Some("finalize".to_owned()), pct);
        }
        Err(StorageError::StateConflict { .. }) => {
            warn!(
                %job_id,
                kind = %kind,
                "stale worker skipped finalize after lease loss or double-finalize"
            );
        }
        Err(error) => error!(%error, kind = %kind, "failed to finalize research job"),
    }
}
