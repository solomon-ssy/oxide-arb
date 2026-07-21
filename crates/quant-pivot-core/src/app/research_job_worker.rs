//! Durable research-job worker: leases queued jobs, executes them off the HTTP
//! hot path, streams progress, and recovers orphaned runs on boot.
//!
//! # Lifecycle & recovery
//!
//! On start the worker runs a **boot recovery sweep** (`reclaim_orphaned`):
//! any `running` row whose lease is owned by a dead epoch or has expired is
//! re-queued (bounded by `recovery_attempt`) or quarantined to `failed`. During
//! steady state a per-job heartbeat renews the lease and doubles as a cooperative
//! stop signal (a job that is no longer `running` under this owner is dropped).
//! A graceful shutdown stops leasing, cooperatively drains in-flight runs
//! (bounded by `shutdown_drain_secs`), and then explicitly re-queues this
//! owner's still-`running` rows (`requeue_inflight`) so
//! the next epoch re-leases them immediately rather than after a lease-expiry
//! wait. Combined with pre-assigned result ids + idempotent result writes, this
//! makes execution effectively-once across restarts.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    config::ResearchJobsConfig,
    domain::{
        ports::{
            BacktestPort, CalibrationArtifactFitPort, CpcvBacktestPort, FeatureParityExecutionPort,
            ModelCalibrationFitPort, ModelTrainingPort, TradePolicyPort, TrainingDatasetPort,
        },
        quant::{JobProgressSink, ResearchJobInfo, ResearchJobResultRef},
    },
    enums::quant::{
        ResearchJobErrorCode, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    types::{
        DatasetCoverage, ResearchJobError, ResearchJobId, ResearchJobParams, ResearchJobProgress,
        WorkerId,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChFeatureParityEventRepository},
    traits::{
        CalibrationArtifactRepository, FactWriter, FeatureParityRepository, PolicyRepository,
        RecommendationReportRepository, ReportRunRepository, ResearchReadinessEvidenceRepository,
        ServingEvidenceRepository, TradePolicyRepository,
    },
};
use tokio::{
    sync::{mpsc, mpsc::UnboundedSender},
    task::JoinSet,
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::{
    AppContext,
    ports::{
        backtest::CoreBacktestPort, cpcv_backtest::CoreCpcvBacktestPort,
        model_training::CoreModelTrainingPort, training_dataset::CoreTrainingDatasetPort,
    },
    research_job::ResearchJobEngine,
    task_id::TaskId,
    task_registry::AppRunner,
};
use crate::service::{
    durable_feature_parity::{DurableFeatureParityDeps, DurableFeatureParitySource},
    feature_parity_executor::{FeatureParityExecutor, ReportFeatureParityIncidentResponse},
    model_calibration_fit::ModelCalibrationFitService,
    research_readiness::{
        EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
    },
    trade_policy::{TradePolicyService, TradePolicyServiceDeps},
};

const ALL_KINDS: [ResearchJobKind; 9] = [
    ResearchJobKind::DatasetBuild,
    ResearchJobKind::ModelTrain,
    ResearchJobKind::Backtest,
    ResearchJobKind::CpcvBacktest,
    ResearchJobKind::BiasTableFit,
    ResearchJobKind::ModelCalibrationFit,
    ResearchJobKind::FeatureParity,
    ResearchJobKind::TradePolicyFit,
    ResearchJobKind::TradePolicyValidation,
];

fn lease_deadline(lease_ttl_secs: i64) -> DateTime<Utc> {
    Utc::now() + ChronoDuration::seconds(lease_ttl_secs)
}

/// Terminal outcome of one job execution.
struct JobOutcome {
    result: Option<ResearchJobResultRef>,
    coverage: Option<DatasetCoverage>,
}

/// Dispatches a leased job to the matching offline service.
#[derive(Clone)]
struct ResearchJobExecutor {
    datasets: Arc<dyn TrainingDatasetPort>,
    training: Arc<dyn ModelTrainingPort>,
    backtests: Arc<dyn BacktestPort>,
    cpcv_backtests: Arc<dyn CpcvBacktestPort>,
    bias_tables: Arc<dyn CalibrationArtifactFitPort>,
    model_calibration_fit: Arc<dyn ModelCalibrationFitPort>,
    feature_parity: Arc<dyn FeatureParityExecutionPort>,
    trade_policies: Arc<dyn TradePolicyPort>,
}

/// Synchronous progress sink handed to the offline service: a lock-free channel
/// push, safe to call from CPU-bound `spawn_blocking` code (which cannot
/// `.await`). The async supervisor ([`run_one`]) drains the channel, throttles,
/// persists a heartbeat (renewing the lease), and pushes a WebSocket update.
struct ChannelProgressSink {
    tx: UnboundedSender<ResearchJobProgress>,
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
        if job.kind != job.params_json.kind() {
            return Err(ResearchError::Serialization {
                detail: format!(
                    "research job kind {} disagrees with typed params kind {}",
                    job.kind.as_str(),
                    job.params_json.kind().as_str()
                ),
            }
            .into());
        }
        match job.params_json.clone() {
            ResearchJobParams::DatasetBuild(request) => {
                let view = self.datasets.build(request, progress, cancel).await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::TrainingDataset,
                        id: view.training_dataset_id.as_uuid(),
                    }),
                    coverage: view.coverage_json,
                })
            }
            ResearchJobParams::ModelTrain(params) => {
                let view = self
                    .training
                    .train(params.model_version_id, params.request, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::ModelVersion,
                        id: view.model_version_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::Backtest(params) => {
                let view = self
                    .backtests
                    .run(params.model_version_id, params.request, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::BacktestReport,
                        id: view.backtest_report_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::CpcvBacktest(params) => {
                let view = self
                    .cpcv_backtests
                    .run(params.model_version_id, params.request, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::BacktestPathSet,
                        id: view.path_set_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::BiasTableFit(params) => {
                let outcome = self.bias_tables.fit(params, progress, cancel).await?;
                // Fail-closed fits succeed with no artifact (result_ref = None).
                Ok(JobOutcome {
                    result: outcome.artifact_id.map(|id| ResearchJobResultRef {
                        kind: ResearchJobResultKind::CalibrationArtifact,
                        id: id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::ModelCalibrationFit(params) => {
                let outcome = self
                    .model_calibration_fit
                    .fit(params, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: outcome.artifact_id.map(|id| ResearchJobResultRef {
                        kind: ResearchJobResultKind::CalibrationArtifact,
                        id: id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::FeatureParity(params) => {
                let view = self
                    .feature_parity
                    .execute(params, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::FeatureParityRun,
                        id: view.parity_run_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::TradePolicyFit(params) => {
                let view = self
                    .trade_policies
                    .fit(
                        &job.job_id,
                        &params.training_dataset_id,
                        params.request,
                        Arc::clone(&progress),
                        cancel.clone(),
                    )
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::TradePolicyArtifact,
                        id: view.artifact_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
            ResearchJobParams::TradePolicyValidation(params) => {
                self.trade_policies
                    .validate(
                        &params.validation_run_id,
                        &params.artifact_id,
                        params.actor_id,
                        params.reason,
                        progress.as_ref(),
                        &cancel,
                    )
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::TradePolicyValidationRun,
                        id: params.validation_run_id.as_uuid(),
                    }),
                    coverage: None,
                })
            }
        }
    }
}

impl AppContext {
    /// Register the durable research-job worker (`TaskId::ResearchJobWorker`).
    pub fn register_research_job_worker(
        &self,
        runner: &mut AppRunner,
        engine: ResearchJobEngine,
    ) -> QuantResult<()> {
        let runtime_config =
            Arc::clone(&self.infra.repos.runtime_config) as Arc<dyn PolicyRepository>;
        let bias_table_repo = Arc::clone(&self.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>;
        let config = self.config.quant.research_jobs;
        let evidence_scope = EvidenceScopeIdentity::from_config(
            &self.config.db.clickhouse,
            &self.config.research.artifact_store,
        )?;
        let readiness = Arc::new(ResearchReadinessEvidenceService::new(
            Arc::clone(&self.infra.repos.research_readiness)
                as Arc<dyn ResearchReadinessEvidenceRepository>,
            Arc::clone(&self.research.artifact_store),
            EvidenceAttestor::from_config(&self.config.research.evidence_attestation)?,
            &evidence_scope,
        )?);
        let backtest_port = Arc::new(CoreBacktestPort::from_research(
            &self.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_table_repo),
        ));
        let cpcv_backtest_port = Arc::new(CoreCpcvBacktestPort::from_research(
            &self.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_table_repo),
        ));
        let model_calibration_fit: Arc<dyn ModelCalibrationFitPort> =
            Arc::new(ModelCalibrationFitService::new(
                Arc::clone(&backtest_port),
                Arc::clone(&self.research.model_registry_repo),
                Arc::clone(&self.research.training_dataset_repo),
                Arc::clone(&bias_table_repo),
                Arc::clone(&runtime_config),
            ));
        let serving_evidence = Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
            &self.infra.ch,
        ))) as Arc<dyn ServingEvidenceRepository>;
        let dataset_port = Arc::new(CoreTrainingDatasetPort::from_research(
            &self.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_table_repo),
            config.max_spine_samples,
            config.plan_sample_slices,
            config.plan_sample_markets,
        )) as Arc<dyn TrainingDatasetPort>;
        let parity_replay = Arc::new(DurableFeatureParitySource::new(DurableFeatureParityDeps {
            parity: Arc::clone(&self.infra.repos.feature_parity)
                as Arc<dyn FeatureParityRepository>,
            model_runs: Arc::clone(&self.research.model_run_repo),
            model_registry: Arc::clone(&self.research.model_registry_repo),
            runtime_configs: Arc::clone(&runtime_config),
            selections: Arc::clone(&self.research.market_selection_repo),
            feature_vectors: Arc::clone(&self.research.feature_repo),
            factors: Arc::clone(&self.research.factor_repo),
            reports: Arc::clone(&self.infra.repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
            report_runs: Arc::clone(&self.infra.repos.report_run) as Arc<dyn ReportRunRepository>,
            serving_evidence,
            fact_read: Arc::clone(&self.research.quant_fact_read),
            catalog: Arc::clone(&self.research.catalog_ledger_repo),
            clob_market_info: Arc::clone(&self.research.clob_market_info_repo),
            linkages: Arc::clone(&self.research.market_linkage_repo),
            calibration_artifacts: Arc::clone(&bias_table_repo),
            runtime_factory: Arc::clone(&self.research.model_runtime_factory_builder),
        }));
        let executor = ResearchJobExecutor {
            datasets: Arc::clone(&dataset_port),
            training: Arc::new(CoreModelTrainingPort::from_research(
                &self.research,
                Arc::clone(&runtime_config),
            )),
            backtests: backtest_port as Arc<dyn BacktestPort>,
            cpcv_backtests: cpcv_backtest_port as Arc<dyn CpcvBacktestPort>,
            bias_tables: Arc::clone(&self.research.calibration_artifact_fit),
            model_calibration_fit,
            feature_parity: Arc::new(FeatureParityExecutor::new(
                Arc::clone(&self.infra.repos.feature_parity) as Arc<dyn FeatureParityRepository>,
                parity_replay,
                Arc::new(ChFactWriter::new(
                    Arc::clone(&self.infra.ch),
                    Arc::clone(&self.infra.ch_write_manager),
                    "quant_feature_parity_event",
                )) as Arc<dyn FactWriter<QuantFeatureParityEventRow>>,
                Arc::new(ReportFeatureParityIncidentResponse::new(
                    self.report_lifecycle(),
                    Arc::clone(&self.infra.repos.recommendation_report)
                        as Arc<dyn RecommendationReportRepository>,
                    Arc::clone(&self.governance.alerts),
                    Arc::clone(&self.infra.metrics),
                )),
                Arc::clone(&self.infra.metrics),
                ChronoDuration::minutes(10),
                Duration::from_secs(config.poll_secs.max(1)),
            )) as Arc<dyn FeatureParityExecutionPort>,
            trade_policies: Arc::new(TradePolicyService::new(TradePolicyServiceDeps {
                datasets: Arc::clone(&self.research.training_dataset_repo),
                dataset_builder: dataset_port,
                artifacts: Arc::clone(&self.research.artifact_store),
                policies: Arc::clone(&self.infra.repos.trade_policy)
                    as Arc<dyn TradePolicyRepository>,
                model_registry: Arc::clone(&self.research.model_registry_repo),
                runtime_configs: Arc::clone(&self.infra.repos.runtime_config)
                    as Arc<dyn PolicyRepository>,
                source_slices: Arc::clone(&self.research.source_slice_repo),
                readiness,
                model_runtime_factory_builder: Arc::clone(
                    &self.research.model_runtime_factory_builder,
                ),
            })),
        };
        runner.spawn(TaskId::ResearchJobWorker, move |token| async move {
            run_worker(engine, executor, config, token).await;
        });
        Ok(())
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
    // `run_one` observes `shutdown.cancelled` and unwinds at its next section
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
    owner: &WorkerId,
    kind: ResearchJobKind,
    terminal: Terminal,
) {
    let (status, result, error, coverage) = match terminal {
        Terminal::Succeeded(outcome) => (
            ResearchJobStatus::Succeeded,
            outcome.result,
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
        .finalize(job_id, owner, status, result, error, coverage)
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
