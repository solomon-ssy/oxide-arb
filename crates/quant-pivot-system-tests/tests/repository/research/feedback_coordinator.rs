//! Feedback coordinator contracts against real `PostgreSQL` cycle/job ledgers.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    app::research_job::ResearchJobEngine,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
    service::feedback_coordinator::{
        FeedbackCoordinator, FeedbackCoordinatorBudget, FeedbackCoordinatorConfig,
        FeedbackCoordinatorDeps, FeedbackShadowCancellationPort, FeedbackStagePort,
        FeedbackStagePreparation, FeedbackStageSuccess,
    },
};
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        api::BuildTrainingDatasetRequest,
        quant::{
            FeedbackCycleInfo, FeedbackStageEventInput, FeedbackStageJobIdentity,
            NewFeedbackStageEvent, NewResearchJob, ResearchJobFinalization, ResearchJobInfo,
        },
        runtime::CoreEventPublisher,
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::{
            DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackStage,
            FeedbackStageEventKind, ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus,
        },
    },
    types::{
        ArtifactUri, DecisionPolicySnapshotId, FeedbackCycleId, ModelSpecId, ResearchJobError,
        ResearchJobId, ResearchJobParams, RoleCode, SchemaVersion, TrainingDatasetId,
        TrainingSampleSources, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExecutionAttemptOutcomeRepository, PgFeedbackCycleRepository,
        PgFeedbackSchedulerRepository, PgRecommendationExecutionRollupRepository,
        PgResearchJobRepository, PgResolutionObservationRepository,
    },
    traits::{
        ExecutionAttemptOutcomeRepository, FeedbackCycleGeneration, FeedbackCycleLeaseGuard,
        FeedbackCycleRepository, FeedbackSchedulerRepository,
        RecommendationExecutionRollupRepository, ResearchJobRepository,
        ResolutionObservationRepository,
    },
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait};
use tokio::{
    sync::Notify,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::feedback_boot_schema::{FeedbackSchemaFixture, content_hash, prepare_fixture};

struct CoordinatorStagePort {
    defer_once: Mutex<Option<DateTime<Utc>>>,
    prepare_calls: AtomicUsize,
    cancellation_calls: AtomicUsize,
    cancellation_failures: AtomicUsize,
    block_cancellation_retry: AtomicBool,
    cancellation_retry: Notify,
}

impl CoordinatorStagePort {
    fn new() -> Self {
        Self {
            defer_once: Mutex::new(None),
            prepare_calls: AtomicUsize::new(0),
            cancellation_calls: AtomicUsize::new(0),
            cancellation_failures: AtomicUsize::new(0),
            block_cancellation_retry: AtomicBool::new(false),
            cancellation_retry: Notify::new(),
        }
    }

    fn defer_once(&self, resume_after: DateTime<Utc>) {
        *self.defer_once.lock().expect("lock stage deferral") = Some(resume_after);
    }

    fn reset_calls(&self) {
        self.prepare_calls.store(0, Ordering::SeqCst);
    }

    fn prepare_calls(&self) -> usize {
        self.prepare_calls.load(Ordering::SeqCst)
    }

    fn fail_cancellation_once(&self) {
        self.cancellation_failures.store(1, Ordering::SeqCst);
        self.block_cancellation_retry.store(true, Ordering::SeqCst);
    }

    fn release_cancellation_retry(&self) {
        self.block_cancellation_retry.store(false, Ordering::SeqCst);
        self.cancellation_retry.notify_one();
    }

    fn job(
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        let window_end = cycle.label_cutoff - Duration::hours(1);
        Ok(NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::DatasetBuild,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: ResearchJobParams::DatasetBuild(BuildTrainingDatasetRequest {
                model_spec_id: ModelSpecId::from_v7(),
                profile_ref: cycle.profile_ref.clone(),
                purpose: DatasetPurpose::Training,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start: window_end - Duration::hours(1),
                window_end,
                pit_cutoff: cycle.label_cutoff,
                sample_interval_secs: 60,
                horizons_secs: vec![3_600],
                knowledge_lag_secs: 1,
                feature_schema_version: SchemaVersion::FIRST,
                sample_sources: TrainingSampleSources::default(),
                reason: format!("feedback-coordinator-{}", identity.feedback_stage()),
                training_dataset_id: Some(TrainingDatasetId::from_v7()),
            }),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(identity)?)
    }
}

#[async_trait]
impl FeedbackStagePort for CoordinatorStagePort {
    async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<FeedbackStagePreparation> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        if cycle.status != FeedbackCycleStatus::Running
            || lease.feedback_cycle_id != cycle.feedback_cycle_id
            || lease.expected_generation != cycle.generation
            || cycle.lease_owner != Some(lease.worker_id)
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coordinator did not pass the exact live cycle lease".to_owned(),
            }
            .into());
        }
        let deferred = self.defer_once.lock().expect("lock stage deferral").take();
        if let Some(resume_after) = deferred {
            return Ok(FeedbackStagePreparation::Deferred {
                resume_after,
                reason_code: "test_stage_pending",
            });
        }
        Ok(FeedbackStagePreparation::Ready(Box::new(Self::job(
            cycle, identity,
        )?)))
    }

    async fn succeeded(
        &self,
        _cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let stage = job
            .feedback_stage
            .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                detail: "test stage job lost feedback stage".to_owned(),
            })?;
        let uri = ArtifactUri::parse(format!(
            "s3://feedback-coordinator/{stage}/{}.json",
            job.job_id
        ))?;
        if stage == FeedbackStage::Decision {
            Ok(FeedbackStageSuccess::try_complete(
                uri,
                content_hash('9'),
                FeedbackDecision::NoAction,
                "test_no_action".to_owned(),
            )?)
        } else {
            Ok(FeedbackStageSuccess::advance(uri, content_hash('9')))
        }
    }
}

#[async_trait]
impl FeedbackShadowCancellationPort for CoordinatorStagePort {
    async fn release_cycle(
        &self,
        _cycle: &FeedbackCycleInfo,
        _reason_code: &str,
    ) -> QuantResult<()> {
        self.cancellation_calls.fetch_add(1, Ordering::SeqCst);
        if self.cancellation_failures.swap(0, Ordering::SeqCst) > 0 {
            return Err(FeedbackError::ShadowBindingConflict {
                detail: "injected transient shadow cancellation failure".to_owned(),
            }
            .into());
        }
        while self.block_cancellation_retry.load(Ordering::SeqCst) {
            self.cancellation_retry.notified().await;
        }
        Ok(())
    }
}

struct CoordinatorHarness {
    cycles: Arc<PgFeedbackCycleRepository>,
    scheduler: Arc<PgFeedbackSchedulerRepository>,
    resolutions: Arc<PgResolutionObservationRepository>,
    attempts: Arc<PgExecutionAttemptOutcomeRepository>,
    rollups: Arc<PgRecommendationExecutionRollupRepository>,
    jobs: Arc<PgResearchJobRepository>,
    engine: ResearchJobEngine,
    stages: Arc<CoordinatorStagePort>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    recordings: Arc<Mutex<Vec<Alert>>>,
}

impl CoordinatorHarness {
    fn new(db: DatabaseConnection) -> Self {
        let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
        let scheduler = Arc::new(PgFeedbackSchedulerRepository::new(db.clone()));
        let resolutions = Arc::new(PgResolutionObservationRepository::new(db.clone()));
        let attempts = Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone()));
        let rollups = Arc::new(PgRecommendationExecutionRollupRepository::new(db.clone()));
        let jobs = Arc::new(PgResearchJobRepository::new(db));
        let (events, _receiver) = CoreEventPublisher::bounded(64);
        let engine =
            ResearchJobEngine::new(Arc::clone(&jobs) as Arc<dyn ResearchJobRepository>, events);
        let recordings = Arc::new(Mutex::new(Vec::new()));
        Self {
            cycles,
            scheduler,
            resolutions,
            attempts,
            rollups,
            jobs,
            engine,
            stages: Arc::new(CoordinatorStagePort::new()),
            metrics: Arc::new(MetricsHub::new()),
            alerts: Arc::new(AlertDispatcher::with_recordings(Arc::clone(&recordings))),
            recordings,
        }
    }

    fn start(&self, config: FeedbackCoordinatorConfig) -> (CancellationToken, JoinHandle<()>) {
        let coordinator = FeedbackCoordinator::new(FeedbackCoordinatorDeps {
            cycles: Arc::clone(&self.cycles) as Arc<dyn FeedbackCycleRepository>,
            scheduler: Arc::clone(&self.scheduler) as Arc<dyn FeedbackSchedulerRepository>,
            resolutions: Arc::clone(&self.resolutions) as Arc<dyn ResolutionObservationRepository>,
            attempts: Arc::clone(&self.attempts) as Arc<dyn ExecutionAttemptOutcomeRepository>,
            rollups: Arc::clone(&self.rollups) as Arc<dyn RecommendationExecutionRollupRepository>,
            jobs: self.engine.clone(),
            stages: Arc::clone(&self.stages) as Arc<dyn FeedbackStagePort>,
            shadow_cancellation: Arc::clone(&self.stages)
                as Arc<dyn FeedbackShadowCancellationPort>,
            metrics: Arc::clone(&self.metrics),
            alerts: Arc::clone(&self.alerts),
            config,
        });
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            coordinator.run(task_shutdown).await;
        });
        (shutdown, task)
    }
}

fn long_poll_config() -> FeedbackCoordinatorConfig {
    FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
        poll_interval: StdDuration::from_secs(30),
        lease_heartbeat: StdDuration::from_secs(10),
        lease_ttl: StdDuration::from_secs(30),
        max_inflight: 2,
        stuck_after: StdDuration::from_secs(31),
        alert_timeout: StdDuration::from_secs(1),
        alert_dedupe_secs: 60,
        shutdown_drain: StdDuration::from_secs(2),
    })
    .expect("valid long-poll coordinator config")
}

fn recovery_config() -> FeedbackCoordinatorConfig {
    FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
        poll_interval: StdDuration::from_secs(30),
        lease_heartbeat: StdDuration::from_secs(1),
        lease_ttl: StdDuration::from_secs(3),
        max_inflight: 2,
        stuck_after: StdDuration::from_secs(4),
        alert_timeout: StdDuration::from_secs(1),
        alert_dedupe_secs: 60,
        shutdown_drain: StdDuration::from_secs(2),
    })
    .expect("valid recovery coordinator config")
}

fn observability_config() -> FeedbackCoordinatorConfig {
    FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
        poll_interval: StdDuration::from_secs(1),
        lease_heartbeat: StdDuration::from_secs(1),
        lease_ttl: StdDuration::from_secs(3),
        max_inflight: 1,
        stuck_after: StdDuration::from_secs(4),
        alert_timeout: StdDuration::from_secs(1),
        alert_dedupe_secs: 60,
        shutdown_drain: StdDuration::from_secs(2),
    })
    .expect("valid observability coordinator config")
}

async fn record_cycle(harness: &CoordinatorHarness, fixture: &FeedbackSchemaFixture) {
    harness
        .cycles
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("record feedback cycle");
}

async fn wait_job(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
    stage: FeedbackStage,
) -> ResearchJobInfo {
    let job_id = FeedbackStageJobIdentity::try_root(cycle_id, stage)
        .expect("freeze expected stage root")
        .job_id();
    let result = timeout(StdDuration::from_secs(3), async {
        loop {
            if let Some(job) = harness
                .jobs
                .find_by_id(&job_id)
                .await
                .expect("load coordinator job")
            {
                break job;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await;
    if let Ok(job) = result {
        job
    } else {
        let cycle = harness
            .cycles
            .find_cycle(&cycle_id)
            .await
            .expect("load enqueue-timeout cycle");
        let events = harness
            .cycles
            .list_stage_events(&cycle_id)
            .await
            .expect("load enqueue-timeout timeline");
        let event_summary = events
            .iter()
            .map(|event| {
                (
                    event.event_sequence,
                    event.stage,
                    event.event_kind,
                    event.reason_code.clone(),
                )
            })
            .collect::<Vec<_>>();
        let coverage_id = FeedbackStageJobIdentity::try_root(cycle_id, FeedbackStage::TruthFreeze)
            .expect("freeze diagnostic truth-freeze root")
            .job_id();
        let coverage = harness
            .jobs
            .find_by_id(&coverage_id)
            .await
            .expect("load enqueue-timeout coverage root");
        panic!(
            "coordinator did not enqueue {stage}: cycle={cycle:?}, events={event_summary:?}, coverage_job={coverage:?}"
        );
    }
}

async fn wait_event(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
    kind: FeedbackStageEventKind,
) {
    timeout(StdDuration::from_secs(3), async {
        loop {
            let events = harness
                .cycles
                .list_stage_events(&cycle_id)
                .await
                .expect("load coordinator timeline");
            if events.iter().any(|event| event.event_kind == kind) {
                break;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("coordinator stage event must appear before timeout");
}

async fn wait_job_link(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
    stage: FeedbackStage,
    job_id: ResearchJobId,
) {
    timeout(StdDuration::from_secs(3), async {
        loop {
            let events = harness
                .cycles
                .list_stage_events(&cycle_id)
                .await
                .expect("load coordinator timeline");
            if events.iter().any(|event| {
                event.stage == stage
                    && event.event_kind == FeedbackStageEventKind::JobLinked
                    && event.research_job_id == Some(job_id)
            }) {
                break;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("exact coordinator job link must commit before worker lease");
}

async fn wait_event_count(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
    kind: FeedbackStageEventKind,
    expected: usize,
) {
    timeout(StdDuration::from_secs(3), async {
        loop {
            let events = harness
                .cycles
                .list_stage_events(&cycle_id)
                .await
                .expect("load coordinator timeline");
            if events
                .iter()
                .filter(|event| event.event_kind == kind)
                .count()
                == expected
            {
                break;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("coordinator event count must converge");
}

async fn wait_status(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
    status: FeedbackCycleStatus,
) {
    let result = timeout(StdDuration::from_secs(3), async {
        loop {
            let cycle = harness
                .cycles
                .find_cycle(&cycle_id)
                .await
                .expect("load feedback cycle")
                .expect("feedback cycle exists");
            if cycle.status == status {
                break;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        let cycle = harness
            .cycles
            .find_cycle(&cycle_id)
            .await
            .expect("load timed-out feedback cycle")
            .expect("timed-out feedback cycle exists");
        let events = harness
            .cycles
            .list_stage_events(&cycle_id)
            .await
            .expect("load timed-out feedback timeline");
        let event_summary = events
            .iter()
            .map(|event| {
                (
                    event.event_sequence,
                    event.stage,
                    event.event_kind,
                    event.reason_code.clone(),
                )
            })
            .collect::<Vec<_>>();
        let root_id = FeedbackStageJobIdentity::try_root(cycle_id, FeedbackStage::TruthFreeze)
            .expect("freeze diagnostic truth-freeze root")
            .job_id();
        let root = harness
            .jobs
            .find_by_id(&root_id)
            .await
            .expect("load diagnostic coverage root");
        panic!(
            "feedback cycle did not reach {status}: cycle={cycle:?}, events={event_summary:?}, coverage_job={root:?}"
        );
    }
}

async fn wait_deferred(
    harness: &CoordinatorHarness,
    cycle_id: FeedbackCycleId,
) -> FeedbackCycleInfo {
    timeout(StdDuration::from_secs(3), async {
        loop {
            let cycle = harness
                .cycles
                .find_cycle(&cycle_id)
                .await
                .expect("load deferred feedback cycle")
                .expect("deferred feedback cycle exists");
            if cycle.stage_resume_after.is_some() {
                break cycle;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("feedback stage must persist its resume boundary")
}

async fn request_cancel(
    harness: &CoordinatorHarness,
    fixture: &FeedbackSchemaFixture,
    stage: FeedbackStage,
) {
    let job_id = FeedbackStageJobIdentity::try_root(fixture.cycle_id, stage)
        .expect("freeze cancellation stage root")
        .job_id();
    wait_job_link(harness, fixture.cycle_id, stage, job_id).await;
    let cycle = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load cycle before cancellation")
        .expect("cycle exists before cancellation");
    let events = harness
        .cycles
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("load timeline before cancellation");
    let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: fixture.cycle_id,
        event_sequence: i64::try_from(events.len() + 1).expect("test sequence fits bigint"),
        stage,
        event_kind: FeedbackStageEventKind::CancellationRequested,
        trigger_family: None,
        research_job_id: None,
        actor: Some("operator".to_owned()),
        reason_code: Some("test_cancelled".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: harness
            .cycles
            .database_time()
            .await
            .expect("read cancellation database time"),
    })
    .expect("seal coordinator cancellation");
    harness
        .cycles
        .request_cancel(FeedbackCycleGeneration::from(&cycle), event)
        .await
        .expect("request feedback cancellation");
    harness.engine.feedback_wake().wake();
}

async fn stop_task(shutdown: CancellationToken, task: JoinHandle<()>) {
    shutdown.cancel();
    timeout(StdDuration::from_secs(4), task)
        .await
        .expect("coordinator stops after cancellation")
        .expect("coordinator task joins");
}

async fn lease_job(
    harness: &CoordinatorHarness,
    expected: &ResearchJobInfo,
    worker: WorkerId,
) -> ResearchJobInfo {
    let cycle_id = expected
        .feedback_cycle_id
        .expect("feedback stage has cycle id");
    let stage = expected
        .feedback_stage
        .expect("feedback stage has stage identity");
    wait_job_link(harness, cycle_id, stage, expected.job_id).await;
    let leased = harness
        .jobs
        .lease_next(
            &[ResearchJobKind::DatasetBuild],
            &worker,
            Utc::now() + Duration::seconds(30),
        )
        .await
        .expect("lease feedback stage job");
    let Some(leased) = leased else {
        let current = harness
            .jobs
            .find_by_id(&expected.job_id)
            .await
            .expect("reload unleased feedback stage");
        let events = harness
            .cycles
            .list_stage_events(&cycle_id)
            .await
            .expect("load unleased stage timeline");
        let event_summary = events
            .iter()
            .map(|event| {
                (
                    event.event_sequence,
                    event.stage,
                    event.event_kind,
                    event.research_job_id,
                )
            })
            .collect::<Vec<_>>();
        panic!(
            "feedback stage lease returned none: expected={expected:?}, current={current:?}, events={event_summary:?}"
        );
    };
    assert_eq!(leased.job_id, expected.job_id);
    leased
}

async fn succeed_job(
    harness: &CoordinatorHarness,
    expected: &ResearchJobInfo,
    worker: WorkerId,
) -> ResearchJobInfo {
    let leased = lease_job(harness, expected, worker).await;
    harness
        .engine
        .publish(&leased, Some("start".to_owned()), None);
    let succeeded = harness
        .jobs
        .finalize(
            &expected.job_id,
            &worker,
            ResearchJobFinalization::succeeded(None, None, None),
        )
        .await
        .expect("finalize feedback stage job");
    harness
        .engine
        .publish(&succeeded, Some("finalize".to_owned()), Some(1.0));
    succeeded
}

pub async fn enqueue_gap_recovers() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;

    let cycle = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load queued cycle")
        .expect("queued cycle exists");
    let identity = FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::TruthFreeze)
        .expect("freeze truth-freeze root");
    let job = CoordinatorStagePort::job(&cycle, identity).expect("prepare pre-crash job");
    harness
        .jobs
        .enqueue(job)
        .await
        .expect("commit job before simulated crash");
    harness.stages.reset_calls();

    let (shutdown, task) = harness.start(long_poll_config());
    wait_event(
        &harness,
        fixture.cycle_id,
        FeedbackStageEventKind::JobLinked,
    )
    .await;
    assert_eq!(
        harness.stages.prepare_calls(),
        0,
        "durable pre-enqueued root must be linked without rebuilding payload"
    );

    request_cancel(&harness, &fixture, FeedbackStage::TruthFreeze).await;
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    let job = harness
        .jobs
        .find_by_id(&identity.job_id())
        .await
        .expect("load cancelled root")
        .expect("cancelled root exists");
    assert_eq!(job.status, ResearchJobStatus::Cancelled);
    stop_task(shutdown, task).await;
}

pub async fn terminal_wake_advances() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let (shutdown, task) = harness.start(long_poll_config());
    let coverage = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;

    succeed_job(&harness, &coverage, WorkerId::from_v7()).await;

    wait_job(&harness, fixture.cycle_id, FeedbackStage::Coverage).await;
    wait_event(
        &harness,
        fixture.cycle_id,
        FeedbackStageEventKind::Succeeded,
    )
    .await;

    request_cancel(&harness, &fixture, FeedbackStage::Coverage).await;
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    stop_task(shutdown, task).await;
}

pub async fn cancellation_release_retries() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    harness.stages.fail_cancellation_once();
    let (shutdown, task) = harness.start(recovery_config());
    wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;

    request_cancel(&harness, &fixture, FeedbackStage::TruthFreeze).await;
    timeout(StdDuration::from_secs(3), async {
        loop {
            if harness.stages.cancellation_calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("coordinator attempts shadow release before cancellation finalization");
    let pending = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load cycle after injected release failure")
        .expect("cycle survives injected release failure");
    assert_eq!(pending.status, FeedbackCycleStatus::Running);
    assert!(pending.completed_at.is_none());

    harness.stages.release_cancellation_retry();
    harness.engine.feedback_wake().wake();
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    assert!(harness.stages.cancellation_calls.load(Ordering::SeqCst) >= 2);
    stop_task(shutdown, task).await;
}

pub async fn running_cancel_waits() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let (shutdown, task) = harness.start(long_poll_config());
    let coverage = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    let worker = WorkerId::from_v7();
    let running = lease_job(&harness, &coverage, worker).await;
    harness
        .engine
        .publish(&running, Some("start".to_owned()), None);
    wait_event(&harness, fixture.cycle_id, FeedbackStageEventKind::Started).await;

    request_cancel(&harness, &fixture, FeedbackStage::TruthFreeze).await;
    sleep(StdDuration::from_millis(100)).await;
    let still_running = harness
        .jobs
        .find_by_id(&coverage.job_id)
        .await
        .expect("load running stage after cycle cancellation")
        .expect("running stage remains present");
    assert_eq!(
        still_running.status,
        ResearchJobStatus::Running,
        "cycle cancellation must not interrupt an in-flight stage"
    );

    let succeeded = harness
        .jobs
        .finalize(
            &coverage.job_id,
            &worker,
            ResearchJobFinalization::succeeded(None, None, None),
        )
        .await
        .expect("finish in-flight stage at boundary");
    harness
        .engine
        .publish(&succeeded, Some("finalize".to_owned()), Some(1.0));
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    let drift_id = FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Coverage)
        .expect("freeze absent coverage root")
        .job_id();
    assert!(
        harness
            .jobs
            .find_by_id(&drift_id)
            .await
            .expect("check absent drift root")
            .is_none(),
        "cancellation at the completed boundary must prevent downstream enqueue"
    );
    stop_task(shutdown, task).await;
}

pub async fn job_orphan_restarts() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let (shutdown, task) = harness.start(long_poll_config());
    let coverage = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    let first_worker = WorkerId::from_v7();
    let first = lease_job(&harness, &coverage, first_worker).await;
    harness
        .engine
        .publish(&first, Some("start".to_owned()), None);
    wait_event_count(
        &harness,
        fixture.cycle_id,
        FeedbackStageEventKind::Started,
        1,
    )
    .await;

    let reclaimed = harness
        .jobs
        .requeue_inflight(&first_worker)
        .await
        .expect("requeue interrupted stage");
    assert_eq!(reclaimed.requeued, 1);
    let second_worker = WorkerId::from_v7();
    let second = lease_job(&harness, &coverage, second_worker).await;
    assert_eq!(second.job_id, coverage.job_id);
    assert_eq!(second.recovery_attempt, 1);
    harness
        .engine
        .publish(&second, Some("restart".to_owned()), None);
    wait_event_count(
        &harness,
        fixture.cycle_id,
        FeedbackStageEventKind::Started,
        2,
    )
    .await;
    let succeeded = harness
        .jobs
        .finalize(
            &coverage.job_id,
            &second_worker,
            ResearchJobFinalization::succeeded(None, None, None),
        )
        .await
        .expect("finalize restarted stage");
    harness
        .engine
        .publish(&succeeded, Some("finalize".to_owned()), Some(1.0));
    wait_job(&harness, fixture.cycle_id, FeedbackStage::Coverage).await;

    request_cancel(&harness, &fixture, FeedbackStage::Coverage).await;
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    stop_task(shutdown, task).await;
}

pub async fn failed_job_stops() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let (shutdown, task) = harness.start(long_poll_config());
    let coverage = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    let worker = WorkerId::from_v7();
    let running = lease_job(&harness, &coverage, worker).await;
    harness
        .engine
        .publish(&running, Some("start".to_owned()), None);
    let failed = harness
        .jobs
        .finalize(
            &coverage.job_id,
            &worker,
            ResearchJobFinalization::failed(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionFailed,
                "expected coordinator test failure",
            )),
        )
        .await
        .expect("finalize failing stage");
    harness
        .engine
        .publish(&failed, Some("finalize".to_owned()), None);
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Failed).await;
    let events = harness
        .cycles
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("load failed cycle timeline");
    assert!(events.iter().any(|event| {
        event.event_kind == FeedbackStageEventKind::Failed
            && event.reason_code.as_deref() == Some("research_job.execution_failed")
    }));
    stop_task(shutdown, task).await;
}

pub async fn dag_completes() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let (shutdown, task) = harness.start(long_poll_config());
    let stages = [
        FeedbackStage::TruthFreeze,
        FeedbackStage::Coverage,
        FeedbackStage::Attribution,
        FeedbackStage::Drift,
        FeedbackStage::RecipePlan,
        FeedbackStage::DatasetSeal,
        FeedbackStage::Training,
        FeedbackStage::Calibration,
        FeedbackStage::Cpcv,
        FeedbackStage::Validation,
        FeedbackStage::Comparison,
        FeedbackStage::ShadowBind,
        FeedbackStage::Shadow,
        FeedbackStage::Decision,
    ];
    for stage in stages {
        let job = wait_job(&harness, fixture.cycle_id, stage).await;
        succeed_job(&harness, &job, WorkerId::from_v7()).await;
    }
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Succeeded).await;
    let cycle = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load completed cycle")
        .expect("completed cycle exists");
    assert_eq!(cycle.decision, Some(FeedbackDecision::NoAction));
    let events = harness
        .cycles
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("load completed DAG timeline");
    for kind in [
        FeedbackStageEventKind::JobLinked,
        FeedbackStageEventKind::Started,
        FeedbackStageEventKind::Succeeded,
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_kind == kind)
                .count(),
            stages.len()
        );
    }
    stop_task(shutdown, task).await;
}

pub async fn empty_recovery_starts() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let first = CoordinatorHarness::new(db.clone());
    record_cycle(&first, &fixture).await;
    first
        .cycles
        .claim_cycle(WorkerId::from_v7(), 1)
        .await
        .expect("claim cycle before pre-enqueue crash")
        .expect("pre-enqueue cycle claim exists");

    sleep(StdDuration::from_millis(1_100)).await;
    let recovered = CoordinatorHarness::new(db);
    let (shutdown, task) = recovered.start(long_poll_config());
    let root = wait_job(&recovered, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    wait_job_link(
        &recovered,
        fixture.cycle_id,
        FeedbackStage::TruthFreeze,
        root.job_id,
    )
    .await;
    sleep(StdDuration::from_millis(100)).await;
    let events = recovered
        .cycles
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("load pre-enqueue recovery timeline");
    assert!(
        events
            .iter()
            .all(|event| event.event_kind != FeedbackStageEventKind::LeaseRecovered),
        "lease recovery must not attribute a pre-enqueue crash to a job created after takeover"
    );

    request_cancel(&recovered, &fixture, FeedbackStage::TruthFreeze).await;
    wait_status(&recovered, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    stop_task(shutdown, task).await;
}

pub async fn lease_recovery_resumes() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let first = CoordinatorHarness::new(db.clone());
    record_cycle(&first, &fixture).await;
    let (shutdown, task) = first.start(recovery_config());
    let root = wait_job(&first, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    wait_event(&first, fixture.cycle_id, FeedbackStageEventKind::JobLinked).await;
    stop_task(shutdown, task).await;

    let recovered = CoordinatorHarness::new(db);
    let (shutdown, task) = recovered.start(long_poll_config());
    wait_event(
        &recovered,
        fixture.cycle_id,
        FeedbackStageEventKind::LeaseRecovered,
    )
    .await;
    let same_root = wait_job(&recovered, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    assert_eq!(same_root.job_id, root.job_id);
    assert_eq!(
        recovered.stages.prepare_calls(),
        0,
        "cycle lease recovery must reuse the durable stage root"
    );

    request_cancel(&recovered, &fixture, FeedbackStage::TruthFreeze).await;
    wait_status(&recovered, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    stop_task(shutdown, task).await;
}

pub async fn stage_deferral_resumes() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let resume_after = harness
        .cycles
        .database_time()
        .await
        .expect("read stage deferral database time")
        + Duration::milliseconds(500);
    harness.stages.defer_once(resume_after);

    let (shutdown, task) = harness.start(observability_config());
    let deferred = wait_deferred(&harness, fixture.cycle_id).await;
    assert_eq!(deferred.stage_resume_after, Some(resume_after));
    assert!(deferred.lease_owner.is_none());
    let root_id = FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::TruthFreeze)
        .expect("freeze deferred root identity")
        .job_id();
    assert!(
        harness
            .jobs
            .find_by_id(&root_id)
            .await
            .expect("check absent deferred root")
            .is_none(),
        "deferred stage must not enqueue before its database-time boundary"
    );

    let root = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    assert_eq!(root.job_id, root_id);
    assert_eq!(harness.stages.prepare_calls(), 2);
    let resumed = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load resumed cycle")
        .expect("resumed cycle exists");
    assert!(resumed.stage_resume_after.is_none());
    let events = harness
        .cycles
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("load resumed cycle events");
    assert!(
        events
            .iter()
            .all(|event| event.event_kind != FeedbackStageEventKind::LeaseRecovered),
        "planned stage resumption must not be misreported as lease recovery"
    );

    request_cancel(&harness, &fixture, FeedbackStage::TruthFreeze).await;
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    stop_task(shutdown, task).await;
}

pub async fn deferred_cancel_finishes() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    let resume_after = harness
        .cycles
        .database_time()
        .await
        .expect("read cancellation deferral time")
        + Duration::seconds(30);
    harness.stages.defer_once(resume_after);

    let (shutdown, task) = harness.start(observability_config());
    let deferred = wait_deferred(&harness, fixture.cycle_id).await;
    let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: fixture.cycle_id,
        event_sequence: 2,
        stage: FeedbackStage::TruthFreeze,
        event_kind: FeedbackStageEventKind::CancellationRequested,
        trigger_family: None,
        research_job_id: None,
        actor: Some("operator".to_owned()),
        reason_code: Some("test_cancelled".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: harness
            .cycles
            .database_time()
            .await
            .expect("read deferred cancellation time"),
    })
    .expect("seal deferred cancellation");
    harness
        .cycles
        .request_cancel(FeedbackCycleGeneration::from(&deferred), event)
        .await
        .expect("cancel deferred cycle");
    let cancelled = harness
        .cycles
        .find_cycle(&fixture.cycle_id)
        .await
        .expect("load cancelled deferred cycle")
        .expect("cancelled deferred cycle exists");
    assert_eq!(cancelled.status, FeedbackCycleStatus::Cancelled);
    assert!(cancelled.stage_resume_after.is_none());
    assert!(cancelled.completed_at.is_some());
    assert!(
        harness
            .jobs
            .find_by_id(
                &FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::TruthFreeze,)
                    .expect("freeze cancelled deferred root")
                    .job_id(),
            )
            .await
            .expect("check cancelled deferred root")
            .is_none()
    );
    stop_task(shutdown, task).await;
}

async fn inject_timeline_corruption(db: &DatabaseConnection, cycle_id: FeedbackCycleId) {
    let transaction = db.begin().await.expect("begin corruption injection");
    transaction
        .execute_raw(Statement::from_string(
            DbBackend::Postgres,
            "SET LOCAL session_replication_role = 'replica'",
        ))
        .await
        .expect("disable WORM trigger inside disposable transaction");
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_feedback_stage_event
             SET actor = 'injected-corruption'
             WHERE feedback_cycle_id = $1 AND event_sequence = 1",
            [cycle_id.as_uuid().into()],
        ))
        .await
        .expect("inject persisted timeline hash corruption");
    transaction
        .commit()
        .await
        .expect("commit corruption injection");
}

async fn assert_quarantine(
    harness: &CoordinatorHarness,
    db: &DatabaseConnection,
    cycle_id: FeedbackCycleId,
) {
    wait_status(harness, cycle_id, FeedbackCycleStatus::Quarantined).await;
    let quarantined = harness
        .cycles
        .find_cycle(&cycle_id)
        .await
        .expect("load quarantined cycle")
        .expect("quarantined cycle exists");
    assert!(quarantined.lease_owner.is_none());
    assert!(quarantined.lease_expires_at.is_none());
    assert_eq!(
        quarantined.terminal_reason_code.as_deref(),
        Some("invalid_coordinator_state")
    );
    let fault = harness
        .cycles
        .find_coordinator_fault(&cycle_id)
        .await
        .expect("load coordinator fault")
        .expect("coordinator fault exists");
    assert_eq!(fault.fault_code, "invalid_coordinator_state");
    assert_eq!(fault.last_event_sequence, Some(1));
    assert!(fault.detail.contains("integrity validation"));
    assert!(
        harness
            .recordings
            .lock()
            .expect("lock coordinator alerts")
            .iter()
            .any(|alert| alert.title.contains("quarantined")),
        "coordinator corruption must dispatch an operations alert"
    );

    let generation = quarantined.generation;
    sleep(StdDuration::from_millis(1_250)).await;
    let stable = harness
        .cycles
        .find_cycle(&cycle_id)
        .await
        .expect("reload quarantined cycle")
        .expect("quarantined cycle remains durable");
    assert_eq!(stable.generation, generation);
    assert!(stable.lease_owner.is_none());
    assert!(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_feedback_coordinator_fault
             SET detail = 'tampered'
             WHERE feedback_cycle_id = $1",
            [cycle_id.as_uuid().into()],
        ))
        .await
        .is_err(),
        "coordinator fault evidence must be WORM"
    );
}

async fn run_replacement_cycle(harness: &CoordinatorHarness, fixture: &FeedbackSchemaFixture) {
    harness
        .cycles
        .record_trigger(
            fixture.second_cycle.clone(),
            fixture.stage_event(fixture.second_cycle_id, "operator-recovery"),
        )
        .await
        .expect("create replacement cycle after quarantine");
    harness.engine.feedback_wake().wake();
    let replacement_job =
        wait_job(harness, fixture.second_cycle_id, FeedbackStage::TruthFreeze).await;
    wait_job_link(
        harness,
        fixture.second_cycle_id,
        FeedbackStage::TruthFreeze,
        replacement_job.job_id,
    )
    .await;
    let replacement = harness
        .cycles
        .find_cycle(&fixture.second_cycle_id)
        .await
        .expect("load replacement cycle")
        .expect("replacement cycle exists");
    let replacement_job_id =
        FeedbackStageJobIdentity::try_root(fixture.second_cycle_id, FeedbackStage::TruthFreeze)
            .expect("freeze replacement job identity")
            .job_id();
    let replacement_events = harness
        .cycles
        .list_stage_events(&fixture.second_cycle_id)
        .await
        .expect("load replacement timeline");
    let cancellation = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: fixture.second_cycle_id,
        event_sequence: i64::try_from(replacement_events.len() + 1)
            .expect("replacement sequence fits bigint"),
        stage: FeedbackStage::TruthFreeze,
        event_kind: FeedbackStageEventKind::CancellationRequested,
        trigger_family: None,
        research_job_id: None,
        actor: Some("operator".to_owned()),
        reason_code: Some("replacement_test_complete".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: harness
            .cycles
            .database_time()
            .await
            .expect("read replacement cancellation time"),
    })
    .expect("seal replacement cancellation");
    harness
        .cycles
        .request_cancel(FeedbackCycleGeneration::from(&replacement), cancellation)
        .await
        .expect("cancel replacement cycle");
    assert!(
        harness
            .jobs
            .find_by_id(&replacement_job_id)
            .await
            .expect("load replacement job")
            .is_some()
    );
    harness.engine.feedback_wake().wake();
    wait_status(
        harness,
        fixture.second_cycle_id,
        FeedbackCycleStatus::Cancelled,
    )
    .await;
}

pub async fn corrupt_timeline_quarantines() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db.clone());
    record_cycle(&harness, &fixture).await;
    inject_timeline_corruption(&db, fixture.cycle_id).await;

    let (shutdown, task) = harness.start(recovery_config());
    assert_quarantine(&harness, &db, fixture.cycle_id).await;
    run_replacement_cycle(&harness, &fixture).await;
    stop_task(shutdown, task).await;
}

pub async fn bounded_metrics_alerts() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let harness = CoordinatorHarness::new(db);
    record_cycle(&harness, &fixture).await;
    harness
        .cycles
        .record_trigger(
            fixture.second_cycle.clone(),
            fixture.stage_event(fixture.second_cycle_id, "operator"),
        )
        .await
        .expect("record second feedback cycle");

    let (shutdown, task) = harness.start(observability_config());
    let coverage = wait_job(&harness, fixture.cycle_id, FeedbackStage::TruthFreeze).await;
    sleep(StdDuration::from_millis(300)).await;
    let second = harness
        .cycles
        .find_cycle(&fixture.second_cycle_id)
        .await
        .expect("load capacity-blocked cycle")
        .expect("second cycle exists");
    assert_eq!(
        second.status,
        FeedbackCycleStatus::Queued,
        "one-slot coordinator cannot claim a second cycle"
    );
    let text = timeout(StdDuration::from_secs(3), async {
        loop {
            let (_, text) = harness
                .metrics
                .gather_prometheus_text()
                .expect("gather bounded coordinator metrics");
            let text = String::from_utf8(text).expect("metrics are UTF-8");
            if text.contains("quant_feedback_cycle_active 1")
                && text.contains("quant_feedback_cycle_queued 1")
                && text.contains("quant_feedback_outbox_pending 3")
            {
                break text;
            }
            sleep(StdDuration::from_millis(50)).await;
        }
    })
    .await
    .expect("bounded coordinator metrics must converge");
    assert!(text.contains("quant_feedback_cycle_active 1"));
    assert!(text.contains("quant_feedback_cycle_queued 1"));
    assert!(text.contains("quant_feedback_outbox_pending 3"));

    succeed_job(&harness, &coverage, WorkerId::from_v7()).await;
    wait_job(&harness, fixture.cycle_id, FeedbackStage::Coverage).await;
    timeout(StdDuration::from_secs(5), async {
        loop {
            let alerted = harness
                .recordings
                .lock()
                .expect("lock alert recordings")
                .iter()
                .any(|alert| {
                    alert.severity == AlertLevel::Warning
                        && alert.category == AlertCategory::SchedulerHealth
                        && alert.source == AlertSource::Scheduler
                        && alert.idempotency_key
                            == format!("feedback-cycle-stuck:{}", fixture.cycle_id)
                        && !alert.affects_trading
                        && !alert.visible_toast
                });
            if alerted {
                break;
            }
            sleep(StdDuration::from_millis(50)).await;
        }
    })
    .await
    .expect("stuck alert must use the DB-clock threshold");
    let (_, text) = harness
        .metrics
        .gather_prometheus_text()
        .expect("gather feedback duration metrics");
    let text = String::from_utf8(text).expect("metrics are UTF-8");
    assert!(text.contains("quant_feedback_stuck_total 1"));
    assert!(text.contains(
        "quant_feedback_stage_duration_seconds_count{stage=\"truth_freeze\",status=\"succeeded\"} 1"
    ));

    request_cancel(&harness, &fixture, FeedbackStage::Coverage).await;
    wait_status(&harness, fixture.cycle_id, FeedbackCycleStatus::Cancelled).await;
    let second_coverage = wait_job(
        &harness,
        fixture.second_cycle_id,
        FeedbackStage::TruthFreeze,
    )
    .await;
    assert_eq!(
        second_coverage.feedback_cycle_id,
        Some(fixture.second_cycle_id)
    );
    let second_fixture = FeedbackSchemaFixture {
        cycle_id: fixture.second_cycle_id,
        cycle: fixture.second_cycle.clone(),
        candidate_family_hash: fixture.second_candidate_family_hash,
        ..fixture
    };
    request_cancel(&harness, &second_fixture, FeedbackStage::TruthFreeze).await;
    wait_status(
        &harness,
        second_fixture.cycle_id,
        FeedbackCycleStatus::Cancelled,
    )
    .await;
    let (_, text) = harness
        .metrics
        .gather_prometheus_text()
        .expect("gather terminal cycle metrics");
    let text = String::from_utf8(text).expect("metrics are UTF-8");
    assert!(text.contains("quant_feedback_cycle_total{decision=\"none\",status=\"cancelled\"} 2"));
    assert!(text.contains("quant_feedback_cycle_duration_seconds_count{status=\"cancelled\"} 2"));
    stop_task(shutdown, task).await;
}
