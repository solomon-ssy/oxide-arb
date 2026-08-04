//! Research-job ledger persistence system contracts.

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::{
    feedback::FeedbackError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        api::{BuildTrainingDatasetRequest, FeatureParityJobParams, RunFullFeatureParityRequest},
        quant::{
            FeedbackStageEventInput, FeedbackStageJobIdentity, NewFeedbackStageEvent,
            NewResearchJob, ResearchJobFinalization,
        },
    },
    enums::quant::{
        DatasetPurpose, FeedbackStage, FeedbackStageEventKind, ResearchJobErrorCode,
        ResearchJobKind, ResearchJobStatus,
    },
    types::{
        DecisionPolicySnapshotId, FeatureParityRunId, FeedbackCycleId, ModelSpecId,
        ResearchJobError, ResearchJobId, ResearchJobParams, ResearchJobProgress, RoleCode,
        SchemaVersion, TrainingDatasetId, TrainingSampleSources, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{PgFeedbackCycleRepository, PgResearchJobRepository},
    traits::{
        FeedbackCycleRepository, ResearchJobEnqueueOutcome, ResearchJobRepository,
        ResearchJobRetryOutcome,
    },
};
use quant_pivot_system_tests::{postgres::setup_pg, support::execution_pg_seed};
use sea_orm::{ActiveModelTrait, ConnectionTrait, DbBackend, IntoActiveModel, Statement};

use super::feedback_boot_schema::{FeedbackSchemaFixture, prepare_fixture};

fn new_job(job_id: ResearchJobId) -> NewResearchJob {
    let window_end = Utc::now();
    NewResearchJob {
        job_id,
        feedback_cycle_id: None,
        feedback_stage: None,
        kind: ResearchJobKind::DatasetBuild,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: None,
        params_json: ResearchJobParams::DatasetBuild(BuildTrainingDatasetRequest {
            model_spec_id: ModelSpecId::from_v7(),
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            purpose: DatasetPurpose::Training,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            window_start: window_end - ChronoDuration::hours(1),
            window_end,
            pit_cutoff: window_end,
            sample_interval_secs: 60,
            horizons_secs: vec![3_600],
            knowledge_lag_secs: 1,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: TrainingSampleSources::default(),
            reason: "pg-research-job-it".to_owned(),
            training_dataset_id: Some(TrainingDatasetId::from_v7()),
        }),
        requested_by: None,
        acting_role: RoleCode::new("system"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
}

fn parity_job(job_id: ResearchJobId) -> NewResearchJob {
    let window_end = Utc::now();
    NewResearchJob {
        job_id,
        feedback_cycle_id: None,
        feedback_stage: None,
        kind: ResearchJobKind::FeatureParity,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: None,
        params_json: ResearchJobParams::FeatureParity(FeatureParityJobParams {
            parity_run_id: FeatureParityRunId::from_v7(),
            materialization_timeout_secs: 600,
            request: RunFullFeatureParityRequest {
                window_start: Some(window_end - ChronoDuration::minutes(1)),
                window_end: Some(window_end),
                reason: "durable evidence-wait contract".to_owned(),
            },
        }),
        requested_by: None,
        acting_role: RoleCode::new("system"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
}

async fn record_feedback_cycles(repo: &PgFeedbackCycleRepository, fixture: &FeedbackSchemaFixture) {
    repo.record_trigger(
        fixture.cycle.clone(),
        fixture.stage_event(fixture.cycle_id, "scheduler"),
    )
    .await
    .expect("record first feedback cycle");
    repo.record_trigger(
        fixture.second_cycle.clone(),
        fixture.stage_event(fixture.second_cycle_id, "operator"),
    )
    .await
    .expect("record second feedback cycle");
}

fn feedback_event(
    fixture: &FeedbackSchemaFixture,
    feedback_cycle_id: FeedbackCycleId,
    job_id: ResearchJobId,
) -> NewFeedbackStageEvent {
    NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id,
        event_sequence: 2,
        stage: FeedbackStage::Coverage,
        event_kind: FeedbackStageEventKind::JobLinked,
        trigger_family: None,
        research_job_id: Some(job_id),
        actor: None,
        reason_code: None,
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: fixture.observed_at,
    })
    .expect("seal feedback job-link event")
}

pub async fn feedback_enqueue_exact_retry() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    record_feedback_cycles(&cycles, &fixture).await;
    let repo = PgResearchJobRepository::new(db.clone());

    let first_identity =
        FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Coverage)
            .expect("freeze feedback-stage identity");
    let repeated_identity =
        FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Coverage)
            .expect("repeat feedback-stage identity");
    assert!(matches!(
        FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Trigger),
        Err(FeedbackError::InvalidJobIdentity { .. })
    ));
    assert_eq!(first_identity.job_id(), repeated_identity.job_id());
    assert_ne!(
        first_identity.job_id(),
        FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Drift)
            .expect("different feedback stage")
            .job_id()
    );
    assert_ne!(
        first_identity.job_id(),
        FeedbackStageJobIdentity::try_root(fixture.second_cycle_id, FeedbackStage::Coverage)
            .expect("different feedback cycle")
            .job_id()
    );

    let job = new_job(ResearchJobId::from_v7())
        .try_bind_feedback(first_identity)
        .expect("bind feedback identity");
    let inserted = repo.enqueue(job.clone()).await.expect("enqueue stage job");
    assert!(matches!(
        inserted,
        ResearchJobEnqueueOutcome::Inserted(ref info)
            if info.job_id == first_identity.job_id()
                && info.feedback_cycle_id == Some(fixture.cycle_id)
                && info.feedback_stage == Some(FeedbackStage::Coverage)
    ));
    assert!(matches!(
        repo.enqueue(job.clone()).await.expect("exact enqueue retry"),
        ResearchJobEnqueueOutcome::AlreadyPresent(ref info)
            if info.job_id == first_identity.job_id()
    ));

    let mut invalid_status = job.clone();
    invalid_status.status = ResearchJobStatus::Running;
    assert!(matches!(
        repo.enqueue(invalid_status)
            .await
            .expect_err("enqueue retry cannot smuggle mutable status"),
        StorageError::InvariantViolation {
            entity: Some(owner),
            ..
        } if owner == entity::QUANT_RESEARCH_JOB
    ));

    let mut invalid_attempt = job.clone();
    invalid_attempt.recovery_attempt = 1;
    assert!(matches!(
        repo.enqueue(invalid_attempt)
            .await
            .expect_err("enqueue retry cannot smuggle recovery state"),
        StorageError::InvariantViolation {
            entity: Some(owner),
            ..
        } if owner == entity::QUANT_RESEARCH_JOB
    ));

    let mut drifted = job;
    drifted.requested_by = Some("different-actor".to_owned());
    let drift_error = repo
        .enqueue(drifted)
        .await
        .expect_err("same stage identity cannot change immutable input");
    assert!(matches!(
        drift_error,
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_RESEARCH_JOB
    ));

    let missing_cycle = FeedbackCycleId::from_v7();
    let missing_job = new_job(ResearchJobId::from_v7())
        .try_bind_feedback(
            FeedbackStageJobIdentity::try_root(missing_cycle, FeedbackStage::Coverage)
                .expect("freeze missing-cycle identity"),
        )
        .expect("bind missing-cycle identity");
    let missing_error = repo
        .enqueue(missing_job)
        .await
        .expect_err("feedback job requires an existing cycle");
    assert!(
        missing_error
            .to_string()
            .contains("fk_quant_research_job_cycle"),
        "unexpected missing-cycle error: {missing_error}"
    );

    let mutation_error = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_research_job SET feedback_stage = NULL WHERE job_id = $1",
            [first_identity.job_id().as_uuid().into()],
        ))
        .await
        .expect_err("feedback lineage must be immutable");
    assert!(
        mutation_error
            .to_string()
            .contains("research-job immutable identity and enqueue contract cannot change"),
        "unexpected lineage-mutation error: {mutation_error}"
    );

    let partial_job_id = ResearchJobId::from_v7();
    let pair_error = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO quant_research_job (
                job_id, feedback_cycle_id, feedback_stage, kind, status, model_spec_id,
                decision_policy_snapshot_id, params_json, progress_json, result_kind, result_ref,
                error_json, coverage_json, requested_by, acting_role, parent_job_id,
                recovery_attempt, max_recovery_attempts, lease_owner, lease_expires_at, started_at,
                finished_at, heartbeat_at
             )
             SELECT
                $1, feedback_cycle_id, NULL, kind, status, model_spec_id,
                decision_policy_snapshot_id, params_json, progress_json, result_kind, result_ref,
                error_json, coverage_json, requested_by, acting_role, parent_job_id,
                recovery_attempt, max_recovery_attempts, lease_owner, lease_expires_at, started_at,
                finished_at, heartbeat_at
             FROM quant_research_job
             WHERE job_id = $2",
            [
                partial_job_id.as_uuid().into(),
                first_identity.job_id().as_uuid().into(),
            ],
        ))
        .await
        .expect_err("feedback cycle and stage must be paired on insert");
    assert!(
        pair_error
            .to_string()
            .contains("ck_quant_research_job_feedback_lineage"),
        "unexpected lineage-pair error: {pair_error}"
    );
}

pub async fn feedback_retry_lineage() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    record_feedback_cycles(&cycles, &fixture).await;
    let repo = PgResearchJobRepository::new(db.clone());

    let root_identity =
        FeedbackStageJobIdentity::try_root(fixture.cycle_id, FeedbackStage::Coverage)
            .expect("freeze root stage identity");
    let root = new_job(ResearchJobId::from_v7())
        .try_bind_feedback(root_identity)
        .expect("bind root identity");
    assert!(matches!(
        repo.enqueue(root).await.expect("enqueue root stage job"),
        ResearchJobEnqueueOutcome::Inserted(_)
    ));

    let retry_identity = FeedbackStageJobIdentity::try_retry(
        fixture.cycle_id,
        FeedbackStage::Coverage,
        root_identity.job_id(),
    )
    .expect("freeze retry identity");
    let retry = new_job(ResearchJobId::from_v7())
        .try_bind_feedback(retry_identity)
        .expect("bind retry identity");
    let active_parent_error = repo
        .enqueue(retry.clone())
        .await
        .expect_err("active parent cannot be retried");
    assert!(matches!(
        active_parent_error,
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_RESEARCH_JOB
    ));

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease root")
        .expect("root job");
    repo.finalize(
        &root_identity.job_id(),
        &worker,
        ResearchJobFinalization::succeeded(None, None, None),
    )
    .await
    .expect("finalize root");

    assert!(matches!(
        repo.enqueue(retry.clone()).await.expect("enqueue retry"),
        ResearchJobEnqueueOutcome::Inserted(ref info)
            if info.parent_job_id == Some(root_identity.job_id())
                && info.job_id == retry_identity.job_id()
    ));
    assert!(matches!(
        repo.enqueue(retry).await.expect("exact retry enqueue"),
        ResearchJobEnqueueOutcome::AlreadyPresent(ref info)
            if info.job_id == retry_identity.job_id()
    ));

    for invalid_identity in [
        FeedbackStageJobIdentity::try_retry(
            fixture.cycle_id,
            FeedbackStage::Drift,
            root_identity.job_id(),
        )
        .expect("freeze cross-stage retry"),
        FeedbackStageJobIdentity::try_retry(
            fixture.second_cycle_id,
            FeedbackStage::Coverage,
            root_identity.job_id(),
        )
        .expect("freeze cross-cycle retry"),
    ] {
        let error = repo
            .enqueue(
                new_job(ResearchJobId::from_v7())
                    .try_bind_feedback(invalid_identity)
                    .expect("bind invalid retry identity"),
            )
            .await
            .expect_err("retry parent must own the same cycle and stage");
        assert!(matches!(
            error,
            StorageError::StateConflict {
                entity: owner,
                ..
            } if owner == entity::QUANT_RESEARCH_JOB
        ));
    }

    feedback_event(&fixture, fixture.cycle_id, root_identity.job_id())
        .into_active_model()
        .insert(&db)
        .await
        .expect("same-cycle stage event links the root job");
    let event_error = feedback_event(&fixture, fixture.second_cycle_id, root_identity.job_id())
        .into_active_model()
        .insert(&db)
        .await
        .expect_err("stage event cannot cross feedback-cycle lineage");
    assert!(
        event_error
            .to_string()
            .contains("fk_quant_feedback_stage_job_lineage"),
        "unexpected stage-job lineage error: {event_error}"
    );
}

pub async fn job_kind_match_params() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db.clone());
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id))
        .await
        .expect("enqueue typed job");

    let corruption = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_research_job SET params_json = jsonb_set(params_json, '{kind}', '\"backtest\"'::jsonb) WHERE job_id = $1",
            [job_id.as_uuid().into()],
        ))
        .await;
    assert!(
        corruption.is_err(),
        "the relational kind and JSON discriminator must not diverge"
    );
}

pub async fn finalize_requires_running_owner() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id)).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    let leased = repo
        .lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");
    assert_eq!(leased.status, ResearchJobStatus::Running);

    let finalized = repo
        .finalize(
            &job_id,
            &worker,
            ResearchJobFinalization::succeeded(None, None, None),
        )
        .await
        .expect("finalize");
    assert_eq!(finalized.status, ResearchJobStatus::Succeeded);
    assert!(finalized.finished_at.is_some());
    assert!(finalized.lease_owner.is_none());
}

pub async fn stale_rejected_after_reclaim() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id)).await.expect("enqueue");

    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker_a, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    // Simulate boot recovery: orphan under a new epoch re-queues the row.
    let outcome = repo
        .reclaim_orphaned(&worker_b, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("reclaim");
    assert_eq!(outcome.requeued, 1);

    // New worker picks it up.
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker_b, lease_expires)
        .await
        .expect("re-lease")
        .expect("job");

    let stale_err = repo
        .finalize(
            &job_id,
            &worker_a,
            ResearchJobFinalization::cancelled(ResearchJobError::new(
                ResearchJobErrorCode::Cancelled,
                "stale worker cancellation",
            )),
        )
        .await
        .expect_err("stale owner must not finalize");
    assert!(matches!(
        stale_err,
        StorageError::StateConflict {
            entity,
            ..
        } if entity == entity::QUANT_RESEARCH_JOB
    ));

    let current = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(current.status, ResearchJobStatus::Running);
    assert_eq!(current.lease_owner.as_ref(), Some(&worker_b));

    let finalized = repo
        .finalize(
            &job_id,
            &worker_b,
            ResearchJobFinalization::succeeded(None, None, None),
        )
        .await
        .expect("current owner finalize");
    assert_eq!(finalized.status, ResearchJobStatus::Succeeded);
}

pub async fn requeue_inflight_requeues_recovery() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id)).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    // Own this row under `worker-a`, then a graceful shutdown drains it.
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    let outcome = repo
        .requeue_inflight(&worker)
        .await
        .expect("requeue inflight");
    assert_eq!(outcome.requeued, 1, "own running row is re-queued");
    assert_eq!(outcome.quarantined, 0);

    let row = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(row.status, ResearchJobStatus::RetryScheduled);
    assert_eq!(
        row.recovery_attempt, 1,
        "graceful requeue counts against cap"
    );
    assert!(row.lease_owner.is_none(), "lease cleared for re-pickup");
    assert!(row.next_attempt_at.is_some(), "retry deadline is durable");
    assert!(
        row.started_at.is_none(),
        "started_at reset for the fresh run"
    );
}

pub async fn transient_retry_is_bounded() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    let mut job = new_job(job_id);
    job.max_recovery_attempts = 1;
    repo.enqueue(job).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    let wrong_worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    let scheduled = repo
        .retry_transient(
            &job_id,
            &worker,
            "temporary S3 transport failure".to_owned(),
            Duration::from_millis(5),
        )
        .await
        .expect("schedule typed retry");
    let ResearchJobRetryOutcome::Scheduled(scheduled) = scheduled else {
        panic!("first transient failure must schedule a retry");
    };
    assert_eq!(scheduled.status, ResearchJobStatus::RetryScheduled);
    assert_eq!(scheduled.recovery_attempt, 1);
    assert!(scheduled.next_attempt_at.is_some());
    assert_eq!(
        scheduled.error_json.as_ref().map(|error| error.code),
        Some(ResearchJobErrorCode::ExecutionRetryScheduled)
    );
    assert!(
        repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
            .await
            .expect("poll before retry deadline")
            .is_none(),
        "retry-scheduled work must not hot-loop before its DB-clock deadline"
    );

    tokio::time::sleep(Duration::from_millis(10)).await;
    let releasable = repo
        .lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease due retry")
        .expect("retry is due");
    assert_eq!(releasable.status, ResearchJobStatus::Running);
    assert!(releasable.next_attempt_at.is_none());
    assert!(releasable.error_json.is_none());

    let stale = repo
        .retry_transient(
            &job_id,
            &wrong_worker,
            "stale worker".to_owned(),
            Duration::from_secs(1),
        )
        .await
        .expect_err("stale owner cannot schedule a retry");
    assert!(matches!(stale, StorageError::StateConflict { .. }));

    let exhausted = repo
        .retry_transient(
            &job_id,
            &worker,
            "temporary S3 transport failure repeated".to_owned(),
            Duration::from_secs(1),
        )
        .await
        .expect("exhaust retry cap");
    let ResearchJobRetryOutcome::Exhausted(exhausted) = exhausted else {
        panic!("second transient failure must exhaust the one-retry cap");
    };
    assert_eq!(exhausted.status, ResearchJobStatus::Failed);
    assert!(exhausted.next_attempt_at.is_none());
    assert_eq!(
        exhausted.error_json.as_ref().map(|error| error.code),
        Some(ResearchJobErrorCode::ExecutionRetryExhausted)
    );
}

pub async fn evidence_wait_releases_slot() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(parity_job(job_id))
        .await
        .expect("enqueue parity");

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::FeatureParity], &worker, lease_expires)
        .await
        .expect("lease parity")
        .expect("parity job");

    let waiting = repo
        .await_evidence(
            &job_id,
            &worker,
            ResearchJobProgress::indeterminate("pending_materialization", 0),
            Duration::from_millis(20),
        )
        .await
        .expect("persist evidence wait");
    assert_eq!(waiting.status, ResearchJobStatus::AwaitingEvidence);
    assert_eq!(waiting.recovery_attempt, 0);
    assert!(waiting.next_attempt_at.is_some());
    assert!(waiting.lease_owner.is_none());
    assert!(waiting.error_json.is_none());
    assert_eq!(
        waiting
            .progress_json
            .as_ref()
            .map(|progress| progress.phase.as_str()),
        Some("pending_materialization")
    );
    assert!(waiting.started_at.is_none());
    assert!(waiting.heartbeat_at.is_none());
    assert!(
        repo.lease_next(&[ResearchJobKind::FeatureParity], &worker, lease_expires)
            .await
            .expect("poll before evidence deadline")
            .is_none(),
        "evidence wait must honor its DB-clock deadline"
    );

    tokio::time::sleep(Duration::from_millis(30)).await;
    let resumed = repo
        .lease_next(&[ResearchJobKind::FeatureParity], &worker, lease_expires)
        .await
        .expect("lease due evidence check")
        .expect("evidence check is due");
    assert_eq!(resumed.status, ResearchJobStatus::Running);
    assert_eq!(resumed.recovery_attempt, 0);
    assert!(resumed.next_attempt_at.is_none());
    assert!(resumed.progress_json.is_none());
}

pub async fn requeue_inflight_ignores_rows() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id)).await.expect("enqueue");

    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker_a, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    // A different epoch's graceful drain must not touch `worker-a`'s row.
    let outcome = repo.requeue_inflight(&worker_b).await.expect("requeue");
    assert_eq!(outcome.requeued, 0);
    assert_eq!(outcome.quarantined, 0);
    let row = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(row.status, ResearchJobStatus::Running);
    assert_eq!(row.lease_owner.as_ref(), Some(&worker_a));
}

pub async fn requeue_inflight_quarantines_cap() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    let mut job = new_job(job_id);
    job.max_recovery_attempts = 1;
    repo.enqueue(job).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");
    let first = repo
        .requeue_inflight(&worker)
        .await
        .expect("consume recovery budget");
    assert_eq!(first.requeued, 1);
    assert_eq!(first.quarantined, 0);

    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("re-lease")
        .expect("requeued job");

    let outcome = repo.requeue_inflight(&worker).await.expect("requeue");
    assert_eq!(outcome.requeued, 0);
    assert_eq!(
        outcome.quarantined, 1,
        "crash-loop guard quarantines at the cap"
    );
    let row = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(row.status, ResearchJobStatus::Failed);
    assert_eq!(row.recovery_attempt, 1);
    assert!(row.lease_owner.is_none());
    assert!(
        row.error_json.is_some(),
        "quarantine records a terminal error"
    );
}

pub async fn double_finalize_returns_conflict() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id)).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    repo.finalize(
        &job_id,
        &worker,
        ResearchJobFinalization::succeeded(None, None, None),
    )
    .await
    .expect("first finalize");

    let err = repo
        .finalize(
            &job_id,
            &worker,
            ResearchJobFinalization::failed(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionFailed,
                "second finalization",
            )),
        )
        .await
        .expect_err("second finalize");
    assert!(matches!(
        err,
        StorageError::StateConflict {
            entity,
            ..
        } if entity == entity::QUANT_RESEARCH_JOB
    ));
}
