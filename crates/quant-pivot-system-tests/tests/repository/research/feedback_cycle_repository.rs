//! Feedback-cycle repository contracts against a real `PostgreSQL` instance.

use std::time::Duration as StdDuration;

use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        api::BuildTrainingDatasetRequest,
        quant::{
            FeedbackCycleTerminal, FeedbackStageEventInput, FeedbackStageJobIdentity,
            NewFeedbackStageEvent, NewResearchJob,
        },
    },
    entities::quant_feedback_cycle::Entity as QuantFeedbackCycleEntity,
    enums::quant::{
        DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackStage,
        FeedbackStageEventKind, ResearchJobKind, ResearchJobStatus,
    },
    types::{
        DecisionPolicySnapshotId, FeedbackCycleId, ModelSpecId, ResearchJobId, ResearchJobParams,
        RoleCode, SchemaVersion, TrainingDatasetId, TrainingSampleSources, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{PgFeedbackCycleRepository, PgResearchJobRepository},
    traits::{
        DriftReportWriteOutcome, FeedbackCycleCasOutcome, FeedbackCycleClaim,
        FeedbackCycleClaimMode, FeedbackCycleGeneration, FeedbackCycleRepository,
        FeedbackCycleWriteOutcome, FeedbackEvaluationWriteOutcome, FeedbackStageWriteOutcome,
        ResearchJobRepository,
    },
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, EntityTrait, QuerySelect, TransactionTrait,
    sea_query::{LockBehavior, LockType},
};
use tokio::time::{sleep, timeout};

use super::feedback_boot_schema::{FeedbackSchemaFixture, content_hash, prepare_fixture};

macro_rules! assert_cycle_conflict {
    ($error:expr) => {
        assert!(matches!(
            $error,
            StorageError::StateConflict {
                entity: owner,
                ..
            } if owner == entity::QUANT_FEEDBACK_CYCLE
        ));
    };
}

fn cancellation_event(
    fixture: &FeedbackSchemaFixture,
    cycle_id: FeedbackCycleId,
    sequence: i64,
) -> NewFeedbackStageEvent {
    NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: cycle_id,
        event_sequence: sequence,
        stage: FeedbackStage::Coverage,
        event_kind: FeedbackStageEventKind::CancellationRequested,
        research_job_id: None,
        actor: Some("operator".to_owned()),
        reason_code: Some("operator_cancelled".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: fixture.observed_at,
    })
    .expect("seal cancellation request")
}

fn stage_event(
    fixture: &FeedbackSchemaFixture,
    job_id: ResearchJobId,
    sequence: i64,
    kind: FeedbackStageEventKind,
) -> NewFeedbackStageEvent {
    NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: fixture.cycle_id,
        event_sequence: sequence,
        stage: FeedbackStage::Coverage,
        event_kind: kind,
        research_job_id: Some(job_id),
        actor: None,
        reason_code: None,
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: fixture.observed_at,
    })
    .expect("seal worker stage event")
}

impl FeedbackSchemaFixture {
    fn coverage_job(&self) -> NewResearchJob {
        NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::DatasetBuild,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: ResearchJobParams::DatasetBuild(BuildTrainingDatasetRequest {
                model_spec_id: ModelSpecId::from_v7(),
                profile_ref: self.profile_ref.clone(),
                purpose: DatasetPurpose::Training,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start: self.evaluation_window_start,
                window_end: self.evaluation_window_end,
                pit_cutoff: self.label_cutoff,
                sample_interval_secs: 60,
                horizons_secs: vec![3_600],
                knowledge_lag_secs: 1,
                feature_schema_version: SchemaVersion::FIRST,
                sample_sources: TrainingSampleSources::default(),
                reason: "feedback-cycle-repository".to_owned(),
                training_dataset_id: Some(TrainingDatasetId::from_v7()),
            }),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(
            FeedbackStageJobIdentity::try_root(self.cycle_id, FeedbackStage::Coverage)
                .expect("freeze feedback-stage job identity"),
        )
        .expect("bind feedback-stage job identity")
    }

    async fn assert_initial_outbox(&self, repo: &PgFeedbackCycleRepository) {
        let retry = repo
            .record_trigger(
                self.cycle.clone(),
                self.stage_event(self.cycle_id, "scheduler"),
            )
            .await
            .expect("retry exact trigger");
        assert!(matches!(
            retry,
            (
                FeedbackCycleWriteOutcome::AlreadyPresent(_),
                FeedbackStageWriteOutcome::AlreadyPresent(_)
            )
        ));
        let replay = repo.list_outbox(0, 10).await.expect("list trigger outbox");
        assert_eq!(replay.len(), 2, "exact trigger retry cannot fork revision");
        assert!(
            replay
                .windows(2)
                .all(|pair| pair[0].revision < pair[1].revision)
        );
        assert_eq!(replay[0].event.feedback_cycle_id, self.cycle_id);
        assert_eq!(replay[1].event.feedback_cycle_id, self.second_cycle_id);
        let snapshot = repo.queue_snapshot().await.expect("read queue snapshot");
        assert_eq!((snapshot.queued, snapshot.running), (2, 0));
        assert_eq!(snapshot.pending_outbox, 2);
        assert!(snapshot.oldest_queued_at.is_some());
        assert!(snapshot.oldest_running_at.is_none());
    }
}

async fn record_cycles(repo: &PgFeedbackCycleRepository, fixture: &FeedbackSchemaFixture) {
    let first = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("record first trigger");
    assert!(matches!(
        first,
        (
            FeedbackCycleWriteOutcome::Inserted(_),
            FeedbackStageWriteOutcome::Inserted(_)
        )
    ));
    let second = repo
        .record_trigger(
            fixture.second_cycle.clone(),
            fixture.stage_event(fixture.second_cycle_id, "operator"),
        )
        .await
        .expect("record second trigger");
    assert!(matches!(
        second,
        (
            FeedbackCycleWriteOutcome::Inserted(_),
            FeedbackStageWriteOutcome::Inserted(_)
        )
    ));
}

pub async fn trigger_exact_retry() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db);
    record_cycles(&repo, &fixture).await;

    let retry = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("retry exact trigger");
    assert!(matches!(
        retry,
        (
            FeedbackCycleWriteOutcome::AlreadyPresent(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));

    let conflict = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "different-actor"),
        )
        .await
        .expect_err("same event sequence cannot bind different content");
    assert!(matches!(
        conflict,
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_STAGE_EVENT
    ));
}

pub async fn outbox_delivery_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    record_cycles(&repo, &fixture).await;
    fixture.assert_initial_outbox(&repo).await;

    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let first = repo
        .claim_outbox(worker_a, 30, 1)
        .await
        .expect("claim first revision");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].publish_attempts, 1);
    let second = repo
        .claim_outbox(worker_b, 30, 1)
        .await
        .expect("claim next unlocked revision");
    assert_eq!(second.len(), 1);
    assert!(second[0].revision > first[0].revision);
    assert!(matches!(
        repo.publish_outbox(first[0].revision, worker_b)
            .await
            .expect_err("wrong owner cannot publish"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_EVENT_OUTBOX
    ));

    repo.fail_outbox(
        first[0].revision,
        worker_a,
        "transient downstream failure".to_owned(),
    )
    .await
    .expect("release failed first delivery");
    repo.fail_outbox(
        first[0].revision,
        worker_a,
        "transient downstream failure".to_owned(),
    )
    .await
    .expect("retry exact failure result");
    let retried = repo
        .claim_outbox(worker_a, 30, 1)
        .await
        .expect("reclaim failed revision");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].revision, first[0].revision);
    assert_eq!(retried[0].publish_attempts, 2);
    repo.publish_outbox(retried[0].revision, worker_a)
        .await
        .expect("publish reclaimed revision");
    repo.publish_outbox(retried[0].revision, worker_a)
        .await
        .expect("retry exact publish");
    repo.publish_outbox(second[0].revision, worker_b)
        .await
        .expect("publish second revision");
    assert!(
        repo.claim_outbox(WorkerId::from_v7(), 30, 10)
            .await
            .expect("claim empty published queue")
            .is_empty()
    );

    let claim = repo
        .claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim cycle for stage append")
        .expect("queued cycle exists");
    let job = fixture.coverage_job();
    let job_id = job.job_id;
    PgResearchJobRepository::new(db)
        .enqueue(job)
        .await
        .expect("persist stage job");
    let event = stage_event(&fixture, job_id, 2, FeedbackStageEventKind::Started);
    assert!(matches!(
        repo.append_stage(claim.lease, event.clone())
            .await
            .expect("append stage with outbox"),
        FeedbackStageWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_stage(claim.lease, event)
            .await
            .expect("retry exact stage append"),
        FeedbackStageWriteOutcome::AlreadyPresent(_)
    ));
    let stage_replay = repo
        .list_outbox(second[0].revision, 10)
        .await
        .expect("replay stage revision");
    assert_eq!(stage_replay.len(), 1);
    assert_eq!(stage_replay[0].event.research_job_id, Some(job_id));
    assert_eq!(
        stage_replay[0].event.event_kind,
        FeedbackStageEventKind::Started
    );
    assert_eq!(
        repo.queue_snapshot()
            .await
            .expect("read post-stage queue snapshot")
            .pending_outbox,
        1
    );

    for error in [
        repo.list_outbox(-1, 1)
            .await
            .expect_err("negative revision cursor must fail closed"),
        repo.list_outbox(0, 0)
            .await
            .expect_err("zero replay limit must fail closed"),
        repo.claim_outbox(WorkerId::from_v7(), 30, 1_001)
            .await
            .expect_err("oversized claim must fail closed"),
    ] {
        assert!(matches!(
            error,
            StorageError::InvariantViolation {
                entity: Some(owner),
                ..
            } if owner == entity::QUANT_FEEDBACK_EVENT_OUTBOX
        ));
    }
}

async fn lock_cycle(db: &DatabaseConnection, cycle_id: FeedbackCycleId) -> DatabaseTransaction {
    let transaction = db.begin().await.expect("begin competing transaction");
    let locked = QuantFeedbackCycleEntity::find_by_id(cycle_id)
        .lock_with_behavior(LockType::Update, LockBehavior::Nowait)
        .one(&transaction)
        .await
        .expect("lock oldest feedback cycle");
    assert!(locked.is_some(), "cycle selected for lock must exist");
    transaction
}

pub async fn skip_locked_claims() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    record_cycles(&repo, &fixture).await;
    let lock = lock_cycle(&db, fixture.cycle_id).await;

    let worker = WorkerId::from_v7();
    let claimed = timeout(StdDuration::from_secs(2), repo.claim_cycle(worker, 30))
        .await
        .expect("SKIP LOCKED claim must not wait")
        .expect("claim unlocked cycle")
        .expect("one unlocked cycle is eligible");
    assert_eq!(claimed.mode, FeedbackCycleClaimMode::Started);
    assert_eq!(claimed.cycle.feedback_cycle_id, fixture.second_cycle_id);
    lock.rollback().await.expect("release cycle lock");

    let contender_a = PgFeedbackCycleRepository::new(db.clone());
    let contender_b = PgFeedbackCycleRepository::new(db);
    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let (claim_a, claim_b) = tokio::join!(
        contender_a.claim_cycle(worker_a, 30),
        contender_b.claim_cycle(worker_b, 30),
    );
    let claims = [claim_a.expect("claim A"), claim_b.expect("claim B")]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1, "one queued cycle has one lease winner");
    assert_eq!(claims[0].cycle.feedback_cycle_id, fixture.cycle_id);
}

pub async fn lease_cas_recovery() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db);
    record_cycles(&repo, &fixture).await;

    let owner_a = WorkerId::from_v7();
    let first = repo
        .claim_cycle(owner_a, 1)
        .await
        .expect("claim cycle")
        .expect("queued cycle");
    sleep(StdDuration::from_millis(1_100)).await;

    let owner_b = WorkerId::from_v7();
    let recovered = repo
        .claim_cycle(owner_b, 30)
        .await
        .expect("recover expired cycle")
        .expect("expired cycle");
    assert_eq!(recovered.mode, FeedbackCycleClaimMode::LeaseRecovered);
    assert_eq!(
        recovered.cycle.feedback_cycle_id,
        first.cycle.feedback_cycle_id
    );
    assert_eq!(recovered.cycle.generation, first.cycle.generation + 1);

    assert_cycle_conflict!(
        repo.renew_cycle_lease(first.lease, 30)
            .await
            .expect_err("stale owner cannot renew")
    );
    let terminal =
        FeedbackCycleTerminal::try_succeeded(FeedbackDecision::NoAction, "no_action".to_owned())
            .expect("valid successful terminal");
    assert_cycle_conflict!(
        repo.finalize_cycle(first.lease, terminal.clone())
            .await
            .expect_err("stale owner cannot finalize")
    );

    let renewed = repo
        .renew_cycle_lease(recovered.lease, 30)
        .await
        .expect("renew current lease");
    let cancel_event = cancellation_event(&fixture, renewed.feedback_cycle_id, 2);
    let cancel = repo
        .request_cancel(
            FeedbackCycleGeneration::from(&renewed),
            cancel_event.clone(),
        )
        .await
        .expect("request running cancellation");
    let (FeedbackCycleCasOutcome::Applied(cancelled), FeedbackStageWriteOutcome::Inserted(_)) =
        cancel
    else {
        panic!("first cancellation request must atomically apply");
    };
    assert_eq!(cancelled.status, FeedbackCycleStatus::Running);
    assert!(cancelled.cancel_requested_at.is_some());

    let retry = repo
        .request_cancel(FeedbackCycleGeneration::from(&renewed), cancel_event)
        .await
        .expect("retry cancellation request");
    assert!(matches!(
        retry,
        (
            FeedbackCycleCasOutcome::AlreadyApplied(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));

    let current_lease = recovered.lease.with_generation(cancelled.generation);
    let finalized = repo
        .finalize_cycle(
            current_lease,
            FeedbackCycleTerminal::try_cancelled("operator_cancelled".to_owned())
                .expect("valid cancellation terminal"),
        )
        .await
        .expect("terminalize cancelled cycle");
    assert!(matches!(finalized, FeedbackCycleCasOutcome::Applied(_)));

    let queued_cancel = cancellation_event(&fixture, fixture.second_cycle_id, 2);
    let queued = repo
        .find_cycle(&fixture.second_cycle_id)
        .await
        .expect("load queued cycle")
        .expect("queued cycle exists");
    let result = repo
        .request_cancel(
            FeedbackCycleGeneration::from(&queued),
            queued_cancel.clone(),
        )
        .await
        .expect("cancel queued cycle");
    let (FeedbackCycleCasOutcome::Applied(cancelled), FeedbackStageWriteOutcome::Inserted(_)) =
        result
    else {
        panic!("first queued cancellation must atomically apply");
    };
    assert_eq!(cancelled.status, FeedbackCycleStatus::Cancelled);
    assert!(matches!(
        repo.request_cancel(FeedbackCycleGeneration::from(&queued), queued_cancel)
            .await
            .expect("retry queued cancellation"),
        (
            FeedbackCycleCasOutcome::AlreadyApplied(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));
    assert!(
        repo.claim_cycle(WorkerId::from_v7(), 30)
            .await
            .expect("claim after queued cancellation")
            .is_none(),
        "terminal queued cancellation cannot be claimed"
    );
}

async fn running_cycle(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
) -> FeedbackCycleClaim {
    record_cycles(repo, fixture).await;
    repo.claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim feedback cycle")
        .expect("queued feedback cycle")
}

async fn stage_append_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    job_id: ResearchJobId,
) {
    let stage = stage_event(fixture, job_id, 2, FeedbackStageEventKind::Started);
    assert!(matches!(
        repo.append_stage(claim.lease, stage.clone())
            .await
            .expect("append stage"),
        FeedbackStageWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_stage(claim.lease, stage)
            .await
            .expect("retry stage"),
        FeedbackStageWriteOutcome::AlreadyPresent(_)
    ));
    let stage_conflict = stage_event(fixture, job_id, 2, FeedbackStageEventKind::JobLinked);
    assert!(matches!(
        repo.append_stage(claim.lease, stage_conflict)
            .await
            .expect_err("sequence conflict must fail"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_STAGE_EVENT
    ));
}

async fn drift_append_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
) {
    let drift = fixture.drift_report(
        fixture.label_cutoff,
        rust_decimal_macros::dec!(0.20),
        content_hash('1'),
    );
    assert!(matches!(
        repo.append_drift(claim.lease, drift.clone())
            .await
            .expect("append drift"),
        DriftReportWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_drift(claim.lease, drift)
            .await
            .expect("retry drift"),
        DriftReportWriteOutcome::AlreadyPresent(_)
    ));
    let drift_conflict = fixture.drift_report(
        fixture.label_cutoff,
        rust_decimal_macros::dec!(0.25),
        content_hash('2'),
    );
    assert!(matches!(
        repo.append_drift(claim.lease, drift_conflict)
            .await
            .expect_err("one metric cannot bind different evidence"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_DRIFT_REPORT
    ));
}

async fn evaluation_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
) {
    let evaluation = fixture.evaluation_use(
        fixture.cycle_id,
        fixture.candidate_family_hash,
        fixture.evaluation_dataset_hash,
        content_hash('3'),
    );
    assert!(matches!(
        repo.append_evaluation(claim.lease, evaluation.clone())
            .await
            .expect("append evaluation use"),
        FeedbackEvaluationWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_evaluation(claim.lease, evaluation)
            .await
            .expect("retry evaluation use"),
        FeedbackEvaluationWriteOutcome::AlreadyPresent(_)
    ));
    let second_claim = repo
        .claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim second feedback cycle")
        .expect("second feedback cycle remains queued");
    assert_eq!(
        second_claim.cycle.feedback_cycle_id,
        fixture.second_cycle_id
    );
    let reused = fixture.evaluation_use(
        fixture.second_cycle_id,
        fixture.second_candidate_family_hash,
        fixture.evaluation_dataset_hash,
        content_hash('4'),
    );
    assert!(matches!(
        repo.append_evaluation(second_claim.lease, reused)
            .await
            .expect_err("evaluation dataset cannot be reused"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_EVALUATION_USE
    ));
}

async fn terminal_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    job_id: ResearchJobId,
) {
    let terminal = FeedbackCycleTerminal::try_succeeded(
        FeedbackDecision::ChallengerRejected,
        "challenger_rejected".to_owned(),
    )
    .expect("valid terminal");
    let finalized = repo
        .finalize_cycle(claim.lease, terminal.clone())
        .await
        .expect("finalize cycle");
    let FeedbackCycleCasOutcome::Applied(done) = finalized else {
        panic!("first finalize must apply");
    };
    assert_eq!(done.status, FeedbackCycleStatus::Succeeded);
    assert!(matches!(
        repo.finalize_cycle(claim.lease, terminal)
            .await
            .expect("retry exact terminal"),
        FeedbackCycleCasOutcome::AlreadyApplied(_)
    ));
    let post_terminal = stage_event(fixture, job_id, 3, FeedbackStageEventKind::JobLinked);
    assert_cycle_conflict!(
        repo.append_stage(claim.lease, post_terminal)
            .await
            .expect_err("terminal cycle cannot append new worker evidence")
    );
}

pub async fn evidence_append_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    let claim = running_cycle(&repo, &fixture).await;
    assert_eq!(claim.cycle.feedback_cycle_id, fixture.cycle_id);

    let job = fixture.coverage_job();
    let job_id = job.job_id;
    PgResearchJobRepository::new(db)
        .enqueue(job)
        .await
        .expect("persist stage job");
    stage_append_contracts(&repo, &fixture, &claim, job_id).await;
    drift_append_contracts(&repo, &fixture, &claim).await;
    evaluation_contracts(&repo, &fixture, &claim).await;
    terminal_contracts(&repo, &fixture, &claim, job_id).await;

    let events = repo
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("read stage timeline");
    let reports = repo
        .list_drift_reports(&fixture.cycle_id)
        .await
        .expect("read drift reports");
    let uses = repo
        .list_evaluation_uses(&fixture.cycle_id)
        .await
        .expect("read evaluation uses");
    assert_eq!((events.len(), reports.len(), uses.len()), (2, 1, 1));
}
