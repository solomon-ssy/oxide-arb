//! Research-job ledger persistence system contracts.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{api::BuildTrainingDatasetRequest, quant::NewResearchJob},
    enums::quant::{DatasetPurpose, ResearchJobKind, ResearchJobStatus},
    types::{
        DecisionPolicySnapshotId, ModelSpecId, ResearchJobId, ResearchJobParams, RoleCode,
        SchemaVersion, TrainingDatasetId, WorkerId, default_sample_sources,
    },
};
use quant_pivot_repository::{postgres::PgResearchJobRepository, traits::ResearchJobRepository};
use quant_pivot_system_tests::{postgres::setup_pg, support::execution_pg_seed};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

fn new_job(job_id: ResearchJobId) -> NewResearchJob {
    let window_end = Utc::now();
    NewResearchJob {
        job_id,
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
            sample_sources: default_sample_sources(),
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
            ResearchJobStatus::Succeeded,
            None,
            None,
            None,
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
            ResearchJobStatus::Cancelled,
            None,
            None,
            None,
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
            ResearchJobStatus::Succeeded,
            None,
            None,
            None,
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
    assert_eq!(row.status, ResearchJobStatus::Queued);
    assert_eq!(
        row.recovery_attempt, 1,
        "graceful requeue counts against cap"
    );
    assert!(row.lease_owner.is_none(), "lease cleared for re-pickup");
    assert!(
        row.started_at.is_none(),
        "started_at reset for the fresh run"
    );
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
    // A job that has already exhausted its recovery budget.
    let mut job = new_job(job_id);
    job.recovery_attempt = 3;
    job.max_recovery_attempts = 3;
    repo.enqueue(job).await.expect("enqueue");

    let worker = WorkerId::from_v7();
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], &worker, lease_expires)
        .await
        .expect("lease")
        .expect("job");

    let outcome = repo.requeue_inflight(&worker).await.expect("requeue");
    assert_eq!(outcome.requeued, 0);
    assert_eq!(
        outcome.quarantined, 1,
        "crash-loop guard quarantines at the cap"
    );
    let row = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(row.status, ResearchJobStatus::Failed);
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
        ResearchJobStatus::Succeeded,
        None,
        None,
        None,
    )
    .await
    .expect("first finalize");

    let err = repo
        .finalize(
            &job_id,
            &worker,
            ResearchJobStatus::Failed,
            None,
            None,
            None,
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
