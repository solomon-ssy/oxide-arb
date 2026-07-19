//! Research-job ledger integration tests (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::NewResearchJob,
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    types::ResearchJobId,
};
use quant_pivot_repository::{postgres::PgResearchJobRepository, traits::ResearchJobRepository};
use quant_pivot_test_support::pg::setup_pg;

fn new_job(job_id: ResearchJobId) -> NewResearchJob {
    NewResearchJob {
        job_id,
        kind: ResearchJobKind::DatasetBuild,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: None,
        params_json: serde_json::json!({"reason": "pg-research-job-it"}),
        requested_by: None,
        acting_role: "system".to_owned(),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn finalize_requires_running_lease_owner() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id.clone()))
        .await
        .expect("enqueue");

    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    let leased = repo
        .lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");
    assert_eq!(leased.status, ResearchJobStatus::Running);

    let finalized = repo
        .finalize(
            &job_id,
            "worker-a",
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn stale_owner_finalize_is_rejected_after_reclaim() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id.clone()))
        .await
        .expect("enqueue");

    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");

    // Simulate boot recovery: orphan under a new epoch re-queues the row.
    let outcome = repo
        .reclaim_orphaned("worker-b", Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("reclaim");
    assert_eq!(outcome.requeued, 1);

    // New worker picks it up.
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-b", lease_expires)
        .await
        .expect("re-lease")
        .expect("job");

    let stale_err = repo
        .finalize(
            &job_id,
            "worker-a",
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
    assert_eq!(current.lease_owner.as_deref(), Some("worker-b"));

    let finalized = repo
        .finalize(
            &job_id,
            "worker-b",
            ResearchJobStatus::Succeeded,
            None,
            None,
            None,
        )
        .await
        .expect("current owner finalize");
    assert_eq!(finalized.status, ResearchJobStatus::Succeeded);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn requeue_inflight_requeues_own_running_row_and_bumps_recovery() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id.clone()))
        .await
        .expect("enqueue");

    // Own this row under `worker-a`, then a graceful shutdown drains it.
    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");

    let outcome = repo
        .requeue_inflight("worker-a")
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn requeue_inflight_ignores_other_owners_running_rows() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id.clone()))
        .await
        .expect("enqueue");

    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");

    // A different epoch's graceful drain must not touch `worker-a`'s row.
    let outcome = repo.requeue_inflight("worker-b").await.expect("requeue");
    assert_eq!(outcome.requeued, 0);
    assert_eq!(outcome.quarantined, 0);
    let row = repo.find_by_id(&job_id).await.expect("find").expect("row");
    assert_eq!(row.status, ResearchJobStatus::Running);
    assert_eq!(row.lease_owner.as_deref(), Some("worker-a"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn requeue_inflight_quarantines_at_recovery_cap() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    // A job that has already exhausted its recovery budget.
    let mut job = new_job(job_id.clone());
    job.recovery_attempt = 3;
    job.max_recovery_attempts = 3;
    repo.enqueue(job).await.expect("enqueue");

    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");

    let outcome = repo.requeue_inflight("worker-a").await.expect("requeue");
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn double_finalize_returns_state_conflict() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchJobRepository::new(db);
    let job_id = ResearchJobId::from_v7();
    repo.enqueue(new_job(job_id.clone()))
        .await
        .expect("enqueue");

    let lease_expires = Utc::now() + ChronoDuration::seconds(90);
    repo.lease_next(&[ResearchJobKind::DatasetBuild], "worker-a", lease_expires)
        .await
        .expect("lease")
        .expect("job");

    repo.finalize(
        &job_id,
        "worker-a",
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
            "worker-a",
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
