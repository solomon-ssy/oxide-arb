//! Feature-parity incident-ledger integration tests (Postgres + testcontainers).

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CompleteFeatureParityRun, FeatureParityJobParams, NewFeatureParityRun, NewResearchJob,
        RunFullFeatureParityRequest,
    },
    entities::quant_research_job,
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus, ResearchJobKind,
        ResearchJobStatus,
    },
    types::{ContentHash, FeatureParityRunId, ResearchJobId, ResearchJobParams},
};
use quant_pivot_repository::{
    postgres::PgFeatureParityRepository,
    traits::{EnqueueFrozenFeatureParityOutcome, FeatureParityLatchActor, FeatureParityRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use sea_orm::EntityTrait;

fn hash(byte: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", byte.to_string().repeat(64)))
        .expect("valid content hash")
}

fn queued_run(window_start: DateTime<Utc>, window_end: DateTime<Utc>) -> NewFeatureParityRun {
    NewFeatureParityRun {
        run_id: FeatureParityRunId::from_v7(),
        kind: FeatureParityRunKind::Full,
        status: FeatureParityRunStatus::Queued,
        window_start,
        window_end,
        report_id: None,
        model_version_id: None,
        training_dataset_id: None,
        triggered_by: "pg_feature_parity_test".to_owned(),
        requested_by: Some("risk-owner".to_owned()),
        acting_role: "risk_owner".to_owned(),
        reason: "incident-ledger integration".to_owned(),
        total_count: 0,
        compared_count: 0,
        matched_count: 0,
        mismatched_count: 0,
        pending_materialization_count: 0,
        feature_contract_hash: Some(hash('a')),
        transform_hash: None,
        failure_code: None,
        failure_detail: None,
        started_at: None,
        pending_since: None,
        containment_completed_at: None,
        finished_at: None,
    }
}

async fn finish_mismatched(
    repo: &PgFeatureParityRepository,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> FeatureParityRunId {
    let run = repo
        .create_run(queued_run(window_start, window_end))
        .await
        .expect("create mismatch run");
    repo.mark_running(&run.run_id)
        .await
        .expect("mark mismatch running");
    repo.complete_run(
        &run.run_id,
        CompleteFeatureParityRun {
            status: FeatureParityRunStatus::Mismatched,
            total_count: 1,
            compared_count: 1,
            matched_count: 0,
            mismatched_count: 1,
            pending_materialization_count: 0,
            feature_contract_hash: Some(hash('a')),
            transform_hash: Some(hash('b')),
            failure_code: None,
            failure_detail: None,
        },
    )
    .await
    .expect("finish mismatch");
    repo.mark_containment_complete(&run.run_id)
        .await
        .expect("mark containment complete");
    run.run_id
}

async fn finish_passed(
    repo: &PgFeatureParityRepository,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> FeatureParityRunId {
    let run = repo
        .create_run(queued_run(window_start, window_end))
        .await
        .expect("create recovery run");
    repo.mark_running(&run.run_id)
        .await
        .expect("mark recovery running");
    repo.complete_run(
        &run.run_id,
        CompleteFeatureParityRun {
            status: FeatureParityRunStatus::Passed,
            total_count: 1,
            compared_count: 1,
            matched_count: 1,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(hash('a')),
            transform_hash: Some(hash('b')),
            failure_code: None,
            failure_detail: None,
        },
    )
    .await
    .expect("finish recovery");
    run.run_id
}

fn actor() -> FeatureParityLatchActor {
    FeatureParityLatchActor {
        actor: Some("risk-owner".to_owned()),
        acting_role: "risk_owner".to_owned(),
        reason: "all deterministic causes replayed".to_owned(),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn cold_window_is_not_eligible_and_writes_no_run_or_job() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFeatureParityRepository::new(pool.connection().clone());
    let window_end = Utc::now();
    let run = queued_run(window_end - Duration::hours(24), window_end);
    let run_id = run.run_id.clone();
    let job_id = ResearchJobId::from_v7();
    let job = NewResearchJob {
        job_id: job_id.clone(),
        kind: ResearchJobKind::FeatureParity,
        status: ResearchJobStatus::Queued,
        model_spec_id: None,
        decision_policy_snapshot_id: None,
        params_json: ResearchJobParams::FeatureParity(FeatureParityJobParams {
            parity_run_id: run_id.clone(),
            materialization_timeout_secs: 600,
            request: RunFullFeatureParityRequest {
                window_start: Some(run.window_start),
                window_end: Some(run.window_end),
                reason: "cold-window eligibility".to_owned(),
            },
        }),
        requested_by: None,
        acting_role: "system".to_owned(),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    };

    let outcome = repo
        .enqueue_frozen_full(run, job)
        .await
        .expect("cold window eligibility");
    assert!(matches!(
        outcome,
        EnqueueFrozenFeatureParityOutcome::NotEligible
    ));
    assert!(repo.find_run(&run_id).await.expect("run lookup").is_none());
    assert!(
        quant_research_job::Entity::find_by_id(job_id)
            .one(pool.connection())
            .await
            .expect("job lookup")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn full_window_is_unique_only_while_a_run_is_active() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFeatureParityRepository::new(pool.connection().clone());
    let window_end = Utc::now();
    let window_start = window_end - Duration::hours(24);

    let first = repo
        .create_run(queued_run(window_start, window_end))
        .await
        .expect("create first active run");
    let duplicate = repo
        .create_run(queued_run(window_start, window_end))
        .await
        .expect_err("a concurrent active replay for the same full window must be rejected");
    assert!(matches!(duplicate, StorageError::Duplicate { .. }));

    repo.mark_running(&first.run_id)
        .await
        .expect("mark first run running");
    repo.complete_run(
        &first.run_id,
        CompleteFeatureParityRun {
            status: FeatureParityRunStatus::Passed,
            total_count: 1,
            compared_count: 1,
            matched_count: 1,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(hash('a')),
            transform_hash: Some(hash('b')),
            failure_code: None,
            failure_detail: None,
        },
    )
    .await
    .expect("complete first run");

    let recovery = repo
        .create_run(queued_run(window_start, window_end))
        .await
        .expect("terminal windows must remain replayable for governed recovery");
    assert_ne!(recovery.run_id, first.run_id);
    assert_eq!(
        repo.find_full_window(window_start, window_end)
            .await
            .expect("active-window lookup")
            .expect("queued recovery is active")
            .run_id,
        recovery.run_id
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn recovery_must_cover_every_open_incident_since_latest_clear() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFeatureParityRepository::new(pool.connection().clone());
    let now = Utc::now();
    let older_start = now - Duration::hours(4);
    let older_end = now - Duration::hours(3);
    let latest_start = now - Duration::hours(1);
    let latest_end = now;

    let older = finish_mismatched(&repo, older_start, older_end).await;
    let latest = finish_mismatched(&repo, latest_start, latest_end).await;
    assert_ne!(older, latest);
    assert_eq!(
        repo.current_state()
            .await
            .expect("current state")
            .expect("open latch")
            .cause_run_id,
        Some(latest)
    );

    let latest_only = finish_passed(&repo, latest_start, latest_end).await;
    let error = repo
        .acknowledge_latch(&latest_only, actor())
        .await
        .expect_err("latest-only recovery cannot discard the older cause");
    assert!(matches!(error, StorageError::StateConflict { .. }));
    assert!(error.to_string().contains("unresolved cause union"));
    assert_eq!(
        repo.current_state()
            .await
            .expect("current state after rejected recovery")
            .expect("latch remains initialized")
            .state,
        FeatureParityLatchState::Open
    );

    let union = finish_passed(&repo, older_start, latest_end).await;
    let cleared = repo
        .acknowledge_latch(&union, actor())
        .await
        .expect("union recovery clears every unresolved incident");
    assert_eq!(cleared.state, FeatureParityLatchState::Clear);
    assert_eq!(cleared.recovery_run_id, Some(union));
    assert!(
        repo.find_full_window(older_start, latest_end)
            .await
            .expect("terminal history does not make active lookup ambiguous")
            .is_none()
    );
}
