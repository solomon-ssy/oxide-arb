//! Durable feedback scheduler repository contracts.

use std::time::Duration as StdDuration;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        FeedbackSchedulerControl, FeedbackSchedulerRetry, FeedbackSchedulerSuccess,
        NewFeedbackSchedulerState,
    },
    enums::quant::FeedbackSchedulerFailureKind,
    types::WorkerId,
};
use quant_pivot_repository::{
    postgres::{PgFeedbackCycleRepository, PgFeedbackSchedulerRepository},
    traits::{FeedbackCycleRepository, FeedbackSchedulerRepository},
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use tokio::time::sleep;

use super::feedback_boot_schema::prepare_fixture;

pub async fn scheduler_lease_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve scheduler profile");
    let repository = PgFeedbackSchedulerRepository::new(db.clone());
    let now = PgFeedbackCycleRepository::new(db.clone())
        .database_time()
        .await
        .expect("read database clock");
    let initial = repository
        .sync_state(
            NewFeedbackSchedulerState::try_new(&profile, now).expect("derive scheduler state"),
        )
        .await
        .expect("insert scheduler state");
    assert_eq!(initial.attempt, 0);
    assert!(!initial.paused);

    let first_worker = WorkerId::from_v7();
    let second_worker = WorkerId::from_v7();
    let competing_repository = PgFeedbackSchedulerRepository::new(db.clone());
    let (first, second) = tokio::join!(
        repository.claim_due(first_worker, 30),
        competing_repository.claim_due(second_worker, 30),
    );
    let claims = [first.expect("first claim"), second.expect("second claim")];
    assert_eq!(
        claims.iter().filter(|claim| claim.is_some()).count(),
        1,
        "SKIP LOCKED must grant one scheduler lease"
    );
    let claim = claims
        .into_iter()
        .flatten()
        .next()
        .expect("one scheduler claim");
    assert_eq!(claim.state.attempt, 1);

    let renewed = repository
        .renew_lease(claim.lease.clone(), 30)
        .await
        .expect("renew scheduler lease");
    assert!(renewed.expected_revision > claim.lease.expected_revision);
    let stale = repository
        .settle_retry(
            claim.lease,
            FeedbackSchedulerRetry {
                failure_kind: FeedbackSchedulerFailureKind::Materialization,
                retry_delay_secs: 5,
                error: "stale lease".to_owned(),
            },
        )
        .await
        .expect_err("stale scheduler revision must fail");
    assert!(matches!(stale, StorageError::StateConflict { .. }));

    let retry = repository
        .settle_retry(
            renewed,
            FeedbackSchedulerRetry {
                failure_kind: FeedbackSchedulerFailureKind::Materialization,
                retry_delay_secs: 5,
                error: "upstream temporarily unavailable".to_owned(),
            },
        )
        .await
        .expect("persist scheduler retry");
    assert!(retry.retry_at.is_some());
    assert_eq!(retry.attempt, 1);
    assert_eq!(retry.pending_cutoff, claim.state.pending_cutoff);
    assert_eq!(retry.pending_started_at, claim.state.pending_started_at);
    assert_eq!(retry.settlement_failure_count, 0);

    let paused = repository
        .apply_control(FeedbackSchedulerControl {
            research_profile_id: fixture.profile_ref.id.clone(),
            expected_pause_revision: 0,
            pause: true,
            reason_code: "operator_maintenance".to_owned(),
            note: "Pause scheduled retraining during controlled maintenance.".to_owned(),
        })
        .await
        .expect("pause scheduler");
    assert!(paused.paused);
    assert_eq!(paused.pause_revision, 1);
    assert!(
        repository
            .claim_due(WorkerId::from_v7(), 30)
            .await
            .expect("claim while paused")
            .is_none()
    );

    let resumed = repository
        .apply_control(FeedbackSchedulerControl {
            research_profile_id: fixture.profile_ref.id,
            expected_pause_revision: 1,
            pause: false,
            reason_code: "operator_resumed".to_owned(),
            note: "Resume scheduled retraining after controlled maintenance.".to_owned(),
        })
        .await
        .expect("resume scheduler");
    assert!(!resumed.paused);
    assert_eq!(resumed.pause_revision, 2);
    assert_eq!(
        repository.list_states().await.expect("list states").len(),
        1
    );
}

pub async fn pending_recovery_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    cycles
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("persist idempotent materialization target");
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve pending-recovery profile");
    let repository = PgFeedbackSchedulerRepository::new(db.clone());
    repository
        .sync_state(
            NewFeedbackSchedulerState::try_new(
                &profile,
                cycles
                    .database_time()
                    .await
                    .expect("read pending-recovery clock"),
            )
            .expect("derive pending-recovery state"),
        )
        .await
        .expect("persist pending-recovery state");

    let first = repository
        .claim_due(WorkerId::from_v7(), 1)
        .await
        .expect("claim first pending cutoff")
        .expect("pending cutoff is due");
    let pending_cutoff = first.state.pending_cutoff.expect("claim freezes cutoff");
    let pending_started_at = first
        .state
        .pending_started_at
        .expect("claim freezes pending start");
    sleep(StdDuration::from_millis(1_100)).await;
    let recovered = repository
        .claim_due(WorkerId::from_v7(), 30)
        .await
        .expect("reclaim expired pending cutoff")
        .expect("expired pending cutoff is reclaimable");
    assert_eq!(recovered.state.pending_cutoff, Some(pending_cutoff));
    assert_eq!(recovered.state.pending_started_at, Some(pending_started_at));
    assert_eq!(recovered.state.attempt, 2);
    assert!(matches!(
        repository.renew_lease(first.lease, 30).await,
        Err(StorageError::StateConflict { .. })
    ));

    let settled = repository
        .settle_success(
            recovered.lease,
            FeedbackSchedulerSuccess {
                feedback_cycle_id: fixture.cycle_id,
                label_cutoff: pending_cutoff,
            },
        )
        .await
        .expect("settle recovered pending cutoff");
    let database_now = cycles
        .database_time()
        .await
        .expect("read post-settlement clock");
    assert_eq!(settled.last_cycle_id, Some(fixture.cycle_id));
    assert_eq!(settled.last_cutoff, Some(pending_cutoff));
    assert!(settled.pending_cutoff.is_none());
    assert!(settled.pending_started_at.is_none());
    assert!(settled.lease_owner.is_none());
    assert!(settled.next_due_at > database_now);
    assert!(
        settled
            .cooldown_until
            .is_some_and(|until| until > database_now)
    );
}

pub async fn gap_settlement_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve gap-coalescing profile");
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    let repository = PgFeedbackSchedulerRepository::new(db.clone());
    repository
        .sync_state(
            NewFeedbackSchedulerState::try_new(
                &profile,
                cycles
                    .database_time()
                    .await
                    .expect("read gap-coalescing clock"),
            )
            .expect("derive gap-coalescing state"),
        )
        .await
        .expect("persist gap-coalescing state");
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_feedback_scheduler_state
         SET next_due_at = next_due_at - cadence_secs * INTERVAL '3 seconds',
             updated_at = statement_timestamp()
         WHERE research_profile_id = $1",
        [fixture.profile_ref.id.as_str().into()],
    ))
    .await
    .expect("simulate three unstarted cadence buckets");

    let claim = repository
        .claim_due(WorkerId::from_v7(), 30)
        .await
        .expect("claim coalesced scheduler gap")
        .expect("coalesced scheduler gap is due");
    assert_eq!(claim.state.coalesced_gap_count, 3);
    assert!(claim.state.last_coalesced_from.is_some());
    assert!(claim.state.last_coalesced_to.is_some());
    let pending_cutoff = claim.state.pending_cutoff;
    let failed = repository
        .settle_retry(
            claim.lease,
            FeedbackSchedulerRetry {
                failure_kind: FeedbackSchedulerFailureKind::Settlement,
                retry_delay_secs: 5,
                error: "commit acknowledgement unavailable".to_owned(),
            },
        )
        .await
        .expect("persist settlement recovery state");
    assert_eq!(failed.pending_cutoff, pending_cutoff);
    assert_eq!(failed.settlement_failure_count, 1);
    assert!(failed.last_settlement_failed_at.is_some());
    assert_eq!(
        failed.last_settlement_error.as_deref(),
        Some("commit acknowledgement unavailable")
    );
    assert_eq!(
        failed.last_failure_kind,
        Some(FeedbackSchedulerFailureKind::Settlement)
    );
}
