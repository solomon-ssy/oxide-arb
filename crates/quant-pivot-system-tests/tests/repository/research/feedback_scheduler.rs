//! Durable feedback scheduler repository contracts.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{FeedbackSchedulerControl, NewFeedbackSchedulerState},
    types::WorkerId,
};
use quant_pivot_repository::{
    postgres::{PgFeedbackCycleRepository, PgFeedbackSchedulerRepository},
    traits::{FeedbackCycleRepository, FeedbackSchedulerRepository},
};
use quant_pivot_system_tests::postgres::setup_pg;

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
        .settle_retry(claim.lease, 5, "stale lease".to_owned())
        .await
        .expect_err("stale scheduler revision must fail");
    assert!(matches!(stale, StorageError::StateConflict { .. }));

    let retry = repository
        .settle_retry(renewed, 5, "upstream temporarily unavailable".to_owned())
        .await
        .expect("persist scheduler retry");
    assert!(retry.retry_at.is_some());
    assert_eq!(retry.attempt, 1);

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
