//! Unit tests for the enqueue-only materialization scheduler.
//!
//! These run against [`MockSchedulerControlFactorRepository`] from test-support.
//! They assert the full decision matrix (due / active / not-due / overdue / stale /
//! build-failure) and, for capital safety, that the scheduler **never** calls
//! `publish_publication`.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorMaterializationRunInfo, DataRequirements, MarketFilterSpec, QualityGatePolicy,
        RequiredInputDomain, RuntimeConfigRef, SimulationConfig,
    },
    enums::control_factor::{
        ControlFactorType, MaterializationOutputPolicy, MaterializationRunStatus,
    },
};
use oxide_arb_test_support::{
    materialization::{execution_quality_hourly_run, scheduler_fixed_now},
    mocks::{EXECUTION_QUALITY_HOURLY_SCHEDULE_ID, MockSchedulerControlFactorRepository},
};

use super::{MaterializationScheduler, ScheduleAlert, ScheduleOutcome, ScheduledMaterialization};
use crate::scheduler::{SchedulePolicy, SchedulerCycleReport};

fn policy_with(cadence: Duration) -> SchedulePolicy {
    let now = scheduler_fixed_now();
    SchedulePolicy {
        tasks: vec![ScheduledMaterialization {
            schedule_id: EXECUTION_QUALITY_HOURLY_SCHEDULE_ID.to_owned(),
            cadence,
            source_delay_secs: 900,
            requested_factor_types: vec![ControlFactorType::ExecutionQuality],
            markets: MarketFilterSpec::default(),
            data_requirements: DataRequirements {
                required_inputs: vec![RequiredInputDomain::RuntimeConfig],
                production_required_inputs: vec![RequiredInputDomain::RuntimeConfig],
                min_l2_coverage_ratio: None,
                require_settlement_truth: false,
                require_token_balances: false,
            },
            runtime_config_ref: RuntimeConfigRef::ActiveAt { at: now },
            simulation_config: SimulationConfig::production_default(),
            quality_gate_policy: QualityGatePolicy::default(),
            output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
            replay_account_scope: None,
        }],
        created_by: "scheduler".to_owned(),
        code_git_sha: "abc".to_owned(),
    }
}

async fn tick_with(
    runs: Vec<ControlFactorMaterializationRunInfo>,
    cadence: Duration,
    now: DateTime<Utc>,
) -> (
    Arc<MockSchedulerControlFactorRepository>,
    SchedulerCycleReport,
) {
    let repo = Arc::new(MockSchedulerControlFactorRepository::with_runs(runs));
    let scheduler = MaterializationScheduler::new(Arc::clone(&repo) as _, policy_with(cadence));
    let report = scheduler.tick(now).await.expect("tick succeeds");
    (repo, report)
}

#[tokio::test]
async fn due_schedule_enqueues_a_single_run() {
    let now = scheduler_fixed_now();
    let (repo, report) = tick_with(Vec::new(), Duration::hours(1), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::Enqueued { schedule_id, .. }]
            if schedule_id == EXECUTION_QUALITY_HOURLY_SCHEDULE_ID
    ));
    assert_eq!(repo.enqueued_count(), 1);
    assert_eq!(repo.publish_calls(), 0);
}

#[tokio::test]
async fn active_run_dedupes_and_does_not_enqueue() {
    let now = scheduler_fixed_now();
    let runs = vec![execution_quality_hourly_run(
        MaterializationRunStatus::Queued,
        now - Duration::minutes(5),
        None,
    )];
    let (repo, report) = tick_with(runs, Duration::hours(1), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::DuplicateActive { .. }]
    ));
    assert_eq!(repo.enqueued_count(), 0);
    assert_eq!(repo.publish_calls(), 0);
}

#[tokio::test]
async fn recent_completed_run_is_not_due() {
    let now = scheduler_fixed_now();
    let runs = vec![execution_quality_hourly_run(
        MaterializationRunStatus::Completed,
        now - Duration::minutes(10),
        Some(now - Duration::minutes(8)),
    )];
    let (repo, report) = tick_with(runs, Duration::hours(1), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::NotDue { .. }]
    ));
    assert_eq!(repo.enqueued_count(), 0);
    assert!(report.alerts.is_empty());
    assert_eq!(repo.publish_calls(), 0);
}

#[tokio::test]
async fn never_succeeded_emits_stale_without_overdue() {
    let now = scheduler_fixed_now();
    // A recent *failed* run keeps the cadence not-due and not-overdue, but the
    // absence of any success must surface a Stale alert.
    let runs = vec![execution_quality_hourly_run(
        MaterializationRunStatus::Failed,
        now - Duration::minutes(5),
        Some(now - Duration::minutes(4)),
    )];
    let (repo, report) = tick_with(runs, Duration::hours(1), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::NotDue { .. }]
    ));
    assert!(
        report
            .alerts
            .iter()
            .any(|alert| matches!(alert, ScheduleAlert::Stale { .. }))
    );
    assert!(
        !report
            .alerts
            .iter()
            .any(|alert| matches!(alert, ScheduleAlert::Overdue { .. }))
    );
    assert_eq!(repo.publish_calls(), 0);
}

#[tokio::test]
async fn old_last_run_emits_overdue_alert_and_enqueues() {
    let now = scheduler_fixed_now();
    let runs = vec![execution_quality_hourly_run(
        MaterializationRunStatus::Completed,
        now - Duration::hours(3),
        Some(now - Duration::hours(3)),
    )];
    let (repo, report) = tick_with(runs, Duration::hours(1), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::Enqueued { .. }]
    ));
    assert!(
        report
            .alerts
            .iter()
            .any(|alert| matches!(alert, ScheduleAlert::Overdue { .. }))
    );
    assert_eq!(repo.enqueued_count(), 1);
    assert_eq!(repo.publish_calls(), 0);
}

#[tokio::test]
async fn invalid_cadence_reports_build_failure() {
    let now = scheduler_fixed_now();
    let (repo, report) = tick_with(Vec::new(), Duration::zero(), now).await;
    assert!(matches!(
        report.outcomes.as_slice(),
        [ScheduleOutcome::BuildFailed { schedule_id, code }]
            if schedule_id == EXECUTION_QUALITY_HOURLY_SCHEDULE_ID && !code.is_empty()
    ));
    assert_eq!(repo.enqueued_count(), 0);
    assert_eq!(repo.publish_calls(), 0);
}
