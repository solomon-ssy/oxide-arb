//! Durable report-coordinator persistence system contracts.

use chrono::{Duration, Utc};
use quant_pivot_error::storage::{StorageError, entity::DECISION_POLICY_SNAPSHOT};
use quant_pivot_models::{
    domain::quant::{
        ClaimReportSchedule, MaterializeReportSchedule, NewReportRun, ReconcileReportSchedule,
        ReportRunClaimConfig,
    },
    enums::{
        quant::{
            ReportRunStatus, ReportRunTerminalReason, ReportScheduleGapReason, ReportTriggerKind,
        },
        runtime_config::ConfigResourceKind,
    },
    types::{ContentHash, DecisionPolicySnapshotId, ReportRunId, ReportTriggerKey},
};
use quant_pivot_repository::{
    postgres::{PgPolicyRepository, PgReportRunRepository},
    traits::{PolicyRepository, ReportRunRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::policy_fixtures::{activate_policy_bundle, bootstrap_default_policy_bundle},
};
use sea_orm::DatabaseConnection;
use uuid::Uuid;

fn hash(byte: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", byte.to_string().repeat(64)))
        .expect("valid content hash")
}

async fn activate_runtime(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-report-scheduler", "scheduler integration fixture")
        .await
}

fn ad_hoc(request_id: &str) -> NewReportRun {
    NewReportRun {
        report_run_id: ReportRunId::from_v7(),
        trigger_kind: ReportTriggerKind::AdHoc,
        trigger_key: ReportTriggerKey::parse(format!("ad_hoc:{request_id}"))
            .expect("report trigger key"),
        schedule_id: None,
        request_id: Some(request_id.into()),
        retry_of_run_id: None,
        scheduled_for: None,
        requested_at: Utc::now(),
        status: ReportRunStatus::Queued,
        top_n: None,
        knowledge_lag_secs: None,
    }
}

fn claim_config(version_id: &DecisionPolicySnapshotId) -> ReportRunClaimConfig {
    ReportRunClaimConfig {
        decision_policy_snapshot_id: *version_id,
        ad_hoc_default_top_n: 20,
        ad_hoc_default_knowledge_lag_secs: 10,
        schedules: vec![ClaimReportSchedule {
            schedule_id: "primary".into(),
            top_n: 20,
            knowledge_lag_secs: 10,
        }],
    }
}

pub async fn two_coordinators_claim_run() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let version_id = activate_runtime(&db).await;
    let first = PgReportRunRepository::new(db.clone());
    let second = PgReportRunRepository::new(db.clone());
    first
        .enqueue_ad_hoc(ad_hoc("first"), 64, 300)
        .await
        .expect("enqueue first");
    first
        .enqueue_ad_hoc(ad_hoc("second"), 64, 300)
        .await
        .expect("enqueue second");

    let (left, right) = tokio::join!(
        first.claim_next_run(Uuid::now_v7().into(), 120, 300, claim_config(&version_id)),
        second.claim_next_run(Uuid::now_v7().into(), 120, 300, claim_config(&version_id)),
    );
    let claimed = [left.expect("first replica"), right.expect("second replica")]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, ReportRunStatus::Running);
    let decision_at = claimed[0]
        .decision_at
        .expect("claimed report has a decision time");
    assert_eq!(decision_at.timestamp_subsec_nanos() % 1_000_000, 0);
    assert_eq!(claimed[0].started_at, Some(decision_at));
}

pub async fn activation_read_stays_coherent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let original_id = activate_runtime(&db).await;
    let policies = PgPolicyRepository::new(db.clone());
    let runs = PgReportRunRepository::new(db);
    let activation = policies
        .load_current_activation(None)
        .await
        .expect("read coordinator activation")
        .expect("active coordinator policy");
    assert_eq!(activation.decision_policy_snapshot_id, original_id);

    // Freeze the exact interleaving: B commits after reading activation A,
    // before the coordinator resolves the snapshot referenced by A.
    let successor_id = activate_policy_bundle(
        &policies,
        ConfigResourceKind::RecommendationPolicy,
        "report-schedule-interleaving",
        "activate between the coordinator's activation and snapshot reads",
        |snapshot| snapshot.recommendation.reports.ad_hoc_default_top_n += 1,
    )
    .await;
    assert_ne!(successor_id, original_id);
    let snapshot = policies
        .load_snapshot(&activation.decision_policy_snapshot_id)
        .await
        .expect("resolve the activation's immutable snapshot")
        .expect("the prior snapshot remains immutable");
    assert_eq!(snapshot.decision_policy_snapshot_id, original_id);
    let current = policies
        .load_current_bundle()
        .await
        .expect("read successor bundle")
        .expect("successor is active");
    assert_eq!(current.decision_policy_snapshot_id, successor_id);
    assert_eq!(
        current.snapshot.recommendation.reports.ad_hoc_default_top_n,
        snapshot
            .snapshot
            .recommendation
            .reports
            .ad_hoc_default_top_n
            + 1
    );

    let stale = runs
        .reconcile_schedules(&snapshot.decision_policy_snapshot_id, Vec::new())
        .await
        .expect_err("the old coherent snapshot must lose the active-policy CAS");
    assert!(matches!(
        stale,
        StorageError::StateConflict {
            entity: DECISION_POLICY_SNAPSHOT,
            id: Some(id),
            detail,
        } if id == original_id.to_string()
            && detail == "runtime config changed during report schedule operation"
    ));
    runs.reconcile_schedules(&successor_id, Vec::new())
        .await
        .expect("the next pass can reconcile the successor policy");
}

pub async fn restart_coalesces_latest_gap() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let version_id = activate_runtime(&db).await;
    let repository = PgReportRunRepository::new(db);
    let first_due = Utc::now() - Duration::minutes(6);
    repository
        .reconcile_schedules(
            &version_id,
            vec![ReconcileReportSchedule {
                schedule_id: "primary".into(),
                decision_policy_snapshot_id: version_id,
                spec_hash: hash('b'),
                next_scheduled_for: first_due,
                enabled: true,
            }],
        )
        .await
        .expect("reconcile schedule");

    let latest = first_due + Duration::minutes(3);
    let next = latest + Duration::minutes(1);
    let materialized = repository
        .materialize_schedule(MaterializeReportSchedule {
            schedule_id: "primary".into(),
            decision_policy_snapshot_id: version_id,
            spec_hash: hash('b'),
            expected_next_scheduled_for: first_due,
            latest_scheduled_for: latest,
            next_scheduled_for: next,
            earlier_first_scheduled_for: Some(first_due),
            earlier_last_scheduled_for: Some(latest - Duration::minutes(1)),
            earlier_missed_count: 3,
        })
        .await
        .expect("materialize latest restart occurrence");
    assert_eq!(materialized.run.scheduled_for, Some(latest));
    assert_eq!(materialized.gaps.len(), 1);
    assert_eq!(
        materialized.gaps[0].reason,
        ReportScheduleGapReason::CoordinatorLag
    );
    assert_eq!(materialized.gaps[0].missed_count, 3);

    let newer = next + Duration::minutes(1);
    let coalesced = repository
        .materialize_schedule(MaterializeReportSchedule {
            schedule_id: "primary".into(),
            decision_policy_snapshot_id: version_id,
            spec_hash: hash('b'),
            expected_next_scheduled_for: next,
            latest_scheduled_for: newer,
            next_scheduled_for: newer + Duration::minutes(1),
            earlier_first_scheduled_for: Some(next),
            earlier_last_scheduled_for: Some(next),
            earlier_missed_count: 1,
        })
        .await
        .expect("coalesce queued occurrence");
    assert_eq!(coalesced.run.scheduled_for, Some(newer));
    assert_eq!(
        coalesced.skipped_run.expect("older queued run").status,
        ReportRunStatus::Skipped
    );
    assert!(
        coalesced
            .gaps
            .iter()
            .any(|gap| { gap.reason == ReportScheduleGapReason::CoalescedByNewerOccurrence })
    );
    let expected_missed_count = materialized
        .gaps
        .iter()
        .chain(coalesced.gaps.iter())
        .map(|gap| gap.missed_count)
        .sum::<i64>();
    let health = repository
        .schedule_health()
        .await
        .expect("load report schedule health");
    assert_eq!(health.missed_occurrence_count_24h, expected_missed_count);
}

pub async fn config_change_skips_occurrence() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let version_id = activate_runtime(&db).await;
    let repository = PgReportRunRepository::new(db);
    let due = Utc::now() - Duration::minutes(1);
    repository
        .reconcile_schedules(
            &version_id,
            vec![ReconcileReportSchedule {
                schedule_id: "primary".into(),
                decision_policy_snapshot_id: version_id,
                spec_hash: hash('b'),
                next_scheduled_for: due,
                enabled: true,
            }],
        )
        .await
        .expect("reconcile initial schedule");
    repository
        .materialize_schedule(MaterializeReportSchedule {
            schedule_id: "primary".into(),
            decision_policy_snapshot_id: version_id,
            spec_hash: hash('b'),
            expected_next_scheduled_for: due,
            latest_scheduled_for: due,
            next_scheduled_for: due + Duration::minutes(1),
            earlier_first_scheduled_for: None,
            earlier_last_scheduled_for: None,
            earlier_missed_count: 0,
        })
        .await
        .expect("materialize queued occurrence");

    let outcome = repository
        .reconcile_schedules(
            &version_id,
            vec![ReconcileReportSchedule {
                schedule_id: "primary".into(),
                decision_policy_snapshot_id: version_id,
                spec_hash: hash('c'),
                next_scheduled_for: due + Duration::minutes(2),
                enabled: true,
            }],
        )
        .await
        .expect("reconcile changed schedule");

    assert_eq!(outcome.skipped_runs.len(), 1);
    assert_eq!(outcome.skipped_runs[0].status, ReportRunStatus::Skipped);
    assert_eq!(
        outcome.skipped_runs[0].terminal_reason,
        Some(ReportRunTerminalReason::ScheduleReconfigured)
    );
    assert_eq!(outcome.gaps.len(), 1);
    assert_eq!(
        outcome.gaps[0].reason,
        ReportScheduleGapReason::ScheduleReconfigured
    );
}
