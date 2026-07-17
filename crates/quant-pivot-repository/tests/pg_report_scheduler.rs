//! Phase 11.8 durable report coordinator integration tests (`PostgreSQL`).

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::{
        ClaimReportSchedule, MaterializeReportSchedule, NewReportRun, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, ReconcileReportSchedule, ReportRunClaimConfig,
    },
    enums::{
        quant::{
            ReportRunStatus, ReportRunTerminalReason, ReportScheduleGapReason, ReportTriggerKind,
        },
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::RUNTIME_CONFIG_SCHEMA_VERSION,
    types::{ContentHash, ReportRunId, RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    postgres::{PgReportRunRepository, PgRuntimeConfigVersionRepository},
    traits::{ReportRunRepository, RuntimeConfigVersionRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use uuid::Uuid;

fn hash(byte: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", byte.to_string().repeat(64)))
        .expect("valid content hash")
}

async fn activate_runtime(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let repository = PgRuntimeConfigVersionRepository::new(db.clone());
    let version_id = RuntimeConfigVersionId::from_v7();
    repository
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: version_id.clone(),
            config_hash: hash('a'),
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-report-scheduler".to_owned(),
            reason: "scheduler integration fixture".to_owned(),
        })
        .await
        .expect("create runtime config");
    repository
        .activate_version(NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: version_id.clone(),
            runtime_config_approval_id: None,
            activated_by: "pg-report-scheduler".to_owned(),
            reason: "scheduler integration fixture".to_owned(),
            activation_kind: RuntimeConfigActivationKind::Initial,
            previous_runtime_config_version_id: None,
            rollback_target_version_id: None,
            audit_event_id: None,
        })
        .await
        .expect("activate runtime config");
    version_id
}

fn ad_hoc(request_id: &str) -> NewReportRun {
    NewReportRun {
        report_run_id: ReportRunId::from_v7(),
        trigger_kind: ReportTriggerKind::AdHoc,
        trigger_key: format!("ad_hoc:{request_id}"),
        schedule_id: None,
        request_id: Some(request_id.to_owned()),
        retry_of_run_id: None,
        scheduled_for: None,
        requested_at: Utc::now(),
        status: ReportRunStatus::Queued,
        top_n: None,
        knowledge_lag_secs: None,
    }
}

fn claim_config(version_id: &RuntimeConfigVersionId) -> ReportRunClaimConfig {
    ReportRunClaimConfig {
        runtime_config_version_id: version_id.clone(),
        ad_hoc_default_top_n: 20,
        ad_hoc_default_knowledge_lag_secs: 10,
        schedules: vec![ClaimReportSchedule {
            schedule_id: "primary".to_owned(),
            top_n: 20,
            knowledge_lag_secs: 10,
        }],
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn two_coordinators_claim_one_global_run() {
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
        first.claim_next_run(Uuid::now_v7(), 120, 300, claim_config(&version_id)),
        second.claim_next_run(Uuid::now_v7(), 120, 300, claim_config(&version_id)),
    );
    let claimed = [left.expect("first replica"), right.expect("second replica")]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, ReportRunStatus::Running);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn restart_coalesces_latest_and_records_aggregate_gap() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let version_id = activate_runtime(&db).await;
    let repository = PgReportRunRepository::new(db);
    let first_due = Utc::now() - Duration::minutes(6);
    repository
        .reconcile_schedules(
            &version_id,
            vec![ReconcileReportSchedule {
                schedule_id: "primary".to_owned(),
                runtime_config_version_id: version_id.clone(),
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
            schedule_id: "primary".to_owned(),
            runtime_config_version_id: version_id.clone(),
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
            schedule_id: "primary".to_owned(),
            runtime_config_version_id: version_id,
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn config_change_skips_old_queued_occurrence() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let version_id = activate_runtime(&db).await;
    let repository = PgReportRunRepository::new(db);
    let due = Utc::now() - Duration::minutes(1);
    repository
        .reconcile_schedules(
            &version_id,
            vec![ReconcileReportSchedule {
                schedule_id: "primary".to_owned(),
                runtime_config_version_id: version_id.clone(),
                spec_hash: hash('b'),
                next_scheduled_for: due,
                enabled: true,
            }],
        )
        .await
        .expect("reconcile initial schedule");
    repository
        .materialize_schedule(MaterializeReportSchedule {
            schedule_id: "primary".to_owned(),
            runtime_config_version_id: version_id.clone(),
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
                schedule_id: "primary".to_owned(),
                runtime_config_version_id: version_id.clone(),
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
