//! Report lifecycle seeding owned by system tests.

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::quant::{NewReportTransaction, RecommendationReportInfo, ReportRunClaim},
    entities::quant_report_run::ActiveModel,
    enums::quant::{
        RecommendationReportStatus, RecommendationStatus, ReportRunStatus, ReportTriggerKind,
    },
    types::{ReportRunId, ReportTriggerKey, WorkerId},
};
use quant_pivot_repository::{
    postgres::{PgRecommendationReportRepository, PgReportRunRepository},
    traits::{RecommendationReportRepository, ReportRunRepository},
};
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection};

/// Persist a complete Prepared artifact, verify its delivery, and atomically
/// publish it. Fixtures must not bypass the durable run/publication FSM.
pub async fn persist_and_publish_report(
    db: &DatabaseConnection,
    transaction: NewReportTransaction,
    trigger_key: &str,
    knowledge_lag_secs: i64,
) -> RecommendationReportInfo {
    let prepared = persist_prepared_report(db, transaction, trigger_key, knowledge_lag_secs).await;
    let now = Utc::now();
    let report_id = prepared.recommendation_report_id;
    let repository = PgRecommendationReportRepository::new(db.clone());
    let delivery_worker = WorkerId::from_v7();
    let claimed = repository
        .claim_fact_delivery(delivery_worker.clone(), 600)
        .await
        .expect("claim report delivery")
        .expect("seeded report delivery is claimable");
    assert_eq!(claimed.recommendation_report_id, report_id);
    repository
        .verify_and_publish_report(&report_id, delivery_worker, now)
        .await
        .expect("publish seeded report")
        .into_applied()
        .expect("seeded report delivery claim must remain held")
        .report
}

/// Persist only the Prepared artifact and Pending delivery for delivery-worker tests.
pub async fn persist_prepared_report(
    db: &DatabaseConnection,
    mut transaction: NewReportTransaction,
    trigger_key: &str,
    knowledge_lag_secs: i64,
) -> RecommendationReportInfo {
    let clock = PgReportRunRepository::new(db.clone());
    let now = loop {
        let now = clock
            .database_time()
            .await
            .expect("read database time for report fixture");
        if now >= transaction.report.decision_at {
            break now;
        }
        let wait = (transaction.report.decision_at - now)
            .to_std()
            .expect("positive database clock wait");
        tokio::time::sleep(wait).await;
    };
    let worker_id = WorkerId::from_v7();
    let lease_expires_at = now + Duration::minutes(10);
    let report_run_id = ReportRunId::from_v7();
    transaction.report.status = RecommendationReportStatus::Prepared;
    transaction.report.published_at = None;
    transaction.report.successor_report_id = None;
    transaction.report.superseded_at = None;
    transaction.report.obsoleted_at = None;
    transaction.report.revoked_at = None;
    transaction.report.expired_at = None;
    transaction.report.status_reason = None;
    for recommendation in &mut transaction.recommendations {
        recommendation.status = RecommendationStatus::Prepared;
    }

    ActiveModel {
        report_run_id: ActiveValue::Set(report_run_id.clone()),
        trigger_kind: ActiveValue::Set(ReportTriggerKind::Scheduled),
        trigger_key: ActiveValue::Set(
            ReportTriggerKey::parse(trigger_key).expect("valid report fixture trigger key"),
        ),
        schedule_id: ActiveValue::Set(Some("test_fixture".into())),
        request_id: ActiveValue::Set(None),
        retry_of_run_id: ActiveValue::Set(None),
        scheduled_for: ActiveValue::Set(Some(transaction.report.decision_at)),
        requested_at: ActiveValue::Set(transaction.report.decision_at),
        status: ActiveValue::Set(ReportRunStatus::Running),
        started_at: ActiveValue::Set(Some(transaction.report.decision_at)),
        decision_at: ActiveValue::Set(Some(transaction.report.decision_at)),
        heartbeat_at: ActiveValue::Set(Some(now)),
        lease_expires_at: ActiveValue::Set(Some(lease_expires_at)),
        finished_at: ActiveValue::Set(None),
        lease_owner: ActiveValue::Set(Some(worker_id.clone())),
        decision_policy_snapshot_id: ActiveValue::Set(Some(
            transaction.report.decision_policy_snapshot_id.clone(),
        )),
        top_n: ActiveValue::Set(Some(transaction.report.top_n)),
        knowledge_lag_secs: ActiveValue::Set(Some(knowledge_lag_secs)),
        output_report_id: ActiveValue::Set(None),
        terminal_reason: ActiveValue::Set(None),
        error_code: ActiveValue::Set(None),
        error_summary: ActiveValue::Set(None),
    }
    .insert(db)
    .await
    .expect("seed running report run");

    let repository = PgRecommendationReportRepository::new(db.clone());
    repository
        .create_prepared_report(
            ReportRunClaim {
                report_run_id,
                lease_owner: worker_id,
                lease_expires_at,
            },
            transaction,
        )
        .await
        .expect("seed prepared report")
        .report
}
