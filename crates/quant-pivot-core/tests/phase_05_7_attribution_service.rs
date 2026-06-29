//! Phase 05.7 — attribution service integration tests (Postgres).
//!
//! Requires Docker. Exercises the final attribution sweep: WORM insert,
//! recommendation `Attributed` promotion, skip rules, and idempotency.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_core::{
    execution::{AttributionService, AttributionServiceDeps},
    observability::attribution_fact_writer::AttributionEventWriter,
};
use quant_pivot_models::{
    clickhouse::QuantRecommendationAttributionEventRow,
    entities::{quant_order_intent, quant_position, quant_recommendation},
    enums::{
        execution::{PositionLedgerState, ReconciliationResult},
        quant::{OrderIntentStatus, RecommendationAttributionOutcome, RecommendationStatus},
    },
    types::Price,
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionRepository, PgExecutionOrderRepository, PgExecutionSubmissionRepository,
        PgOrderIntentRepository, PgPositionRepository, PgRecommendationRepository,
        PgReconciliationRepository,
    },
    traits::{
        AttributionRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
        OrderIntentRepository, PositionRepository, RecommendationRepository,
        ReconciliationRepository,
    },
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use quant_pivot_test_support::{
    execution_pg_seed::{
        close_position_full, fill_entry_lot, seed_approved_intent, seed_report_fixture,
    },
    pg::setup_pg,
};
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter,
};

struct NoopAttributionEvents;

impl NoopAttributionEvents {
    fn writer() -> Arc<AttributionEventWriter> {
        let (writer, _worker) = AsyncWriter::new(
            AsyncWriterConfig::new("phase-05-7-attribution").capacity(64),
            |rows: Vec<QuantRecommendationAttributionEventRow>| {
                Box::pin(async move {
                    let _ = rows;
                    Ok(())
                })
            },
            prometheus::IntCounter::new("phase_05_7_attribution_drops", "d").expect("counter"),
            AsyncWriterObservability::default(),
        );
        Arc::new(AttributionEventWriter::new(Arc::new(writer)))
    }
}

fn attribution_service(db: &sea_orm::DatabaseConnection) -> AttributionService {
    let db = db.clone();
    AttributionService::new(AttributionServiceDeps {
        attribution: Arc::new(PgAttributionRepository::new(db.clone()))
            as Arc<dyn AttributionRepository>,
        intents: Arc::new(PgOrderIntentRepository::new(db.clone()))
            as Arc<dyn OrderIntentRepository>,
        recommendations: Arc::new(PgRecommendationRepository::new(db.clone()))
            as Arc<dyn RecommendationRepository>,
        execution_orders: Arc::new(PgExecutionOrderRepository::new(db.clone()))
            as Arc<dyn ExecutionOrderRepository>,
        positions: Arc::new(PgPositionRepository::new(db.clone())) as Arc<dyn PositionRepository>,
        reconciliation: Arc::new(PgReconciliationRepository::new(db))
            as Arc<dyn ReconciliationRepository>,
        attribution_events: NoopAttributionEvents::writer(),
    })
}

async fn resolve_reconciliations_filled(
    db: &sea_orm::DatabaseConnection,
    intent_id: &quant_pivot_models::types::OrderIntentId,
) {
    use quant_pivot_models::entities::quant_reconciliation;

    let rows = quant_reconciliation::Entity::find()
        .filter(quant_reconciliation::Column::OrderIntentId.eq(intent_id.clone()))
        .all(db)
        .await
        .expect("load reconciliations");
    for row in rows {
        let mut active = row.into_active_model();
        active.result = ActiveValue::Set(ReconciliationResult::Filled);
        active.resolved_at = ActiveValue::Set(Some(Utc::now()));
        active.resolved_by = ActiveValue::Set(Some("test".to_owned()));
        active.update(db).await.expect("resolve reconciliation");
    }
}

async fn patch_intent_status(
    db: &sea_orm::DatabaseConnection,
    intent_id: &quant_pivot_models::types::OrderIntentId,
    status: OrderIntentStatus,
) {
    let row = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .expect("load intent")
        .expect("intent row");
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(status);
    active.update(db).await.expect("patch intent status");
}

async fn patch_recommendation_status(
    db: &sea_orm::DatabaseConnection,
    recommendation_id: &quant_pivot_models::types::RecommendationId,
    status: RecommendationStatus,
) {
    let row = quant_recommendation::Entity::find_by_id(recommendation_id.clone())
        .one(db)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(status);
    active
        .update(db)
        .await
        .expect("patch recommendation status");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_filled_exited_marks_attributed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    close_position_full(&submission, &ids, &intent_id, Some(Price::new(dec!(0.8)))).await;
    resolve_reconciliations_filled(&db, &intent_id).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db.clone())
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::FilledExited
    );
    assert!(attribution.max_favorable_excursion_bps.is_some());
    assert!(attribution.max_adverse_excursion_bps.is_none());

    let recommendation = PgRecommendationRepository::new(db)
        .find_by_id(&ids.recommendation)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    assert_eq!(recommendation.status, RecommendationStatus::Attributed);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_cancelled_unfilled() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    patch_intent_status(&db, &intent_id, OrderIntentStatus::Cancelled).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db)
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::CancelledUnfilled
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_failed_unfilled_admission_rejected() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    submission
        .reject_admission(
            &intent_id,
            "liquidity thin".to_owned(),
            Some("check:liquidity".to_owned()),
        )
        .await
        .expect("reject admission");

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db)
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::FailedUnfilled
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_expired_unfilled_via_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    patch_intent_status(&db, &intent_id, OrderIntentStatus::Expired).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db)
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::ExpiredUnfilled
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_expired_unfilled_without_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;

    let rec = quant_recommendation::Entity::find_by_id(ids.recommendation.clone())
        .one(&db)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    let mut active = rec.into_active_model();
    active.status = ActiveValue::Set(RecommendationStatus::Expired);
    active
        .update(&db)
        .await
        .expect("mark recommendation expired");

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db)
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::ExpiredUnfilled
    );
    assert_eq!(attribution.max_adverse_excursion_bps, Some(dec!(0)));
    assert_eq!(attribution.max_favorable_excursion_bps, Some(dec!(0)));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_expired_deferred_with_open_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&submission, &ids, &intent_id).await;

    let rec = quant_recommendation::Entity::find_by_id(ids.recommendation.clone())
        .one(&db)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    let mut active = rec.into_active_model();
    active.status = ActiveValue::Set(RecommendationStatus::Expired);
    active
        .update(&db)
        .await
        .expect("mark recommendation expired");

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 0);
    assert!(summary.skipped >= 1);

    assert!(
        PgAttributionRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("find attribution")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_skips_open_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&submission, &ids, &intent_id).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 0);
    assert_eq!(summary.skipped, 1);

    assert!(
        PgAttributionRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("find attribution")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_skips_unresolvable_reconciliation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    close_position_full(&submission, &ids, &intent_id, None).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 0);
    assert_eq!(summary.skipped, 1);

    assert!(
        PgAttributionRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("find attribution")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_duplicate_is_idempotent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    close_position_full(&submission, &ids, &intent_id, None).await;
    resolve_reconciliations_filled(&db, &intent_id).await;

    let service = attribution_service(&db);
    let first = service.run_pass(Utc::now(), 10).await.expect("first pass");
    assert_eq!(first.written, 1);

    let second = service.run_pass(Utc::now(), 10).await.expect("second pass");
    assert_eq!(second.written, 0);
    assert_eq!(second.skipped, 1);

    let rows = quant_pivot_models::entities::quant_recommendation_attribution::Entity::find()
        .filter(
            quant_pivot_models::entities::quant_recommendation_attribution::Column::RecommendationId
                .eq(ids.recommendation.clone()),
        )
        .all(&db)
        .await
        .expect("count attribution rows");
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_filled_settled() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&submission, &ids, &intent_id).await;
    resolve_reconciliations_filled(&db, &intent_id).await;

    let position = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(intent_id.clone()))
        .one(&db)
        .await
        .expect("load position")
        .expect("position row");
    let mut active = position.into_active_model();
    active.state = ActiveValue::Set(PositionLedgerState::Settled);
    active.closed_at = ActiveValue::Set(Some(Utc::now()));
    active.update(&db).await.expect("mark position settled");

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 1);

    let attribution = PgAttributionRepository::new(db)
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution")
        .expect("attribution row");
    assert_eq!(
        attribution.outcome,
        RecommendationAttributionOutcome::FilledSettled
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_pass_skips_revoked_recommendation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    patch_recommendation_status(&db, &ids.recommendation, RecommendationStatus::Revoked).await;
    patch_intent_status(&db, &intent_id, OrderIntentStatus::Invalidated).await;

    let service = attribution_service(&db);
    let summary = service.run_pass(Utc::now(), 10).await.expect("run pass");
    assert_eq!(summary.written, 0);
    assert_eq!(summary.skipped, 1);

    let attribution = PgAttributionRepository::new(db.clone())
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("find attribution");
    assert!(attribution.is_none());

    let recommendation = PgRecommendationRepository::new(db)
        .find_by_id(&ids.recommendation)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    assert_eq!(recommendation.status, RecommendationStatus::Revoked);
}
