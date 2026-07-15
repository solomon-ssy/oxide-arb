//! Attribution ledger-guard SQL integration tests (Postgres + testcontainers).
//!
//! Regression coverage for `qp_order_intent_status` enum filters used by the
//! expired-recommendation attribution path.

use quant_pivot_models::{
    entities::{quant_order_intent, quant_recommendation},
    enums::quant::{OrderIntentStatus, RecommendationStatus},
    types::OrderIntentId,
};
use quant_pivot_repository::{
    postgres::PgRecommendationRepository, traits::RecommendationRepository,
};
use quant_pivot_test_support::{
    execution_pg_seed::{ExecutionTxnIds, seed_approved_intent, seed_report_fixture},
    pg::setup_pg,
};
use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait, IntoActiveModel};

async fn mark_recommendation_expired(db: &sea_orm::DatabaseConnection, ids: &ExecutionTxnIds) {
    let rec = quant_recommendation::Entity::find_by_id(ids.recommendation.clone())
        .one(db)
        .await
        .expect("load recommendation")
        .expect("recommendation row");
    let mut active = rec.into_active_model();
    active.status = ActiveValue::Set(RecommendationStatus::Expired);
    active
        .update(db)
        .await
        .expect("mark recommendation expired");
}

async fn patch_intent_status(
    db: &sea_orm::DatabaseConnection,
    intent_id: &OrderIntentId,
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn find_unfilled_attribution_candidates_succeeds_without_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    mark_recommendation_expired(&db, &ids).await;

    let repo = PgRecommendationRepository::new(db);
    let candidates = repo
        .find_unfilled_attribution_candidates(10)
        .await
        .expect("expired attribution query must not fail on enum cast");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].recommendation_id, ids.recommendation);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn find_unfilled_attribution_candidates_includes_terminal_intent_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    patch_intent_status(&db, &intent_id, OrderIntentStatus::Expired).await;
    mark_recommendation_expired(&db, &ids).await;

    let repo = PgRecommendationRepository::new(db);
    let candidates = repo
        .find_unfilled_attribution_candidates(10)
        .await
        .expect("terminal intent must not break expired ledger guards");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].recommendation_id, ids.recommendation);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn find_unfilled_attribution_candidates_excludes_non_terminal_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let _intent_id = seed_approved_intent(&db, &ids).await;
    mark_recommendation_expired(&db, &ids).await;

    let repo = PgRecommendationRepository::new(db);
    let candidates = repo
        .find_unfilled_attribution_candidates(10)
        .await
        .expect("non-terminal intent guard query");

    assert!(
        candidates.is_empty(),
        "approved intent still in flight must defer expired attribution"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn blocks_attribution_reflects_non_terminal_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let _intent_id = seed_approved_intent(&db, &ids).await;

    let repo = PgRecommendationRepository::new(db);
    let blocked = repo
        .recommendation_blocks_final_attribution(&ids.recommendation)
        .await
        .expect("blocks_attribution query");

    assert!(blocked);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn blocks_attribution_clear_without_intents() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;

    let repo = PgRecommendationRepository::new(db);
    let blocked = repo
        .recommendation_blocks_final_attribution(&ids.recommendation)
        .await
        .expect("blocks_attribution query");

    assert!(!blocked);
}
