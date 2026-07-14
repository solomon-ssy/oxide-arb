//! Entry-condition semantic CAS and transactional-outbox integration tests.

use chrono::{Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::ApplyEntryConditionEvaluation,
    entities::{quant_execution_order, quant_order_intent},
    enums::quant::{EntryConditionState, QuantRuntimeMode},
    types::{ConditionTruth, ContentHash, EntryConditionFoldState, EntryConditionInstanceId},
};
use quant_pivot_repository::{
    postgres::{PgEntryConditionRepository, PgRecommendationReportRepository},
    traits::{EntryConditionRepository, RecommendationReportRepository},
};
use quant_pivot_test_support::{
    execution_pg_seed::{
        ReportSeedConfig, seed_report_only_conditional_price_report_on_infra,
        seed_shared_demo_infra,
    },
    pg::setup_pg,
};
use sea_orm::{DatabaseConnection, EntityTrait, PaginatorTrait};
use uuid::Uuid;

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn semantic_revision_and_outbox_claims_are_atomic_and_deduplicated() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let ids = seed_report_only_conditional_price_report_on_infra(
        &db,
        &infra,
        ReportSeedConfig {
            event_id: "condition-eval-event".to_owned(),
            market_id: "condition-eval-market".to_owned(),
            market_question: "Will condition evaluation remain atomic?".to_owned(),
            market_slug: "condition-eval-market".to_owned(),
            token_id: "condition-eval-token".to_owned(),
            trigger_key: "condition-eval-trigger".to_owned(),
        },
    )
    .await;
    let report = PgRecommendationReportRepository::new(db.clone())
        .find_by_id(&ids.report)
        .await
        .expect("report lookup")
        .expect("report");
    assert_eq!(report.runtime_mode, QuantRuntimeMode::ReportOnly);
    let repo = PgEntryConditionRepository::new(db.clone());
    let worker = Uuid::now_v7();
    let now = Utc::now();
    assert_initial_transition(&repo, &ids.condition_instance, worker, now).await;
    let second_time = now + Duration::seconds(2);
    assert_unchanged_evaluation_race(&db, &repo, &ids.condition_instance, worker, second_time)
        .await;
    assert_outbox_claim_race(&db, &repo, second_time + Duration::seconds(1)).await;
    assert_report_only_has_no_execution_rows(&db).await;
}

async fn assert_initial_transition(
    repo: &PgEntryConditionRepository,
    instance_id: &EntryConditionInstanceId,
    worker: Uuid,
    now: chrono::DateTime<Utc>,
) {
    let leased = repo
        .lease_next(worker, now, now + Duration::minutes(1))
        .await
        .expect("lease initial condition")
        .expect("condition due");
    let first = repo
        .apply_evaluation(
            instance_id,
            worker,
            ApplyEntryConditionEvaluation {
                expected_revision: leased.revision,
                expected_lease_epoch: leased.lease_epoch,
                state: EntryConditionState::Waiting,
                truth: ConditionTruth::Unsatisfied,
                evaluation_hash: hash('1'),
                input_fingerprint: hash('2'),
                continuity_hash: hash('3'),
                fold_state: EntryConditionFoldState::default(),
                confirmation_started_at: None,
                evaluated_at: now,
                next_evaluation_at: Some(now + Duration::seconds(1)),
                evaluator_version: 1,
                tree_json: "{\"truth\":\"unsatisfied\"}".to_owned(),
            },
        )
        .await
        .expect("apply first semantic transition");
    assert!(first.transitioned);
    assert_eq!(first.instance.revision, 1);
}

async fn assert_unchanged_evaluation_race(
    db: &DatabaseConnection,
    repo: &PgEntryConditionRepository,
    instance_id: &EntryConditionInstanceId,
    worker: Uuid,
    second_time: chrono::DateTime<Utc>,
) {
    let leased_again = repo
        .lease_next(worker, second_time, second_time + Duration::minutes(1))
        .await
        .expect("lease unchanged evaluation")
        .expect("condition due again");
    let unchanged = ApplyEntryConditionEvaluation {
        expected_revision: leased_again.revision,
        expected_lease_epoch: leased_again.lease_epoch,
        state: EntryConditionState::Waiting,
        truth: ConditionTruth::Unsatisfied,
        evaluation_hash: hash('4'),
        input_fingerprint: hash('5'),
        continuity_hash: hash('3'),
        fold_state: EntryConditionFoldState::default(),
        confirmation_started_at: None,
        evaluated_at: second_time,
        next_evaluation_at: Some(second_time + Duration::seconds(1)),
        evaluator_version: 1,
        tree_json: "{\"truth\":\"unsatisfied\",\"tick\":2}".to_owned(),
    };
    let evaluator_left = PgEntryConditionRepository::new(db.clone());
    let evaluator_right = PgEntryConditionRepository::new(db.clone());
    let (left, right) = tokio::join!(
        evaluator_left.apply_evaluation(instance_id, worker, unchanged.clone(),),
        evaluator_right.apply_evaluation(instance_id, worker, unchanged,),
    );
    assert_eq!(
        usize::from(left.is_ok()) + usize::from(right.is_ok()),
        1,
        "the lease/revision CAS must have one evaluator winner"
    );
    let winner = left.or(right).expect("one evaluator wins");
    assert!(!winner.transitioned);
    assert_eq!(winner.instance.revision, 1);
    assert_eq!(repo.audits(instance_id).await.expect("audits").len(), 2);
}

async fn assert_outbox_claim_race(
    db: &DatabaseConnection,
    repo: &PgEntryConditionRepository,
    claim_time: chrono::DateTime<Utc>,
) {
    let outbox_worker_a = Uuid::now_v7();
    let outbox_worker_b = Uuid::now_v7();
    let outbox_repo_b = PgEntryConditionRepository::new(db.clone());
    let (claimed_a, claimed_b) = tokio::join!(
        repo.claim_pending_evaluations(
            outbox_worker_a,
            claim_time,
            claim_time + Duration::minutes(1),
            10,
        ),
        outbox_repo_b.claim_pending_evaluations(
            outbox_worker_b,
            claim_time,
            claim_time + Duration::minutes(1),
            10,
        ),
    );
    let claimed_a = claimed_a.expect("worker A claim");
    let claimed_b = claimed_b.expect("worker B claim");
    assert_eq!(claimed_a.len() + claimed_b.len(), 2);
    let mut ids_and_kinds = claimed_a
        .iter()
        .chain(&claimed_b)
        .map(|event| (event.evaluation_id.clone(), event.trace_kind.clone()))
        .collect::<Vec<_>>();
    ids_and_kinds.sort();
    ids_and_kinds.dedup();
    assert_eq!(ids_and_kinds.len(), 2);
    assert!(ids_and_kinds.iter().any(|(_, kind)| kind == "applied"));
    assert!(ids_and_kinds.iter().any(|(_, kind)| kind == "observed"));

    let wrong_owner = if let Some(event) = claimed_a.first() {
        repo.mark_evaluation_published(&event.evaluation_id, outbox_worker_b, claim_time)
            .await
    } else if let Some(event) = claimed_b.first() {
        repo.mark_evaluation_published(&event.evaluation_id, outbox_worker_a, claim_time)
            .await
    } else {
        Err(StorageError::InvariantViolation {
            entity: None,
            detail: "neither worker claimed an event".to_owned(),
        })
    };
    assert!(
        matches!(wrong_owner, Err(StorageError::StateConflict { .. })),
        "a non-owner must not acknowledge an outbox row"
    );
}

async fn assert_report_only_has_no_execution_rows(db: &DatabaseConnection) {
    // ReportOnly still owns the complete condition evidence ledger, but no row
    // can reach the signing/submission boundary because no intent is created.
    assert_eq!(
        quant_order_intent::Entity::find()
            .count(db)
            .await
            .expect("intent count"),
        0,
    );
    assert_eq!(
        quant_execution_order::Entity::find()
            .count(db)
            .await
            .expect("execution-order count"),
        0,
    );
}
