//! Point-in-time feedback-cohort repository contracts.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
    domain::quant::{
        ExecutionAttemptReconciliationResult, ExecutionRollupReconciliationResult,
        FeedbackCohortCandidate, FeedbackCohortCursor, FeedbackCohortPage, FeedbackCohortPageQuery,
        FeedbackCohortSnapshot, FeedbackCohortWindow, InsertResolutionOutcomeResult,
        NewReconciliation, RecommendationExecutionRollupInfo,
    },
    entities::{
        quant_execution_order::Entity as QuantExecutionOrderEntity,
        quant_order_intent::{
            ActiveModel as QuantOrderIntentActiveModel, Entity as QuantOrderIntentEntity,
        },
        quant_recommendation::{
            ActiveModel as QuantRecommendationActiveModel, Entity as QuantRecommendationEntity,
        },
        quant_reconciliation::Entity as QuantReconciliationEntity,
    },
    enums::{
        execution::{ReconciliationEvidenceKind, ReconciliationResult, VenueOrderStatus},
        quant::{ExecutionOrderState, FeedbackCohort, OrderIntentStatus, RecommendationStatus},
    },
    types::{
        ContentHash, EvmBlockHash, EvmTransactionHash, PayoutRatio, RecommendationId,
        ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId, Shares, TokenId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExecutionAttemptOutcomeRepository, PgFeedbackCohortRepository,
        PgRecommendationExecutionRollupRepository, PgRecommendationResolutionOutcomeRepository,
    },
    traits::{
        ExecutionAttemptOutcomeRepository, FeedbackCohortRepository,
        RecommendationExecutionRollupRepository, RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::execution_pg_seed::{
        ExecutionTxnIds, FEEDBACK_SCALE_REPORT_COUNT, FEEDBACK_SCALE_TOTAL, ReportSeedConfig,
        SharedDemoInfra, entry_execution_order, fixture_profile_ref, seed_approved_intent,
        seed_feedback_scale, seed_report_fixture, seed_report_on_infra,
        seed_settlement_report_fixture, seed_shared_demo_infra,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait,
    IntoActiveModel, Statement,
};

pub async fn candidate_page_keyset_frozen() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let cutoff = db.statement_time().await;
    let window = feedback_window(ids.decision_at - Duration::minutes(1), cutoff);
    let repository = PgFeedbackCohortRepository::new(db);

    let page = repository
        .list_page(page_query(
            FeedbackCohort::PolicyEvaluation,
            window,
            None,
            1,
        ))
        .await
        .expect("read first feedback page");

    assert_eq!(page.candidates().len(), 1);
    assert_eq!(
        page.candidates()[0].context().recommendation_id(),
        ids.recommendation
    );
    assert_eq!(page.next_cursor(), None);
}

pub async fn cohort_truth_planes_exact() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    let resolution_repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    resolution_repository
        .reconcile_fact(
            &ids.recommendation,
            &resolution_fact(
                &ids,
                Utc::now() - Duration::minutes(2),
                Utc::now() - Duration::minutes(1),
                71,
            ),
        )
        .await
        .expect("seal visible resolution outcome");
    let execution = seed_unfilled_execution_rollup(&db, &ids).await;
    let cutoff = db.statement_time().await;
    let window_start = ids.decision_at - Duration::minutes(1);
    let repository = PgFeedbackCohortRepository::new(db.clone());

    let model = only_candidate(
        &repository
            .list_page(page_query(
                FeedbackCohort::ModelLearning,
                feedback_window(window_start, cutoff),
                None,
                10,
            ))
            .await
            .expect("read model-learning plane"),
    );
    assert!(model.resolution_outcome().is_some());
    assert!(model.execution_rollup().is_none());

    let execution_candidate = only_candidate(
        &repository
            .list_page(page_query(
                FeedbackCohort::ExecutionLearning,
                feedback_window(window_start, cutoff),
                None,
                10,
            ))
            .await
            .expect("read execution-learning plane"),
    );
    assert!(execution_candidate.resolution_outcome().is_none());
    assert_eq!(
        execution_candidate
            .execution_rollup()
            .expect("visible execution rollup")
            .rollup_hash,
        execution.rollup_hash
    );

    let policy = only_candidate(
        &repository
            .list_page(page_query(
                FeedbackCohort::PolicyEvaluation,
                feedback_window(window_start, cutoff),
                None,
                10,
            ))
            .await
            .expect("read policy-evaluation plane"),
    );
    assert!(policy.resolution_outcome().is_some());
    assert!(policy.execution_rollup().is_some());

    corrupt_execution_rollup_hash(&db, ids.recommendation).await;
    assert!(
        repository
            .list_page(page_query(
                FeedbackCohort::ModelLearning,
                feedback_window(window_start, cutoff),
                None,
                10,
            ))
            .await
            .is_ok(),
        "an execution-plane corruption must not block ModelLearning"
    );
    assert!(matches!(
        repository
            .list_page(page_query(
                FeedbackCohort::ExecutionLearning,
                feedback_window(window_start, cutoff),
                None,
                10,
            ))
            .await
            .expect_err("consumed execution corruption must fail closed"),
        StorageError::InvariantViolation { .. }
    ));
}

pub async fn cutoff_excludes_late_pages() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let first = seed_feedback_report(&db, &infra, 1).await;
    let second = seed_feedback_report(&db, &infra, 2).await;
    let window_start = first.decision_at.min(second.decision_at) - Duration::minutes(1);
    let cutoff = db.statement_time().await;
    let repository = PgFeedbackCohortRepository::new(db.clone());
    let first_page = repository
        .list_page(page_query(
            FeedbackCohort::PolicyEvaluation,
            feedback_window(window_start, cutoff),
            None,
            1,
        ))
        .await
        .expect("read first frozen page");
    let first_candidate = only_candidate(&first_page);
    let first_cursor = first_page.next_cursor().expect("one remaining candidate");
    let late_target = if first_candidate.context().recommendation_id() == first.recommendation {
        &second
    } else {
        &first
    };

    let late_outcome = PgRecommendationResolutionOutcomeRepository::new(db.clone())
        .reconcile_fact(
            &late_target.recommendation,
            &resolution_fact(
                late_target,
                cutoff - Duration::minutes(2),
                cutoff - Duration::minutes(1),
                72,
            ),
        )
        .await
        .expect("seal truth after the frozen cutoff");
    let late_available_at = match late_outcome {
        InsertResolutionOutcomeResult::Inserted(outcome)
        | InsertResolutionOutcomeResult::AlreadyPresent(outcome) => outcome.available_at,
    };
    assert!(late_available_at > cutoff);
    let third = seed_feedback_report(&db, &infra, 3).await;

    let old_page = repository
        .list_page(page_query(
            FeedbackCohort::ModelLearning,
            feedback_window(window_start, cutoff),
            Some(first_cursor),
            2,
        ))
        .await
        .expect("resume old frozen window");
    assert_eq!(old_page.candidates().len(), 1);
    assert_eq!(
        old_page.candidates()[0].context().recommendation_id(),
        late_target.recommendation
    );
    assert!(old_page.candidates()[0].resolution_outcome().is_none());
    assert_eq!(old_page.next_cursor(), None);

    let new_cutoff = db.statement_time().await;
    let mature_same_window = repository
        .list_page(page_query_with_truth(
            FeedbackCohort::ModelLearning,
            feedback_window(window_start, cutoff),
            new_cutoff,
            Some(first_cursor),
            2,
        ))
        .await
        .expect("re-read fixed decision window at later truth cutoff");
    assert_eq!(mature_same_window.candidates().len(), 1);
    assert_eq!(
        mature_same_window.candidates()[0]
            .context()
            .recommendation_id(),
        late_target.recommendation
    );
    assert!(
        mature_same_window.candidates()[0]
            .resolution_outcome()
            .is_some()
    );
    assert!(
        mature_same_window
            .candidates()
            .iter()
            .all(|candidate| candidate.context().recommendation_id() != third.recommendation),
        "later truth visibility must not widen the frozen decision cohort"
    );

    let new_page = repository
        .list_page(page_query(
            FeedbackCohort::ModelLearning,
            feedback_window(window_start, new_cutoff),
            Some(first_cursor),
            2,
        ))
        .await
        .expect("resume under next cycle cutoff");
    assert_eq!(new_page.candidates().len(), 2);
    assert!(
        new_page
            .candidates()
            .iter()
            .find(|candidate| {
                candidate.context().recommendation_id() == late_target.recommendation
            })
            .expect("late-truth recommendation")
            .resolution_outcome()
            .is_some()
    );
    assert!(
        new_page
            .candidates()
            .iter()
            .any(|candidate| candidate.context().recommendation_id() == third.recommendation)
    );
}

pub async fn keyset_reads_without_duplicates() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let mut reports = Vec::with_capacity(FEEDBACK_SCALE_REPORT_COUNT);
    for ordinal in 0..FEEDBACK_SCALE_REPORT_COUNT {
        reports.push(seed_feedback_report(&db, &infra, ordinal + 100).await);
    }
    let window_start = reports
        .iter()
        .map(|ids| ids.decision_at)
        .min()
        .expect("scale reports")
        - Duration::minutes(1);
    let report_cutoff = db.statement_time().await;
    seed_feedback_scale(&db, &reports, window_start, report_cutoff).await;
    let cutoff = db.statement_time().await;
    let repository = PgFeedbackCohortRepository::new(db);
    let mut after = None;
    let mut seen = HashSet::with_capacity(FEEDBACK_SCALE_TOTAL);
    let mut previous = None;
    let mut page_count = 0_usize;

    loop {
        let page = repository
            .list_page(page_query(
                FeedbackCohort::PolicyEvaluation,
                feedback_window(window_start, cutoff),
                after,
                257,
            ))
            .await
            .expect("read scale keyset page");
        assert!(!page.candidates().is_empty());
        assert!(page.candidates().len() <= 257);
        for candidate in page.candidates() {
            let cursor = candidate.cursor();
            if let Some(previous_cursor) = previous {
                assert!(previous_cursor < cursor);
            }
            previous = Some(cursor);
            assert!(seen.insert(candidate.context().recommendation_id()));
        }
        page_count += 1;
        let Some(next) = page.next_cursor() else {
            break;
        };
        assert_eq!(
            Some(next),
            page.candidates()
                .last()
                .map(FeedbackCohortCandidate::cursor)
        );
        after = Some(next);
    }

    assert_eq!(seen.len(), FEEDBACK_SCALE_TOTAL);
    assert_eq!(page_count, FEEDBACK_SCALE_TOTAL.div_ceil(257));
}

fn page_query(
    cohort: FeedbackCohort,
    window: FeedbackCohortWindow,
    after: Option<FeedbackCohortCursor>,
    limit: u32,
) -> FeedbackCohortPageQuery {
    let truth_cutoff = window.cutoff();
    page_query_with_truth(cohort, window, truth_cutoff, after, limit)
}

fn page_query_with_truth(
    cohort: FeedbackCohort,
    window: FeedbackCohortWindow,
    truth_cutoff: DateTime<Utc>,
    after: Option<FeedbackCohortCursor>,
    limit: u32,
) -> FeedbackCohortPageQuery {
    let snapshot =
        FeedbackCohortSnapshot::try_new(window, truth_cutoff).expect("valid feedback snapshot");
    FeedbackCohortPageQuery::try_new(cohort, snapshot, after, limit)
        .expect("valid feedback page query")
}

fn feedback_window(window_start: DateTime<Utc>, cutoff: DateTime<Utc>) -> FeedbackCohortWindow {
    FeedbackCohortWindow::try_new(fixture_profile_ref(), window_start, cutoff)
        .expect("valid feedback window")
}

fn only_candidate(page: &FeedbackCohortPage) -> FeedbackCohortCandidate {
    assert_eq!(page.candidates().len(), 1);
    page.candidates().first().expect("one candidate").clone()
}

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

fn resolution_fact(
    ids: &ExecutionTxnIds,
    resolved_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    log_index: u64,
) -> MarketResolutionRow {
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: ids.market.as_str().into(),
        token_ids: [TokenId::new(&ids.token), TokenId::new("999999")],
        payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
        resolved_at: resolved_at.timestamp_millis(),
        observed_at: observed_at.timestamp_millis(),
        source_block_number: 71,
        source_block_hash: EvmBlockHash::parse(format!("0x{}", "71".repeat(32)))
            .expect("block hash"),
        source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "72".repeat(32)))
            .expect("transaction hash"),
        source_log_index: log_index,
        source_checkpoint_hash: hash('7'),
    })
    .expect("sealed resolution fact")
}

async fn seed_unfilled_execution_rollup(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
) -> RecommendationExecutionRollupInfo {
    let order_intent_id = seed_approved_intent(db, ids).await;
    let terminal_at = Utc::now() - Duration::seconds(1);
    let submitted_at = terminal_at - Duration::seconds(1);
    let mut order = entry_execution_order(&order_intent_id, ids);
    order.state = ExecutionOrderState::Failed;
    order.venue_status = Some(VenueOrderStatus::Rejected);
    order.submitted_at = Some(submitted_at);
    order.cancelled_at = Some(terminal_at);
    let entry_execution_order_id = order.execution_order_id;
    QuantExecutionOrderEntity::insert(order.into_active_model())
        .exec(db)
        .await
        .expect("persist submitted unfilled entry order");
    QuantReconciliationEntity::insert(
        NewReconciliation {
            reconciliation_id: ReconciliationId::from_v7(),
            execution_order_id: entry_execution_order_id,
            order_intent_id,
            result: ReconciliationResult::NotFilled,
            evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
                kind: ReconciliationEvidenceKind::ClobOrderStatus,
                observed_at: terminal_at,
                detail: "feedback cohort unfilled execution".to_owned(),
                venue_ref: None,
                shares: Some(Shares::ZERO),
                price: None,
                fee_evidence: None,
            }]),
            venue_filled_shares: Some(Shares::ZERO),
            venue_avg_price: None,
            expected_cash_delta_usd: None,
            venue_cash_delta_usd: None,
            realized_pnl_usd: None,
            resolved_by: Some("feedback-cohort-contract".to_owned()),
            resolved_at: Some(terminal_at),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("persist unfilled entry reconciliation");
    let intent = QuantOrderIntentEntity::find_by_id(order_intent_id)
        .one(db)
        .await
        .expect("read submitted intent")
        .expect("submitted intent");
    let mut active: QuantOrderIntentActiveModel = intent.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::AuthorizationRejected);
    active.updated_at = ActiveValue::Set(terminal_at);
    active.update(db).await.expect("mark intent rejected");

    match PgExecutionAttemptOutcomeRepository::new(db.clone())
        .reconcile_intent(&order_intent_id, db.statement_time().await)
        .await
        .expect("seal unfilled execution outcome")
    {
        ExecutionAttemptReconciliationResult::Inserted(_) => {}
        ExecutionAttemptReconciliationResult::AlreadyPresent(_)
        | ExecutionAttemptReconciliationResult::Deferred(_) => {
            panic!("complete isolated execution source must insert")
        }
    }
    let recommendation = QuantRecommendationEntity::find_by_id(ids.recommendation)
        .one(db)
        .await
        .expect("read recommendation")
        .expect("recommendation");
    let closed_at = db.statement_time().await;
    let mut active: QuantRecommendationActiveModel = recommendation.into_active_model();
    active.status = ActiveValue::Set(RecommendationStatus::Expired);
    active.status_changed_at = ActiveValue::Set(closed_at);
    active
        .update(db)
        .await
        .expect("close recommendation authority");
    match PgRecommendationExecutionRollupRepository::new(db.clone())
        .reconcile_recommendation(ids.recommendation, db.statement_time().await)
        .await
        .expect("seal final recommendation execution rollup")
    {
        ExecutionRollupReconciliationResult::Inserted(rollup) => rollup,
        ExecutionRollupReconciliationResult::AlreadyPresent(_)
        | ExecutionRollupReconciliationResult::Deferred(_) => {
            panic!("complete isolated recommendation graph must insert")
        }
    }
}

async fn corrupt_execution_rollup_hash(
    db: &DatabaseConnection,
    recommendation_id: RecommendationId,
) {
    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_execution_rollup \
         DISABLE TRIGGER trg_quant_execution_rollup_append_only",
    )
    .await
    .expect("disable WORM trigger for corruption fixture");
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_recommendation_execution_rollup \
         SET rollup_hash = $1 WHERE recommendation_id = $2",
        [hash('f').into(), recommendation_id.into()],
    ))
    .await
    .expect("corrupt execution outcome hash");
    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_execution_rollup \
         ENABLE TRIGGER trg_quant_execution_rollup_append_only",
    )
    .await
    .expect("restore WORM trigger");
}

async fn seed_feedback_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    ordinal: usize,
) -> ExecutionTxnIds {
    seed_report_on_infra(
        db,
        infra,
        ReportSeedConfig {
            event_id: format!("feedback-event-{ordinal}"),
            market_id: format!("0xfeedback-market-{ordinal}"),
            market_question: format!("Will feedback fixture {ordinal} settle?"),
            market_slug: format!("feedback-fixture-{ordinal}"),
            token_id: format!("{}", 80_000 + ordinal),
            trigger_key: format!("feedback-cohort:{ordinal}"),
        },
    )
    .await
}
