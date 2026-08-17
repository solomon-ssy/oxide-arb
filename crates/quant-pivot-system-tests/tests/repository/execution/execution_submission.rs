//! Execution-submission persistence system contracts.
//!
//! Requires Docker. Exercises the money-critical cross-table transactions:
//! claim (double-submit guard), capital lock on write-ahead, and venue-result
//! settlement (full fill → spent + position; ambiguous → hold + reconcile;
//! rejected → release), plus boot recovery of in-flight orders.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_ORDER_INTENT, QUANT_RECOMMENDATION},
};
use quant_pivot_models::{
    domain::{
        governance::NewOperationLog,
        quant::{
            ApproveOrderIntent, ApproveOrderIntentOutcome, CapitalReconcileSettlement,
            CapitalSettlement, CumulativePositionFill, ExecutionIdentityEnrichment,
            ExecutionIdentityRefs, ExecutionTradeObservation, ExitLedgerWrite,
            NewCapitalAllocation, NewExecutionOrder, NewFeatureParityState, NewMarketSelection,
            NewOrderIntent, NewReconciliation, NewReportTransaction, PositionExit, PositionFill,
            ReconciliationLedgerWrite, ReportRunClaim, SubmissionLedgerWrite,
        },
    },
    entities::{
        operation_log::Entity,
        quant_entry_condition_audit::Entity as QuantEntryConditionAuditEntity,
        quant_execution_order::{Column, Entity as QuantExecutionOrderEntity},
        quant_feature_parity_state::Entity as QuantFeatureParityStateEntity,
        quant_order_intent::Entity as QuantOrderIntentEntity,
        quant_recommendation::Entity as QuantRecommendationEntity,
        quant_report_run::ActiveModel,
    },
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, ExitReason, ExitState, OrderIntentKind,
            OrderTypeKind, PositionLedgerState, ReconciliationEvidenceKind, ReconciliationResult,
            VenueOrderStatus, VenueTradeStatus,
        },
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, EntryConditionState, ExecutionOrderState,
            ExitSettlementMode, FeatureParityLatchState, FeatureParityStateTransition,
            OrderIntentStatus, OutcomeSide, QuantRuntimeMode, RecommendationReportStatus,
            RecommendationStatus, RedeemPolicy, ReportFactDeliveryStatus, ReportKind,
            ReportRunStatus, ReportRunTerminalReason, ReportTriggerKind,
        },
        rbac::ResourceType,
    },
    types::{
        AccountSnapshotId, Bps, CalibrationArtifactId, CapitalAllocationId, ContentHash,
        DecisionPolicySnapshotId, EntryConditionInstanceId, EntryMakerRebateTerms, EntryOrderSpec,
        EventId, EvmTransactionHash, ExecutionAccountId, ExecutionOrderId, ExitPolicySpec,
        FeatureParityStateId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId,
        OperationDetailDocument, OperationLogId, OpportunisticExitPolicy, OrderAmount, OrderId,
        OrderIntentId, PendingScaleOut, PortfolioPlanId, Price, Probability, RecommendationId,
        RecommendationReportId, ReconciliationEvidence, ReconciliationEvidenceChain,
        ReconciliationId, ReportDataQualitySnapshotId, ReportRunId, ReportTriggerKey, RoleCode,
        SelectionExclusionSummary, Shares, ThesisInvalidationPolicy, TokenId,
        TradePolicyCohortProvenance, Usd, UserId, VenueOrderAmount, VenueTradeId, WorkerId,
        factor::FactorServingPlane,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgEntryConditionRepository, PgEventRepository,
        PgExecutionSubmissionRepository, PgMarketRepository, PgMarketSelectionRepository,
        PgOrderIntentRepository, PgPolicyRepository, PgPositionRepository,
        PgRecommendationReportRepository, PgRecommendationRepository, PgReconciliationRepository,
        PgReportRunRepository,
    },
    traits::{
        CapitalAllocationRepository, EntryConditionRepository, EventRepository,
        ExecutionSubmissionRepository, MarketRepository, MarketSelectionRepository,
        OrderIntentRepository, PolicyRepository, PositionRepository,
        RecommendationReportRepository, RecommendationRepository, ReconciliationRepository,
        ReportRunRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        SelectorFixture,
        catalog_fixtures::{make_event, make_market},
        execution_pg_seed,
        execution_pg_seed::{
            ExecutionTxnIds, ReportBuildOptions, ReportSeedConfig, SharedDemoInfra,
            build_custom_report_transaction, claim_entry_for_test, enable_test_admission,
            entry_claim_for_test, fixture_profile_ref, prepared_order, seed_price_report,
            seed_shared_demo_infra,
        },
        policy_fixtures::bootstrap_default_policy_bundle,
        report_lifecycle_seed::{persist_and_publish_report, persist_prepared_report},
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend,
    EntityTrait, IntoActiveModel, QueryFilter, Statement,
};

/// shares (100) * `limit_price` (0.6).
const NOTIONAL: Decimal = dec!(60);
const PARTIAL_SHARES: Decimal = dec!(40);
const PARTIAL_SPENT: Decimal = dec!(24);

// ── Tests ────────────────────────────────────────────────────────────────────

pub async fn claim_guards_against_submit() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    let claim = entry_claim_for_test(&db, &intent_id).await;
    let (claimed, consumed) = submission
        .claim_for_submission(claim.clone())
        .await
        .expect("first claim succeeds");
    assert_eq!(claimed.status, OrderIntentStatus::AdmissionPending);
    assert_eq!(consumed.state, EntryConditionState::Consumed);

    let second = submission.claim_for_submission(claim).await;
    assert!(
        second.is_err(),
        "a second concurrent claim must fail (intent no longer submittable)",
    );
}

pub async fn entry_condition_artifact_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let ids = seed_price_report(
        &db,
        &infra,
        ReportSeedConfig {
            event_id: "condition-worm-event".to_owned(),
            market_id: "condition-worm-market".to_owned(),
            market_question: "Will condition evidence remain immutable?".to_owned(),
            market_slug: "condition-worm-market".to_owned(),
            token_id: "condition-worm-token".to_owned(),
            trigger_key: "condition-worm-trigger".to_owned(),
        },
    )
    .await;
    let condition = PgEntryConditionRepository::new(db.clone())
        .find_instance(&ids.condition_instance)
        .await
        .expect("condition lookup")
        .expect("condition instance");
    let artifact_id = condition.artifact_id.expect("conditional artifact");
    let artifact_update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_entry_condition_artifact SET artifact_hash = artifact_hash \
             WHERE artifact_id = $1",
            [artifact_id.as_uuid().into()],
        ))
        .await;
    assert!(
        artifact_update.is_err(),
        "entry-condition artifact UPDATE must be rejected by the WORM trigger"
    );
    let audit_delete = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_entry_condition_audit WHERE condition_instance_id = $1",
            [condition.condition_instance_id.as_uuid().into()],
        ))
        .await;
    assert!(
        audit_delete.is_err(),
        "entry-condition audit DELETE must be rejected by the WORM trigger"
    );
}

pub async fn concurrent_approval_one_truth() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    seed_approval_governance(&db, &ids.decision_policy_snapshot).await;

    let intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_pending_intent_id(&ids, intent_id),
            new_allocation_for(&ids, intent_id),
        )
        .await
        .expect("create pending intent");

    let mut first_entry = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("load pending intent")
        .expect("pending intent")
        .entry_order_json;
    let mut second_entry = first_entry.clone();
    first_entry.amount = OrderAmount::Shares(Shares::new(dec!(80)));
    second_entry.amount = OrderAmount::Shares(Shares::new(dec!(70)));

    let first_repo = PgOrderIntentRepository::new(db.clone());
    let second_repo = PgOrderIntentRepository::new(db.clone());
    let now = Utc::now();
    let (first, second) = tokio::join!(
        first_repo.approve(
            &intent_id,
            ApproveOrderIntent {
                approved_by: UserId::from_v7(),
                approval_reason: "first concurrent approval".to_owned(),
                approved_at: now,
            },
            Some(first_entry),
            Some(Usd::new(dec!(48))),
            now,
        ),
        second_repo.approve(
            &intent_id,
            ApproveOrderIntent {
                approved_by: UserId::from_v7(),
                approval_reason: "second concurrent approval".to_owned(),
                approved_at: now,
            },
            Some(second_entry),
            Some(Usd::new(dec!(42))),
            now,
        ),
    );

    let (expected_amount, expected_allocation) = match (&first, &second) {
        (Ok(ApproveOrderIntentOutcome::Approved(_)), Err(StorageError::StateConflict { .. })) => (
            OrderAmount::Shares(Shares::new(dec!(80))),
            Usd::new(dec!(48)),
        ),
        (Err(StorageError::StateConflict { .. }), Ok(ApproveOrderIntentOutcome::Approved(_))) => (
            OrderAmount::Shares(Shares::new(dec!(70))),
            Usd::new(dec!(42)),
        ),
        other => panic!("exactly one concurrent approval must win: {other:?}"),
    };

    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("load approved intent")
        .expect("approved intent");
    assert_eq!(intent.status, OrderIntentStatus::Approved);
    assert_eq!(intent.approval_status, ApprovalStatus::Approved);
    assert_eq!(intent.entry_order_json.amount, expected_amount);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital row");
    assert_eq!(capital.state, CapitalAllocationState::Allocated);
    assert_eq!(capital.allocated_usd, expected_allocation);
    assert_eq!(capital.released_usd, Usd::ZERO);
}

pub async fn expiry_atomic_idempotent_audit() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let intents = PgOrderIntentRepository::new(db.clone());
    let expired = intents
        .expire(
            &intent_id,
            Utc::now(),
            intent_expiry_operation_log(&intent_id, "first"),
        )
        .await
        .expect("expire intent");
    assert_eq!(expired.status, OrderIntentStatus::Expired);

    let repeated = intents
        .expire(
            &intent_id,
            Utc::now(),
            intent_expiry_operation_log(&intent_id, "repeated"),
        )
        .await
        .expect("repeat expiry is idempotent");
    assert_eq!(repeated.status, OrderIntentStatus::Expired);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital row");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));

    let expiry_audits = Entity::find()
        .all(&db)
        .await
        .expect("operation log lookup")
        .into_iter()
        .filter(|row| row.action.as_str() == "quant.intent.expire.test")
        .collect::<Vec<_>>();
    assert_eq!(expiry_audits.len(), 1, "expiry audit must be WORM once");
    let intent_id_text = intent_id.to_string();
    assert_eq!(
        expiry_audits[0].resource_id.as_deref(),
        Some(intent_id_text.as_str())
    );
}

pub async fn expiry_cancel_race_owner() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();

    let cancel_ids = seed_report_fixture(&db).await;
    let cancel_intent = seed_approved_intent(&db, &cancel_ids).await;
    let expire_repo = PgOrderIntentRepository::new(db.clone());
    let cancel_repo = PgOrderIntentRepository::new(db.clone());
    let (expire_result, cancel_result) = tokio::join!(
        expire_repo.expire(
            &cancel_intent,
            Utc::now(),
            intent_expiry_operation_log(&cancel_intent, "cancel-race"),
        ),
        cancel_repo.cancel(
            &cancel_intent,
            "operator cancelled before claim".to_owned(),
            Utc::now(),
            intent_operation_log(&cancel_intent, "quant.intent.cancel.test", "cancel-race"),
        ),
    );
    assert_eq!(
        usize::from(expire_result.is_ok()) + usize::from(cancel_result.is_ok()),
        1,
        "expiry and cancel must not both own the terminal transition"
    );
    let cancel_race_state = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&cancel_intent)
        .await
        .expect("cancel-race lookup")
        .expect("cancel-race intent");
    assert!(matches!(
        cancel_race_state.status,
        OrderIntentStatus::Expired | OrderIntentStatus::Cancelled
    ));
    let cancel_race_capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&cancel_intent)
        .await
        .expect("cancel-race capital lookup")
        .expect("cancel-race capital");
    assert_eq!(cancel_race_capital.state, CapitalAllocationState::Released);
    assert_eq!(cancel_race_capital.released_usd, Usd::new(NOTIONAL));
}

pub async fn expiry_submission_claim_owner() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let claim_ids = seed_report_fixture(&db).await;
    let claim_intent = seed_approved_intent(&db, &claim_ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let expire_repo = PgOrderIntentRepository::new(db.clone());
    let claim = entry_claim_for_test(&db, &claim_intent).await;
    let (claim_result, expire_result) = tokio::join!(
        submission.claim_for_submission(claim),
        expire_repo.expire(
            &claim_intent,
            Utc::now(),
            intent_expiry_operation_log(&claim_intent, "claim-race"),
        ),
    );
    assert_eq!(
        usize::from(claim_result.is_ok()) + usize::from(expire_result.is_ok()),
        1,
        "expiry and the real submission claim must not both win"
    );
    let claim_race_state = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&claim_intent)
        .await
        .expect("claim-race lookup")
        .expect("claim-race intent");
    assert!(matches!(
        claim_race_state.status,
        OrderIntentStatus::AdmissionPending | OrderIntentStatus::Expired
    ));
    let claim_race_capital = PgCapitalAllocationRepository::new(db)
        .find_by_intent(&claim_intent)
        .await
        .expect("claim-race capital lookup")
        .expect("claim-race capital");
    match claim_race_state.status {
        OrderIntentStatus::AdmissionPending => {
            assert_eq!(claim_race_capital.state, CapitalAllocationState::Allocated);
            assert_eq!(claim_race_capital.released_usd, Usd::ZERO);
        }
        OrderIntentStatus::Expired => {
            assert_eq!(claim_race_capital.state, CapitalAllocationState::Released);
            assert_eq!(claim_race_capital.released_usd, Usd::new(NOTIONAL));
        }
        status => panic!("unexpected claim-race status {status:?}"),
    }
}

pub async fn report_revoke_atomically_capital() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let report_repo = PgRecommendationReportRepository::new(db.clone());

    let (report, invalidated) = report_repo
        .revoke(
            &ids.report,
            "operator containment",
            Utc::now(),
            ids.report_operation_log(),
        )
        .await
        .expect("atomic report revoke");
    assert_eq!(report.status, RecommendationReportStatus::Revoked);
    assert_eq!(invalidated.len(), 1);
    assert_eq!(invalidated[0].order_intent_id, intent_id);
    assert_eq!(invalidated[0].status, OrderIntentStatus::Invalidated);

    let recommendation = PgRecommendationRepository::new(db.clone())
        .find_by_id(&ids.recommendation)
        .await
        .expect("recommendation lookup")
        .expect("recommendation");
    assert_eq!(recommendation.status, RecommendationStatus::Revoked);
    let condition = PgEntryConditionRepository::new(db.clone())
        .find_instance(&ids.condition_instance)
        .await
        .expect("condition lookup")
        .expect("condition");
    assert_eq!(condition.state, EntryConditionState::Invalidated);
    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));

    let second = report_repo
        .revoke(
            &ids.report,
            "idempotent retry",
            Utc::now(),
            ids.report_operation_log(),
        )
        .await
        .expect("idempotent revoke retry");
    assert!(second.1.is_empty());
    let intent_id_text = intent_id.to_string();
    let intent_logs = Entity::find()
        .all(&db)
        .await
        .expect("operation log lookup")
        .into_iter()
        .filter(|row| {
            row.action.as_str() == "quant.intent.invalidate"
                && row.resource_id.as_deref() == Some(intent_id_text.as_str())
        })
        .count();
    assert_eq!(intent_logs, 1, "terminal intent log must be written once");
}

pub async fn report_revoke_cancel_audit() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let report_repo = PgRecommendationReportRepository::new(db.clone());
    let intent_repo = PgOrderIntentRepository::new(db.clone());
    let (revoke_result, cancel_result) = tokio::join!(
        report_repo.revoke(
            &ids.report,
            "concurrent revoke",
            Utc::now(),
            ids.report_operation_log(),
        ),
        intent_repo.cancel(
            &intent_id,
            "concurrent cancel".to_owned(),
            Utc::now(),
            intent_operation_log(&intent_id, "quant.intent.cancel.test", "revoke-race"),
        ),
    );
    revoke_result.expect("report revoke must complete");
    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent lookup")
        .expect("intent");
    assert!(matches!(
        intent.status,
        OrderIntentStatus::Invalidated | OrderIntentStatus::Cancelled
    ));
    assert_eq!(
        cancel_result.is_ok(),
        intent.status == OrderIntentStatus::Cancelled
    );
    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));
    let terminal_audits = QuantEntryConditionAuditEntity::find()
        .all(&db)
        .await
        .expect("condition audit lookup")
        .into_iter()
        .filter(|row| {
            row.condition_instance_id == ids.condition_instance
                && row.to_state == EntryConditionState::Invalidated
        })
        .count();
    assert_eq!(
        terminal_audits, 1,
        "condition terminal audit must be unique"
    );
}

pub async fn create_entry_advances_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");

    assert_eq!(order.state, ExecutionOrderState::Submitted);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("capital row");
    assert_eq!(capital.state, CapitalAllocationState::Locked);
    assert_eq!(capital.locked_usd, Usd::new(NOTIONAL));

    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent")
        .expect("intent row");
    assert_eq!(intent.status, OrderIntentStatus::Submitted);
}

pub async fn supersession_wins_before_capital() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;

    let (successor, delivery_worker) = seed_successor_prepared(&db, &ids).await;
    PgRecommendationReportRepository::new(db.clone())
        .verify_and_publish_report(&successor.report, delivery_worker, Utc::now())
        .await
        .expect("supersession commits first")
        .into_applied()
        .expect("successor delivery claim must remain held");

    let result = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await;
    assert!(
        matches!(&result, Err(StorageError::StateConflict { .. })),
        "expired exact report claim must fail as a state conflict: {result:?}"
    );
    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent lookup")
        .expect("intent");
    assert_eq!(intent.status, OrderIntentStatus::Invalidated);
    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));
    let orders = QuantExecutionOrderEntity::find()
        .filter(Column::OrderIntentId.eq(intent_id))
        .all(&db)
        .await
        .expect("execution-order lookup");
    assert!(orders.is_empty());
}

pub async fn submitted_order_survives_supersession() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("write-ahead submission commits first");

    let (successor, delivery_worker) = seed_successor_prepared(&db, &ids).await;
    PgRecommendationReportRepository::new(db.clone())
        .verify_and_publish_report(&successor.report, delivery_worker, Utc::now())
        .await
        .expect("later supersession")
        .into_applied()
        .expect("successor delivery claim must remain held");

    let persisted_order = QuantExecutionOrderEntity::find_by_id(order.execution_order_id)
        .one(&db)
        .await
        .expect("execution-order lookup")
        .expect("execution order");
    assert_eq!(persisted_order.state, ExecutionOrderState::Submitted);
    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent lookup")
        .expect("intent");
    assert_eq!(intent.status, OrderIntentStatus::Submitted);
    let capital = PgCapitalAllocationRepository::new(db)
        .find_by_intent(&intent_id)
        .await
        .expect("capital lookup")
        .expect("capital");
    assert_eq!(capital.state, CapitalAllocationState::Locked);
    assert_eq!(capital.locked_usd, Usd::new(NOTIONAL));
}

pub async fn report_not_before_verification() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (candidate, _delivery_worker) = seed_successor_prepared(&db, &predecessor).await;
    let reports = PgRecommendationReportRepository::new(db.clone());

    let current = reports
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("predecessor remains current");
    assert_eq!(current.recommendation_report_id, predecessor.report);
    let prepared = reports
        .find_by_id(&candidate.report)
        .await
        .expect("load prepared report")
        .expect("prepared report");
    assert_eq!(prepared.status, RecommendationReportStatus::Prepared);
    assert!(!prepared.status.is_current_authority());
    let recommendations = PgRecommendationRepository::new(db)
        .find_by_report(&candidate.report)
        .await
        .expect("load prepared recommendations");
    assert!(
        recommendations
            .iter()
            .all(|recommendation| !recommendation.status.allows_new_intent())
    );
}

pub async fn verified_publication_atomically_current() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (candidate, delivery_worker) = seed_successor_prepared(&db, &predecessor).await;
    let reports = PgRecommendationReportRepository::new(db.clone());

    let outcome = reports
        .verify_and_publish_report(&candidate.report, delivery_worker, Utc::now())
        .await
        .expect("verify and atomically publish candidate")
        .into_applied()
        .expect("candidate delivery claim must remain held");
    assert_eq!(outcome.report.status, RecommendationReportStatus::Published);
    assert_eq!(outcome.superseded_reports.len(), 1);
    assert_eq!(
        outcome.superseded_reports[0].recommendation_report_id,
        predecessor.report
    );
    let current = reports
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("candidate is current");
    assert_eq!(current.recommendation_report_id, candidate.report);
}

pub async fn fact_failure_leaves_untouched() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (candidate, delivery_worker) = seed_successor_prepared(&db, &predecessor).await;
    let reports = PgRecommendationReportRepository::new(db.clone());

    reports
        .fail_fact_delivery(
            &candidate.report,
            delivery_worker,
            ReportFactDeliveryStatus::Failed,
            "injected terminal delivery failure",
        )
        .await
        .expect("terminalize candidate fact delivery")
        .into_applied()
        .expect("failure settlement must retain its claim");
    let current = reports
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("predecessor remains current");
    assert_eq!(current.recommendation_report_id, predecessor.report);
    let candidate = reports
        .find_by_id(&candidate.report)
        .await
        .expect("load candidate")
        .expect("candidate report");
    assert_eq!(candidate.status, RecommendationReportStatus::Prepared);
}

pub async fn concurrent_publications_leave_scope() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &older).await;
    let older_repo = PgRecommendationReportRepository::new(db.clone());
    let newer_repo = PgRecommendationReportRepository::new(db.clone());

    let (older_result, newer_result) = tokio::join!(
        older_repo.verify_and_publish_report(&older.report, older_worker, Utc::now()),
        newer_repo.verify_and_publish_report(&newer.report, newer_worker, Utc::now()),
    );
    if let Err(lost) = older_result
        .expect("older concurrent publication")
        .into_applied()
    {
        assert_eq!(lost.status, ReportFactDeliveryStatus::Cancelled);
    }
    newer_result
        .expect("newer concurrent publication")
        .into_applied()
        .expect("newer publication must settle before cancellation");

    let repo = PgRecommendationReportRepository::new(db);
    let current = repo
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("current report");
    assert_eq!(current.recommendation_report_id, newer.report);
    let older_report = repo
        .find_by_id(&older.report)
        .await
        .expect("load older report")
        .expect("older report");
    assert_ne!(
        older_report.status,
        RecommendationReportStatus::Published,
        "the partial unique scope must expose only the newest authority"
    );
}

pub async fn out_order_verification_candidate() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &older).await;
    let repo = PgRecommendationReportRepository::new(db.clone());

    repo.verify_and_publish_report(&newer.report, newer_worker, Utc::now())
        .await
        .expect("newer facts verify first")
        .into_applied()
        .expect("newer delivery claim must remain held");
    let late_verification = repo
        .verify_and_publish_report(&older.report, older_worker, Utc::now())
        .await;
    let lost = late_verification
        .expect("cancelled delivery is a typed settlement outcome")
        .into_applied()
        .expect_err("older delivery claim must be cancelled");
    assert_eq!(lost.status, ReportFactDeliveryStatus::Cancelled);

    let obsolete = repo
        .find_by_id(&older.report)
        .await
        .expect("load obsolete report")
        .expect("obsolete report");
    assert_eq!(obsolete.status, RecommendationReportStatus::Obsolete);
    assert_eq!(obsolete.successor_report_id, Some(newer.report));
    let cancelled = repo
        .find_fact_delivery(&older.report)
        .await
        .expect("load cancelled delivery")
        .expect("cancelled delivery");
    assert_eq!(cancelled.status, ReportFactDeliveryStatus::Cancelled);
    let current = repo
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("current report");
    assert_eq!(current.recommendation_report_id, newer.report);
}

pub async fn cancelled_delivery_returns_lost() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &older).await;
    let repo = PgRecommendationReportRepository::new(db);

    repo.verify_and_publish_report(&newer.report, newer_worker, Utc::now())
        .await
        .expect("publish newer report")
        .into_applied()
        .expect("newer delivery claim must remain held");

    let lost = repo
        .fail_fact_delivery(
            &older.report,
            older_worker,
            ReportFactDeliveryStatus::Retrying,
            "late ClickHouse failure after cancellation",
        )
        .await
        .expect("claim loss is not a repository error")
        .into_applied()
        .expect_err("older delivery must already be cancelled");
    assert_eq!(lost.status, ReportFactDeliveryStatus::Cancelled);
    assert!(lost.claim_owner.is_none());
    assert!(lost.lease_expires_at.is_none());
}

pub async fn empty_report_published_current() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (empty, delivery_worker) = seed_empty_successor_prepared(&db, &predecessor).await;
    let repo = PgRecommendationReportRepository::new(db.clone());

    let outcome = repo
        .verify_and_publish_report(&empty.report, delivery_worker, Utc::now())
        .await
        .expect("publish formal empty report")
        .into_applied()
        .expect("empty report delivery claim must remain held");

    assert_eq!(outcome.report.status, RecommendationReportStatus::Published);
    assert_eq!(
        outcome.report.summary_json.published_recommendation_count,
        0
    );
    assert!(outcome.report.summary_json.empty_reason.is_some());
    let old_report = repo
        .find_by_id(&predecessor.report)
        .await
        .expect("load predecessor")
        .expect("predecessor");
    assert_eq!(old_report.status, RecommendationReportStatus::Superseded);
    assert_eq!(old_report.successor_report_id, Some(empty.report));
    let current = repo
        .current(ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("current report");
    assert_eq!(current.recommendation_report_id, empty.report);
}

pub async fn lost_lease_prevents_abandoned() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let ids = predecessor.successor_ids(&db).await;
    let transaction = ids.build_report_transaction();
    let decision_at = transaction.report.decision_at;
    let worker_id = WorkerId::from_v7();
    let run_id = ReportRunId::from_v7();
    let run_started_at = decision_at;
    let lease_expires_at = run_started_at + Duration::seconds(1);
    ActiveModel {
        report_run_id: ActiveValue::Set(run_id),
        trigger_kind: ActiveValue::Set(ReportTriggerKind::Scheduled),
        trigger_key: ActiveValue::Set(
            ReportTriggerKey::parse(format!("scheduled:expired:{}", ids.report))
                .expect("report trigger key"),
        ),
        schedule_id: ActiveValue::Set(Some("expired_fixture".into())),
        request_id: ActiveValue::Set(None),
        retry_of_run_id: ActiveValue::Set(None),
        scheduled_for: ActiveValue::Set(Some(run_started_at)),
        requested_at: ActiveValue::Set(run_started_at),
        status: ActiveValue::Set(ReportRunStatus::Running),
        started_at: ActiveValue::Set(Some(run_started_at)),
        decision_at: ActiveValue::Set(Some(decision_at)),
        heartbeat_at: ActiveValue::Set(Some(lease_expires_at - Duration::seconds(1))),
        lease_expires_at: ActiveValue::Set(Some(lease_expires_at)),
        finished_at: ActiveValue::Set(None),
        lease_owner: ActiveValue::Set(Some(worker_id)),
        decision_policy_snapshot_id: ActiveValue::Set(Some(ids.decision_policy_snapshot)),
        top_n: ActiveValue::Set(Some(transaction.report.top_n)),
        knowledge_lag_secs: ActiveValue::Set(Some(10)),
        output_report_id: ActiveValue::Set(None),
        terminal_reason: ActiveValue::Set(None),
        error_code: ActiveValue::Set(None),
        error_summary: ActiveValue::Set(None),
    }
    .insert(&db)
    .await
    .expect("seed expired running report run");

    let available_at = transaction
        .recommendations
        .iter()
        .map(|recommendation| recommendation.created_at)
        .fold(transaction.report.created_at, DateTime::max);
    loop {
        let database_now = PgReportRunRepository::new(db.clone())
            .database_time()
            .await
            .expect("read database time before lost-lease assertion");
        if database_now >= available_at {
            break;
        }
        tokio::time::sleep(
            (available_at - database_now)
                .to_std()
                .expect("positive report availability wait"),
        )
        .await;
    }

    let reports = PgRecommendationReportRepository::new(db.clone());
    let result = reports
        .create_prepared_report(
            ReportRunClaim {
                report_run_id: run_id,
                lease_owner: worker_id,
                lease_expires_at,
            },
            transaction,
        )
        .await;
    assert!(matches!(result, Err(StorageError::StateConflict { .. })));
    assert!(
        reports
            .find_by_id(&ids.report)
            .await
            .expect("load rejected artifact")
            .is_none()
    );

    let abandoned = PgReportRunRepository::new(db)
        .abandon_expired_runs()
        .await
        .expect("recover expired run");
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].report_run_id, run_id);
    assert_eq!(abandoned[0].status, ReportRunStatus::Abandoned);
    assert_eq!(
        abandoned[0].terminal_reason,
        Some(ReportRunTerminalReason::LeaseExpired)
    );
}

pub async fn stale_parity_blocks_ahead() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;

    let stale_generation = FeatureParityStateId::from_v7();
    let error = submission
        .create_entry_order(new_execution_order(&intent_id, &ids), &stale_generation)
        .await
        .expect_err("stale clear generation must fail before write-ahead");
    assert!(matches!(error, StorageError::StateConflict { .. }));

    let orders = QuantExecutionOrderEntity::find()
        .all(&db)
        .await
        .expect("execution orders");
    assert!(orders.is_empty());
    let intent = QuantOrderIntentEntity::find_by_id(intent_id)
        .one(&db)
        .await
        .expect("intent lookup")
        .expect("intent row");
    assert_eq!(intent.status, OrderIntentStatus::AdmissionPending);
}

pub async fn create_entry_advances_executed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");

    let rec = PgRecommendationRepository::new(db.clone())
        .find_by_id(&ids.recommendation)
        .await
        .expect("recommendation")
        .expect("recommendation row");
    assert_eq!(rec.status, RecommendationStatus::Executed);
}

pub async fn reject_admission_releases_rejected() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    let rejected = submission
        .reject_admission(
            &intent_id,
            "liquidity thin".to_owned(),
            Some("check:liquidity".to_owned()),
        )
        .await
        .expect("reject admission");
    assert_eq!(rejected.status, OrderIntentStatus::AdmissionRejected);
    assert_eq!(
        rejected.admission_trace_ref.as_deref(),
        Some("check:liquidity")
    );

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));
}

pub async fn revert_claim_restores_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let reverted = submission
        .revert_claim(&intent_id)
        .await
        .expect("revert claim");
    assert_eq!(reverted.status, OrderIntentStatus::ApprovedByPolicy);
}

pub async fn partial_fill_splits_locked() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    let partial_cost = Usd::new(dec!(30)); // 50 shares * 0.6
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::PartiallyFilled,
                intent_status: OrderIntentStatus::PartiallyFilled,
                venue_order_id: Some(OrderId::new("venue-partial")),
                venue_status: Some(VenueOrderStatus::PartiallyFilled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettlePartial {
                    spent_usd: partial_cost,
                },
                fill: Some(PositionFill {
                    order_intent_id: intent_id,
                    execution_account_id: execution_pg_seed::fixture_execution_account()
                        .execution_account_id,
                    token_id: TokenId::new("token-1"),
                    market_id: MarketId::new(&ids.market),
                    event_id: Some(EventId::new(&ids.event)),
                    category: MarketCategory::Politics,
                    side: OutcomeSide::Yes,
                    shares: Shares::new(dec!(50)),
                    price: Price::new(dec!(0.6)),
                    cost_usd: partial_cost,
                    filled_at: Utc::now(),
                    source: AccountSource::Polymarket,
                }),
                reconciliation: Some(reconciliation_row(&order.execution_order_id, &intent_id)),
            },
        )
        .await
        .expect("record partial fill");

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Locked);
    assert_eq!(capital.locked_usd, Usd::new(NOTIONAL));
    assert_eq!(capital.spent_usd, partial_cost);
    assert_eq!(capital.released_usd, Usd::ZERO);
}

pub async fn position_upsert_weighted_cost() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let position_repo = PgPositionRepository::new(db.clone());

    // Two fills of the *same* entry intent merge into one lot (weighted average).
    let mut first = position_fill(&ids, &intent_id);
    first.cost_usd = Usd::new(dec!(60.25));
    let first_position = position_repo
        .apply_fill(first.clone())
        .await
        .expect("first fill");
    assert_eq!(first_position.avg_price, Price::new(dec!(0.6025)));
    assert_eq!(first_position.cost_usd, Usd::new(dec!(60.25)));

    let second = PositionFill {
        shares: Shares::new(dec!(50)),
        price: Price::new(dec!(0.8)),
        cost_usd: Usd::new(dec!(40.10)),
        ..first
    };
    position_repo.apply_fill(second).await.expect("second fill");

    let position = position_repo
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(position.shares, Shares::new(dec!(150)));
    // Fee-inclusive average cost: (60.25 + 40.10) / 150 = 0.669.
    assert_eq!(position.cost_usd, Usd::new(dec!(100.35)));
    // avg_price is cost / shares; Postgres NUMERIC round-trip can widen precision.
    let implied_cost = position.avg_price.inner() * position.shares.inner();
    let drift = (implied_cost - position.cost_usd.inner()).abs();
    assert!(
        drift <= dec!(0.00000001),
        "avg_price * shares should reconcile to cost_usd (drift={drift})",
    );
}

pub async fn full_fill_writes_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    let recorded = submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::Filled,
                intent_status: OrderIntentStatus::Filled,
                venue_order_id: Some(OrderId::new("venue-1")),
                venue_status: Some(VenueOrderStatus::Filled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettleFull {
                    spent_usd: Usd::new(NOTIONAL),
                },
                fill: Some(position_fill(&ids, &intent_id)),
                reconciliation: Some(reconciliation_row(&order.execution_order_id, &intent_id)),
            },
        )
        .await
        .expect("record full fill");
    assert_eq!(recorded.state, ExecutionOrderState::Filled);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);
    assert_eq!(capital.spent_usd, Usd::new(NOTIONAL));
    assert_eq!(capital.released_usd, Usd::ZERO);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("position row");
    assert_eq!(position.state, PositionLedgerState::Open);
    assert_eq!(position.shares, Shares::new(dec!(100)));
}

fn execution_identity_refs(trade_ids: &[&str], hash_digits: &[char]) -> ExecutionIdentityRefs {
    ExecutionIdentityRefs {
        trade_ids: trade_ids.iter().map(VenueTradeId::new).collect(),
        transaction_hashes: hash_digits
            .iter()
            .map(|digit| {
                EvmTransactionHash::parse(format!("0x{}", digit.to_string().repeat(64)))
                    .expect("canonical test transaction hash")
            })
            .collect(),
        observed_at: Utc::now(),
    }
}

fn ambiguous_identity_write(
    order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
    identity_refs: ExecutionIdentityRefs,
) -> SubmissionLedgerWrite {
    SubmissionLedgerWrite {
        identity_refs,
        state: ExecutionOrderState::Ambiguous,
        intent_status: OrderIntentStatus::Submitted,
        venue_order_id: Some(OrderId::new(format!("venue-{order_id}"))),
        venue_status: None,
        submitted_at: Utc::now(),
        filled_at: None,
        cancelled_at: None,
        error_message: Some("awaiting venue truth".to_owned()),
        capital: CapitalSettlement::Hold,
        fill: None,
        reconciliation: Some(pending_recon_row(order_id, intent_id)),
    }
}

pub async fn submission_persists_multi_atomically() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create order");

    submission
        .record_submission_result(
            &order.execution_order_id,
            ambiguous_identity_write(
                &order.execution_order_id,
                &intent_id,
                execution_identity_refs(&["trade-a", "trade-b"], &['a', 'b']),
            ),
        )
        .await
        .expect("record identities");

    let identities = submission
        .load_identity_refs(&order.execution_order_id)
        .await
        .expect("load identities");
    assert_eq!(identities.trades.len(), 2);
    assert_eq!(identities.transactions.len(), 2);
    assert_eq!(identities.trades[0].venue_trade_id.as_str(), "trade-a");
    assert_eq!(
        identities.transactions[0].transaction_hash.as_str(),
        format!("0x{}", "a".repeat(64))
    );
}

pub async fn duplicate_trade_identity_outcome() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let first_ids = seed_report_fixture(&db).await;
    let first_intent = seed_approved_intent(&db, &first_ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &first_intent).await;
    let first_order = submission
        .create_entry_order(
            new_execution_order(&first_intent, &first_ids),
            &first_ids.feature_parity_state_id,
        )
        .await
        .expect("create first order");
    submission
        .record_submission_result(
            &first_order.execution_order_id,
            ambiguous_identity_write(
                &first_order.execution_order_id,
                &first_intent,
                execution_identity_refs(&["globally-unique-trade"], &['c']),
            ),
        )
        .await
        .expect("record first identity");

    let (second_ids, delivery_worker) = seed_successor_prepared(&db, &first_ids).await;
    PgRecommendationReportRepository::new(db.clone())
        .verify_and_publish_report(&second_ids.report, delivery_worker, Utc::now())
        .await
        .expect("publish second report")
        .into_applied()
        .expect("second delivery claim");
    let second_intent = seed_approved_intent(&db, &second_ids).await;
    claim_entry_for_test(&db, &submission, &second_intent).await;
    let second_order = submission
        .create_entry_order(
            new_execution_order(&second_intent, &second_ids),
            &second_ids.feature_parity_state_id,
        )
        .await
        .expect("create second order");
    let error = submission
        .record_submission_result(
            &second_order.execution_order_id,
            ambiguous_identity_write(
                &second_order.execution_order_id,
                &second_intent,
                execution_identity_refs(&["globally-unique-trade"], &['d']),
            ),
        )
        .await
        .expect_err("duplicate venue trade id must fail");
    assert!(
        matches!(error, StorageError::Duplicate { .. }),
        "unexpected duplicate identity error: {error:?}"
    );

    let rolled_back_order = QuantExecutionOrderEntity::find_by_id(second_order.execution_order_id)
        .one(&db)
        .await
        .expect("load rolled-back order")
        .expect("order row");
    assert_eq!(rolled_back_order.state, ExecutionOrderState::Submitted);
    assert!(
        submission
            .load_identity_refs(&second_order.execution_order_id)
            .await
            .expect("load second identities")
            .trades
            .is_empty()
    );
    let capital = PgCapitalAllocationRepository::new(db)
        .find_by_intent(&second_intent)
        .await
        .expect("capital query")
        .expect("capital row");
    assert_eq!(capital.state, CapitalAllocationState::Locked);
}

pub async fn concurrent_orders_preserves_identity() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let first_ids = seed_report_fixture(&db).await;
    let first_intent = seed_approved_intent(&db, &first_ids).await;
    let first_repo = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &first_repo, &first_intent).await;
    let first_order = first_repo
        .create_entry_order(
            new_execution_order(&first_intent, &first_ids),
            &first_ids.feature_parity_state_id,
        )
        .await
        .expect("create first order");

    let (second_ids, delivery_worker) = seed_successor_prepared(&db, &first_ids).await;
    PgRecommendationReportRepository::new(db.clone())
        .verify_and_publish_report(&second_ids.report, delivery_worker, Utc::now())
        .await
        .expect("publish second report")
        .into_applied()
        .expect("second delivery claim");
    let second_intent = seed_approved_intent(&db, &second_ids).await;
    let second_repo = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &second_repo, &second_intent).await;
    let second_order = second_repo
        .create_entry_order(
            new_execution_order(&second_intent, &second_ids),
            &second_ids.feature_parity_state_id,
        )
        .await
        .expect("create second order");

    let (first_result, second_result) = tokio::join!(
        first_repo.record_submission_result(
            &first_order.execution_order_id,
            ambiguous_identity_write(
                &first_order.execution_order_id,
                &first_intent,
                execution_identity_refs(&["trade-concurrent-a"], &['e']),
            ),
        ),
        second_repo.record_submission_result(
            &second_order.execution_order_id,
            ambiguous_identity_write(
                &second_order.execution_order_id,
                &second_intent,
                execution_identity_refs(&["trade-concurrent-b"], &['e']),
            ),
        )
    );
    first_result.expect("first concurrent result");
    second_result.expect("second concurrent result");

    let first_refs = first_repo
        .load_identity_refs(&first_order.execution_order_id)
        .await
        .expect("first identities");
    let second_refs = second_repo
        .load_identity_refs(&second_order.execution_order_id)
        .await
        .expect("second identities");
    assert_eq!(
        first_refs.trades[0].venue_trade_id.as_str(),
        "trade-concurrent-a"
    );
    assert_eq!(
        second_refs.trades[0].venue_trade_id.as_str(),
        "trade-concurrent-b"
    );
    assert_eq!(
        first_refs.transactions[0].transaction_hash,
        second_refs.transactions[0].transaction_hash
    );
}

pub async fn restart_enrichment_backfills_identity() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create order");
    let mut write = ambiguous_identity_write(
        &order.execution_order_id,
        &intent_id,
        execution_identity_refs(&["trade-restart"], &[]),
    );
    write.venue_order_id = None;
    submission
        .record_submission_result(&order.execution_order_id, write)
        .await
        .expect("record trade id before restart");
    drop(submission);

    let restarted = PgExecutionSubmissionRepository::new(db.clone());
    let transaction_hash = EvmTransactionHash::parse(format!("0x{}", "f".repeat(64)))
        .expect("canonical transaction hash");
    restarted
        .enrich_identity_refs(
            &order.execution_order_id,
            ExecutionIdentityEnrichment {
                discovered_order_id: Some(OrderId::new("venue-recovered")),
                trades: vec![ExecutionTradeObservation {
                    venue_trade_id: VenueTradeId::new("trade-restart"),
                    trade_status: VenueTradeStatus::Confirmed,
                    transaction_hash: None,
                }],
                observed_at: Utc::now(),
            },
        )
        .await
        .expect("confirmed trade may arrive before its transaction hash");
    let confirmed = restarted
        .enrich_identity_refs(
            &order.execution_order_id,
            ExecutionIdentityEnrichment {
                discovered_order_id: None,
                trades: vec![ExecutionTradeObservation {
                    venue_trade_id: VenueTradeId::new("trade-restart"),
                    trade_status: VenueTradeStatus::Confirmed,
                    transaction_hash: Some(transaction_hash.clone()),
                }],
                observed_at: Utc::now(),
            },
        )
        .await
        .expect("late transaction hash enriches the confirmed trade");

    assert_eq!(
        confirmed.trades[0].trade_status,
        Some(VenueTradeStatus::Confirmed)
    );
    assert_eq!(
        confirmed.trades[0].transaction_hash.as_ref(),
        Some(&transaction_hash)
    );
    assert_eq!(confirmed.transactions.len(), 1);
    let recovered_order = QuantExecutionOrderEntity::find_by_id(order.execution_order_id)
        .one(&db)
        .await
        .expect("load recovered order")
        .expect("recovered order");
    assert_eq!(
        recovered_order.venue_order_id,
        Some(OrderId::new("venue-recovered"))
    );

    let conflicting_hash = EvmTransactionHash::parse(format!("0x{}", "1".repeat(64)))
        .expect("canonical conflicting hash");
    let error = restarted
        .enrich_identity_refs(
            &order.execution_order_id,
            ExecutionIdentityEnrichment {
                discovered_order_id: None,
                trades: vec![ExecutionTradeObservation {
                    venue_trade_id: VenueTradeId::new("trade-restart"),
                    trade_status: VenueTradeStatus::Confirmed,
                    transaction_hash: Some(conflicting_hash),
                }],
                observed_at: Utc::now(),
            },
        )
        .await
        .expect_err("confirmed transaction hash is immutable");
    assert!(matches!(error, StorageError::StateConflict { .. }));
}

pub async fn execution_atomic_unique_concurrent() {
    submission_persists_multi_atomically().await;
    Box::pin(duplicate_trade_identity_outcome()).await;
    Box::pin(concurrent_orders_preserves_identity()).await;
    restart_enrichment_backfills_identity().await;
}

pub async fn ambiguous_holds_capital_reconciliation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    let recorded = submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::Ambiguous,
                intent_status: OrderIntentStatus::Submitted,
                venue_order_id: None,
                venue_status: None,
                submitted_at: Utc::now(),
                filled_at: None,
                cancelled_at: None,
                error_message: Some("venue timeout".to_owned()),
                capital: CapitalSettlement::Hold,
                fill: None,
                reconciliation: Some(reconciliation_row(&order.execution_order_id, &intent_id)),
            },
        )
        .await
        .expect("record ambiguous");
    assert_eq!(recorded.state, ExecutionOrderState::Ambiguous);

    // Fail-closed: capital stays locked (the order may have filled on the venue).
    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Locked);
    assert_eq!(capital.locked_usd, Usd::new(NOTIONAL));
    assert_eq!(capital.spent_usd, Usd::ZERO);
    assert_eq!(capital.released_usd, Usd::ZERO);

    // No position is written on an unconfirmed submission.
    assert!(
        PgPositionRepository::new(db.clone())
            .find_by_intent(&intent_id)
            .await
            .expect("position")
            .is_none()
    );

    // The order is enqueued for reconciliation.
    let recon = PgReconciliationRepository::new(db.clone())
        .find_by_execution_order(&order.execution_order_id)
        .await
        .expect("recon");
    assert!(
        recon.is_some(),
        "ambiguous orders must enqueue reconciliation"
    );
}

pub async fn rejected_releases_without_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::Failed,
                intent_status: OrderIntentStatus::Failed,
                venue_order_id: Some(OrderId::new("venue-2")),
                venue_status: Some(VenueOrderStatus::Rejected),
                submitted_at: Utc::now(),
                filled_at: None,
                cancelled_at: None,
                error_message: Some("rejected".to_owned()),
                capital: CapitalSettlement::ReleaseAll,
                fill: None,
                reconciliation: Some(reconciliation_row(&order.execution_order_id, &intent_id)),
            },
        )
        .await
        .expect("record rejected");

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.spent_usd, Usd::ZERO);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));
}

pub async fn recover_dangling_returns_orders() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    // Crash before record_submission_result: the Submitted write-ahead row is
    // recovered for reconciliation.
    let dangling = submission.recover_dangling(100).await.expect("recover");
    assert!(
        dangling
            .iter()
            .any(|o| o.execution_order_id == order.execution_order_id),
        "in-flight Submitted order must be recovered",
    );
}

pub async fn create_advances_recommendation_created() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    seed_approved_intent(&db, &ids).await;

    let rec = PgRecommendationRepository::new(db.clone())
        .find_by_id(&ids.recommendation)
        .await
        .expect("load rec")
        .expect("rec");
    assert_eq!(rec.status, RecommendationStatus::IntentCreated);
}

pub async fn create_rejects_recommendation_executed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;

    let rec = QuantRecommendationEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("load")
        .expect("rec");
    let mut active = rec.into_active_model();
    active.status = ActiveValue::Set(RecommendationStatus::Executed);
    active.update(&db).await.expect("mark executed");

    let intent_id = OrderIntentId::from_v7();
    let err = PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_pending_intent_id(&ids, intent_id),
            new_allocation_for(&ids, intent_id),
        )
        .await
        .expect_err("executed rec must block create");
    assert!(matches!(
        err,
        StorageError::StateConflict {
            entity: QUANT_RECOMMENDATION,
            ..
        }
    ));
}

pub async fn create_rejects_submitted_blocks() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;

    let row = QuantOrderIntentEntity::find_by_id(intent_id)
        .one(&db)
        .await
        .expect("load")
        .expect("intent");
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(OrderIntentStatus::Submitted);
    active.update(&db).await.expect("mark submitted");

    let second_id = OrderIntentId::from_v7();
    let err = PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_pending_intent_id(&ids, second_id),
            new_allocation_for(&ids, second_id),
        )
        .await
        .expect_err("submitted intent must block create");
    assert!(matches!(
        err,
        StorageError::Duplicate {
            entity: QUANT_ORDER_INTENT,
            ..
        }
    ));
}

fn new_pending_intent_id(ids: &TxnIds, order_intent_id: OrderIntentId) -> NewOrderIntent {
    NewOrderIntent {
        order_intent_id,
        recommendation_id: ids.recommendation,
        execution_account_id: execution_pg_seed::fixture_execution_account().execution_account_id,
        runtime_mode: QuantRuntimeMode::SemiAuto,
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        model_version_id: ids.model_version,
        research_profile_artifact_id: fixture_profile_ref().artifact_id(),
        intent_kind: OrderIntentKind::Buy,
        status: OrderIntentStatus::PendingApproval,
        approval_status: ApprovalStatus::Pending,
        approved_by: None,
        approval_reason: None,
        approved_at: None,
        policy_id: None,
        policy_hash: None,
        status_reason: None,
        admission_trace_ref: None,
        condition_instance_id: ids.condition_instance,
        entry_order_json: EntryOrderSpec {
            token_id: TokenId::new("token-1"),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            post_only: false,
            limit_price: Price::new(dec!(0.6)),
            amount: OrderAmount::Shares(Shares::new(dec!(100))),
            maker_rebate_terms: EntryMakerRebateTerms::AggressiveNotApplicable,
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now() + Duration::hours(1),
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: Some(Price::new(dec!(0.8))),
            take_profit_pct: None,
            stop_loss_price: Some(Price::new(dec!(0.5))),
            stop_loss_pct: None,
            time_exit_at: None,
            max_hold_secs: None,
            trailing_stop: None,
            thesis_invalidation: ThesisInvalidationPolicy {
                min_score_retention: dec!(0.6),
                min_expected_return_bps: Bps::ZERO,
                require_route_gate_eligibility: true,
            },
            opportunistic_exit: opportunistic_exit_policy(),
            scale_out_targets: Vec::new(),
            settlement_mode: ExitSettlementMode::ExitBeforeResolution,
            redeem_policy: RedeemPolicy::Manual,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.6)),
            entry_composite_score: Probability::new(dec!(0.8)),
        },
        risk_envelope_hash: ids.risk_envelope_hash(),
        expires_at: Utc::now() + Duration::hours(1),
    }
}

fn new_allocation_for(ids: &TxnIds, order_intent_id: OrderIntentId) -> NewCapitalAllocation {
    NewCapitalAllocation {
        capital_allocation_id: CapitalAllocationId::from_v7(),
        order_intent_id,
        recommendation_id: ids.recommendation,
        state: CapitalAllocationState::Allocated,
        planned_usd: Usd::new(NOTIONAL),
        allocated_usd: Usd::new(NOTIONAL),
        locked_usd: Usd::ZERO,
        spent_usd: Usd::ZERO,
        released_usd: Usd::ZERO,
        reason: "intent created".to_owned(),
    }
}

// ── Submission payloads ──────────────────────────────────────────────────────

// ── Reconciliation correction (apply_reconciliation) ───────────────

/// Drive an intent to an `Ambiguous` order with locked capital and a submit-time
/// `Pending` reconciliation row — the worker's primary correction input.
async fn ambiguous_order(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &TxnIds,
) -> (OrderIntentId, ExecutionOrderId) {
    let intent_id = seed_approved_intent(db, ids).await;
    claim_entry_for_test(db, submission, &intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(&intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::Ambiguous,
                intent_status: OrderIntentStatus::Submitted,
                venue_order_id: Some(OrderId::new("venue-amb")),
                venue_status: None,
                submitted_at: Utc::now(),
                filled_at: None,
                cancelled_at: None,
                error_message: Some("venue timeout".to_owned()),
                capital: CapitalSettlement::Hold,
                fill: None,
                reconciliation: Some(pending_recon_row(&order.execution_order_id, &intent_id)),
            },
        )
        .await
        .expect("record ambiguous");
    (intent_id, order.execution_order_id)
}

fn pending_recon_row(eo: &ExecutionOrderId, intent: &OrderIntentId) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: *eo,
        order_intent_id: *intent,
        result: ReconciliationResult::Pending,
        evidence_json: ReconciliationEvidenceChain(vec![recon_evidence("submit: ambiguous")]),
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

fn recon_evidence(detail: &str) -> ReconciliationEvidence {
    ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::ClobOrderStatus,
        observed_at: Utc::now(),
        detail: detail.to_owned(),
        venue_ref: None,
        shares: None,
        price: None,
        fee_evidence: None,
    }
}

fn filled_write() -> ReconciliationLedgerWrite {
    ReconciliationLedgerWrite {
        order_state: ExecutionOrderState::Filled,
        intent_status: OrderIntentStatus::Filled,
        venue_status: Some(VenueOrderStatus::Filled),
        venue_order_id: Some(OrderId::new("venue-amb")),
        filled_at: Some(Utc::now()),
        cancelled_at: None,
        error_message: None,
        capital: CapitalReconcileSettlement::Settle {
            spent_usd: Usd::new(NOTIONAL),
        },
        cumulative_fill: None,
        cumulative_exit: None,
        exit_state: None,
        revert_lot: false,
        result: ReconciliationResult::Filled,
        evidence: ReconciliationEvidenceChain(vec![recon_evidence("recon: filled")]),
        venue_filled_shares: Some(Shares::new(dec!(100))),
        venue_avg_price: Some(Price::new(dec!(0.6))),
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: Some("system:reconciliation_worker".to_owned()),
        resolved_at: Some(Utc::now()),
    }
}

pub async fn reconcile_ambiguous_writes_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let mut write = filled_write();
    write.cumulative_fill = Some(cumulative_position_fill(&ids, &intent_id));
    let recorded = submission
        .apply_reconciliation(&order_id, write)
        .await
        .expect("apply reconciliation");
    assert_eq!(recorded.state, ExecutionOrderState::Filled);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);
    assert_eq!(capital.spent_usd, Usd::new(NOTIONAL));

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("position row");
    assert_eq!(position.shares, Shares::new(dec!(100)));

    let recon = PgReconciliationRepository::new(db.clone())
        .find_by_execution_order(&order_id)
        .await
        .expect("recon")
        .expect("recon row");
    assert_eq!(recon.result, ReconciliationResult::Filled);
    assert!(recon.resolved_at.is_some());
    // WORM: the submit-time evidence is preserved and the recon evidence appended.
    assert_eq!(recon.evidence_json.0.len(), 2);
    assert_eq!(recon.evidence_json.0[0].detail, "submit: ambiguous");
}

pub async fn reconcile_ambiguous_not_capital() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let write = ReconciliationLedgerWrite {
        order_state: ExecutionOrderState::Failed,
        intent_status: OrderIntentStatus::Failed,
        venue_status: Some(VenueOrderStatus::Expired),
        venue_order_id: Some(OrderId::new("venue-amb")),
        filled_at: None,
        cancelled_at: Some(Utc::now()),
        error_message: None,
        capital: CapitalReconcileSettlement::Release,
        cumulative_fill: None,
        cumulative_exit: None,
        exit_state: None,
        revert_lot: false,
        result: ReconciliationResult::NotFilled,
        evidence: ReconciliationEvidenceChain(vec![recon_evidence("recon: not filled")]),
        venue_filled_shares: Some(Shares::ZERO),
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: Some("system:reconciliation_worker".to_owned()),
        resolved_at: Some(Utc::now()),
    };
    let recorded = submission
        .apply_reconciliation(&order_id, write)
        .await
        .expect("apply");
    assert_eq!(recorded.state, ExecutionOrderState::Failed);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Released);
    assert_eq!(capital.released_usd, Usd::new(NOTIONAL));
    assert!(
        PgPositionRepository::new(db.clone())
            .find_by_intent(&intent_id)
            .await
            .expect("position")
            .is_none()
    );
}

pub async fn reconcile_unresolvable_impairs_ambiguous() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let write = ReconciliationLedgerWrite {
        order_state: ExecutionOrderState::Ambiguous,
        intent_status: OrderIntentStatus::Submitted,
        venue_status: None,
        venue_order_id: Some(OrderId::new("venue-amb")),
        filled_at: None,
        cancelled_at: None,
        error_message: Some("conflicting evidence".to_owned()),
        capital: CapitalReconcileSettlement::Impair,
        cumulative_fill: None,
        cumulative_exit: None,
        exit_state: None,
        revert_lot: false,
        result: ReconciliationResult::Unresolvable,
        evidence: ReconciliationEvidenceChain(vec![recon_evidence("recon: unresolvable")]),
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    };
    let recorded = submission
        .apply_reconciliation(&order_id, write)
        .await
        .expect("apply");
    assert_eq!(recorded.state, ExecutionOrderState::Ambiguous);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Impaired);

    let recon_repo = PgReconciliationRepository::new(db.clone());
    let recon = recon_repo
        .find_by_execution_order(&order_id)
        .await
        .expect("recon")
        .expect("row");
    assert_eq!(recon.result, ReconciliationResult::Unresolvable);
    assert!(recon.resolved_at.is_none());
    assert!(
        recon_repo
            .has_unresolvable()
            .await
            .expect("has_unresolvable"),
        "an unresolvable verdict must block auto execution",
    );
}

pub async fn reconcile_partial_writes_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let mut write = ReconciliationLedgerWrite {
        order_state: ExecutionOrderState::PartiallyFilled,
        intent_status: OrderIntentStatus::PartiallyFilled,
        venue_status: Some(VenueOrderStatus::PartiallyFilled),
        venue_order_id: Some(OrderId::new("venue-amb")),
        filled_at: Some(Utc::now()),
        cancelled_at: None,
        error_message: None,
        capital: CapitalReconcileSettlement::Settle {
            spent_usd: Usd::new(PARTIAL_SPENT),
        },
        cumulative_fill: None,
        cumulative_exit: None,
        exit_state: None,
        revert_lot: false,
        result: ReconciliationResult::PartiallyFilled,
        evidence: ReconciliationEvidenceChain(vec![recon_evidence("recon: partial fill")]),
        venue_filled_shares: Some(Shares::new(PARTIAL_SHARES)),
        venue_avg_price: Some(Price::new(dec!(0.6))),
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: Some(Usd::new(-PARTIAL_SPENT)),
        realized_pnl_usd: None,
        expected_fee_usd: Some(Usd::new(PARTIAL_SPENT - PARTIAL_SHARES * dec!(0.6))),
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: Some("system:reconciliation_worker".to_owned()),
        resolved_at: Some(Utc::now()),
    };
    write.cumulative_fill = Some(CumulativePositionFill {
        order_intent_id: intent_id,
        execution_account_id: execution_pg_seed::fixture_execution_account().execution_account_id,
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        cumulative_shares: Shares::new(PARTIAL_SHARES),
        cumulative_cost_usd: Usd::new(PARTIAL_SPENT),
        observed_at: Utc::now(),
        source: AccountSource::Polymarket,
    });

    let recorded = submission
        .apply_reconciliation(&order_id, write)
        .await
        .expect("apply partial reconciliation");
    assert_eq!(recorded.state, ExecutionOrderState::PartiallyFilled);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);
    assert_eq!(capital.spent_usd, Usd::new(PARTIAL_SPENT));
    assert_eq!(
        capital.released_usd,
        Usd::new(NOTIONAL - PARTIAL_SPENT),
        "unfilled remainder must be released on partial reconciliation",
    );

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("position row");
    assert_eq!(position.shares, Shares::new(PARTIAL_SHARES));
}

pub async fn reconcile_correction_is_idempotent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let mut first = filled_write();
    first.cumulative_fill = Some(cumulative_position_fill(&ids, &intent_id));
    submission
        .apply_reconciliation(&order_id, first)
        .await
        .expect("first apply");

    // Second identical correction must be a no-op (order is already terminal):
    // capital is not double-spent and the position is not double-written.
    let mut second = filled_write();
    second.cumulative_fill = Some(cumulative_position_fill(&ids, &intent_id));
    submission
        .apply_reconciliation(&order_id, second)
        .await
        .expect("second apply");

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.spent_usd, Usd::new(NOTIONAL));

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(
        position.shares,
        Shares::new(dec!(100)),
        "idempotent reconciliation must not double the position",
    );

    let recon = PgReconciliationRepository::new(db.clone())
        .find_by_execution_order(&order_id)
        .await
        .expect("recon")
        .expect("row");
    assert_eq!(
        recon.evidence_json.0.len(),
        2,
        "the second no-op correction must not append more evidence",
    );

    let mut conflicting = filled_write();
    conflicting.result = ReconciliationResult::NotFilled;
    let error = submission
        .apply_reconciliation(&order_id, conflicting)
        .await
        .expect_err("a terminal replay with a different result must fail closed");
    assert!(matches!(error, StorageError::StateConflict { .. }));
}

pub async fn operator_resolve_impaired_capital() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    // Machine escalates to unresolvable (capital impaired)...
    submission
        .apply_reconciliation(
            &order_id,
            ReconciliationLedgerWrite {
                order_state: ExecutionOrderState::Ambiguous,
                intent_status: OrderIntentStatus::Submitted,
                venue_status: None,
                venue_order_id: Some(OrderId::new("venue-amb")),
                filled_at: None,
                cancelled_at: None,
                error_message: Some("unresolvable".to_owned()),
                capital: CapitalReconcileSettlement::Impair,
                cumulative_fill: None,
                cumulative_exit: None,
                exit_state: None,
                revert_lot: false,
                result: ReconciliationResult::Unresolvable,
                evidence: ReconciliationEvidenceChain(vec![recon_evidence("unresolvable")]),
                venue_filled_shares: None,
                venue_avg_price: None,
                expected_cash_delta_usd: None,
                venue_cash_delta_usd: None,
                realized_pnl_usd: None,
                expected_fee_usd: None,
                derived_fee_usd: None,
                settled_fee_usd: None,
                fee_delta_usd: None,
                resolved_by: None,
                resolved_at: None,
            },
        )
        .await
        .expect("impair");

    //...then an operator resolves it to filled (Impaired -> Spent).
    let mut resolve = filled_write();
    resolve.cumulative_fill = Some(cumulative_position_fill(&ids, &intent_id));
    resolve.resolved_by = Some("operator:alice".to_owned());
    let recorded = submission
        .apply_reconciliation(&order_id, resolve)
        .await
        .expect("operator resolve");
    assert_eq!(recorded.state, ExecutionOrderState::Filled);

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);
    assert_eq!(capital.spent_usd, Usd::new(NOTIONAL));

    let recon_repo = PgReconciliationRepository::new(db.clone());
    assert!(
        !recon_repo
            .has_unresolvable()
            .await
            .expect("has_unresolvable"),
        "operator resolve clears the unresolvable block",
    );
    let recon = recon_repo
        .find_by_execution_order(&order_id)
        .await
        .expect("recon")
        .expect("row");
    assert_eq!(recon.resolved_by.as_deref(), Some("operator:alice"));
}

fn new_execution_order(intent_id: &OrderIntentId, ids: &TxnIds) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: *intent_id,
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Buy,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(NOTIONAL),
        prepared_order_json: prepared_order(
            TokenId::new("token-1"),
            Side::Buy,
            OrderType::Gtc,
            VenueOrderAmount::Shares(Shares::new(dec!(100))),
            Usd::ZERO,
            Shares::new(dec!(100)),
            Price::new(dec!(0.6)),
        ),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

fn position_fill(ids: &TxnIds, intent_id: &OrderIntentId) -> PositionFill {
    PositionFill {
        order_intent_id: *intent_id,
        execution_account_id: execution_pg_seed::fixture_execution_account().execution_account_id,
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        shares: Shares::new(dec!(100)),
        price: Price::new(dec!(0.6)),
        cost_usd: Usd::new(NOTIONAL),
        filled_at: Utc::now(),
        source: AccountSource::Polymarket,
    }
}

fn cumulative_position_fill(ids: &TxnIds, intent_id: &OrderIntentId) -> CumulativePositionFill {
    let fill = position_fill(ids, intent_id);
    CumulativePositionFill {
        order_intent_id: fill.order_intent_id,
        execution_account_id: fill.execution_account_id,
        token_id: fill.token_id,
        market_id: fill.market_id,
        event_id: fill.event_id,
        category: fill.category,
        side: fill.side,
        cumulative_shares: fill.shares,
        cumulative_cost_usd: fill.cost_usd,
        observed_at: fill.filled_at,
        source: fill.source,
    }
}

fn reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: *execution_order_id,
        order_intent_id: *intent_id,
        result: ReconciliationResult::Unresolvable,
        evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: Utc::now(),
            detail: "submission result".to_owned(),
            venue_ref: None,
            shares: None,
            price: None,
            fee_evidence: None,
        }]),
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        derived_fee_usd: None,
        settled_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

// ── Fixture chain (self-contained; mirrors pg_account_capital) ────────────────

async fn seed_approved_intent(db: &DatabaseConnection, ids: &TxnIds) -> OrderIntentId {
    enable_test_admission(db, "pg-execution-submission-it-operator").await;
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id,
                recommendation_id: ids.recommendation,
                execution_account_id: execution_pg_seed::fixture_execution_account()
                    .execution_account_id,
                runtime_mode: QuantRuntimeMode::AutoExecution,
                decision_policy_snapshot_id: ids.decision_policy_snapshot,
                model_version_id: ids.model_version,
                research_profile_artifact_id: fixture_profile_ref().artifact_id(),
                intent_kind: OrderIntentKind::Buy,
                status: OrderIntentStatus::ApprovedByPolicy,
                approval_status: ApprovalStatus::NotRequired,
                approved_by: None,
                approval_reason: Some("policy".to_owned()),
                approved_at: Some(Utc::now()),
                policy_id: Some("auto".to_owned()),
                policy_hash: None,
                status_reason: None,
                admission_trace_ref: None,
                condition_instance_id: ids.condition_instance,
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    post_only: false,
                    limit_price: Price::new(dec!(0.6)),
                    amount: OrderAmount::Shares(Shares::new(dec!(100))),
                    maker_rebate_terms: EntryMakerRebateTerms::AggressiveNotApplicable,
                    max_slippage_bps: Bps::new(dec!(50)),
                    valid_until: Utc::now() + Duration::hours(1),
                },
                exit_policy_json: ExitPolicySpec {
                    take_profit_price: Some(Price::new(dec!(0.8))),
                    take_profit_pct: None,
                    stop_loss_price: Some(Price::new(dec!(0.5))),
                    stop_loss_pct: None,
                    time_exit_at: None,
                    max_hold_secs: None,
                    trailing_stop: None,
                    thesis_invalidation: ThesisInvalidationPolicy {
                        min_score_retention: dec!(0.6),
                        min_expected_return_bps: Bps::ZERO,
                        require_route_gate_eligibility: true,
                    },
                    opportunistic_exit: opportunistic_exit_policy(),
                    scale_out_targets: Vec::new(),
                    settlement_mode: ExitSettlementMode::ExitBeforeResolution,
                    redeem_policy: RedeemPolicy::Manual,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: ids.risk_envelope_hash(),
                expires_at: Utc::now() + Duration::hours(1),
            },
            NewCapitalAllocation {
                capital_allocation_id: CapitalAllocationId::from_v7(),
                order_intent_id,
                recommendation_id: ids.recommendation,
                state: CapitalAllocationState::Allocated,
                planned_usd: Usd::new(NOTIONAL),
                allocated_usd: Usd::new(NOTIONAL),
                locked_usd: Usd::ZERO,
                spent_usd: Usd::ZERO,
                released_usd: Usd::ZERO,
                reason: "intent created".to_owned(),
            },
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

async fn clear_feature_parity(db: &DatabaseConnection) -> FeatureParityStateId {
    let state_id = FeatureParityStateId::from_v7();
    QuantFeatureParityStateEntity::insert(
        NewFeatureParityState {
            state_id,
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("pg-execution-test".to_owned()),
            acting_role: Some(RoleCode::new("risk_owner")),
            reason: "test fixture clear generation".to_owned(),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("seed feature parity clear generation");
    state_id
}

async fn seed_report_fixture(db: &DatabaseConnection) -> TxnIds {
    let feature_parity_state_id = clear_feature_parity(db).await;
    let rc_id = seed_runtime_config(db).await;
    let infra = seed_model_version(db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(db, event_id, market_id).await;
    let ids = TxnIds::seed(
        db,
        feature_parity_state_id,
        &rc_id,
        &infra,
        market_id,
        event_id,
    )
    .await;
    persist_and_publish_report(
        db,
        ids.build_report_transaction(),
        &format!("scheduled:test:{}", ids.report),
        10,
    )
    .await;
    ids
}

async fn seed_successor_prepared(
    db: &DatabaseConnection,
    predecessor: &TxnIds,
) -> (TxnIds, WorkerId) {
    let ids = predecessor.successor_ids(db).await;
    persist_prepared_report(
        db,
        ids.build_report_transaction(),
        &format!("scheduled:successor:{}", ids.report),
        10,
    )
    .await;
    let worker = WorkerId::from_v7();
    let claimed = PgRecommendationReportRepository::new(db.clone())
        .claim_fact_delivery(worker, 600)
        .await
        .expect("claim successor delivery")
        .expect("successor delivery");
    assert_eq!(claimed.recommendation_report_id, ids.report);
    (ids, worker)
}

impl TxnIds {
    async fn successor_ids(&self, db: &DatabaseConnection) -> Self {
        let now = DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("successor decision time must fit epoch milliseconds");
        let minimum = self.decision_at + Duration::milliseconds(1);
        let decision_at = if now > minimum { now } else { minimum };
        let market_selection = seed_market_selection(
            db,
            &self.decision_policy_snapshot,
            decision_at,
            &self.market,
        )
        .await;
        let infra = SharedDemoInfra {
            feature_parity_state_id: self.feature_parity_state_id,
            decision_policy_snapshot_id: self.decision_policy_snapshot,
            model_version_id: self.model_version,
            calibration_artifact_id: self.calibration_artifact,
            model_run_id: self.model_run,
            trade_policy: self.trade_policy.clone(),
            factor_serving_plane: self.factor_serving_plane.clone(),
        };
        let model_run =
            execution_pg_seed::seed_report_model_run(db, &infra, &market_selection, decision_at)
                .await;
        let ids = Self {
            decision_at,
            feature_parity_state_id: self.feature_parity_state_id,
            account_snapshot: AccountSnapshotId::from_v7(),
            execution_account: self.execution_account,
            data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
            portfolio_plan: PortfolioPlanId::from_v7(),
            report: RecommendationReportId::from_v7(),
            recommendation: RecommendationId::from_v7(),
            condition_instance: EntryConditionInstanceId::from_v7(),
            model_version: self.model_version,
            calibration_artifact: self.calibration_artifact,
            model_run,
            trade_policy: self.trade_policy.clone(),
            factor_serving_plane: self.factor_serving_plane.clone(),
            market_selection,
            decision_policy_snapshot: self.decision_policy_snapshot,
            market: self.market.clone(),
            event: self.event.clone(),
            token: self.token.clone(),
        };
        ids.execution_ids().complete_model_run(db).await;
        ids
    }
}

async fn seed_empty_successor_prepared(
    db: &DatabaseConnection,
    predecessor: &TxnIds,
) -> (TxnIds, WorkerId) {
    let ids = predecessor.successor_ids(db).await;
    let execution_ids = ids.execution_ids();
    let empty_options = ReportBuildOptions::empty_report(&execution_ids);
    let transaction = build_custom_report_transaction(&execution_ids, empty_options);
    persist_prepared_report(
        db,
        transaction,
        &format!("scheduled:empty-successor:{}", ids.report),
        10,
    )
    .await;
    let worker = WorkerId::from_v7();
    let claimed = PgRecommendationReportRepository::new(db.clone())
        .claim_fact_delivery(worker, 600)
        .await
        .expect("claim empty successor delivery")
        .expect("empty successor delivery");
    assert_eq!(claimed.recommendation_report_id, ids.report);
    (ids, worker)
}

async fn seed_market_catalog(db: &DatabaseConnection, event_id: &str, market_id: &str) {
    execution_pg_seed::ensure_fixture_execution_account(db).await;
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            event_id,
            "Event",
            "event",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            market_id,
            event_id,
            "Will it?",
            "will-it",
            MarketCategory::Politics,
            None,
        ))
        .await
        .expect("seed market");
}

async fn seed_approval_governance(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
) {
    let active = PgPolicyRepository::new(db.clone())
        .load_current()
        .await
        .expect("load active policy bundle")
        .expect("active policy bundle");
    assert_eq!(
        &active.decision_policy_snapshot_id, decision_policy_snapshot_id,
        "execution fixture must use the active typed policy bundle"
    );
    execution_pg_seed::enable_test_admission(db, "concurrent-approval-test").await;
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-exec-it", "integration test").await
}

async fn seed_model_version(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> SharedDemoInfra {
    let infra = seed_shared_demo_infra(db).await;
    assert_eq!(
        infra.decision_policy_snapshot_id, *rc_id,
        "shared execution lineage must use the active submission-test policy"
    );
    infra
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    decision_at: DateTime<Utc>,
    _market_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id,
                decision_at,
                decision_policy_snapshot_id: *rc_id,
                selector_hash: content_hash('b'),
                selector_evidence: SelectorFixture::evidence(content_hash('b')),
                market_count: 1,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

struct TxnIds {
    decision_at: DateTime<Utc>,
    feature_parity_state_id: FeatureParityStateId,
    account_snapshot: AccountSnapshotId,
    execution_account: ExecutionAccountId,
    data_quality_snapshot: ReportDataQualitySnapshotId,
    portfolio_plan: PortfolioPlanId,
    report: RecommendationReportId,
    recommendation: RecommendationId,
    condition_instance: EntryConditionInstanceId,
    model_version: ModelVersionId,
    calibration_artifact: CalibrationArtifactId,
    model_run: ModelRunId,
    trade_policy: TradePolicyCohortProvenance,
    factor_serving_plane: FactorServingPlane,
    market_selection: MarketSelectionId,
    decision_policy_snapshot: DecisionPolicySnapshotId,
    market: String,
    event: String,
    token: String,
}

impl TxnIds {
    async fn seed(
        db: &DatabaseConnection,
        feature_parity_state_id: FeatureParityStateId,
        rc_id: &DecisionPolicySnapshotId,
        infra: &SharedDemoInfra,
        market_id: &str,
        event_id: &str,
    ) -> Self {
        let decision_at = DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("report decision time must fit epoch milliseconds");
        let market_selection = seed_market_selection(db, rc_id, decision_at, market_id).await;
        let model_run =
            execution_pg_seed::seed_report_model_run(db, infra, &market_selection, decision_at)
                .await;
        let ids = Self {
            decision_at,
            feature_parity_state_id,
            account_snapshot: AccountSnapshotId::from_v7(),
            execution_account: execution_pg_seed::fixture_execution_account().execution_account_id,
            data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
            portfolio_plan: PortfolioPlanId::from_v7(),
            report: RecommendationReportId::from_v7(),
            recommendation: RecommendationId::from_v7(),
            condition_instance: EntryConditionInstanceId::from_v7(),
            model_version: infra.model_version_id,
            calibration_artifact: infra.calibration_artifact_id,
            model_run,
            trade_policy: infra.trade_policy.clone(),
            factor_serving_plane: infra.factor_serving_plane.clone(),
            market_selection,
            decision_policy_snapshot: *rc_id,
            market: market_id.to_owned(),
            event: event_id.to_owned(),
            token: "token-1".to_owned(),
        };
        ids.execution_ids().complete_model_run(db).await;
        ids
    }

    fn build_report_transaction(&self) -> NewReportTransaction {
        let ids = self.execution_ids();
        build_custom_report_transaction(&ids, ReportBuildOptions::published_single(&ids))
    }

    fn risk_envelope_hash(&self) -> ContentHash {
        self.build_report_transaction()
            .recommendations
            .into_iter()
            .next()
            .expect("report fixture has one recommendation")
            .trade_plan
            .risk_envelope
            .envelope_hash
    }

    fn execution_ids(&self) -> ExecutionTxnIds {
        ExecutionTxnIds {
            decision_at: self.decision_at,
            feature_parity_state_id: self.feature_parity_state_id,
            account_snapshot: self.account_snapshot,
            execution_account: self.execution_account,
            data_quality_snapshot: self.data_quality_snapshot,
            portfolio_plan: self.portfolio_plan,
            report: self.report,
            recommendation: self.recommendation,
            condition_instance: self.condition_instance,
            model_version: self.model_version,
            calibration_artifact: self.calibration_artifact,
            model_run: self.model_run,
            market_selection: self.market_selection,
            decision_policy_snapshot: self.decision_policy_snapshot,
            trade_policy: self.trade_policy.clone(),
            factor_serving_plane: self.factor_serving_plane.clone(),
            market: self.market.clone(),
            event: self.event.clone(),
            token: self.token.clone(),
        }
    }
}

impl TxnIds {
    fn report_operation_log(&self) -> NewOperationLog {
        NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: format!("scheduled:test:{}", self.report).into(),
            actor_user_id: None,
            actor_username: Some("system".to_owned()),
            acting_role: Some("test".into()),
            category: OperationCategory::QuantReport,
            action: "publish".into(),
            resource_type: Some(ResourceType::QuantReport),
            resource_id: Some(self.report.to_string()),
            http_method: OperationHttpMethod::System,
            http_path: "/test/quant/report".to_owned(),
            http_status: 201,
            outcome: OperationOutcome::Success,
            client_ip: None,
            user_agent: None,
            latency_ms: 0,
            detail: OperationDetailDocument::try_from(serde_json::json!({ "test": true }))
                .expect("static operation detail"),
            before_hash: None,
            after_hash: None,
            governance_audit_event_id: None,
            governance_audit_sequence: None,
        }
    }
}

fn intent_expiry_operation_log(intent_id: &OrderIntentId, request_suffix: &str) -> NewOperationLog {
    intent_operation_log(intent_id, "quant.intent.expire.test", request_suffix)
}

fn intent_operation_log(
    intent_id: &OrderIntentId,
    action: &str,
    request_suffix: &str,
) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("intent-terminal-test:{intent_id}:{request_suffix}").into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("test".into()),
        category: OperationCategory::Other,
        action: action.into(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: "/test/quant/intents/expire".to_owned(),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::try_from(serde_json::json!({ "test": true }))
            .expect("static operation detail"),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

const fn opportunistic_exit_policy() -> OpportunisticExitPolicy {
    OpportunisticExitPolicy {
        min_confidence: Probability::new(dec!(0.65)),
        min_expected_alpha_bps: Bps::new(dec!(50)),
        min_p_exit_better: Probability::new(dec!(0.5)),
        max_cumulative_exit_pct: dec!(1),
        min_incremental_exit_pct: dec!(0.1),
    }
}

// Exit submission: per-lot capital and position settlement.

/// Drive an approved intent's entry to a confirmed full fill: capital `Spent`,
/// one open lot (100 @ 0.60), intent `Filled`.
async fn fill_entry_lot(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &TxnIds,
    intent_id: &OrderIntentId,
) {
    claim_entry_for_test(db, submission, intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                state: ExecutionOrderState::Filled,
                intent_status: OrderIntentStatus::Filled,
                venue_order_id: Some(OrderId::new("venue-entry")),
                venue_status: Some(VenueOrderStatus::Filled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettleFull {
                    spent_usd: Usd::new(NOTIONAL),
                },
                fill: Some(position_fill(ids, intent_id)),
                reconciliation: Some(reconciliation_row(&order.execution_order_id, intent_id)),
            },
        )
        .await
        .expect("record entry fill");
}

pub async fn entry_fill_freezes_denominator() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let intents = PgOrderIntentRepository::new(db.clone());

    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    let intent = intents
        .find_by_id(&intent_id)
        .await
        .expect("find intent")
        .expect("intent row");
    assert_eq!(
        intent.scale_out_state.denominator_shares,
        Some(Shares::new(dec!(100)))
    );
}

fn exit_order(
    intent_id: &OrderIntentId,
    ids: &TxnIds,
    shares: Decimal,
    price: Decimal,
) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: *intent_id,
        order_phase: ExecutionOrderPhase::Exit,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Sell,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(price),
        shares: Shares::new(shares),
        cost_usd: Shares::new(shares) * Price::new(price),
        prepared_order_json: prepared_order(
            TokenId::new("token-1"),
            Side::Sell,
            OrderType::Gtc,
            VenueOrderAmount::Shares(Shares::new(shares)),
            Usd::ZERO,
            Shares::new(shares),
            Price::new(price),
        ),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

pub async fn exit_full_releases_pnl() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    // Write-ahead the exit: lot Open -> Closing.
    let exit = submission
        .create_exit_order(
            exit_order(&intent_id, &ids, dec!(100), dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("exit order");
    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(position.state, PositionLedgerState::Closing);

    // Settle a full exit fill at 0.55 (entry cost 0.60 → realized -5).
    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::Exited,
                exit_reason: ExitReason::StopLoss,
                position_exit: Some(PositionExit {
                    shares: Shares::new(dec!(100)),
                    avg_price: Price::new(dec!(0.55)),
                    proceeds_usd: Usd::new(dec!(55)),
                    realized_pnl_usd: Usd::new(dec!(-5)),
                    exited_at: Utc::now(),
                    reason: ExitReason::StopLoss,
                }),
                fully_exited: true,
                revert_to_open: false,
                reconciliation: None,
            },
        )
        .await
        .expect("record exit");

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Released);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(position.state, PositionLedgerState::Closed);
    assert_eq!(position.shares, Shares::ZERO);
    assert_eq!(position.realized_pnl_usd, Usd::new(dec!(-5)));
}

pub async fn exit_partial_keeps_lot() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    let exit = submission
        .create_exit_order(
            exit_order(&intent_id, &ids, dec!(40), dec!(0.70)),
            ExitReason::PartialExit,
            Some(PendingScaleOut {
                target_id: Some("tp1".to_owned()),
                target_cumulative_exit_pct: dec!(0.4),
            }),
        )
        .await
        .expect("exit order");

    // Sell 40 @ 0.70 (entry 0.60 → realized +4); lot keeps 60, capital stays Spent.
    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                identity_refs: execution_pg_seed::empty_identity_refs(),
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit-partial")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::PartiallyExited,
                exit_reason: ExitReason::PartialExit,
                position_exit: Some(PositionExit {
                    shares: Shares::new(dec!(40)),
                    avg_price: Price::new(dec!(0.70)),
                    proceeds_usd: Usd::new(dec!(28)),
                    realized_pnl_usd: Usd::new(dec!(4)),
                    exited_at: Utc::now(),
                    reason: ExitReason::PartialExit,
                }),
                fully_exited: false,
                revert_to_open: false,
                reconciliation: None,
            },
        )
        .await
        .expect("record partial exit");

    let capital = PgCapitalAllocationRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("capital")
        .expect("row");
    assert_eq!(capital.state, CapitalAllocationState::Spent);

    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(position.state, PositionLedgerState::Closing);
    assert_eq!(position.shares, Shares::new(dec!(60)));
    assert_eq!(position.realized_pnl_usd, Usd::new(dec!(4)));

    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(&intent_id)
        .await
        .expect("intent")
        .expect("row");
    assert!(intent.scale_out_state.contains("tp1"));
    assert!(intent.scale_out_state.pending_target.is_none());
}

pub async fn exit_rejects_second_order() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    submission
        .create_exit_order(
            exit_order(&intent_id, &ids, dec!(100), dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("first exit order");

    let err = submission
        .create_exit_order(
            exit_order(&intent_id, &ids, dec!(100), dec!(0.54)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect_err("second in-flight exit must be rejected");
    assert!(
        err.to_string().contains("in-flight exit order"),
        "unexpected error: {err}"
    );
}
