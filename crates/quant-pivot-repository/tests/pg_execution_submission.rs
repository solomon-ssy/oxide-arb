//! Phase 05.4 — execution-submission repository integration tests (Postgres).
//!
//! Requires Docker. Exercises the money-critical cross-table transactions:
//! claim (double-submit guard), capital lock on write-ahead, and venue-result
//! settlement (full fill → spent + position; ambiguous → hold + reconcile;
//! rejected → release), plus boot recovery of in-flight orders.

use std::{collections::BTreeMap, str::FromStr};

use chrono::{Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_ORDER_INTENT, QUANT_RECOMMENDATION},
};
use quant_pivot_models::{
    domain::{
        ApproveOrderIntent, ApproveOrderIntentOutcome, CapitalReconcileSettlement,
        CapitalSettlement, ExitLedgerWrite, NewAccountSnapshot, NewCapitalAllocation,
        NewEntryConditionInstance, NewEquitySnapshot, NewExecutionOrder, NewFeatureParityState,
        NewMarketSelection, NewModelRun, NewModelSpec, NewModelVersion, NewOperationLog,
        NewOrderIntent, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
        NewReconciliation, NewReportDataQualitySnapshot, NewReportTransaction,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, PositionExit, PositionFill,
        ReconciliationLedgerWrite, ReportRunClaim, SubmissionLedgerWrite, UpsertKillSwitchState,
    },
    entities::{
        operation_log, quant_entry_condition_audit, quant_execution_order,
        quant_feature_parity_state, quant_order_intent, quant_recommendation, quant_report_run,
    },
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, ExitReason, ExitState, KillSwitchState,
            OrderIntentKind, OrderTypeKind, PositionLedgerState, ReconciliationEvidenceKind,
            ReconciliationResult, VenueOrderStatus,
        },
        factor::{FactorFamily, FactorValueState, NormalizationSource},
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, EntryConditionState,
            ExecutionOrderState, ExitSettlementMode, FactorDirection, FeatureParityLatchState,
            FeatureParityStateTransition, ModelRunKind, ModelRunStatus, OrderIntentStatus,
            OutcomeSide, PublicationStatus, QuantRuntimeMode, RecommendationReportStatus,
            RecommendationStatus, RedeemPolicy, ReportFactDeliveryStatus, ReportKind,
            ReportRunStatus, ReportRunTerminalReason, ReportTriggerKind, SizingModelKind,
        },
        rbac::ResourceType,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    types::{
        AccountPositions, AccountSnapshotId, BookSnapshotRef, Bps, CapitalAllocationId,
        ConditionTruth, ConfidenceSummary, ContentHash, DataQualitySummary, EligibilitySummary,
        EntryConditionFoldState, EntryConditionInstanceId, EntryConditionPlan, EntryOrderPolicy,
        EntryOrderSpec, EntryPlan, EquitySnapshotId, EventId, EvidenceRefs, ExecutionEligibility,
        ExecutionOrderId, ExitPlan, ExitPolicySpec, ExposureBreakdown, FactorBreakdownEntry,
        FeatureParityStateId, FeatureVectorId, MarketContext, MarketId, MarketSelectionId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        OperationLogId, OpportunisticExitPolicy, OrderAmount, OrderId, OrderIntentId,
        PendingScaleOut, PortfolioConstraintsSnapshot, PortfolioOptimizerMeta, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, PositionSnapshot, Price, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportRunId, ReportSummary, RiskEnvelope,
        RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
        SelectionExclusionSummary, Shares, SignalCandidateId, SizingPlan, ThesisInvalidationPolicy,
        TokenId, TradePolicyArtifactId, TradePolicyCohortDimension, TradePolicyCohortKey,
        TradePolicyCohortProvenance, Usd, builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgEntryConditionRepository, PgEventRepository,
        PgExecutionSubmissionRepository, PgKillSwitchStateRepository, PgMarketRepository,
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgOrderIntentRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReconciliationRepository, PgReportRunRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        CapitalAllocationRepository, EntryConditionRepository, EventRepository,
        ExecutionSubmissionRepository, KillSwitchStateRepository, MarketRepository,
        MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        OrderIntentRepository, PositionRepository, RecommendationReportRepository,
        RecommendationRepository, ReconciliationRepository, ReportRunRepository,
        RuntimeConfigVersionRepository,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    execution_pg_seed::{
        ReportBuildOptions, ReportSeedConfig, claim_entry_for_test,
        enable_entry_admission_for_test, entry_claim_for_test, fixture_profile_ref, prepared_order,
        seed_conditional_price_report_on_infra, seed_shared_demo_infra,
    },
    pg::setup_pg,
    report_fixtures,
    report_lifecycle_seed::{persist_and_publish_report, persist_prepared_report},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait,
    IntoActiveModel, QueryFilter, Statement,
};

/// shares (100) * `limit_price` (0.6).
const NOTIONAL: Decimal = dec!(60);
const PARTIAL_SHARES: Decimal = dec!(40);
const PARTIAL_SPENT: Decimal = dec!(24);

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker"]
async fn claim_guards_against_double_submit() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn entry_condition_artifact_and_audit_are_database_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let ids = seed_conditional_price_report_on_infra(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn concurrent_approval_has_one_winner_and_one_amount_truth() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    seed_approval_governance(&db, &ids.runtime_config_version).await;

    let intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_pending_intent_with_id(&ids, intent_id.clone()),
            new_allocation_for(&ids, intent_id.clone()),
            None,
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
                approved_by: uuid::Uuid::now_v7(),
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
                approved_by: uuid::Uuid::now_v7(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn expiry_is_atomic_and_idempotent_across_capital_and_audit() {
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

    let expiry_audits = operation_log::Entity::find()
        .all(&db)
        .await
        .expect("operation log lookup")
        .into_iter()
        .filter(|row| row.action == "quant.intent.expire.test")
        .collect::<Vec<_>>();
    assert_eq!(expiry_audits.len(), 1, "expiry audit must be WORM once");
    let intent_id_text = intent_id.to_string();
    assert_eq!(
        expiry_audits[0].resource_id.as_deref(),
        Some(intent_id_text.as_str())
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn expiry_and_cancel_race_has_one_terminal_owner() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn expiry_and_submission_claim_race_has_one_owner() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_revoke_atomically_terminates_intent_condition_and_capital() {
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
            report_operation_log(&ids),
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
            report_operation_log(&ids),
        )
        .await
        .expect("idempotent revoke retry");
    assert!(second.1.is_empty());
    let intent_id_text = intent_id.to_string();
    let intent_logs = operation_log::Entity::find()
        .all(&db)
        .await
        .expect("operation log lookup")
        .into_iter()
        .filter(|row| {
            row.action == "quant.intent.invalidate"
                && row.resource_id.as_deref() == Some(intent_id_text.as_str())
        })
        .count();
    assert_eq!(intent_logs, 1, "terminal intent log must be written once");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_revoke_and_cancel_race_has_one_intent_terminal_audit() {
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
            report_operation_log(&ids),
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
    let terminal_audits = quant_entry_condition_audit::Entity::find()
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_entry_locks_capital_and_advances_intent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn supersession_wins_before_submission_and_releases_capital() {
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
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await;
    assert!(matches!(result, Err(StorageError::StateConflict { .. })));
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
    let orders = quant_execution_order::Entity::find()
        .filter(quant_execution_order::Column::OrderIntentId.eq(intent_id))
        .all(&db)
        .await
        .expect("execution-order lookup");
    assert!(orders.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn submitted_order_survives_later_supersession() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
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

    let persisted_order = quant_execution_order::Entity::find_by_id(order.execution_order_id)
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn prepared_report_is_not_actionable_before_fact_verification() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (candidate, _delivery_worker) = seed_successor_prepared(&db, &predecessor).await;
    let reports = PgRecommendationReportRepository::new(db.clone());

    let current = reports
        .current(&fixture_profile_ref().id, ReportKind::TopN)
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn verified_publication_atomically_supersedes_prior_current() {
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
        .current(&fixture_profile_ref().id, ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("candidate is current");
    assert_eq!(current.recommendation_report_id, candidate.report);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn fact_failure_leaves_existing_current_untouched() {
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
        .current(&fixture_profile_ref().id, ReportKind::TopN)
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn concurrent_publications_leave_one_current_per_scope() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &predecessor).await;
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
        .current(&fixture_profile_ref().id, ReportKind::TopN)
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn out_of_order_verification_obsoletes_older_candidate() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &predecessor).await;
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
    assert_eq!(obsolete.successor_report_id, Some(newer.report.clone()));
    let cancelled = repo
        .find_fact_delivery(&older.report)
        .await
        .expect("load cancelled delivery")
        .expect("cancelled delivery");
    assert_eq!(cancelled.status, ReportFactDeliveryStatus::Cancelled);
    let current = repo
        .current(&fixture_profile_ref().id, ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("current report");
    assert_eq!(current.recommendation_report_id, newer.report);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn cancelled_delivery_settlement_returns_claim_lost() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let (older, older_worker) = seed_successor_prepared(&db, &predecessor).await;
    let (newer, newer_worker) = seed_successor_prepared(&db, &predecessor).await;
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn empty_report_is_published_and_becomes_current() {
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
    assert_eq!(old_report.successor_report_id, Some(empty.report.clone()));
    let current = repo
        .current(&fixture_profile_ref().id, ReportKind::TopN)
        .await
        .expect("load current report")
        .expect("current report");
    assert_eq!(current.recommendation_report_id, empty.report);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn lost_lease_prevents_report_commit_and_marks_abandoned() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let predecessor = seed_report_fixture(&db).await;
    let ids = successor_ids(&predecessor);
    let transaction = build_report_transaction(&ids);
    let decision_at = transaction.report.decision_at;
    let worker_id = uuid::Uuid::now_v7();
    let run_id = ReportRunId::from_v7();
    let run_started_at = decision_at - Duration::minutes(1);
    let lease_expires_at = run_started_at + Duration::seconds(30);
    quant_report_run::ActiveModel {
        report_run_id: ActiveValue::Set(run_id.clone()),
        trigger_kind: ActiveValue::Set(ReportTriggerKind::Scheduled),
        trigger_key: ActiveValue::Set(format!("scheduled:expired:{}", ids.report)),
        schedule_id: ActiveValue::Set(Some("expired_fixture".to_owned())),
        request_id: ActiveValue::Set(None),
        retry_of_run_id: ActiveValue::Set(None),
        scheduled_for: ActiveValue::Set(Some(run_started_at)),
        requested_at: ActiveValue::Set(run_started_at),
        status: ActiveValue::Set(ReportRunStatus::Running),
        started_at: ActiveValue::Set(Some(run_started_at)),
        decision_at: ActiveValue::Set(Some(run_started_at)),
        heartbeat_at: ActiveValue::Set(Some(lease_expires_at - Duration::seconds(1))),
        lease_expires_at: ActiveValue::Set(Some(lease_expires_at)),
        finished_at: ActiveValue::Set(None),
        lease_owner: ActiveValue::Set(Some(worker_id)),
        runtime_config_version_id: ActiveValue::Set(Some(ids.runtime_config_version.clone())),
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

    let reports = PgRecommendationReportRepository::new(db.clone());
    let result = reports
        .create_prepared_report(
            ReportRunClaim {
                report_run_id: run_id.clone(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn stale_parity_generation_blocks_entry_write_ahead() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    claim_entry_for_test(&db, &submission, &intent_id).await;

    let stale_generation = FeatureParityStateId::from_v7();
    let error = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, &ids),
            &stale_generation,
        )
        .await
        .expect_err("stale clear generation must fail before write-ahead");
    assert!(matches!(error, StorageError::StateConflict { .. }));

    let orders = quant_execution_order::Entity::find()
        .all(&db)
        .await
        .expect("execution orders");
    assert!(orders.is_empty());
    let intent = quant_order_intent::Entity::find_by_id(intent_id)
        .one(&db)
        .await
        .expect("intent lookup")
        .expect("intent row");
    assert_eq!(intent.status, OrderIntentStatus::AdmissionPending);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_entry_advances_recommendation_to_executed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    submission
        .create_entry_order_and_lock_capital(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn reject_admission_releases_capital_and_marks_rejected() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn revert_claim_restores_approved_by_policy_for_auto_intent() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn partial_fill_splits_capital_while_locked() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
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
                    order_intent_id: intent_id.clone(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn position_upsert_weighted_average_cost() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn full_fill_spends_capital_and_writes_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    let recorded = submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn ambiguous_holds_capital_and_enqueues_reconciliation() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    let recorded = submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn rejected_releases_capital_without_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, &ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");

    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn recover_dangling_returns_in_flight_orders() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());

    claim_entry_for_test(&db, &submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_advances_recommendation_to_intent_created() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_rejects_when_recommendation_executed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;

    let rec = quant_recommendation::Entity::find_by_id(ids.recommendation.clone())
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
            new_pending_intent_with_id(&ids, intent_id.clone()),
            new_allocation_for(&ids, intent_id),
            None,
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_rejects_when_submitted_intent_blocks() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;

    let row = quant_order_intent::Entity::find_by_id(intent_id.clone())
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
            new_pending_intent_with_id(&ids, second_id.clone()),
            new_allocation_for(&ids, second_id),
            None,
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

fn new_pending_intent_with_id(ids: &TxnIds, order_intent_id: OrderIntentId) -> NewOrderIntent {
    NewOrderIntent {
        order_intent_id,
        recommendation_id: ids.recommendation.clone(),
        runtime_mode: QuantRuntimeMode::SemiAuto,
        runtime_config_version_id: ids.runtime_config_version.clone(),
        model_version_id: ids.model_version.clone(),
        profile_ref: fixture_profile_ref(),
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
        condition_instance_id: ids.condition_instance.clone(),
        entry_order_json: EntryOrderSpec {
            token_id: TokenId::new("token-1"),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            post_only: false,
            limit_price: Price::new(dec!(0.6)),
            amount: OrderAmount::Shares(Shares::new(dec!(100))),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now() + chrono::Duration::hours(1),
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
                require_execution_eligibility: true,
            },
            opportunistic_exit: opportunistic_exit_policy(),
            scale_out_targets: Vec::new(),
            settlement_mode: ExitSettlementMode::ExitBeforeResolution,
            redeem_policy: RedeemPolicy::Manual,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.6)),
            entry_composite_score: Probability::new(dec!(0.8)),
        },
        risk_envelope_hash: content_hash('f'),
        expires_at: Utc::now() + chrono::Duration::hours(1),
    }
}

fn new_allocation_for(ids: &TxnIds, order_intent_id: OrderIntentId) -> NewCapitalAllocation {
    NewCapitalAllocation {
        capital_allocation_id: CapitalAllocationId::from_v7(),
        order_intent_id,
        recommendation_id: ids.recommendation.clone(),
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

// ── Phase 05.5 — reconciliation correction (apply_reconciliation) ─────────────

/// Drive an intent to an `Ambiguous` order with locked capital and a submit-time
/// `Pending` reconciliation row — the worker's primary correction input.
async fn ambiguous_order(
    db: &sea_orm::DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &TxnIds,
) -> (OrderIntentId, ExecutionOrderId) {
    let intent_id = seed_approved_intent(db, ids).await;
    claim_entry_for_test(db, submission, &intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(&intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
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
        execution_order_id: eo.clone(),
        order_intent_id: intent.clone(),
        result: ReconciliationResult::Pending,
        evidence_json: ReconciliationEvidenceChain(vec![recon_evidence("submit: ambiguous")]),
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        observed_fee_usd: None,
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
        fill: None,
        exit: None,
        exit_fully: false,
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
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: Some("system:reconciliation_worker".to_owned()),
        resolved_at: Some(Utc::now()),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconcile_ambiguous_to_filled_spends_capital_and_writes_position() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let mut write = filled_write();
    write.fill = Some(position_fill(&ids, &intent_id));
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconcile_ambiguous_to_not_filled_releases_capital() {
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
        fill: None,
        exit: None,
        exit_fully: false,
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
        observed_fee_usd: None,
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconcile_unresolvable_impairs_capital_and_leaves_order_ambiguous() {
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
        fill: None,
        exit: None,
        exit_fully: false,
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
        observed_fee_usd: None,
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconcile_partial_fill_splits_capital_and_writes_position() {
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
        fill: None,
        exit: None,
        exit_fully: false,
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
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: Some("system:reconciliation_worker".to_owned()),
        resolved_at: Some(Utc::now()),
    };
    write.fill = Some(PositionFill {
        order_intent_id: intent_id.clone(),
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        shares: Shares::new(PARTIAL_SHARES),
        price: Price::new(dec!(0.6)),
        cost_usd: Usd::new(PARTIAL_SPENT),
        filled_at: Utc::now(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn reconcile_correction_is_idempotent() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let (intent_id, order_id) = ambiguous_order(&db, &submission, &ids).await;

    let mut first = filled_write();
    first.fill = Some(position_fill(&ids, &intent_id));
    submission
        .apply_reconciliation(&order_id, first)
        .await
        .expect("first apply");

    // Second identical correction must be a no-op (order is already terminal):
    // capital is not double-spent and the position is not double-written.
    let mut second = filled_write();
    second.fill = Some(position_fill(&ids, &intent_id));
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
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn operator_resolve_impaired_to_filled_spends_capital() {
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
                fill: None,
                exit: None,
                exit_fully: false,
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
                observed_fee_usd: None,
                fee_delta_usd: None,
                resolved_by: None,
                resolved_at: None,
            },
        )
        .await
        .expect("impair");

    // ...then an operator resolves it to filled (Impaired -> Spent).
    let mut resolve = filled_write();
    resolve.fill = Some(position_fill(&ids, &intent_id));
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
        order_intent_id: intent_id.clone(),
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Buy,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(NOTIONAL),
        prepared_order_json: prepared_order(
            Side::Buy,
            OrderType::Gtc,
            quant_pivot_models::types::VenueOrderAmount::Shares(Shares::new(dec!(100))),
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
        order_intent_id: intent_id.clone(),
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

fn reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order_id.clone(),
        order_intent_id: intent_id.clone(),
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
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

// ── Fixture chain (self-contained; mirrors pg_account_capital) ────────────────

async fn seed_approved_intent(db: &sea_orm::DatabaseConnection, ids: &TxnIds) -> OrderIntentId {
    enable_entry_admission_for_test(db, "pg-execution-submission-it-operator").await;
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id: order_intent_id.clone(),
                recommendation_id: ids.recommendation.clone(),
                runtime_mode: QuantRuntimeMode::AutoExecution,
                runtime_config_version_id: ids.runtime_config_version.clone(),
                model_version_id: ids.model_version.clone(),
                profile_ref: fixture_profile_ref(),
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
                condition_instance_id: ids.condition_instance.clone(),
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    post_only: false,
                    limit_price: Price::new(dec!(0.6)),
                    amount: OrderAmount::Shares(Shares::new(dec!(100))),
                    max_slippage_bps: Bps::new(dec!(50)),
                    valid_until: Utc::now() + chrono::Duration::hours(1),
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
                        require_execution_eligibility: true,
                    },
                    opportunistic_exit: opportunistic_exit_policy(),
                    scale_out_targets: Vec::new(),
                    settlement_mode: ExitSettlementMode::ExitBeforeResolution,
                    redeem_policy: RedeemPolicy::Manual,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: content_hash('f'),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            },
            NewCapitalAllocation {
                capital_allocation_id: CapitalAllocationId::from_v7(),
                order_intent_id: order_intent_id.clone(),
                recommendation_id: ids.recommendation.clone(),
                state: CapitalAllocationState::Allocated,
                planned_usd: Usd::new(NOTIONAL),
                allocated_usd: Usd::new(NOTIONAL),
                locked_usd: Usd::ZERO,
                spent_usd: Usd::ZERO,
                released_usd: Usd::ZERO,
                reason: "intent created".to_owned(),
            },
            None,
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

async fn seed_clear_feature_parity_state(db: &sea_orm::DatabaseConnection) -> FeatureParityStateId {
    let state_id = FeatureParityStateId::from_v7();
    quant_feature_parity_state::Entity::insert(
        NewFeatureParityState {
            state_id: state_id.clone(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("pg-execution-test".to_owned()),
            acting_role: Some("risk_owner".to_owned()),
            reason: "test fixture clear generation".to_owned(),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("seed feature parity clear generation");
    state_id
}

async fn seed_report_fixture(db: &sea_orm::DatabaseConnection) -> TxnIds {
    let feature_parity_state_id = seed_clear_feature_parity_state(db).await;
    let rc_id = seed_runtime_config(db).await;
    let (model_version_id, model_run_id) = seed_model_version(db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(db, &rc_id, market_id).await;
    let ids = TxnIds {
        feature_parity_state_id,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        runtime_config_version: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    persist_and_publish_report(
        db,
        build_report_transaction(&ids),
        &format!("scheduled:test:{}", ids.report),
        10,
    )
    .await;
    ids
}

async fn seed_successor_prepared(
    db: &sea_orm::DatabaseConnection,
    predecessor: &TxnIds,
) -> (TxnIds, uuid::Uuid) {
    let ids = successor_ids(predecessor);
    persist_prepared_report(
        db,
        build_report_transaction(&ids),
        &format!("scheduled:successor:{}", ids.report),
        10,
    )
    .await;
    let worker = uuid::Uuid::now_v7();
    let claimed = PgRecommendationReportRepository::new(db.clone())
        .claim_fact_delivery(worker, 600)
        .await
        .expect("claim successor delivery")
        .expect("successor delivery");
    assert_eq!(claimed.recommendation_report_id, ids.report);
    (ids, worker)
}

fn successor_ids(predecessor: &TxnIds) -> TxnIds {
    TxnIds {
        feature_parity_state_id: predecessor.feature_parity_state_id.clone(),
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: predecessor.model_version.clone(),
        model_run: predecessor.model_run.clone(),
        market_selection: predecessor.market_selection.clone(),
        runtime_config_version: predecessor.runtime_config_version.clone(),
        market: predecessor.market.clone(),
        event: predecessor.event.clone(),
    }
}

async fn seed_empty_successor_prepared(
    db: &sea_orm::DatabaseConnection,
    predecessor: &TxnIds,
) -> (TxnIds, uuid::Uuid) {
    let ids = TxnIds {
        feature_parity_state_id: predecessor.feature_parity_state_id.clone(),
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: predecessor.model_version.clone(),
        model_run: predecessor.model_run.clone(),
        market_selection: predecessor.market_selection.clone(),
        runtime_config_version: predecessor.runtime_config_version.clone(),
        market: predecessor.market.clone(),
        event: predecessor.event.clone(),
    };
    let empty_options = ReportBuildOptions::empty_report();
    let mut transaction = build_report_transaction(&ids);
    transaction.report.summary_json = empty_options.summary;
    transaction.portfolio_plan.allocated_usd = Usd::ZERO;
    transaction.recommendations.clear();
    transaction.entry_condition_artifacts.clear();
    transaction.entry_condition_instances.clear();
    persist_prepared_report(
        db,
        transaction,
        &format!("scheduled:empty-successor:{}", ids.report),
        10,
    )
    .await;
    let worker = uuid::Uuid::now_v7();
    let claimed = PgRecommendationReportRepository::new(db.clone())
        .claim_fact_delivery(worker, 600)
        .await
        .expect("claim empty successor delivery")
        .expect("empty successor delivery");
    assert_eq!(claimed.recommendation_report_id, ids.report);
    (ids, worker)
}

async fn seed_market_catalog(db: &sea_orm::DatabaseConnection, event_id: &str, market_id: &str) {
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
    db: &sea_orm::DatabaseConnection,
    runtime_config_version_id: &RuntimeConfigVersionId,
) {
    PgRuntimeConfigVersionRepository::new(db.clone())
        .activate_version(NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: runtime_config_version_id.clone(),
            runtime_config_approval_id: None,
            activated_by: "concurrent-approval-test".to_owned(),
            reason: "activate intent runtime contract".to_owned(),
            activation_kind: RuntimeConfigActivationKind::Initial,
            previous_runtime_config_version_id: None,
            rollback_target_version_id: None,
            audit_event_id: None,
        })
        .await
        .expect("activate runtime config");
    PgKillSwitchStateRepository::new(db.clone())
        .upsert(UpsertKillSwitchState {
            id: 1,
            state: KillSwitchState::Closed,
            changed_by: "concurrent-approval-test".to_owned(),
            reason: "allow governed entry".to_owned(),
            requires_operator_ack: false,
            changed_at: Utc::now(),
        })
        .await
        .expect("seed closed kill switch");
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-exec-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_version(
    db: &sea_orm::DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-exec-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
            artifact_hash: content_hash('a'),
            training_dataset_id: None,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
        })
        .await
        .expect("model version");
    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(model_version_id.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('d'),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");
    (model_version_id, model_run_id)
}

async fn seed_market_selection(
    db: &sea_orm::DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
    _market_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id.clone(),
                decision_at: Utc::now(),
                runtime_config_version_id: rc_id.clone(),
                selector_hash: content_hash('b'),
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
    feature_parity_state_id: FeatureParityStateId,
    account_snapshot: AccountSnapshotId,
    data_quality_snapshot: ReportDataQualitySnapshotId,
    portfolio_plan: PortfolioPlanId,
    report: RecommendationReportId,
    recommendation: RecommendationId,
    condition_instance: EntryConditionInstanceId,
    model_version: ModelVersionId,
    model_run: ModelRunId,
    market_selection: MarketSelectionId,
    runtime_config_version: RuntimeConfigVersionId,
    market: String,
    event: String,
}

fn build_report_transaction(ids: &TxnIds) -> NewReportTransaction {
    let equity_snapshot_id = EquitySnapshotId::from_v7();
    let report = report_row(ids, equity_snapshot_id.clone());
    let decision_at = report.decision_at;
    let sampled_feature_parity = report_fixtures::sampled_parity(&report);
    let recommendation = report_recommendation(ids);
    let entry_condition_instance = NewEntryConditionInstance {
        condition_instance_id: ids.condition_instance.clone(),
        recommendation_id: ids.recommendation.clone(),
        artifact_id: None,
        artifact_hash: None,
        state: EntryConditionState::NotRequired,
        truth_json: Some(ConditionTruth::Satisfied),
        revision: 0,
        evaluation_hash: None,
        input_fingerprint: None,
        continuity_hash: None,
        fold_state_json: EntryConditionFoldState::default(),
        confirmation_started_at: None,
        last_evaluated_at: None,
        next_evaluation_at: None,
        expires_at: recommendation.valid_until,
        lease_owner: None,
        lease_expires_at: None,
        lease_epoch: 0,
        claimed_by_intent_id: None,
        claim_admission_state_version: None,
        consumed_at: None,
    };
    NewReportTransaction {
        feature_parity_state_id: Some(ids.feature_parity_state_id.clone()),
        account_snapshot: NewAccountSnapshot {
            account_snapshot_id: ids.account_snapshot.clone(),
            ..new_account_snapshot()
        },
        equity_snapshot: report_equity_snapshot(ids, &equity_snapshot_id),
        data_quality_snapshot: NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ids.data_quality_snapshot.clone(),
            decision_at,
            runtime_config_version_id: ids.runtime_config_version.clone(),
            tokens_json: ReportDataQualityTokens(Vec::new()),
        },
        portfolio_plan: report_portfolio_plan(ids),
        report,
        recommendations: vec![recommendation],
        entry_condition_artifacts: Vec::new(),
        entry_condition_instances: vec![entry_condition_instance],
        sampled_feature_parity: Some(sampled_feature_parity),
        fact_delivery: Some(report_fixtures::pending_fact_delivery(&ids.report)),
        operation_log: report_operation_log(ids),
    }
}

fn report_equity_snapshot(
    ids: &TxnIds,
    equity_snapshot_id: &EquitySnapshotId,
) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: equity_snapshot_id.clone(),
        as_of: Utc::now(),
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd: Usd::new(dec!(10000)),
        drawdown_pct: Decimal::ZERO,
        account_snapshot_ref: Some(ids.account_snapshot.clone()),
    }
}

fn report_portfolio_plan(ids: &TxnIds) -> NewPortfolioPlan {
    NewPortfolioPlan {
        portfolio_plan_id: ids.portfolio_plan.clone(),
        model_run_id: Some(ids.model_run.clone()),
        market_selection_id: ids.market_selection.clone(),
        decision_at: Utc::now(),
        budget_usd: Usd::new(dec!(10000)),
        allocated_usd: Usd::new(NOTIONAL),
        risk_budget_json: PortfolioRiskBudget::default(),
        constraints_json: PortfolioConstraintsSnapshot::default(),
        rejected_summary: PortfolioRejectedSummary::default(),
        optimizer_meta_json: PortfolioOptimizerMeta::default(),
    }
}

fn report_row(ids: &TxnIds, equity_snapshot_id: EquitySnapshotId) -> NewRecommendationReport {
    NewRecommendationReport {
        recommendation_report_id: ids.report.clone(),
        profile_id: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref().id,
        profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
        report_kind: ReportKind::TopN,
        decision_at: Utc::now(),
        horizon_secs: 86_400,
        runtime_mode: QuantRuntimeMode::AutoExecution,
        runtime_config_version_id: ids.runtime_config_version.clone(),
        model_run_id: None,
        model_version_id: ids.model_version.clone(),
        market_selection_id: ids.market_selection.clone(),
        portfolio_plan_id: ids.portfolio_plan.clone(),
        top_n: 20,
        status: RecommendationReportStatus::Prepared,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: ids.account_snapshot.clone(),
        equity_snapshot_ref: equity_snapshot_id,
        data_quality_snapshot_ref: ids.data_quality_snapshot.clone(),
        summary_json: report_summary(),
        published_at: None,
        successor_report_id: None,
        superseded_at: None,
        obsoleted_at: None,
        valid_until: Some(Utc::now() + chrono::Duration::hours(1)),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
    }
}

fn report_recommendation(ids: &TxnIds) -> NewRecommendation {
    NewRecommendation {
        profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
        recommendation_id: ids.recommendation.clone(),
        recommendation_report_id: ids.report.clone(),
        rank: 1,
        market_id: MarketId::new(&ids.market),
        event_id: EventId::new(&ids.event),
        token_id: TokenId::new("token-1"),
        outcome_side: OutcomeSide::Yes,
        composite_score: Probability::new(dec!(0.7)),
        risk_adjusted_score: Probability::new(dec!(0.65)),
        confidence: Probability::new(dec!(0.72)),
        expected_return_bps: Bps::new(dec!(150)),
        downside_bps: Bps::new(dec!(80)),
        identity: recommendation_identity(),
        market_context: market_context(),
        rank_before_portfolio: 1,
        liquidity_score: Probability::new(dec!(0.8)),
        data_quality_score: Probability::new(dec!(0.9)),
        model_score_percentile: Probability::new(dec!(0.75)),
        trade_plan: trade_plan(),
        factor_breakdown: factor_breakdown(),
        evidence_refs: evidence_refs(),
        execution_eligibility: execution_eligibility(),
        valid_from: Utc::now(),
        valid_until: Utc::now() + chrono::Duration::hours(1),
        status: RecommendationStatus::Prepared,
    }
}

fn report_operation_log(ids: &TxnIds) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("scheduled:test:{}", ids.report),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("test".to_owned()),
        category: OperationCategory::QuantReport,
        action: "publish".to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(ids.report.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: "/test/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "test": true }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
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
        request_id: format!("intent-terminal-test:{intent_id}:{request_suffix}"),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("test".to_owned()),
        category: OperationCategory::Other,
        action: action.to_owned(),
        resource_type: Some(ResourceType::OrderIntent),
        resource_id: Some(intent_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: "/test/quant/intents/expire".to_owned(),
        http_status: 200,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "test": true }),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

fn new_account_snapshot() -> NewAccountSnapshot {
    let positions = vec![PositionSnapshot {
        token_id: TokenId::new("token-1"),
        market_id: MarketId::new("0xmarket"),
        event_id: Some(EventId::new("evt-1")),
        category: MarketCategory::Politics,
        outcome: "Yes".to_owned(),
        size: Shares::new(dec!(100)),
        avg_price: Price::new(dec!(0.5)),
        cur_price: Price::new(dec!(0.6)),
        current_value: Usd::new(dec!(60)),
        redeemable: false,
    }];
    NewAccountSnapshot {
        account_snapshot_id: AccountSnapshotId::from_v7(),
        as_of: Utc::now(),
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Passive {
            limit_price: Price::new(dec!(0.6)),
            post_only: true,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now() + chrono::Duration::hours(1),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        cancel_if_not_triggered: true,
        entry_reason: "immediate".to_owned(),
    }
}

fn sizing_plan() -> SizingPlan {
    SizingPlan {
        suggested_usd: Usd::new(NOTIONAL),
        suggested_shares: Shares::new(dec!(100)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.025),
        market_exposure_after_usd: Usd::new(NOTIONAL),
        event_exposure_after_usd: Usd::new(NOTIONAL),
        category_exposure_after_usd: Usd::new(NOTIONAL),
        binding_constraint: BindingConstraint::KellyCap,
        sizing_reason: "kelly".to_owned(),
        sizing_model: SizingModelKind::Kelly,
        edge_bps: Some(Bps::new(dec!(100))),
        kelly_fraction_applied: Some(dec!(0.5)),
        edge_uncertainty_shrink_applied: None,
        correlation_shrink_applied: None,
        f_star_applied: None,
        kelly_fraction_config_applied: None,
        confidence_shrink_applied: None,
        drawdown_shrink_applied: None,
        raw_fraction_applied: None,
        position_cap_fraction_applied: None,
    }
}

fn exit_plan() -> ExitPlan {
    ExitPlan {
        take_profit_price: Some(Price::new(dec!(0.8))),
        take_profit_pct: None,
        stop_loss_price: Some(Price::new(dec!(0.4))),
        stop_loss_pct: None,
        time_exit_at: None,
        max_hold_secs: Some(86_400),
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: dec!(0.6),
            min_expected_return_bps: Bps::ZERO,
            require_execution_eligibility: true,
        },
        opportunistic_exit: opportunistic_exit_policy(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl".to_owned(),
    }
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

fn trade_plan() -> RecommendationTradePlan {
    let artifact_hash = content_hash('f');
    let dimension = TradePolicyCohortDimension {
        methodology_id: "fixture-v1".to_owned(),
        methodology_hash: artifact_hash.clone(),
        bucket_id: "fixture".to_owned(),
    };
    RecommendationTradePlan::Frozen {
        policy: Box::new(TradePolicyCohortProvenance {
            artifact_id: TradePolicyArtifactId::from_content_hash(&artifact_hash),
            artifact_hash,
            cohort_index: 0,
            cohort_key: TradePolicyCohortKey {
                profile_ref: builtin_research_profiles()
                    .expect("research profiles")
                    .into_iter()
                    .next()
                    .expect("control profile")
                    .profile_ref,
                category: MarketCategory::Politics,
                horizon_secs: 86_400,
                entry_price_min: Price::new(dec!(0.01)),
                entry_price_max: Price::new(dec!(0.99)),
                cash_budget_tier: Usd::new(NOTIONAL),
                liquidity: dimension.clone(),
                volatility: dimension,
            },
        }),
        entry: entry_plan(),
        sizing: Box::new(sizing_plan()),
        exit: Box::new(exit_plan()),
        risk_envelope: Box::new(risk_envelope()),
    }
}

fn risk_envelope() -> RiskEnvelope {
    RiskEnvelope {
        max_loss_usd: Usd::new(dec!(120)),
        max_slippage_bps: Bps::new(dec!(50)),
        max_position_usd: Usd::new(dec!(500)),
        max_market_exposure_usd: Usd::new(dec!(500)),
        max_event_exposure_usd: Usd::new(dec!(750)),
        max_category_exposure_usd: Usd::new(dec!(1500)),
        requires_approval: true,
        auto_execution_allowed: true,
        risk_notes: Vec::new(),
        envelope_hash: content_hash('f'),
    }
}

fn factor_breakdown() -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(vec![FactorBreakdownEntry {
        factor_name: "liquidity_depth".to_owned(),
        family: FactorFamily::Liquidity,
        value_state: FactorValueState::Scored,
        raw_value: Some(dec!(1234.5)),
        normalized_score: Some(Probability::new(dec!(0.8))),
        normalization_source: Some(NormalizationSource::CrossSection),
        indeterminate_reason: None,
        weight: dec!(0.4),
        contribution: dec!(0.32),
        confidence: Probability::new(dec!(0.75)),
        direction: FactorDirection::Positive,
        explanation: "deep".to_owned(),
        source_refs: Vec::new(),
    }])
}

fn recommendation_identity() -> RecommendationIdentity {
    RecommendationIdentity {
        category: MarketCategory::Politics,
        question: "Will the event resolve Yes?".to_owned(),
        outcome_name: "Yes".to_owned(),
    }
}

const fn market_context() -> MarketContext {
    MarketContext {
        best_bid: Some(Price::new(dec!(0.41))),
        best_ask: Some(Price::new(dec!(0.43))),
        mid_price: Some(Price::new(dec!(0.42))),
        spread_bps: Some(Bps::new(dec!(50))),
        depth_usd: Usd::new(dec!(5000)),
        volume_24h_usd: Some(Usd::new(dec!(10000))),
        book_age_ms: 500,
        time_to_resolution_secs: Some(86_400),
        market_status: MarketStatus::Active,
        neg_risk: false,
        tick_size: quant_pivot_models::enums::common::TickSize::Hundredth,
        fee_rate: None,
    }
}

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: BookSnapshotRef::from_str(&format!(
            "book:l2|token-1|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
            "1".repeat(64),
            "0".repeat(64),
        ))
        .expect("book ref"),
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        model_version_id: ModelVersionId::from_v7(),
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ReportDataQualitySnapshotId::from_v7(),
    }
}

fn execution_eligibility() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![
            QuantRuntimeMode::ReportOnly,
            QuantRuntimeMode::SemiAuto,
            QuantRuntimeMode::AutoExecution,
        ],
        ineligibility_reasons: Vec::new(),
        approval_required: false,
        auto_policy_id: None,
        uncalibrated_watermark: false,
    }
}

fn report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        candidate_count: 1,
        rejected_count: 0,
        published_recommendation_count: 1,
        total_suggested_usd: Usd::new(NOTIONAL),
        max_single_recommendation_usd: Usd::new(NOTIONAL),
        aggregate_exposure_cap_usd: None,
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        average_score: Probability::new(dec!(0.7)),
        min_score: Probability::new(dec!(0.7)),
        model_confidence_summary: ConfidenceSummary::default(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary::default(),
        empty_reason: None,
        warnings: Vec::new(),
    }
}

// ── Phase 05.6: exit submission (per-lot capital + position settlement) ───────

/// Drive an approved intent's entry to a confirmed full fill: capital `Spent`,
/// one open lot (100 @ 0.60), intent `Filled`.
async fn fill_entry_lot(
    db: &sea_orm::DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &TxnIds,
    intent_id: &OrderIntentId,
) {
    claim_entry_for_test(db, submission, intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn entry_fill_freezes_scale_out_denominator() {
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
        order_intent_id: intent_id.clone(),
        order_phase: ExecutionOrderPhase::Exit,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new("token-1"),
        side: Side::Sell,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(price),
        shares: Shares::new(shares),
        cost_usd: Shares::new(shares) * Price::new(price),
        prepared_order_json: prepared_order(
            Side::Sell,
            OrderType::Gtc,
            quant_pivot_models::types::VenueOrderAmount::Shares(Shares::new(shares)),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn exit_full_releases_capital_with_realized_pnl() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    // Write-ahead the exit: lot Open -> Closing.
    let exit = submission
        .create_exit_order_and_mark_closing(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn exit_partial_keeps_capital_spent_and_reduces_lot() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    let exit = submission
        .create_exit_order_and_mark_closing(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn exit_rejects_second_in_flight_order() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    submission
        .create_exit_order_and_mark_closing(
            exit_order(&intent_id, &ids, dec!(100), dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("first exit order");

    let err = submission
        .create_exit_order_and_mark_closing(
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
