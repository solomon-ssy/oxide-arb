//! Phase 05.4 — execution-submission repository integration tests (Postgres).
//!
//! Requires Docker. Exercises the money-critical cross-table transactions:
//! claim (double-submit guard), capital lock on write-ahead, and venue-result
//! settlement (full fill → spent + position; ambiguous → hold + reconcile;
//! rejected → release), plus boot recovery of in-flight orders.

use std::{collections::BTreeMap, str::FromStr};

use chrono::Utc;
use quant_pivot_models::{
    domain::{
        CapitalReconcileSettlement, CapitalSettlement, ExitLedgerWrite, NewAccountSnapshot,
        NewCapitalAllocation, NewExecutionOrder, NewMarketSelection, NewModelRun, NewModelSpec,
        NewModelVersion, NewOperationLog, NewOrderIntent, NewPortfolioPlan, NewRecommendation,
        NewRecommendationReport, NewReconciliation, NewReportDataQualitySnapshot,
        NewReportTransaction, NewRuntimeConfigVersion, PositionExit, PositionFill,
        ReconciliationLedgerWrite, SubmissionLedgerWrite,
    },
    entities::quant_market_selection::{SelectionExcludedMarketIds, SelectionIncludedMarketIds},
    entities::{quant_order_intent, quant_recommendation},
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, ExitReason, ExitState, OrderIntentKind,
            OrderTypeKind, PositionLedgerState, ReconciliationEvidenceKind, ReconciliationResult,
            VenueOrderStatus,
        },
        factor::FactorFamily,
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, EntryTriggerKind,
            ExecutionOrderState, ModelRunKind, ModelRunStatus, OrderIntentStatus, OutcomeSide,
            PublicationStatus, QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus,
            ReportKind, ReportTriggerKind, SettlementPolicy, SizingModelKind,
        },
        rbac::ResourceType,
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        AccountPositions, AccountSnapshotId, Bps, CapitalAllocationId, ConfidenceSummary,
        ContentHash, DataQualitySummary, EligibilitySummary, EntryOrderSpec, EntryPlan, EventId,
        EvidenceRefs, ExecutionEligibility, ExecutionOrderId, ExitPlan, ExitPolicySpec,
        ExposureBreakdown, FactorBreakdownEntry, FeatureVectorId, MarketContext, MarketId,
        MarketSelectionId, ModelRunId, ModelSpecId, ModelVersionId, OperationLogId, OrderId,
        OrderIntentId, PortfolioConstraintsSnapshot, PortfolioPlanId, PortfolioRejectedSummary,
        PortfolioRiskBudget, PositionSnapshot, Price, Probability, RecommendationFactorBreakdown,
        RecommendationId, RecommendationIdentity, RecommendationReportId, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportSummary, RiskEnvelope, RuntimeConfigVersionId,
        SchemaVersion, SelectionExclusionSummary, Shares, SignalCandidateId, SizingPlan, TokenId,
        Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgEventRepository, PgExecutionSubmissionRepository,
        PgMarketRepository, PgMarketSelectionRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgOrderIntentRepository, PgPositionRepository,
        PgRecommendationReportRepository, PgRecommendationRepository, PgReconciliationRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        CapitalAllocationRepository, EventRepository, ExecutionSubmissionRepository,
        MarketRepository, MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        OrderIntentRepository, PositionRepository, RecommendationReportRepository,
        RecommendationRepository, ReconciliationRepository, RuntimeConfigVersionRepository,
    },
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait, IntoActiveModel};

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

    let claimed = submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("first claim succeeds");
    assert_eq!(claimed.status, OrderIntentStatus::AdmissionPending);

    let second = submission
        .claim_for_submission(&intent_id, Utc::now())
        .await;
    assert!(
        second.is_err(),
        "a second concurrent claim must fail (intent no longer submittable)",
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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
async fn create_entry_advances_recommendation_to_executed() {
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
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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
    let first = position_fill(&ids, &intent_id);
    position_repo
        .apply_fill(first.clone())
        .await
        .expect("first fill");

    let second = PositionFill {
        shares: Shares::new(dec!(50)),
        price: Price::new(dec!(0.8)),
        cost_usd: Usd::new(dec!(40)),
        ..first
    };
    position_repo.apply_fill(second).await.expect("second fill");

    let position = position_repo
        .find_by_intent(&intent_id)
        .await
        .expect("position")
        .expect("row");
    assert_eq!(position.shares, Shares::new(dec!(150)));
    // (60 + 40) / 150 = 0.666...
    assert_eq!(position.cost_usd, Usd::new(dec!(100)));
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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

    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, &ids))
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

    let err = PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_pending_intent(&ids),
            new_allocation_for(&ids, OrderIntentId::from_v7()),
        )
        .await
        .expect_err("executed rec must block create");
    assert!(matches!(
        err,
        quant_pivot_error::storage::StorageError::Conflict(_)
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
        )
        .await
        .expect_err("submitted intent must block create");
    assert!(matches!(
        err,
        quant_pivot_error::storage::StorageError::Conflict(_)
    ));
}

fn new_pending_intent(ids: &TxnIds) -> NewOrderIntent {
    new_pending_intent_with_id(ids, OrderIntentId::from_v7())
}

fn new_pending_intent_with_id(ids: &TxnIds, order_intent_id: OrderIntentId) -> NewOrderIntent {
    NewOrderIntent {
        order_intent_id,
        recommendation_id: ids.recommendation.clone(),
        runtime_mode: QuantRuntimeMode::SemiAuto,
        runtime_config_version_id: ids.runtime_config_version.clone(),
        model_version_id: ids.model_version.clone(),
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
        entry_order_json: EntryOrderSpec {
            token_id: TokenId::new("token-1"),
            side: Side::Buy,
            order_type: OrderType::Gtc,
            limit_price: Price::new(dec!(0.6)),
            shares: Shares::new(dec!(100)),
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
            signal_invalidation_rules: Vec::new(),
            partial_exit_nodes: Vec::new(),
            settlement_policy: SettlementPolicy::ExitBeforeResolution,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.6)),
            entry_composite_score: Probability::new(dec!(0.8)),
        },
        risk_envelope_hash: content_hash('e'),
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
    submission
        .claim_for_submission(&intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(&intent_id, ids))
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
        discrepancy_usd: None,
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
        discrepancy_usd: None,
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
        discrepancy_usd: None,
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
        discrepancy_usd: None,
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
        discrepancy_usd: Some(Usd::new(PARTIAL_SPENT - PARTIAL_SHARES * dec!(0.6))),
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
                discrepancy_usd: None,
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
        }]),
        venue_filled_shares: None,
        venue_avg_price: None,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

// ── Fixture chain (self-contained; mirrors pg_account_capital) ────────────────

async fn seed_approved_intent(db: &sea_orm::DatabaseConnection, ids: &TxnIds) -> OrderIntentId {
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            NewOrderIntent {
                order_intent_id: order_intent_id.clone(),
                recommendation_id: ids.recommendation.clone(),
                runtime_mode: QuantRuntimeMode::AutoExecution,
                runtime_config_version_id: ids.runtime_config_version.clone(),
                model_version_id: ids.model_version.clone(),
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
                entry_order_json: EntryOrderSpec {
                    token_id: TokenId::new("token-1"),
                    side: Side::Buy,
                    order_type: OrderType::Gtc,
                    limit_price: Price::new(dec!(0.6)),
                    shares: Shares::new(dec!(100)),
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
                    signal_invalidation_rules: Vec::new(),
                    partial_exit_nodes: Vec::new(),
                    settlement_policy: SettlementPolicy::ExitBeforeResolution,
                    manual_review_at: None,
                    entry_reference_price: Price::new(dec!(0.6)),
                    entry_composite_score: Probability::new(dec!(0.8)),
                },
                risk_envelope_hash: content_hash('e'),
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
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

async fn seed_report_fixture(db: &sea_orm::DatabaseConnection) -> TxnIds {
    let rc_id = seed_runtime_config(db).await;
    let (model_version_id, model_run_id) = seed_model_version(db, &rc_id).await;
    let event_id = "evt-1";
    let market_id = "0xmarket";
    seed_market_catalog(db, event_id, market_id).await;
    let market_selection_id = seed_market_selection(db, &rc_id, market_id).await;
    let ids = TxnIds {
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        model_version: model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        runtime_config_version: rc_id,
        market: market_id.to_owned(),
        event: event_id.to_owned(),
    };
    PgRecommendationReportRepository::new(db.clone())
        .create_report(build_report_transaction(&ids))
        .await
        .expect("create report");
    ids
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
            artifact_hash: content_hash('a'),
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
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
    market_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id.clone(),
                as_of: Utc::now(),
                runtime_config_version_id: rc_id.clone(),
                selector_hash: content_hash('b'),
                market_count: 1,
                included_market_ids: SelectionIncludedMarketIds(vec![market_id.to_owned()]),
                excluded_market_ids: SelectionExcludedMarketIds(Vec::new()),
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

struct TxnIds {
    account_snapshot: AccountSnapshotId,
    data_quality_snapshot: ReportDataQualitySnapshotId,
    portfolio_plan: PortfolioPlanId,
    report: RecommendationReportId,
    recommendation: RecommendationId,
    model_version: ModelVersionId,
    model_run: ModelRunId,
    market_selection: MarketSelectionId,
    runtime_config_version: RuntimeConfigVersionId,
    market: String,
    event: String,
}

fn build_report_transaction(ids: &TxnIds) -> NewReportTransaction {
    NewReportTransaction {
        account_snapshot: NewAccountSnapshot {
            account_snapshot_id: ids.account_snapshot.clone(),
            ..new_account_snapshot()
        },
        data_quality_snapshot: NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ids.data_quality_snapshot.clone(),
            as_of: Utc::now(),
            runtime_config_version_id: ids.runtime_config_version.clone(),
            tokens_json: ReportDataQualityTokens(Vec::new()),
        },
        portfolio_plan: NewPortfolioPlan {
            portfolio_plan_id: ids.portfolio_plan.clone(),
            model_run_id: Some(ids.model_run.clone()),
            market_selection_id: ids.market_selection.clone(),
            as_of: Utc::now(),
            budget_usd: Usd::new(dec!(10000)),
            allocated_usd: Usd::new(NOTIONAL),
            risk_budget_json: PortfolioRiskBudget::default(),
            constraints_json: PortfolioConstraintsSnapshot::default(),
            rejected_summary: PortfolioRejectedSummary::default(),
        },
        report: NewRecommendationReport {
            recommendation_report_id: ids.report.clone(),
            report_kind: ReportKind::TopN,
            trigger_kind: ReportTriggerKind::Scheduled,
            trigger_key: format!("scheduled:test:{}", ids.report),
            trigger_time: Utc::now(),
            source_delay_secs: 10,
            as_of: Utc::now(),
            horizon_secs: 86_400,
            runtime_mode: QuantRuntimeMode::AutoExecution,
            runtime_config_version_id: ids.runtime_config_version.clone(),
            model_version_id: ids.model_version.clone(),
            market_selection_id: ids.market_selection.clone(),
            portfolio_plan_id: ids.portfolio_plan.clone(),
            top_n: 20,
            status: RecommendationReportStatus::Published,
            account_source: AccountSource::Polymarket,
            capital_base_usd: Usd::new(dec!(10000)),
            account_snapshot_ref: ids.account_snapshot.clone(),
            data_quality_snapshot_ref: ids.data_quality_snapshot.clone(),
            summary_json: report_summary(),
            published_at: Some(Utc::now()),
            valid_until: Some(Utc::now() + chrono::Duration::hours(1)),
            revoked_at: None,
            expired_at: None,
            status_reason: None,
        },
        recommendations: vec![NewRecommendation {
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
            entry_plan: entry_plan(),
            sizing_plan: sizing_plan(),
            exit_plan: exit_plan(),
            risk_envelope: risk_envelope(),
            factor_breakdown: factor_breakdown(),
            evidence_refs: evidence_refs(),
            execution_eligibility: execution_eligibility(),
            valid_from: Utc::now(),
            valid_until: Utc::now() + chrono::Duration::hours(1),
            status: RecommendationStatus::Published,
        }],
        operation_log: report_operation_log(ids),
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
        equity_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        trigger_kind: EntryTriggerKind::Immediate,
        trigger_price: None,
        limit_price: Some(Price::new(dec!(0.6))),
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now() + chrono::Duration::hours(1),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        confirmation_window_secs: 30,
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
        partial_exit_nodes: Vec::new(),
        trailing_stop: None,
        signal_invalidation_rules: Vec::new(),
        settlement_policy: SettlementPolicy::HoldToResolution,
        manual_review_at: None,
        exit_reason: "tp/sl".to_owned(),
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
        raw_value: Some(dec!(1234.5)),
        normalized_score: Probability::new(dec!(0.8)),
        weight: dec!(0.4),
        contribution: dec!(0.32),
        confidence: Probability::new(dec!(0.75)),
        direction: quant_pivot_models::enums::quant::FactorDirection::Positive,
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
        fee_rate: None,
    }
}

fn evidence_refs() -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        book_snapshot_ref: quant_pivot_models::types::BookSnapshotRef::from_str(&format!(
            "book:live:token-1:1:1700000000@blake3:{}",
            "0".repeat(64)
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
    submission
        .claim_for_submission(intent_id, Utc::now())
        .await
        .expect("claim");
    let order = submission
        .create_entry_order_and_lock_capital(new_execution_order(intent_id, ids))
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
    let _ = db;
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
            Some("tp1".to_owned()),
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
    assert!(intent.executed_partial_exit_node_ids.contains("tp1"));
    assert!(intent.pending_partial_exit_node_id.is_none());
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
