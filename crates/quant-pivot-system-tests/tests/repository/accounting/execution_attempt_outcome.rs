//! Immutable execution-attempt outcome persistence contracts.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        ExecutionAttemptDeferredReason, ExecutionAttemptOutcomeInfo,
        ExecutionAttemptReconciliationResult, NewExecutionAttemptOutcome, NewExecutionOrder,
        NewPosition, NewReconciliation, OutcomeTaskSettlement,
    },
    entities::{
        quant_execution_order::Entity as QuantExecutionOrderEntity,
        quant_order_intent::{
            ActiveModel as QuantOrderIntentActiveModel, Entity as QuantOrderIntentEntity,
        },
        quant_position::Entity as QuantPositionEntity,
        quant_reconciliation::Entity as QuantReconciliationEntity,
    },
    enums::{
        common::{MarketCategory, OrderType, Side},
        execution::{
            ExecutionOrderPhase, ExitReason, ExitState, OrderTypeKind, PositionLedgerState,
            ReconciliationEvidenceKind, ReconciliationResult, VenueOrderStatus,
        },
        quant::{
            AccountSource, ExecutionAttemptNoFillReason, ExecutionAttemptTerminalState,
            ExecutionOrderState, OrderIntentStatus, OutcomeSide, QuantRuntimeMode,
        },
    },
    types::{
        ContentHash, EventId, ExecutionOrderId, MarketId, OrderId, OrderIntentId, PositionId,
        Price, ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId,
        SchemaVersion, Shares, TokenId, Usd, VenueOrderAmount, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::PgExecutionAttemptOutcomeRepository, traits::ExecutionAttemptOutcomeRepository,
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::execution_pg_seed::{
        ExecutionTxnIds, entry_execution_order, prepared_order, seed_approved_intent,
        seed_report_fixture,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel,
};

#[derive(Clone, Copy)]
enum SourceShape {
    Unfilled,
    PartiallyFilled,
    FullyFilled,
}

#[derive(Clone, Copy)]
struct SourceShapeContract {
    intent_status: OrderIntentStatus,
    order_state: ExecutionOrderState,
    venue_status: VenueOrderStatus,
    reconciliation_result: ReconciliationResult,
    filled_shares: Shares,
    average_price: Option<Price>,
}

impl SourceShape {
    const fn contract(self) -> SourceShapeContract {
        match self {
            Self::Unfilled => SourceShapeContract {
                intent_status: OrderIntentStatus::Rejected,
                order_state: ExecutionOrderState::Failed,
                venue_status: VenueOrderStatus::Rejected,
                reconciliation_result: ReconciliationResult::NotFilled,
                filled_shares: Shares::ZERO,
                average_price: None,
            },
            Self::PartiallyFilled => SourceShapeContract {
                intent_status: OrderIntentStatus::PartiallyFilled,
                order_state: ExecutionOrderState::PartiallyFilled,
                venue_status: VenueOrderStatus::PartiallyFilled,
                reconciliation_result: ReconciliationResult::PartiallyFilled,
                filled_shares: Shares::new(dec!(16)),
                average_price: Some(Price::new(dec!(0.6))),
            },
            Self::FullyFilled => SourceShapeContract {
                intent_status: OrderIntentStatus::Filled,
                order_state: ExecutionOrderState::Filled,
                venue_status: VenueOrderStatus::Filled,
                reconciliation_result: ReconciliationResult::Filled,
                filled_shares: Shares::new(dec!(40)),
                average_price: Some(Price::new(dec!(0.6))),
            },
        }
    }
}

struct ExecutionSourceFixture {
    ids: ExecutionTxnIds,
    order_intent_id: OrderIntentId,
    entry_execution_order_id: ExecutionOrderId,
    entry_reconciliation_id: ReconciliationId,
    position_id: Option<PositionId>,
    terminal_at: DateTime<Utc>,
}

struct ExecutionSourceSeed<'a> {
    db: &'a DatabaseConnection,
    ids: &'a ExecutionTxnIds,
    order_intent_id: OrderIntentId,
    shape: SourceShape,
    contract: SourceShapeContract,
    terminal_at: DateTime<Utc>,
    entry_filled_at: DateTime<Utc>,
}

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

pub async fn terminal_preserve_zero_semantics() {
    let unfilled = (SourceShape::Unfilled).persist_shape().await;
    assert_eq!(
        unfilled.terminal_state,
        ExecutionAttemptTerminalState::Unfilled
    );
    assert_eq!(unfilled.filled_shares, Shares::ZERO);
    assert_eq!(
        unfilled.fill_ratio().expect("valid unfilled ratio"),
        Decimal::ZERO
    );
    assert_eq!(unfilled.entry_fee_usd, Some(Usd::ZERO));
    assert_eq!(unfilled.realized_pnl_usd, None);
    assert_eq!(unfilled.position_id, None);

    let partial = (SourceShape::PartiallyFilled).persist_shape().await;
    assert_eq!(
        partial.terminal_state,
        ExecutionAttemptTerminalState::PartiallyFilled
    );
    assert_eq!(partial.filled_shares, Shares::new(dec!(16)));
    assert_eq!(
        partial.fill_ratio().expect("valid partial-fill ratio"),
        dec!(0.4)
    );
    assert_eq!(partial.entry_fee_usd, Some(Usd::ZERO));
    assert_eq!(partial.exit_fee_usd, Some(Usd::ZERO));
    assert_eq!(partial.realized_pnl_usd, Some(Usd::ZERO));

    let full = (SourceShape::FullyFilled).persist_shape().await;
    assert_eq!(
        full.terminal_state,
        ExecutionAttemptTerminalState::FullyFilled
    );
    assert_eq!(
        full.fill_ratio().expect("valid full-fill ratio"),
        Decimal::ONE
    );
    assert_eq!(full.realized_pnl_usd, Some(Usd::ZERO));
}

pub async fn invalid_state_report_rejects() {
    let (pool, database) = setup_pg().await;
    let db = pool.connection().clone();
    let source = seed_execution_source(&db, SourceShape::Unfilled).await;

    let mut fake_pnl = new_outcome(&source, SourceShape::Unfilled);
    fake_pnl.realized_pnl_usd = Some(Usd::ZERO);
    fake_pnl
        .validate()
        .expect_err("an unfilled attempt has no PnL value, including zero");

    let mut report_only = new_outcome(&source, SourceShape::Unfilled);
    report_only.runtime_mode = QuantRuntimeMode::ReportOnly;
    report_only
        .validate()
        .expect_err("ReportOnly cannot produce an execution outcome");

    let mut wrong_fill_shape = new_outcome(&source, SourceShape::PartiallyFilled);
    wrong_fill_shape.filled_shares = wrong_fill_shape.requested_shares;
    wrong_fill_shape
        .validate()
        .expect_err("partial fill must remain strictly below requested shares");

    let mut future_source = new_outcome(&source, SourceShape::Unfilled);
    future_source.terminal_at = Utc::now() + Duration::days(1);
    future_source
        .expected_outcome_hash(Utc::now(), Utc::now())
        .expect_err("terminal time not backed by the frozen source graph must fail");

    drop(db);
    drop(pool);
    drop(database);

    let (report_pool, _report_database) = setup_pg().await;
    let report_db = report_pool.connection().clone();
    let _never_submitted = seed_report_fixture(&report_db).await;
    let report_repository = PgExecutionAttemptOutcomeRepository::new(report_db);
    assert!(
        report_repository
            .claim_reconciliation(Utc::now(), WorkerId::from_v7(), 60, 10,)
            .await
            .expect("scan absent execution outcomes")
            .is_empty(),
        "a recommendation without a submitted order must not acquire an unfilled row"
    );
}

pub async fn reconcile_idempotent_worm_evident() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let source = seed_execution_source(&db, SourceShape::FullyFilled).await;
    let repository = PgExecutionAttemptOutcomeRepository::new(db.clone());
    let initial_cutoff = db.statement_time().await;
    let inserted = repository
        .reconcile_intent(&source.order_intent_id, initial_cutoff)
        .await
        .expect("reconcile execution outcome");
    let inserted = match inserted {
        ExecutionAttemptReconciliationResult::Inserted(outcome) => outcome,
        ExecutionAttemptReconciliationResult::AlreadyPresent(_) => {
            panic!("first append must insert the outcome")
        }
        ExecutionAttemptReconciliationResult::Deferred(reason) => {
            panic!("complete source graph must not defer: {reason:?}")
        }
    };
    assert!(inserted.source_observed_at <= inserted.available_at);
    assert!(inserted.available_at <= inserted.created_at);

    let duplicate = repository
        .reconcile_intent(&source.order_intent_id, initial_cutoff)
        .await
        .expect("idempotent execution-outcome retry");
    let duplicate = match duplicate {
        ExecutionAttemptReconciliationResult::AlreadyPresent(outcome) => outcome,
        ExecutionAttemptReconciliationResult::Inserted(_) => {
            panic!("exact retry must not insert another outcome")
        }
        ExecutionAttemptReconciliationResult::Deferred(reason) => {
            panic!("complete source graph must not defer: {reason:?}")
        }
    };
    assert_eq!(duplicate.outcome_hash, inserted.outcome_hash);

    let position = QuantPositionEntity::find_by_id(source.position_id.expect("terminal position"))
        .one(&db)
        .await
        .expect("load terminal position")
        .expect("terminal position exists");
    let mut conflicting_position = position.into_active_model();
    conflicting_position.realized_pnl_usd = ActiveValue::Set(Usd::new(dec!(1)));
    conflicting_position.updated_at = ActiveValue::Set(Utc::now());
    conflicting_position
        .update(&db)
        .await
        .expect("simulate late conflicting source correction");
    assert!(matches!(
        repository
            .reconcile_intent(&source.order_intent_id, db.statement_time().await)
            .await
            .expect_err("same recommendation cannot acquire different immutable content"),
        StorageError::StateConflict { .. }
    ));

    let mutation = db
        .execute_unprepared(
            "UPDATE quant_execution_attempt_outcome \
             SET realized_pnl_usd = 1",
        )
        .await;
    assert!(mutation.is_err(), "WORM trigger must reject updates");
    let deletion = db
        .execute_unprepared("DELETE FROM quant_execution_attempt_outcome")
        .await;
    assert!(deletion.is_err(), "WORM trigger must reject deletes");

    db.execute_unprepared(
        "ALTER TABLE quant_execution_attempt_outcome \
         DISABLE TRIGGER trg_quant_execution_attempt_outcome_append_only",
    )
    .await
    .expect("disable trigger to simulate storage corruption");
    db.execute_unprepared(
        "UPDATE quant_execution_attempt_outcome \
         SET realized_pnl_usd = 1",
    )
    .await
    .expect("simulate stored semantic-content tampering");
    db.execute_unprepared(
        "ALTER TABLE quant_execution_attempt_outcome \
         ENABLE TRIGGER trg_quant_execution_attempt_outcome_append_only",
    )
    .await
    .expect("restore append-only trigger");

    assert!(matches!(
        repository
            .find_by_intent(&source.order_intent_id)
            .await
            .expect_err("read boundary must detect stored content tampering"),
        StorageError::InvariantViolation { .. }
    ));
}

pub async fn reconciliation_candidates_require_source() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let terminal = seed_execution_source(&db, SourceShape::Unfilled).await;
    let pre_submission = seed_report_fixture(&db).await;
    let pre_submission_intent = seed_approved_intent(&db, &pre_submission).await;
    let repository = PgExecutionAttemptOutcomeRepository::new(db.clone());
    let cutoff = db.statement_time().await;
    let worker = WorkerId::from_v7();

    let claims = repository
        .claim_reconciliation(cutoff, worker, 60, 10)
        .await
        .expect("execution reconciliation candidates");
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0].candidate.order_intent_id,
        terminal.order_intent_id
    );
    assert_eq!(
        claims[0].candidate.recommendation_id,
        terminal.ids.recommendation
    );
    assert!(
        claims
            .iter()
            .all(|claim| claim.candidate.order_intent_id != pre_submission_intent),
        "pre-submission intent must not be fabricated as an unfilled execution outcome"
    );
    let lagging_barrier = repository
        .barrier(cutoff)
        .await
        .expect("read attempt barrier with pending terminal intent");
    assert_eq!(lagging_barrier.eligible_unsealed_count, 1);
    assert!(
        lagging_barrier.sealed_through <= cutoff,
        "pending attempt work must keep the truth frontier at or before its cutoff"
    );

    let competing_claim = repository
        .claim_reconciliation(cutoff, WorkerId::from_v7(), 60, 10)
        .await
        .expect("competing execution task claim");
    assert!(competing_claim.is_empty());

    repository
        .reconcile_intent(&terminal.order_intent_id, cutoff)
        .await
        .expect("seal terminal execution outcome");
    repository
        .settle_reconciliation(
            terminal.order_intent_id,
            worker,
            OutcomeTaskSettlement::Completed,
        )
        .await
        .expect("complete durable reconciliation task");
    let sealed_barrier = repository
        .barrier(cutoff)
        .await
        .expect("read sealed attempt barrier");
    assert_eq!(sealed_barrier.eligible_unsealed_count, 0);
    assert_eq!(sealed_barrier.sealed_through, cutoff);
    assert!(
        repository
            .claim_reconciliation(cutoff, WorkerId::from_v7(), 60, 10)
            .await
            .expect("execution candidates after seal")
            .is_empty()
    );
}

pub async fn late_source_defers() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let source = seed_execution_source(&db, SourceShape::Unfilled).await;
    let repository = PgExecutionAttemptOutcomeRepository::new(db);
    let frozen_before_source = source.terminal_at;

    assert!(
        repository
            .claim_reconciliation(frozen_before_source, WorkerId::from_v7(), 60, 10,)
            .await
            .expect("scan candidates before database source visibility")
            .is_empty(),
        "an intent created after the cutoff cannot enter the frozen candidate universe"
    );
    assert_eq!(
        repository
            .reconcile_intent(&source.order_intent_id, frozen_before_source)
            .await
            .expect("late execution source returns a typed deferred result"),
        ExecutionAttemptReconciliationResult::Deferred(
            ExecutionAttemptDeferredReason::SourceAvailableAfterCutoff
        )
    );
    assert!(
        repository
            .find_by_intent(&source.order_intent_id)
            .await
            .expect("read outcome after late-source deferral")
            .is_none(),
        "late source must not create an execution outcome"
    );
}

impl SourceShape {
    async fn persist_shape(self) -> ExecutionAttemptOutcomeInfo {
        let (pool, _database) = setup_pg().await;
        let db = pool.connection().clone();
        let source = seed_execution_source(&db, self).await;
        let cutoff = db.statement_time().await;
        let inserted = PgExecutionAttemptOutcomeRepository::new(db)
            .reconcile_intent(&source.order_intent_id, cutoff)
            .await
            .expect("reconcile execution outcome");
        match inserted {
            ExecutionAttemptReconciliationResult::Inserted(outcome) => outcome,
            ExecutionAttemptReconciliationResult::AlreadyPresent(_) => {
                panic!("isolated fixture must insert a new outcome")
            }
            ExecutionAttemptReconciliationResult::Deferred(reason) => {
                panic!("terminal source fixture must not defer: {reason:?}")
            }
        }
    }
}

fn new_outcome(source: &ExecutionSourceFixture, shape: SourceShape) -> NewExecutionAttemptOutcome {
    let (
        terminal_state,
        no_fill_reason,
        entry_order_state,
        filled_shares,
        entry_avg_price,
        entry_filled_at,
        position_terminal_state,
        exit_reason,
        exit_filled_shares,
        exit_avg_price,
        exit_fee_usd,
        exit_at,
        settlement_payout_usd,
        realized_pnl_usd,
    ) = match shape {
        SourceShape::Unfilled => (
            ExecutionAttemptTerminalState::Unfilled,
            Some(ExecutionAttemptNoFillReason::VenueRejected),
            ExecutionOrderState::Failed,
            Shares::ZERO,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        SourceShape::PartiallyFilled => (
            ExecutionAttemptTerminalState::PartiallyFilled,
            None,
            ExecutionOrderState::PartiallyFilled,
            Shares::new(dec!(16)),
            Some(Price::new(dec!(0.6))),
            Some(source.terminal_at - Duration::minutes(5)),
            Some(PositionLedgerState::Closed),
            Some(ExitReason::Manual),
            Some(Shares::new(dec!(16))),
            Some(Price::new(dec!(0.6))),
            Some(Usd::ZERO),
            Some(source.terminal_at),
            None,
            Some(Usd::ZERO),
        ),
        SourceShape::FullyFilled => (
            ExecutionAttemptTerminalState::FullyFilled,
            None,
            ExecutionOrderState::Filled,
            Shares::new(dec!(40)),
            Some(Price::new(dec!(0.6))),
            Some(source.terminal_at - Duration::minutes(5)),
            Some(PositionLedgerState::Closed),
            Some(ExitReason::Manual),
            Some(Shares::new(dec!(40))),
            Some(Price::new(dec!(0.6))),
            Some(Usd::ZERO),
            Some(source.terminal_at),
            None,
            Some(Usd::ZERO),
        ),
    };
    NewExecutionAttemptOutcome {
        recommendation_id: source.ids.recommendation,
        order_intent_id: source.order_intent_id,
        entry_execution_order_id: source.entry_execution_order_id,
        entry_reconciliation_id: source.entry_reconciliation_id,
        position_id: source.position_id,
        execution_account_id: source.ids.execution_account,
        market_id: source.ids.market.as_str().into(),
        token_id: source.ids.token.as_str().into(),
        runtime_mode: QuantRuntimeMode::AutoExecution,
        terminal_state,
        no_fill_reason,
        entry_order_state,
        requested_shares: Shares::new(dec!(40)),
        filled_shares,
        entry_avg_price,
        entry_fee_usd: Some(Usd::ZERO),
        entry_filled_at,
        position_terminal_state,
        exit_reason,
        exit_filled_shares,
        exit_avg_price,
        exit_fee_usd,
        exit_at,
        settlement_payout_usd,
        realized_pnl_usd,
        terminal_at: source.terminal_at,
        source_checkpoint_hash: hash('a'),
        execution_fact_hash: hash('b'),
        execution_fact_schema_version: SchemaVersion::FIRST,
    }
}

async fn seed_execution_source(
    db: &DatabaseConnection,
    shape: SourceShape,
) -> ExecutionSourceFixture {
    let ids = seed_report_fixture(db).await;
    let order_intent_id = seed_approved_intent(db, &ids).await;
    let terminal_at = Utc::now() - Duration::minutes(1);
    let entry_filled_at = terminal_at - Duration::minutes(5);
    let seed = ExecutionSourceSeed {
        db,
        ids: &ids,
        order_intent_id,
        shape,
        contract: shape.contract(),
        terminal_at,
        entry_filled_at,
    };
    let (entry_execution_order_id, entry_reconciliation_id) = seed.persist_entry().await;
    seed.persist_exit().await;
    let position_id = seed.persist_position().await;
    seed.mark_intent_terminal().await;

    ExecutionSourceFixture {
        ids,
        order_intent_id,
        entry_execution_order_id,
        entry_reconciliation_id,
        position_id,
        terminal_at,
    }
}

impl ExecutionSourceSeed<'_> {
    async fn persist_entry(&self) -> (ExecutionOrderId, ReconciliationId) {
        let mut order = entry_execution_order(&self.order_intent_id, self.ids);
        order.state = self.contract.order_state;
        order.venue_status = Some(self.contract.venue_status);
        order.cost_usd = Usd::new(dec!(24));
        order.prepared_order_json.cash_budget = Some(Usd::new(dec!(24)));
        order.prepared_order_json.expected_fee = Usd::ZERO;
        order.prepared_order_json.total_cash_delta = dec!(-24);
        order.submitted_at = Some(self.entry_filled_at - Duration::seconds(1));
        order.filled_at = self.contract.average_price.map(|_| self.entry_filled_at);
        order.cancelled_at =
            matches!(self.shape, SourceShape::Unfilled).then_some(self.terminal_at);
        let execution_order_id = order.execution_order_id;
        QuantExecutionOrderEntity::insert(order.into_active_model())
            .exec(self.db)
            .await
            .expect("persist terminal entry order");

        let reconciliation_id = ReconciliationId::from_v7();
        QuantReconciliationEntity::insert(
            NewReconciliation {
                reconciliation_id,
                execution_order_id,
                order_intent_id: self.order_intent_id,
                result: self.contract.reconciliation_result,
                evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
                    kind: ReconciliationEvidenceKind::ClobOrderStatus,
                    observed_at: self.terminal_at,
                    detail: "terminal execution-outcome fixture".to_owned(),
                    venue_ref: None,
                    shares: Some(self.contract.filled_shares),
                    price: self.contract.average_price,
                    fee_evidence: None,
                }]),
                venue_filled_shares: Some(self.contract.filled_shares),
                venue_avg_price: self.contract.average_price,
                expected_cash_delta_usd: None,
                venue_cash_delta_usd: None,
                realized_pnl_usd: None,
                expected_fee_usd: None,
                observed_fee_usd: Some(Usd::ZERO),
                fee_delta_usd: None,
                resolved_by: Some("repository-contract".to_owned()),
                resolved_at: Some(self.terminal_at),
            }
            .into_active_model(),
        )
        .exec(self.db)
        .await
        .expect("persist terminal entry reconciliation");
        (execution_order_id, reconciliation_id)
    }

    async fn persist_exit(&self) {
        if matches!(self.shape, SourceShape::Unfilled) {
            return;
        }
        let exit_execution_order_id = ExecutionOrderId::from_v7();
        QuantExecutionOrderEntity::insert(
            NewExecutionOrder {
                execution_order_id: exit_execution_order_id,
                order_intent_id: self.order_intent_id,
                order_phase: ExecutionOrderPhase::Exit,
                market_id: MarketId::new(&self.ids.market),
                token_id: TokenId::new(&self.ids.token),
                side: Side::Sell,
                order_type: OrderTypeKind::Gtc,
                price: Price::new(dec!(0.6)),
                shares: self.contract.filled_shares,
                cost_usd: self.contract.filled_shares * Price::new(dec!(0.6)),
                prepared_order_json: prepared_order(
                    TokenId::new(&self.ids.token),
                    Side::Sell,
                    OrderType::Gtc,
                    VenueOrderAmount::Shares(self.contract.filled_shares),
                    Usd::ZERO,
                    self.contract.filled_shares,
                    Price::new(dec!(0.6)),
                ),
                venue_order_id: Some(OrderId::new("repository-contract-exit")),
                venue_status: Some(VenueOrderStatus::Filled),
                state: ExecutionOrderState::Filled,
                submitted_at: Some(self.terminal_at - Duration::seconds(1)),
                filled_at: Some(self.terminal_at),
                cancelled_at: None,
                gtd_expiration_at: None,
                error_message: None,
            }
            .into_active_model(),
        )
        .exec(self.db)
        .await
        .expect("persist terminal exit order");
        QuantReconciliationEntity::insert(
            NewReconciliation {
                reconciliation_id: ReconciliationId::from_v7(),
                execution_order_id: exit_execution_order_id,
                order_intent_id: self.order_intent_id,
                result: ReconciliationResult::Filled,
                evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
                    kind: ReconciliationEvidenceKind::ClobOrderStatus,
                    observed_at: self.terminal_at,
                    detail: "terminal execution-outcome exit fixture".to_owned(),
                    venue_ref: Some("repository-contract-exit".to_owned()),
                    shares: Some(self.contract.filled_shares),
                    price: Some(Price::new(dec!(0.6))),
                    fee_evidence: None,
                }]),
                venue_filled_shares: Some(self.contract.filled_shares),
                venue_avg_price: Some(Price::new(dec!(0.6))),
                expected_cash_delta_usd: None,
                venue_cash_delta_usd: None,
                realized_pnl_usd: Some(Usd::ZERO),
                expected_fee_usd: Some(Usd::ZERO),
                observed_fee_usd: Some(Usd::ZERO),
                fee_delta_usd: Some(Usd::ZERO),
                resolved_by: Some("repository-contract".to_owned()),
                resolved_at: Some(self.terminal_at),
            }
            .into_active_model(),
        )
        .exec(self.db)
        .await
        .expect("persist terminal exit reconciliation");
    }

    async fn persist_position(&self) -> Option<PositionId> {
        if matches!(self.shape, SourceShape::Unfilled) {
            return None;
        }
        let position_id = PositionId::from_v7();
        QuantPositionEntity::insert(
            NewPosition {
                position_id,
                order_intent_id: self.order_intent_id,
                execution_account_id: self.ids.execution_account,
                token_id: self.ids.token.as_str().into(),
                market_id: self.ids.market.as_str().into(),
                event_id: Some(EventId::new(&self.ids.event)),
                category: MarketCategory::Politics,
                side: OutcomeSide::Yes,
                state: PositionLedgerState::Closed,
                shares: Shares::ZERO,
                avg_price: Price::ZERO,
                cost_usd: Usd::ZERO,
                realized_pnl_usd: Usd::ZERO,
                source: AccountSource::Polymarket,
                opened_at: self.entry_filled_at,
                closed_at: Some(self.terminal_at),
            }
            .into_active_model(),
        )
        .exec(self.db)
        .await
        .expect("persist terminal position");
        Some(position_id)
    }

    async fn mark_intent_terminal(&self) {
        let intent = QuantOrderIntentEntity::find_by_id(self.order_intent_id)
            .one(self.db)
            .await
            .expect("read intent")
            .expect("seeded intent");
        let mut active: QuantOrderIntentActiveModel = intent.into_active_model();
        active.status = ActiveValue::Set(self.contract.intent_status);
        if matches!(self.shape, SourceShape::FullyFilled) {
            active.peak_mark_price = ActiveValue::Set(Some(Price::new(dec!(0.6))));
        }
        if !matches!(self.shape, SourceShape::Unfilled) {
            active.exit_state = ActiveValue::Set(ExitState::Exited);
            active.exit_reason = ActiveValue::Set(Some(ExitReason::Manual));
        }
        active.updated_at = ActiveValue::Set(self.terminal_at);
        active.update(self.db).await.expect("mark intent terminal");
    }
}
