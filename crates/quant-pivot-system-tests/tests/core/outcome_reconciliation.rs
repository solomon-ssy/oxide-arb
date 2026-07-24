//! Outcome-reconciliation source contracts against disposable `PostgreSQL`.

use chrono::Duration;
use quant_pivot_models::{
    domain::quant::{
        ExecutionOutcomeDeferredReason, ExecutionOutcomeDerivation,
        ExecutionOutcomeReconciliationError, ExecutionOutcomeReconciliationResult,
        ExecutionOutcomeSourceGraph, NewRecommendationExecutionOutcome,
    },
    enums::{
        execution::{ExecutionOrderPhase, PositionLedgerState, ReconciliationResult},
        quant::{ExecutionOrderState, RecommendationExecutionTerminalState},
    },
    types::{
        ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, ReconciliationId, Shares,
        TokenId, Usd, VenueOrderAmount,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExecutionOrderRepository, PgExecutionSubmissionRepository, PgOrderIntentRepository,
        PgPositionRepository, PgRecommendationExecutionOutcomeRepository,
        PgReconciliationRepository,
    },
    traits::{
        ExecutionOrderRepository, OrderIntentRepository, PositionRepository,
        RecommendationExecutionOutcomeRepository, ReconciliationRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{self, setup_pg},
    support::execution_pg_seed::{
        ExecutionTxnIds, close_position_full, fill_entry_lot, seed_approved_intent,
        seed_report_fixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outcome_reconciliation_source_contracts() {
    Box::pin(postgres::with_postgres_suite(async {
        immediate_confirmed_fill_is_terminal_source_truth().await;
        closed_execution_source_graph_is_idempotently_reconciled().await;
    }))
    .await
    .expect("start outcome-reconciliation PostgreSQL suite");
}

async fn immediate_confirmed_fill_is_terminal_source_truth() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    fill_entry_lot(&db, &submission, &ids, &intent_id).await;

    let orders = PgExecutionOrderRepository::new(db.clone())
        .find_by_intent(&intent_id)
        .await
        .expect("load execution orders");
    let entry = orders.first().expect("one persisted entry order");
    let reconciliation = PgReconciliationRepository::new(db)
        .find_by_execution_order(&entry.execution_order_id)
        .await
        .expect("load entry reconciliation")
        .expect("confirmed fill reconciliation");

    assert_eq!(reconciliation.result, ReconciliationResult::Filled);
    assert!(
        reconciliation.resolved_at.is_some(),
        "a venue-confirmed terminal fill must freeze its source-visible terminal timestamp"
    );
    assert_eq!(
        reconciliation.resolved_by.as_deref(),
        Some("venue_submit_response")
    );
}

async fn closed_execution_source_graph_is_idempotently_reconciled() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let ids = seed_report_fixture(&db).await;
    let intent_id = seed_approved_intent(&db, &ids).await;
    close_position_full(
        &db,
        &submission,
        &ids,
        &intent_id,
        Some(Price::new(dec!(0.66))),
    )
    .await;

    let graph = load_source_graph(&db, &ids, &intent_id).await;
    assert_deferred_source_reasons(&graph);
    assert_input_order_does_not_change_hashes(&graph);
    assert_partial_entry_fill_is_not_coerced_to_full(&graph);
    assert_multiple_exit_fills_are_aggregated(&graph);
    assert_order_identity_mismatch_fails_closed(&graph);

    let repository = PgRecommendationExecutionOutcomeRepository::new(db);
    let first = repository
        .reconcile_intent(&intent_id)
        .await
        .expect("reconcile complete execution source graph");
    let inserted = match first {
        ExecutionOutcomeReconciliationResult::Inserted(outcome) => outcome,
        other => panic!("expected inserted execution outcome, got {other:?}"),
    };
    let second = repository
        .reconcile_intent(&intent_id)
        .await
        .expect("retry execution source graph");
    let existing = match second {
        ExecutionOutcomeReconciliationResult::AlreadyPresent(outcome) => outcome,
        other => panic!("expected idempotent existing outcome, got {other:?}"),
    };

    assert_eq!(
        inserted.terminal_state,
        RecommendationExecutionTerminalState::FullyFilled
    );
    assert_eq!(inserted.requested_shares, Shares::new(dec!(40)));
    assert_eq!(inserted.filled_shares, Shares::new(dec!(40)));
    assert_eq!(inserted.entry_fee_usd, Some(Usd::new(dec!(1))));
    assert_eq!(inserted.exit_filled_shares, Some(Shares::new(dec!(40))));
    assert_eq!(inserted.realized_pnl_usd, Some(Usd::new(dec!(-3))));
    assert_eq!(inserted.outcome_hash, existing.outcome_hash);
    assert_eq!(inserted.execution_fact_hash, existing.execution_fact_hash);
    assert_eq!(
        inserted.source_checkpoint_hash,
        existing.source_checkpoint_hash
    );
}

async fn load_source_graph(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) -> ExecutionOutcomeSourceGraph {
    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(intent_id)
        .await
        .expect("load source intent")
        .expect("source intent exists");
    let orders = PgExecutionOrderRepository::new(db.clone())
        .find_by_intent(intent_id)
        .await
        .expect("load source orders");
    let reconciliations_repo = PgReconciliationRepository::new(db.clone());
    let mut reconciliations = Vec::with_capacity(orders.len());
    for order in &orders {
        if let Some(reconciliation) = reconciliations_repo
            .find_by_execution_order(&order.execution_order_id)
            .await
            .expect("load source reconciliation")
        {
            reconciliations.push(reconciliation);
        }
    }
    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(intent_id)
        .await
        .expect("load source position");
    ExecutionOutcomeSourceGraph {
        recommendation_id: ids.recommendation,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        intent,
        orders,
        reconciliations,
        position,
        settlement_lot: None,
    }
}

fn assert_deferred_source_reasons(graph: &ExecutionOutcomeSourceGraph) {
    let entry_order_id = graph
        .orders
        .iter()
        .find(|order| order.order_phase == ExecutionOrderPhase::Entry)
        .expect("entry order")
        .execution_order_id;
    let exit_order_id = graph
        .orders
        .iter()
        .find(|order| order.order_phase == ExecutionOrderPhase::Exit)
        .expect("exit order")
        .execution_order_id;

    let mut missing_entry = graph.clone();
    missing_entry
        .orders
        .retain(|order| order.execution_order_id != entry_order_id);
    missing_entry
        .reconciliations
        .retain(|row| row.execution_order_id != entry_order_id);
    assert_deferred(
        missing_entry,
        ExecutionOutcomeDeferredReason::EntryOrderMissing,
    );

    let mut missing_entry_reconciliation = graph.clone();
    missing_entry_reconciliation
        .reconciliations
        .retain(|row| row.execution_order_id != entry_order_id);
    assert_deferred(
        missing_entry_reconciliation,
        ExecutionOutcomeDeferredReason::EntryReconciliationMissing,
    );

    let mut missing_position = graph.clone();
    missing_position.position = None;
    assert_deferred(
        missing_position,
        ExecutionOutcomeDeferredReason::FilledPositionMissing,
    );

    let mut open_position = graph.clone();
    open_position.position.as_mut().expect("position").state = PositionLedgerState::Open;
    assert_deferred(
        open_position,
        ExecutionOutcomeDeferredReason::PositionNotTerminal,
    );

    let mut missing_exit_reconciliation = graph.clone();
    missing_exit_reconciliation
        .reconciliations
        .retain(|row| row.execution_order_id != exit_order_id);
    assert_deferred(
        missing_exit_reconciliation,
        ExecutionOutcomeDeferredReason::ExitReconciliationMissing,
    );

    let mut pending_exit_reconciliation = graph.clone();
    let pending = pending_exit_reconciliation
        .reconciliations
        .iter_mut()
        .find(|row| row.execution_order_id == exit_order_id)
        .expect("exit reconciliation");
    pending.result = ReconciliationResult::Pending;
    pending.resolved_at = None;
    assert_deferred(
        pending_exit_reconciliation,
        ExecutionOutcomeDeferredReason::ExitReconciliationPending,
    );

    let mut missing_settlement_lot = graph.clone();
    missing_settlement_lot
        .position
        .as_mut()
        .expect("position")
        .state = PositionLedgerState::Settled;
    assert_deferred(
        missing_settlement_lot,
        ExecutionOutcomeDeferredReason::SettlementLotMissing,
    );
}

fn assert_input_order_does_not_change_hashes(graph: &ExecutionOutcomeSourceGraph) {
    let expected = expect_ready(graph.clone());
    let mut reordered = graph.clone();
    reordered.orders.reverse();
    reordered.reconciliations.reverse();
    let actual = expect_ready(reordered);
    assert_eq!(actual, expected);
}

fn assert_multiple_exit_fills_are_aggregated(graph: &ExecutionOutcomeSourceGraph) {
    let mut split = graph.clone();
    let exit_order_index = split
        .orders
        .iter()
        .position(|order| order.order_phase == ExecutionOrderPhase::Exit)
        .expect("exit order");
    let original_exit_order_id = split.orders[exit_order_index].execution_order_id;
    let exit_reconciliation_index = split
        .reconciliations
        .iter()
        .position(|row| row.execution_order_id == original_exit_order_id)
        .expect("exit reconciliation");

    let mut second_order = split.orders[exit_order_index].clone();
    let second_order_id = ExecutionOrderId::from_v7();
    second_order.execution_order_id = second_order_id;
    second_order.venue_order_id = Some(OrderId::new("venue-exit-2"));
    second_order.price = Price::new(dec!(0.6));
    second_order.shares = Shares::new(dec!(20));
    second_order.cost_usd = Usd::new(dec!(12));
    second_order.prepared_order_json.worst_price = Price::new(dec!(0.6));
    second_order.prepared_order_json.venue_amount = VenueOrderAmount::Shares(Shares::new(dec!(20)));
    second_order.prepared_order_json.expected_filled_shares = Shares::new(dec!(20));
    second_order.prepared_order_json.total_cash_delta = dec!(12);
    second_order.updated_at += Duration::milliseconds(1);

    let first_order = &mut split.orders[exit_order_index];
    first_order.price = Price::new(dec!(0.5));
    first_order.shares = Shares::new(dec!(20));
    first_order.cost_usd = Usd::new(dec!(10));
    first_order.prepared_order_json.worst_price = Price::new(dec!(0.5));
    first_order.prepared_order_json.venue_amount = VenueOrderAmount::Shares(Shares::new(dec!(20)));
    first_order.prepared_order_json.expected_filled_shares = Shares::new(dec!(20));
    first_order.prepared_order_json.total_cash_delta = dec!(10);

    let mut second_reconciliation = split.reconciliations[exit_reconciliation_index].clone();
    second_reconciliation.reconciliation_id = ReconciliationId::from_v7();
    second_reconciliation.execution_order_id = second_order_id;
    second_reconciliation.venue_filled_shares = Some(Shares::new(dec!(20)));
    second_reconciliation.venue_avg_price = Some(Price::new(dec!(0.6)));
    second_reconciliation.updated_at += Duration::milliseconds(1);
    let second_evidence = second_reconciliation
        .evidence_json
        .0
        .first_mut()
        .expect("second exit evidence");
    second_evidence.shares = Some(Shares::new(dec!(20)));
    second_evidence.price = Some(Price::new(dec!(0.6)));

    let first_reconciliation = &mut split.reconciliations[exit_reconciliation_index];
    first_reconciliation.venue_filled_shares = Some(Shares::new(dec!(20)));
    first_reconciliation.venue_avg_price = Some(Price::new(dec!(0.5)));
    let first_evidence = first_reconciliation
        .evidence_json
        .0
        .first_mut()
        .expect("first exit evidence");
    first_evidence.shares = Some(Shares::new(dec!(20)));
    first_evidence.price = Some(Price::new(dec!(0.5)));

    split.orders.push(second_order);
    split.reconciliations.push(second_reconciliation);
    let outcome = expect_ready(split);
    assert_eq!(outcome.exit_filled_shares, Some(Shares::new(dec!(40))));
    assert_eq!(outcome.exit_avg_price, Some(Price::new(dec!(0.55))));
}

fn assert_partial_entry_fill_is_not_coerced_to_full(graph: &ExecutionOutcomeSourceGraph) {
    let mut partial = graph.clone();
    let entry_order_id = partial
        .orders
        .iter_mut()
        .find(|order| order.order_phase == ExecutionOrderPhase::Entry)
        .map(|order| {
            order.state = ExecutionOrderState::PartiallyFilled;
            order.execution_order_id
        })
        .expect("entry order");
    let entry_reconciliation = partial
        .reconciliations
        .iter_mut()
        .find(|row| row.execution_order_id == entry_order_id)
        .expect("entry reconciliation");
    entry_reconciliation.result = ReconciliationResult::PartiallyFilled;
    entry_reconciliation.venue_filled_shares = Some(Shares::new(dec!(20)));
    entry_reconciliation.observed_fee_usd = Some(Usd::new(dec!(0.5)));
    let entry_evidence = entry_reconciliation
        .evidence_json
        .0
        .first_mut()
        .expect("entry evidence");
    entry_evidence.shares = Some(Shares::new(dec!(20)));

    let exit_order_id = partial
        .orders
        .iter_mut()
        .find(|order| order.order_phase == ExecutionOrderPhase::Exit)
        .map(|order| {
            order.shares = Shares::new(dec!(20));
            order.cost_usd = Usd::new(dec!(11));
            order.prepared_order_json.venue_amount =
                VenueOrderAmount::Shares(Shares::new(dec!(20)));
            order.prepared_order_json.expected_filled_shares = Shares::new(dec!(20));
            order.prepared_order_json.total_cash_delta = dec!(11);
            order.execution_order_id
        })
        .expect("exit order");
    let exit_reconciliation = partial
        .reconciliations
        .iter_mut()
        .find(|row| row.execution_order_id == exit_order_id)
        .expect("exit reconciliation");
    exit_reconciliation.venue_filled_shares = Some(Shares::new(dec!(20)));
    let exit_evidence = exit_reconciliation
        .evidence_json
        .0
        .first_mut()
        .expect("exit evidence");
    exit_evidence.shares = Some(Shares::new(dec!(20)));
    partial
        .position
        .as_mut()
        .expect("terminal position")
        .realized_pnl_usd = Usd::new(dec!(-1.5));

    let outcome = expect_ready(partial);
    assert_eq!(
        outcome.terminal_state,
        RecommendationExecutionTerminalState::PartiallyFilled
    );
    assert_eq!(outcome.requested_shares, Shares::new(dec!(40)));
    assert_eq!(outcome.filled_shares, Shares::new(dec!(20)));
    assert_eq!(outcome.entry_fee_usd, Some(Usd::new(dec!(0.5))));
    assert_eq!(outcome.exit_filled_shares, Some(Shares::new(dec!(20))));
    assert_eq!(outcome.realized_pnl_usd, Some(Usd::new(dec!(-1.5))));
}

fn assert_order_identity_mismatch_fails_closed(graph: &ExecutionOutcomeSourceGraph) {
    let mut mismatched = graph.clone();
    mismatched
        .orders
        .iter_mut()
        .find(|order| order.order_phase == ExecutionOrderPhase::Exit)
        .expect("exit order")
        .token_id = TokenId::new("wrong-token");
    assert!(matches!(
        mismatched.derive(),
        Err(ExecutionOutcomeReconciliationError::IdentityMismatch { .. })
    ));
}

fn assert_deferred(graph: ExecutionOutcomeSourceGraph, expected: ExecutionOutcomeDeferredReason) {
    assert_eq!(
        graph.derive().expect("valid incomplete source graph"),
        ExecutionOutcomeDerivation::Deferred(expected)
    );
}

fn expect_ready(graph: ExecutionOutcomeSourceGraph) -> NewRecommendationExecutionOutcome {
    match graph.derive().expect("valid complete source graph") {
        ExecutionOutcomeDerivation::Ready(outcome) => *outcome,
        other @ ExecutionOutcomeDerivation::Deferred(_) => {
            panic!("expected ready source graph, got {other:?}")
        }
    }
}
