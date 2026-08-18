//! Outcome-reconciliation source contracts against disposable `PostgreSQL`.

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::quant::{
        AccountExecutionFeeFact, ExecutionAttemptDeferredReason, ExecutionAttemptDerivation,
        ExecutionAttemptReconciliationError, ExecutionAttemptReconciliationResult,
        ExecutionAttemptSourceGraph, NewAccountChainExecution, NewExecutionAttemptOutcome,
    },
    enums::{
        execution::{
            AccountChainExecutionRole, ExecutionOrderPhase, PositionLedgerState,
            ReconciliationResult,
        },
        quant::{ExecutionAttemptTerminalState, ExecutionOrderState},
    },
    hashing::CanonicalDigest,
    types::{
        AccountChainExecutionId, ContentHash, EvmAddress, EvmBlockHash, EvmTransactionHash,
        ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, ReconciliationId, Shares,
        TokenId, Usd, VenueOrderAmount,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAccountChainExecutionRepository, PgAccountRecoveryRepository,
        PgExecutionAttemptOutcomeRepository, PgExecutionOrderRepository,
        PgExecutionSubmissionRepository, PgOrderIntentRepository, PgReconciliationRepository,
        PgStrategyPositionLotRepository,
    },
    traits::{
        AccountChainExecutionRepository, AccountRecoveryRepository,
        ExecutionAttemptOutcomeRepository, ExecutionOrderRepository, OrderIntentRepository,
        ReconciliationRepository, StrategyPositionLotRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{self, PostgresClock, setup_pg},
    support::execution_pg_seed::{
        ExecutionTxnIds, close_position_full, fill_entry_lot, fixture_execution_account,
        seed_approved_intent, seed_report_fixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outcome_reconciliation_source_contracts() {
    Box::pin(postgres::with_postgres_suite(async {
        immediate_confirmed_fill_truth().await;
        Box::pin(closed_execution_source_reconciled()).await;
    }))
    .await
    .expect("start outcome-reconciliation PostgreSQL suite");
}

async fn immediate_confirmed_fill_truth() {
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

async fn closed_execution_source_reconciled() {
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
    assert_input_order_stable(&graph);
    assert_partial_not_full(&graph);
    assert_multiple_exit_aggregated(&graph);
    assert_order_mismatch_rejects(&graph);

    let repository = PgExecutionAttemptOutcomeRepository::new(db.clone());
    let missing_fee = repository
        .reconcile_intent(&intent_id, db.statement_time().await)
        .await
        .expect("reconcile without account fee facts");
    assert_eq!(
        missing_fee,
        ExecutionAttemptReconciliationResult::Deferred(
            ExecutionAttemptDeferredReason::AccountChainExecutionMissing
        )
    );
    seed_account_fees(&db, &graph).await;
    let cutoff = db.statement_time().await;
    let first = repository
        .reconcile_intent(&intent_id, cutoff)
        .await
        .expect("reconcile complete execution source graph");
    let inserted = match first {
        ExecutionAttemptReconciliationResult::Inserted(outcome) => outcome,
        other => panic!("expected inserted execution outcome, got {other:?}"),
    };
    let second = repository
        .reconcile_intent(&intent_id, cutoff)
        .await
        .expect("retry execution source graph");
    let existing = match second {
        ExecutionAttemptReconciliationResult::AlreadyPresent(outcome) => outcome,
        other => panic!("expected idempotent existing outcome, got {other:?}"),
    };

    assert_eq!(
        inserted.terminal_state,
        ExecutionAttemptTerminalState::FullyFilled
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

async fn seed_account_fees(db: &DatabaseConnection, graph: &ExecutionAttemptSourceGraph) {
    let account = fixture_execution_account();
    let executions = PgAccountChainExecutionRepository::new(db.clone());
    let recovery = PgAccountRecoveryRepository::new(db.clone());
    for (index, order) in graph.orders.iter().enumerate() {
        let reconciliation = graph
            .reconciliations
            .iter()
            .find(|reconciliation| reconciliation.execution_order_id == order.execution_order_id)
            .expect("fee reconciliation");
        if !matches!(
            reconciliation.result,
            ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled
        ) {
            continue;
        }
        let source_event_hash =
            CanonicalDigest::content_hash_json(&order.execution_order_id).expect("source hash");
        let account_chain_execution_id =
            AccountChainExecutionId::from_content_hash(&source_event_hash);
        let observed_at = Utc::now();
        let digit =
            char::from_digit(u32::try_from(index + 1).expect("index") % 10, 10).expect("hex digit");
        executions
            .append(vec![NewAccountChainExecution {
                account_chain_execution_id,
                execution_account_id: graph.intent.execution_account_id,
                role: AccountChainExecutionRole::Maker,
                chain_id: 137,
                protocol_version: 2,
                exchange_address: EvmAddress::parse("0xe111180000d2663c0091e4f400237545b87b996b")
                    .expect("exchange"),
                block_number: 100 + i64::try_from(index).expect("block index"),
                block_hash: EvmBlockHash::parse(format!("0x{}", digit.to_string().repeat(64)))
                    .expect("block hash"),
                transaction_hash: EvmTransactionHash::parse(format!(
                    "0x{}",
                    digit.to_string().repeat(64)
                ))
                .expect("transaction hash"),
                transaction_index: i64::try_from(index).expect("transaction index"),
                log_index: i64::try_from(index).expect("log index"),
                order_id: order.venue_order_id.clone().expect("venue order id"),
                maker_address: account.funder_address.clone(),
                taker_address: EvmAddress::parse("0x2222222222222222222222222222222222222222")
                    .expect("taker"),
                order_side: order.side,
                order_token_id: order.token_id.clone(),
                maker_amount_raw: "1000000".to_owned(),
                taker_amount_raw: "1000000".to_owned(),
                account_side: Some(order.side),
                account_token_id: Some(order.token_id.clone()),
                shares: reconciliation.venue_filled_shares,
                principal_usd: reconciliation
                    .venue_filled_shares
                    .zip(reconciliation.venue_avg_price)
                    .map(|(shares, price)| shares * price),
                exact_fee_usd: Some(order.prepared_order_json.expected_fee),
                builder_code: None,
                metadata: None,
                source_event_hash,
                availability_policy_hash: ContentHash::from_bytes([9; 32]),
                observed_at,
                available_at: observed_at,
            }])
            .await
            .expect("account fee execution");
        let associated = recovery
            .associate_execution(&account_chain_execution_id, observed_at)
            .await
            .expect("account fee association");
        assert!(associated.incident.is_none());
    }
}

async fn load_source_graph(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) -> ExecutionAttemptSourceGraph {
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
    let position = PgStrategyPositionLotRepository::new(db.clone())
        .find_by_intent(intent_id)
        .await
        .expect("load source position");
    let account_execution_fees = reconciliations
        .iter()
        .filter_map(|reconciliation| {
            matches!(
                reconciliation.result,
                ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled
            )
            .then(|| {
                let order = orders
                    .iter()
                    .find(|order| order.execution_order_id == reconciliation.execution_order_id)
                    .expect("fee execution order");
                let source_event_hash =
                    CanonicalDigest::content_hash_json(&reconciliation.execution_order_id)
                        .expect("fee source hash");
                AccountExecutionFeeFact {
                    account_chain_execution_id: AccountChainExecutionId::from_content_hash(
                        &source_event_hash,
                    ),
                    execution_order_id: reconciliation.execution_order_id,
                    exact_fee_usd: Usd::new(
                        order.prepared_order_json.expected_fee.inner()
                            * reconciliation
                                .venue_filled_shares
                                .expect("filled reconciliation shares")
                                .inner()
                            / order.shares.inner(),
                    ),
                    source_event_hash,
                    available_at: reconciliation.updated_at,
                }
            })
        })
        .collect();
    ExecutionAttemptSourceGraph {
        recommendation_id: ids.recommendation,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        intent,
        orders,
        reconciliations,
        account_execution_fees,
        position,
        settlement_lot: None,
    }
}

fn assert_deferred_source_reasons(graph: &ExecutionAttemptSourceGraph) {
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
        ExecutionAttemptDeferredReason::EntryOrderMissing,
    );

    let mut missing_entry_reconciliation = graph.clone();
    missing_entry_reconciliation
        .reconciliations
        .retain(|row| row.execution_order_id != entry_order_id);
    assert_deferred(
        missing_entry_reconciliation,
        ExecutionAttemptDeferredReason::EntryReconciliationMissing,
    );

    let mut missing_position = graph.clone();
    missing_position.position = None;
    assert_deferred(
        missing_position,
        ExecutionAttemptDeferredReason::FilledPositionMissing,
    );

    let mut open_position = graph.clone();
    open_position.position.as_mut().expect("position").state = PositionLedgerState::Open;
    assert_deferred(
        open_position,
        ExecutionAttemptDeferredReason::PositionNotTerminal,
    );

    let mut missing_exit_reconciliation = graph.clone();
    missing_exit_reconciliation
        .reconciliations
        .retain(|row| row.execution_order_id != exit_order_id);
    assert_deferred(
        missing_exit_reconciliation,
        ExecutionAttemptDeferredReason::ExitReconciliationMissing,
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
        ExecutionAttemptDeferredReason::ExitReconciliationPending,
    );

    let mut missing_settlement_lot = graph.clone();
    missing_settlement_lot
        .position
        .as_mut()
        .expect("position")
        .state = PositionLedgerState::Settled;
    assert_deferred(
        missing_settlement_lot,
        ExecutionAttemptDeferredReason::SettlementLotMissing,
    );
}

fn assert_input_order_stable(graph: &ExecutionAttemptSourceGraph) {
    let expected = expect_ready(graph.clone());
    let mut reordered = graph.clone();
    reordered.orders.reverse();
    reordered.reconciliations.reverse();
    let actual = expect_ready(reordered);
    assert_eq!(actual, expected);
}

fn assert_multiple_exit_aggregated(graph: &ExecutionAttemptSourceGraph) {
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

    let first_fee = split
        .account_execution_fees
        .iter_mut()
        .find(|fee| fee.execution_order_id == original_exit_order_id)
        .expect("first exit fee");
    first_fee.exact_fee_usd = Usd::new(dec!(0.5));
    let mut second_fee = first_fee.clone();
    let second_source_hash =
        CanonicalDigest::content_hash_json(&second_order_id).expect("second fee source hash");
    second_fee.account_chain_execution_id =
        AccountChainExecutionId::from_content_hash(&second_source_hash);
    second_fee.execution_order_id = second_order_id;
    second_fee.source_event_hash = second_source_hash;

    split.orders.push(second_order);
    split.reconciliations.push(second_reconciliation);
    split.account_execution_fees.push(second_fee);
    let outcome = expect_ready(split);
    assert_eq!(outcome.exit_filled_shares, Some(Shares::new(dec!(40))));
    assert_eq!(outcome.exit_avg_price, Some(Price::new(dec!(0.55))));
}

fn assert_partial_not_full(graph: &ExecutionAttemptSourceGraph) {
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
    for fee in &mut partial.account_execution_fees {
        if fee.execution_order_id == entry_order_id || fee.execution_order_id == exit_order_id {
            fee.exact_fee_usd = Usd::new(dec!(0.5));
        }
    }
    assert_deferred(
        partial.clone(),
        ExecutionAttemptDeferredReason::EntryOrderNotTerminal,
    );
    partial
        .orders
        .iter_mut()
        .find(|order| order.execution_order_id == entry_order_id)
        .expect("entry order")
        .state = ExecutionOrderState::Cancelled;

    let outcome = expect_ready(partial);
    assert_eq!(
        outcome.terminal_state,
        ExecutionAttemptTerminalState::PartiallyFilled
    );
    assert_eq!(outcome.requested_shares, Shares::new(dec!(40)));
    assert_eq!(outcome.filled_shares, Shares::new(dec!(20)));
    assert_eq!(outcome.entry_fee_usd, Some(Usd::new(dec!(0.5))));
    assert_eq!(outcome.exit_filled_shares, Some(Shares::new(dec!(20))));
    assert_eq!(outcome.realized_pnl_usd, Some(Usd::new(dec!(-2))));
}

fn assert_order_mismatch_rejects(graph: &ExecutionAttemptSourceGraph) {
    let mut mismatched = graph.clone();
    mismatched
        .orders
        .iter_mut()
        .find(|order| order.order_phase == ExecutionOrderPhase::Exit)
        .expect("exit order")
        .token_id = TokenId::new("wrong-token");
    assert!(matches!(
        mismatched.derive(),
        Err(ExecutionAttemptReconciliationError::IdentityMismatch { .. })
    ));
}

fn assert_deferred(graph: ExecutionAttemptSourceGraph, expected: ExecutionAttemptDeferredReason) {
    assert_eq!(
        graph.derive().expect("valid incomplete source graph"),
        ExecutionAttemptDerivation::Deferred(expected)
    );
}

fn expect_ready(graph: ExecutionAttemptSourceGraph) -> NewExecutionAttemptOutcome {
    match graph.derive().expect("valid complete source graph") {
        ExecutionAttemptDerivation::Ready(outcome) => *outcome,
        other @ ExecutionAttemptDerivation::Deferred(_) => {
            panic!("expected ready source graph, got {other:?}")
        }
    }
}
