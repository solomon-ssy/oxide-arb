//! Exit dispatcher — submits a triggered Sell exit order and settles it in one
//! transaction (Phase 05.6).
//!
//! Mirrors the entry dispatcher's write-ahead / venue-call / settle shape but
//! for the exit side: it does **not** run the 24-check admission engine (an exit
//! reduces existing exposure; its lightweight gates — kill-switch `allows_auto_exit`,
//! a readable mark — are enforced in [`decide_exit`](super::decide_exit)). The
//! per-intent lot's capital is already `Spent` from entry; a full exit completes
//! it to `Released`, a partial keeps it `Spent`. Realized `PnL` is exact (per-lot
//! average cost, net the venue exit fee). Unconfirmed venue responses become
//! `Ambiguous` (position untouched, recon enqueued) — never silently exited.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_error::{
    QuantResult,
    execution::ExecutionError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExitLedgerWrite, NewExecutionOrder, NewReconciliation, PositionExit,
        PositionInfo,
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        common::Side,
        execution::{ExecutionOrderPhase, ExitReason, ExitState, ReconciliationEvidenceKind},
        quant::ExecutionOrderState,
    },
    types::{
        ExecutionOrderId, OrderAmount, OrderIntentId, PendingScaleOut, Price, RecommendationId,
        ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId, Usd,
    },
};
use quant_pivot_repository::traits::{ExecutionSubmissionRepository, OrderIntentRepository};

use crate::{
    execution::{
        breaker::ExecutionBreaker,
        dispatcher::{gtd_expiration_at, order_type_kind},
        exit_monitor::ExitOrderSpec,
        order_client::{PolymarketOrderClient, VenueOrder, VenueOutcome, VenueSubmitResult},
    },
    observability::{
        execution_fact_writer::ExecutionEventWriter,
        ledger_fact_projection::project_execution_event, metrics_hub::MetricsHub,
    },
};

/// A triggered exit to submit for one open position lot.
pub struct ExitSubmitRequest {
    /// The lot being (partially) exited.
    pub lot: PositionInfo,
    /// Why the exit fired (recorded on the order + intent).
    pub reason: ExitReason,
    /// The concrete sell order (side / type / limit / shares).
    pub order: ExitOrderSpec,
    /// Stable deterministic scale-out target id, when applicable.
    pub pending_scale_out: Option<PendingScaleOut>,
}

/// Collaborators the exit dispatcher needs.
pub struct ExitDispatcherDeps {
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub execution_events: Arc<ExecutionEventWriter>,
    pub intents: Arc<dyn OrderIntentRepository>,
}

/// Submits exit (Sell) orders and settles their venue outcome atomically.
pub struct CoreExitDispatcher {
    deps: ExitDispatcherDeps,
}

impl CoreExitDispatcher {
    #[must_use]
    pub const fn new(deps: ExitDispatcherDeps) -> Self {
        Self { deps }
    }

    /// Submit one triggered exit: write-ahead the Exit order (lot `-> Closing`),
    /// call the venue (no DB lock), feed the breaker (venue + realized-loss), and
    /// settle the position/capital/exit-FSM in one transaction.
    pub async fn submit_exit(&self, request: ExitSubmitRequest) -> QuantResult<ExecutionOrderInfo> {
        let ExitSubmitRequest {
            lot,
            reason,
            order,
            pending_scale_out,
        } = request;

        // 1. Write-ahead the Exit order (Submitted) + lot Open->Closing + exit FSM.
        let new_order = build_exit_order(&lot, &order)?;
        let execution_order = self
            .deps
            .submission
            .create_exit_order_and_mark_closing(new_order, reason, pending_scale_out)
            .await?;
        let recommendation_id = self
            .recommendation_id_for_intent(&lot.order_intent_id)
            .await?;
        self.deps.execution_events.write(project_execution_event(
            &execution_order,
            recommendation_id.clone(),
            ChQuantLedgerEventKind::ExitSubmitted,
            Utc::now(),
        ));

        // 2. Venue submission — NO DB lock held across this network call.
        let venue_order = VenueOrder {
            market_id: lot.market_id.clone(),
            token_id: lot.token_id.clone(),
            side: Side::Sell,
            price: order.limit_price,
            amount: OrderAmount::Shares(order.shares),
            order_type: order.order_type,
            post_only: false,
            category: lot.category,
        };
        let result = self
            .deps
            .order_client
            .submit(venue_order)
            .await
            .with_order_type_semantics(&order.order_type);

        // 3. Feed venue health (unconfirmed == failure for breaker purposes).
        let venue_ok = result.outcome != VenueOutcome::Ambiguous;
        self.deps
            .breaker
            .observe_venue(venue_ok, result.detail.as_deref().unwrap_or("venue exit"))
            .await;

        // 4. Build the atomic ledger write + the confirmed realized PnL (if any).
        let (write, realized_pnl) =
            build_exit_ledger_write(&result, &lot, reason, &execution_order);

        // 5. Feed the daily realized-loss dimension with the confirmed PnL.
        if let Some(pnl) = realized_pnl {
            self.deps
                .breaker
                .observe_realized_pnl(pnl, result.responded_at)
                .await;
        }

        // 6. Settle in one transaction.
        let recorded = self
            .deps
            .submission
            .record_exit_result(&execution_order.execution_order_id, write)
            .await?;
        self.deps.execution_events.write(project_execution_event(
            &recorded,
            recommendation_id,
            ChQuantLedgerEventKind::ExitSubmissionResult,
            Utc::now(),
        ));
        self.deps.metrics.inc_exit_trigger(reason.as_str());
        Ok(recorded)
    }

    async fn recommendation_id_for_intent(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<RecommendationId> {
        let intent = self
            .deps
            .intents
            .find_by_id(intent_id)
            .await?
            .ok_or_else(|| StorageError::not_found(entity::QUANT_ORDER_INTENT, intent_id))?;
        Ok(intent.recommendation_id)
    }
}

/// Build the write-ahead Exit execution-order row (`state = Submitted`).
fn build_exit_order(
    lot: &PositionInfo,
    order: &ExitOrderSpec,
) -> Result<NewExecutionOrder, ExecutionError> {
    Ok(NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: lot.order_intent_id.clone(),
        order_phase: ExecutionOrderPhase::Exit,
        market_id: lot.market_id.clone(),
        token_id: lot.token_id.clone(),
        side: Side::Sell,
        order_type: order_type_kind(&order.order_type),
        price: order.limit_price,
        shares: order.shares,
        cost_usd: order.shares * order.limit_price,
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: gtd_expiration_at(&order.order_type)?,
        error_message: None,
    })
}

/// The realized average exit price (venue avg, or the submitted limit fallback).
fn exit_avg_price(result: &VenueSubmitResult, order_limit: Price) -> Price {
    result.avg_fill_price.unwrap_or(order_limit)
}

/// Translate an exit venue outcome into the atomic ledger write, returning the
/// confirmed realized `PnL` (for the breaker's daily-loss dimension) when filled.
fn build_exit_ledger_write(
    result: &VenueSubmitResult,
    lot: &PositionInfo,
    reason: ExitReason,
    execution_order: &ExecutionOrderInfo,
) -> (ExitLedgerWrite, Option<Usd>) {
    let outcome = result.outcome;
    let venue_status = outcome.venue_order_status();
    let filled = result.filled_shares;
    let exit_avg = exit_avg_price(result, execution_order.price);

    // Exact per-lot average-cost realized PnL, net the venue exit fee.
    let proceeds_usd = filled * exit_avg - result.fee_paid;
    let cost_basis = lot.avg_price * filled;
    let realized_pnl_usd = proceeds_usd - cost_basis;
    let fully_exited = filled >= lot.shares;

    let position_exit = |state: ExitState| ExitLedgerWrite {
        order_state: outcome_order_state(outcome),
        venue_order_id: result.venue_order_id.clone(),
        venue_status,
        filled_at: Some(result.responded_at),
        cancelled_at: None,
        error_message: None,
        exit_state: state,
        exit_reason: reason,
        position_exit: Some(PositionExit {
            shares: filled,
            avg_price: exit_avg,
            proceeds_usd,
            realized_pnl_usd,
            exited_at: result.responded_at,
            reason,
        }),
        fully_exited,
        revert_to_open: false,
        reconciliation: Some(exit_reconciliation_row(result, execution_order, outcome)),
    };

    match outcome {
        VenueOutcome::Filled => {
            let state = if fully_exited {
                ExitState::Exited
            } else {
                ExitState::PartiallyExited
            };
            (position_exit(state), Some(realized_pnl_usd))
        }
        VenueOutcome::PartiallyFilled => (
            position_exit(ExitState::PartiallyExited),
            Some(realized_pnl_usd),
        ),
        // Resting sell limit: stays Submitted, no position change; recon sweep
        // polls it (no recon row, like a resting entry order).
        VenueOutcome::Open => (resting_open_exit_write(result, reason), None),
        // Clean rejection / cancellation: revert the lot to Open and re-monitor.
        VenueOutcome::Rejected => (
            failed_exit_write(
                result,
                execution_order,
                reason,
                ExecutionOrderState::Failed,
                outcome,
            ),
            None,
        ),
        VenueOutcome::Cancelled | VenueOutcome::Expired => (
            failed_exit_write(
                result,
                execution_order,
                reason,
                ExecutionOrderState::Cancelled,
                outcome,
            ),
            None,
        ),
        // Unconfirmed: hold the position, enqueue recon (fail-closed). No PnL.
        VenueOutcome::Ambiguous => (ambiguous_exit_write(result, execution_order, reason), None),
    }
}

/// Ledger write for a resting `Open` sell limit: stays `Submitted`, no position
/// change; the recon sweep polls it (no recon row, like a resting entry order).
fn resting_open_exit_write(result: &VenueSubmitResult, reason: ExitReason) -> ExitLedgerWrite {
    ExitLedgerWrite {
        order_state: ExecutionOrderState::Submitted,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: VenueOutcome::Open.venue_order_status(),
        filled_at: None,
        cancelled_at: None,
        error_message: None,
        exit_state: ExitState::OrderSubmitted,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: false,
        reconciliation: None,
    }
}

/// Ledger write for an unconfirmed (`Ambiguous`) venue response: hold the
/// position untouched and enqueue reconciliation (fail-closed; no `PnL`).
fn ambiguous_exit_write(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    reason: ExitReason,
) -> ExitLedgerWrite {
    ExitLedgerWrite {
        order_state: ExecutionOrderState::Ambiguous,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: None,
        filled_at: None,
        cancelled_at: None,
        error_message: result.detail.clone(),
        exit_state: ExitState::OrderSubmitted,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: false,
        reconciliation: Some(exit_reconciliation_row(
            result,
            execution_order,
            VenueOutcome::Ambiguous,
        )),
    }
}

/// Ledger write for a failed/cancelled exit attempt: revert `Closing -> Open`
/// and route the lot back to `Monitoring` so the worker re-evaluates it.
fn failed_exit_write(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    reason: ExitReason,
    order_state: ExecutionOrderState,
    outcome: VenueOutcome,
) -> ExitLedgerWrite {
    ExitLedgerWrite {
        order_state,
        venue_order_id: result.venue_order_id.clone(),
        venue_status: outcome.venue_order_status(),
        filled_at: None,
        cancelled_at: Some(result.responded_at),
        error_message: result.detail.clone(),
        exit_state: ExitState::Monitoring,
        exit_reason: reason,
        position_exit: None,
        fully_exited: false,
        revert_to_open: true,
        reconciliation: Some(exit_reconciliation_row(result, execution_order, outcome)),
    }
}

/// The terminal exit-order state for a (partial) fill venue outcome.
const fn outcome_order_state(outcome: VenueOutcome) -> ExecutionOrderState {
    match outcome {
        VenueOutcome::PartiallyFilled => ExecutionOrderState::PartiallyFilled,
        _ => ExecutionOrderState::Filled,
    }
}

/// Build the submit-time reconciliation row for an exit order.
fn exit_reconciliation_row(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    outcome: VenueOutcome,
) -> NewReconciliation {
    let detail = format!(
        "exit venue outcome {outcome:?}; order_id={:?}; filled={}",
        result.venue_order_id, result.filled_shares
    );
    let evidence = ReconciliationEvidenceChain(vec![ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::ClobOrderStatus,
        observed_at: result.responded_at,
        detail,
        venue_ref: result.venue_order_id.as_ref().map(ToString::to_string),
        shares: Some(result.filled_shares),
        price: result.avg_fill_price,
    }]);
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order.execution_order_id.clone(),
        order_intent_id: execution_order.order_intent_id.clone(),
        result: outcome.reconciliation_result(),
        evidence_json: evidence,
        venue_filled_shares: Some(result.filled_shares),
        venue_avg_price: result.avg_fill_price,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}
