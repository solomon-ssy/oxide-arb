//! Exit dispatcher — submits a triggered Sell exit order and settles it in one
//! transaction.
//!
//! Mirrors the entry dispatcher's write-ahead / venue-call / settle shape but
//! for the exit side: it does **not** run the 25-check admission engine (an exit
//! reduces existing exposure; its lightweight gates — kill-switch `allows_auto_exit`,
//! a readable mark — are enforced in [`decide_exit`](super::decide_exit)). The
//! per-intent lot's capital is already `Spent` from entry; a full exit completes
//! it to `Released`, a partial keeps it `Spent`. Realized `PnL` is exact (per-lot
//! average cost, net the venue exit fee). Unconfirmed venue responses become
//! `Ambiguous` (position untouched, recon enqueued) — never silently exited.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    QuantResult,
    execution::ExecutionError,
    storage::{StorageError, entity::QUANT_ORDER_INTENT},
};
use quant_pivot_models::{
    domain::quant::{
        ExecutionOrderInfo, ExitLedgerWrite, NewExecutionOrder, NewReconciliation, PositionExit,
        PositionInfo,
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        common::{OrderType, Side},
        execution::{ExecutionOrderPhase, ExitReason, ExitState, ReconciliationEvidenceKind},
        quant::{ExecutionOrderState, FillRequirement},
    },
    hashing::CanonicalDigest,
    types::{
        ExecutionOrderId, PendingScaleOut, PreparedFeeSchedule, PreparedVenueOrder,
        ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId, ResearchProfileRef,
        Usd, VenueOrderAmount,
    },
};
use quant_pivot_repository::traits::{
    ClobMarketInfoRepository, ExecutionSubmissionRepository, OrderIntentRepository,
};
use quant_pivot_research::execution_semantics::{
    BookWalkOutcome, LiquidityRole, walk_sell_exact_shares,
};

use crate::{
    execution::{
        admission::pit_fee_schedule,
        breaker::ExecutionBreaker,
        dispatcher::{gtd_expiration_at, submit_response_resolution},
        exit_monitor::ExitOrderSpec,
        order_client::{PolymarketOrderClient, VenueOrder, VenueOutcome, VenueSubmitResult},
    },
    ingest::book_store::BookStore,
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
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub book_store: Arc<BookStore>,
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

        let intent = self
            .deps
            .intents
            .find_by_id(&lot.order_intent_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, lot.order_intent_id))?;
        let prepared_order = self
            .prepare_exit_order(&lot, &order, &intent.profile_ref)
            .await?;

        // 1. Write-ahead the Exit order (Submitted) + lot Open->Closing + exit FSM.
        let new_order = build_exit_order(&lot, &order, prepared_order.clone())?;
        let execution_order = self
            .deps
            .submission
            .create_exit_order(new_order, reason, pending_scale_out)
            .await?;
        let recommendation_id = intent.recommendation_id;
        self.deps.execution_events.write(project_execution_event(
            &execution_order,
            recommendation_id,
            ChQuantLedgerEventKind::ExitSubmitted,
            Utc::now(),
        ));

        // 2. Venue submission — NO DB lock held across this network call.
        let venue_order = VenueOrder {
            market_id: lot.market_id.clone(),
            token_id: prepared_order.token_id.clone(),
            side: prepared_order.side,
            price: prepared_order.worst_price,
            amount: prepared_order.venue_amount,
            order_type: prepared_order.order_type,
            post_only: prepared_order.post_only,
            expected_fee: prepared_order.expected_fee,
            fee_schedule_hash: prepared_order.fee_schedule.schedule_hash,
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

    async fn prepare_exit_order(
        &self,
        lot: &PositionInfo,
        order: &ExitOrderSpec,
        profile_ref: &ResearchProfileRef,
    ) -> QuantResult<PreparedVenueOrder> {
        let now = Utc::now();
        let book = self
            .deps
            .book_store
            .load_fresh_by_id(&lot.token_id)
            .map_err(|unavailable| ExecutionError::IntentDenied {
                reason: format!("cannot prepare exit without a fresh L2 book: {unavailable:?}"),
            })?;
        let market_info = self
            .deps
            .clob_market_info
            .at(&lot.market_id, now, now)
            .await?
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "cannot prepare exit without PIT CLOB market info".to_owned(),
            })?;
        let schedule = pit_fee_schedule(&market_info, now)?;
        let requirement = match order.order_type {
            OrderType::Fok => FillRequirement::AllOrNothing,
            OrderType::Fak | OrderType::Gtc | OrderType::Gtd { .. } => {
                FillRequirement::AllowPartial
            }
        };
        let fill = walk_sell_exact_shares(
            &book.bids,
            order.shares,
            order.limit_price,
            requirement,
            &schedule,
            LiquidityRole::Taker,
            now,
        )
        .map_err(|error| ExecutionError::IntentDenied {
            reason: format!("exit execution preparation failed: {error:?}"),
        })?;
        if order.order_type == OrderType::Fok && fill.outcome == BookWalkOutcome::Unfilled {
            return Err(ExecutionError::IntentDenied {
                reason: "FOK exit cannot fill from the current L2 book".to_owned(),
            }
            .into());
        }
        let book_hash = CanonicalDigest::content_hash_json(&(
            book.timestamp_ms,
            book.version,
            book.bids.as_ref(),
            book.asks.as_ref(),
        ))?;
        let clob_market_info_hash = market_info.payload_hash;
        let valid_until = match order.order_type {
            OrderType::Gtd { expiration } => DateTime::from_timestamp(
                i64::try_from(expiration).map_err(|error| ExecutionError::TimeConversion {
                    field: "exit.gtd.expiration",
                    value: expiration.to_string(),
                    detail: error.to_string(),
                })?,
                0,
            )
            .ok_or_else(|| ExecutionError::TimeConversion {
                field: "exit.gtd.expiration",
                value: expiration.to_string(),
                detail: "timestamp is outside chrono range".to_owned(),
            })?,
            OrderType::Fok | OrderType::Fak | OrderType::Gtc => now + Duration::minutes(1),
        };
        Ok(PreparedVenueOrder {
            profile_ref: profile_ref.clone(),
            token_id: lot.token_id.clone(),
            side: Side::Sell,
            order_type: order.order_type,
            post_only: false,
            worst_price: fill.worst_price.unwrap_or(order.limit_price),
            cash_budget: None,
            venue_amount: VenueOrderAmount::Shares(order.shares),
            expected_fee: fill.expected_fee,
            total_cash_delta: fill.total_cash_delta,
            expected_filled_shares: fill.filled_shares,
            book_hash,
            clob_market_info_hash,
            fee_schedule: PreparedFeeSchedule {
                schedule_hash: schedule.schedule_hash,
                effective_at: schedule.effective_at,
                available_at: schedule.available_at,
                platform_rate: schedule.platform_rate,
                exponent: schedule.exponent,
                taker_only: schedule.taker_only,
                builder_maker_fee_bps: schedule.builder_maker_fee_bps,
                builder_taker_fee_bps: schedule.builder_taker_fee_bps,
                builder_attribution: schedule.builder_attribution,
            },
            prepared_at: now,
            valid_until,
        })
    }
}

/// Build the write-ahead Exit execution-order row (`state = Submitted`).
fn build_exit_order(
    lot: &PositionInfo,
    order: &ExitOrderSpec,
    prepared_order_json: PreparedVenueOrder,
) -> Result<NewExecutionOrder, ExecutionError> {
    Ok(NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: lot.order_intent_id,
        order_phase: ExecutionOrderPhase::Exit,
        market_id: lot.market_id.clone(),
        token_id: lot.token_id.clone(),
        side: Side::Sell,
        order_type: order.order_type.into(),
        price: order.limit_price,
        shares: order.shares,
        cost_usd: order.shares * order.limit_price,
        prepared_order_json,
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
    let exit_avg = result.avg_fill_price.unwrap_or(execution_order.price);

    // Exact per-lot average-cost realized PnL, net the venue exit fee.
    let proceeds_usd = filled * exit_avg - result.expected_fee;
    let cost_basis = lot.avg_price * filled;
    let realized_pnl_usd = proceeds_usd - cost_basis;
    let fully_exited = filled >= lot.shares;

    let position_exit = |state: ExitState| ExitLedgerWrite {
        identity_refs: result.identity_refs(),
        order_state: (outcome).outcome_order_state(),
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
        identity_refs: result.identity_refs(),
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
        identity_refs: result.identity_refs(),
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
        identity_refs: result.identity_refs(),
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

impl VenueOutcome {
    /// The terminal exit-order state for a (partial) fill venue outcome.
    const fn outcome_order_state(self) -> ExecutionOrderState {
        match self {
            Self::PartiallyFilled => ExecutionOrderState::PartiallyFilled,
            _ => ExecutionOrderState::Filled,
        }
    }
}

/// Build the submit-time reconciliation row for an exit order.
fn exit_reconciliation_row(
    result: &VenueSubmitResult,
    execution_order: &ExecutionOrderInfo,
    outcome: VenueOutcome,
) -> NewReconciliation {
    let reconciliation_result = outcome.reconciliation_result();
    let (resolved_by, resolved_at) =
        submit_response_resolution(reconciliation_result, result.responded_at);
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
        fee_evidence: Some(result.fee_evidence.clone()),
    }]);
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order.execution_order_id,
        order_intent_id: execution_order.order_intent_id,
        result: reconciliation_result,
        evidence_json: evidence,
        venue_filled_shares: Some(result.filled_shares),
        venue_avg_price: result.avg_fill_price,
        expected_cash_delta_usd: Some(Usd::new(
            execution_order.prepared_order_json.total_cash_delta,
        )),
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: Some(result.expected_fee),
        observed_fee_usd: result.observed_fee,
        fee_delta_usd: result.observed_fee.map(|fee| fee - result.expected_fee),
        resolved_by,
        resolved_at,
    }
}
