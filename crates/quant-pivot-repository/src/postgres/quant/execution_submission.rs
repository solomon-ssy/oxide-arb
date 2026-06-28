//! Postgres-backed execution-submission repository (Phase 05.4 — real money).
//!
//! Every method owns exactly one transaction spanning the execution order,
//! order intent, capital allocation, position ledger, recommendation, and
//! reconciliation tables, reusing the shared `&impl ConnectionTrait` helpers so
//! a submission's money state can never partially apply. Venue network I/O
//! happens between [`create_entry_order_and_lock_capital`] and
//! [`record_submission_result`] — never inside a transaction.

use crate::{
    postgres::quant::{
        capital_allocation::{
            complete_exit_capital, lock_capital, reconcile_capital, release_capital, settle_capital,
        },
        execution_order::validate_execution_order_transition,
        order_intent::{load_intent_for_update, validate_intent_transition},
        position,
    },
    traits::ExecutionSubmissionRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExitLedgerWrite, NewExecutionOrder, NewReconciliation, OrderIntentInfo,
        ReconciliationLedgerWrite, SubmissionLedgerWrite,
    },
    entities::{
        quant_execution_order, quant_order_intent, quant_recommendation, quant_reconciliation,
    },
    enums::{
        execution::{ExecutionOrderPhase, ExitReason, ExitState},
        quant::{ExecutionOrderState, OrderIntentStatus, RecommendationStatus},
    },
    types::{
        ExecutedPartialExitNodes, ExecutionOrderId, OrderIntentId, Price, RecommendationId,
        ReconciliationId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

/// In-flight execution-order states scanned by boot recovery.
const DANGLING_STATES: [ExecutionOrderState; 2] = [
    ExecutionOrderState::Submitted,
    ExecutionOrderState::Ambiguous,
];

/// Exit-order states that must not overlap on the same intent (double-exit guard).
const IN_FLIGHT_EXIT_STATES: [ExecutionOrderState; 2] = [
    ExecutionOrderState::Submitted,
    ExecutionOrderState::Ambiguous,
];

/// Postgres-backed execution-submission repository.
pub struct PgExecutionSubmissionRepository {
    db: DatabaseConnection,
}

impl PgExecutionSubmissionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExecutionSubmissionRepository for PgExecutionSubmissionRepository {
    async fn claim_for_submission(
        &self,
        intent_id: &OrderIntentId,
        now: DateTime<Utc>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent_for_update(&txn, intent_id).await?;
        if !matches!(
            row.status,
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
        ) {
            return Err(StorageError::Conflict(format!(
                "intent {intent_id} is not submittable from {}",
                row.status.as_str()
            )));
        }
        if row.expires_at <= now {
            return Err(StorageError::Conflict(format!(
                "intent {intent_id} has expired and cannot be submitted"
            )));
        }
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionPending, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionPending);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn revert_claim(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent_for_update(&txn, intent_id).await?;
        // No-op if the claim is already gone (e.g. report-cascade invalidation).
        if row.status != OrderIntentStatus::AdmissionPending {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        let revert_to = revert_target_status(&row);
        validate_intent_transition(row.status, revert_to, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(revert_to);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn reject_admission(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_intent_for_update(&txn, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionRejected, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionRejected);
        active.status_reason = ActiveValue::Set(Some(status_reason.clone()));
        active.admission_trace_ref = ActiveValue::Set(admission_trace_ref);
        let intent_model = active.update(&txn).await.map_err(StorageError::from)?;
        release_capital(
            &txn,
            intent_id,
            format!("admission rejected: {status_reason}"),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(intent_model.into())
    }

    async fn create_entry_order_and_lock_capital(
        &self,
        order: NewExecutionOrder,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let intent_id = order.order_intent_id.clone();
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        // Re-lock the intent and re-verify the claim is still held (double-submit guard).
        let intent = load_intent_for_update(&txn, &intent_id).await?;
        if intent.status != OrderIntentStatus::AdmissionPending {
            return Err(StorageError::Conflict(format!(
                "intent {intent_id} must be admission_pending to create an entry order, got {}",
                intent.status.as_str()
            )));
        }
        let recommendation_id = intent.recommendation_id.clone();

        // Write-ahead the venue intent: the row exists in `Submitted` before any
        // network call, so a crash mid-submit is recoverable via reconciliation.
        let execution_order = quant_execution_order::Entity::insert(order.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;

        lock_capital(&txn, &intent_id, "locked for submission".to_owned()).await?;

        validate_intent_transition(intent.status, OrderIntentStatus::Submitted, &intent_id)?;
        let mut intent_active = intent.into_active_model();
        intent_active.status = ActiveValue::Set(OrderIntentStatus::Submitted);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        advance_recommendation_executed(&txn, &recommendation_id).await?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn record_submission_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: SubmissionLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::Conflict(format!("execution order not found: {execution_order_id}"))
            })?;
        validate_execution_order_transition(order.state, write.state, execution_order_id)?;
        let intent_id = order.order_intent_id.clone();

        // Lock the intent so its status advances atomically with the ledger.
        let intent = load_intent_for_update(&txn, &intent_id).await?;

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.state);
        order_active.venue_order_id = ActiveValue::Set(write.venue_order_id);
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.submitted_at = ActiveValue::Set(Some(write.submitted_at));
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message);
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        settle_capital(
            &txn,
            &intent_id,
            &write.capital,
            "submission result".to_owned(),
        )
        .await?;

        if let Some(fill) = write.fill {
            position::apply_fill(&txn, fill).await?;
        }

        // Only transition the intent when the target differs (resting `Open` and
        // `Ambiguous` keep the intent at `Submitted`).
        if write.intent_status != intent.status {
            validate_intent_transition(intent.status, write.intent_status, &intent_id)?;
            let mut intent_active = intent.into_active_model();
            intent_active.status = ActiveValue::Set(write.intent_status);
            intent_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        if let Some(reconciliation) = write.reconciliation {
            quant_reconciliation::Entity::insert(reconciliation.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn mark_exit_manual(
        &self,
        intent_id: &OrderIntentId,
        reason: ExitReason,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent = load_intent_for_update(&txn, intent_id).await?;
        let mut active = intent.into_active_model();
        active.exit_state = ActiveValue::Set(ExitState::ManualRequired);
        active.exit_reason = ActiveValue::Set(Some(reason));
        active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn touch_exit_monitor(
        &self,
        intent_id: &OrderIntentId,
        next_check_at: DateTime<Utc>,
        peak_mark_price: Option<Price>,
        last_signal_recheck_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let intent = load_intent_for_update(&txn, intent_id).await?;
        let promote = intent.exit_state == ExitState::NotStarted;
        let mut active = intent.into_active_model();
        if promote {
            active.exit_state = ActiveValue::Set(ExitState::Monitoring);
        }
        active.next_check_at = ActiveValue::Set(Some(next_check_at));
        if let Some(peak) = peak_mark_price {
            active.peak_mark_price = ActiveValue::Set(Some(peak));
        }
        if let Some(recheck) = last_signal_recheck_at {
            active.last_signal_recheck_at = ActiveValue::Set(Some(recheck));
        }
        active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn create_exit_order_and_mark_closing(
        &self,
        order: NewExecutionOrder,
        exit_reason: ExitReason,
        partial_exit_node_id: Option<String>,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        if partial_exit_node_id.is_some() && exit_reason != ExitReason::PartialExit {
            return Err(StorageError::Conflict(format!(
                "partial_exit_node_id requires PartialExit reason, got {}",
                exit_reason.as_str()
            )));
        }
        let intent_id = order.order_intent_id.clone();
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        // At most one in-flight exit order per intent — prevents oversell when a
        // partial exit is re-triggered while a resting GTC/Ambiguous order exists.
        let inflight = quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::OrderIntentId.eq(intent_id.clone()))
            .filter(quant_execution_order::Column::OrderPhase.eq(ExecutionOrderPhase::Exit))
            .filter(quant_execution_order::Column::State.is_in(IN_FLIGHT_EXIT_STATES))
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        if inflight.is_some() {
            return Err(StorageError::Conflict(format!(
                "intent {intent_id} already has an in-flight exit order (Submitted/Ambiguous)"
            )));
        }

        // Write-ahead the venue exit intent: the Exit order row exists in
        // `Submitted` before any network call (crash-recoverable via recon).
        let execution_order = quant_execution_order::Entity::insert(order.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;

        // Mark the lot `Open -> Closing` and advance the intent exit FSM.
        position::mark_closing(&txn, &intent_id).await?;
        let intent = load_intent_for_update(&txn, &intent_id).await?;
        let mut intent_active = intent.into_active_model();
        intent_active.exit_state = ActiveValue::Set(ExitState::OrderSubmitted);
        intent_active.exit_reason = ActiveValue::Set(Some(exit_reason));
        intent_active.pending_partial_exit_node_id = ActiveValue::Set(partial_exit_node_id);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn record_exit_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        write: ExitLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::Conflict(format!("execution order not found: {execution_order_id}"))
            })?;

        // Idempotency guard: a terminal exit order already settled its position +
        // capital. Never re-apply.
        if order.state.is_terminal() {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(order.into());
        }

        validate_execution_order_transition(order.state, write.order_state, execution_order_id)?;
        let intent_id = order.order_intent_id.clone();
        let existing_venue_order_id = order.venue_order_id.clone();

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.order_state);
        order_active.venue_order_id =
            ActiveValue::Set(write.venue_order_id.clone().or(existing_venue_order_id));
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message.clone());
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        // Reduce/close the lot on a (partial) fill; complete capital on full exit.
        let exit_filled = write.position_exit.is_some();
        if let Some(exit) = write.position_exit {
            position::apply_exit(&txn, &intent_id, exit).await?;
            if write.fully_exited {
                complete_exit_capital(&txn, &intent_id, "exit settled".to_owned()).await?;
            }
        }

        // Revert a failed/cancelled exit attempt so the lot is re-monitored.
        if write.revert_to_open {
            position::revert_lot_to_open(&txn, &intent_id).await?;
        }

        // Advance the intent's exit FSM (status is unchanged — entry already filled).
        let intent = load_intent_for_update(&txn, &intent_id).await?;
        let (executed_nodes, pending_node) =
            partial_exit_nodes_after_exit_settlement(&intent, write.revert_to_open, exit_filled);
        let mut intent_active = intent.into_active_model();
        intent_active.exit_state = ActiveValue::Set(write.exit_state);
        intent_active.exit_reason = ActiveValue::Set(Some(write.exit_reason));
        intent_active.executed_partial_exit_node_ids = ActiveValue::Set(executed_nodes);
        intent_active.pending_partial_exit_node_id = ActiveValue::Set(pending_node);
        intent_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        if let Some(reconciliation) = write.reconciliation {
            quant_reconciliation::Entity::insert(reconciliation.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }

    async fn recover_dangling(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::State.is_in(DANGLING_STATES))
            .order_by_asc(quant_execution_order::Column::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn apply_reconciliation(
        &self,
        execution_order_id: &ExecutionOrderId,
        mut write: ReconciliationLedgerWrite,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        let order = quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::Conflict(format!("execution order not found: {execution_order_id}"))
            })?;

        // Idempotency guard: a terminal order has already had its capital and
        // position settled (at submit or by a prior reconciliation). Never
        // re-apply — return it unchanged.
        if order.state.is_terminal() {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(order.into());
        }

        validate_execution_order_transition(order.state, write.order_state, execution_order_id)?;
        let intent_id = order.order_intent_id.clone();
        let order_intent_id_for_recon = intent_id.clone();
        let existing_venue_order_id = order.venue_order_id.clone();
        let is_exit = order.order_phase == ExecutionOrderPhase::Exit;

        // Lock the intent so its status advances atomically with the ledger.
        let intent = load_intent_for_update(&txn, &intent_id).await?;

        let mut order_active = order.into_active_model();
        order_active.state = ActiveValue::Set(write.order_state);
        order_active.venue_order_id =
            ActiveValue::Set(write.venue_order_id.clone().or(existing_venue_order_id));
        order_active.venue_status = ActiveValue::Set(write.venue_status);
        order_active.filled_at = ActiveValue::Set(write.filled_at);
        order_active.cancelled_at = ActiveValue::Set(write.cancelled_at);
        order_active.error_message = ActiveValue::Set(write.error_message.clone());
        let execution_order = order_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        if is_exit {
            // Exit-order reconciliation: the entry intent stays terminal; correct
            // the lot via `apply_exit` (never `apply_fill`) and complete the
            // capital `Spent -> Released` on a full exit.
            let exit_filled = write.exit.is_some();
            let revert_lot = write.revert_lot;
            if let Some(exit) = write.exit.take() {
                position::apply_exit(&txn, &intent_id, exit).await?;
                if write.exit_fully {
                    complete_exit_capital(&txn, &intent_id, "exit reconciliation".to_owned())
                        .await?;
                }
            }
            if revert_lot {
                position::revert_lot_to_open(&txn, &intent_id).await?;
            }
            let (executed_nodes, pending_node) =
                partial_exit_nodes_after_exit_settlement(&intent, revert_lot, exit_filled);
            let mut intent_active = intent.into_active_model();
            if let Some(exit_state) = write.exit_state {
                intent_active.exit_state = ActiveValue::Set(exit_state);
            }
            intent_active.executed_partial_exit_node_ids = ActiveValue::Set(executed_nodes);
            intent_active.pending_partial_exit_node_id = ActiveValue::Set(pending_node);
            intent_active
                .update(&txn)
                .await
                .map_err(StorageError::from)?;
        } else {
            reconcile_capital(
                &txn,
                &intent_id,
                &write.capital,
                "reconciliation".to_owned(),
            )
            .await?;

            if let Some(fill) = write.fill.take() {
                position::apply_fill(&txn, fill).await?;
            }

            if write.intent_status != intent.status {
                validate_intent_transition(intent.status, write.intent_status, &intent_id)?;
                let mut intent_active = intent.into_active_model();
                intent_active.status = ActiveValue::Set(write.intent_status);
                intent_active
                    .update(&txn)
                    .await
                    .map_err(StorageError::from)?;
            }
        }

        upsert_reconciliation_summary(&txn, execution_order_id, &order_intent_id_for_recon, write)
            .await?;

        txn.commit().await.map_err(StorageError::from)?;
        Ok(execution_order.into())
    }
}

/// Upsert the single reconciliation summary row for an execution order.
///
/// Updates the existing row in place (e.g. an `Ambiguous` order's submit-time
/// `Pending` row), appending the freshly-collected evidence to the chain so the
/// row stays append-only (WORM). Inserts a fresh row for an order that never
/// had one (a resting `Open` order). The unique index on `execution_order_id`
/// guarantees at most one summary per order.
async fn upsert_reconciliation_summary(
    db: &impl ConnectionTrait,
    execution_order_id: &ExecutionOrderId,
    order_intent_id: &OrderIntentId,
    write: ReconciliationLedgerWrite,
) -> Result<(), StorageError> {
    let existing = quant_reconciliation::Entity::find()
        .filter(quant_reconciliation::Column::ExecutionOrderId.eq(execution_order_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?;

    if let Some(row) = existing {
        let mut chain = row.evidence_json.clone();
        for evidence in write.evidence.into_inner() {
            chain.push(evidence);
        }
        let mut active = row.into_active_model();
        active.result = ActiveValue::Set(write.result);
        active.evidence_json = ActiveValue::Set(chain);
        active.venue_filled_shares = ActiveValue::Set(write.venue_filled_shares);
        active.venue_avg_price = ActiveValue::Set(write.venue_avg_price);
        active.discrepancy_usd = ActiveValue::Set(write.discrepancy_usd);
        active.resolved_by = ActiveValue::Set(write.resolved_by);
        active.resolved_at = ActiveValue::Set(write.resolved_at);
        active.update(db).await.map_err(StorageError::from)?;
        return Ok(());
    }

    let new = NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: execution_order_id.clone(),
        order_intent_id: order_intent_id.clone(),
        result: write.result,
        evidence_json: write.evidence,
        venue_filled_shares: write.venue_filled_shares,
        venue_avg_price: write.venue_avg_price,
        discrepancy_usd: write.discrepancy_usd,
        resolved_by: write.resolved_by,
        resolved_at: write.resolved_at,
    };
    quant_reconciliation::Entity::insert(new.into_active_model())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Restore the pre-claim approval status after a transient admission defer.
///
/// Auto-execution intents (`policy_id` set) revert to `ApprovedByPolicy` so the
/// dispatcher worker can retry; semi-auto manual approvals revert to `Approved`.
const fn revert_target_status(row: &quant_order_intent::Model) -> OrderIntentStatus {
    if row.policy_id.is_some() {
        OrderIntentStatus::ApprovedByPolicy
    } else {
        OrderIntentStatus::Approved
    }
}

/// Advance a recommendation to `Executed` on submission (idempotent forward-only:
/// terminal `Revoked`/`Expired` rows are left untouched).
async fn advance_recommendation_executed(
    db: &impl ConnectionTrait,
    recommendation_id: &RecommendationId,
) -> Result<(), StorageError> {
    let row = quant_recommendation::Entity::find_by_id(recommendation_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "recommendation",
            id: recommendation_id.to_string(),
        })?;
    if row.status.is_actionable_for_intent() {
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(RecommendationStatus::Executed);
        active.update(db).await.map_err(StorageError::from)?;
    }
    Ok(())
}

/// After exit settlement or reconciliation, compute the next partial-node ledger.
fn partial_exit_nodes_after_exit_settlement(
    intent: &quant_order_intent::Model,
    revert: bool,
    filled: bool,
) -> (ExecutedPartialExitNodes, Option<String>) {
    if revert {
        return (intent.executed_partial_exit_node_ids.clone(), None);
    }
    if !filled {
        return (
            intent.executed_partial_exit_node_ids.clone(),
            intent.pending_partial_exit_node_id.clone(),
        );
    }
    let Some(pending) = intent.pending_partial_exit_node_id.clone() else {
        return (
            intent.executed_partial_exit_node_ids.clone(),
            intent.pending_partial_exit_node_id.clone(),
        );
    };
    let mut executed = intent.executed_partial_exit_node_ids.clone();
    executed.record(&pending);
    (executed, None)
}
