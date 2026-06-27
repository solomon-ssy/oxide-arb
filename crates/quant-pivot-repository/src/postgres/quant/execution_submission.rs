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
        capital_allocation::{lock_capital, release_capital, settle_capital},
        execution_order::validate_execution_order_transition,
        order_intent::{load_intent_for_update, validate_intent_transition},
        position,
    },
    traits::ExecutionSubmissionRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ExecutionOrderInfo, NewExecutionOrder, OrderIntentInfo, SubmissionLedgerWrite},
    entities::{
        quant_execution_order, quant_order_intent, quant_recommendation, quant_reconciliation,
    },
    enums::quant::{ExecutionOrderState, OrderIntentStatus, RecommendationStatus},
    types::{ExecutionOrderId, OrderIntentId, RecommendationId},
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
