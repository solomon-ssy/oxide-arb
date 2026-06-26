//! Postgres-backed order intent repository.

use crate::traits::OrderIntentRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ApproveOrderIntent, NewOrderIntent, OrderIntentInfo},
    entities::quant_order_intent,
    enums::{
        execution::ApprovalInvalidation,
        quant::{ApprovalStatus, OrderIntentStatus},
    },
    types::OrderIntentId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QuerySelect,
};

/// Postgres-backed order intent repository.
pub struct PgOrderIntentRepository {
    db: DatabaseConnection,
}

impl PgOrderIntentRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl OrderIntentRepository for PgOrderIntentRepository {
    async fn create_pending(
        &self,
        intent: NewOrderIntent,
    ) -> Result<OrderIntentInfo, StorageError> {
        quant_order_intent::Entity::insert(intent.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn create_policy_approved(
        &self,
        intent: NewOrderIntent,
    ) -> Result<OrderIntentInfo, StorageError> {
        if intent.status != OrderIntentStatus::ApprovedByPolicy {
            return Err(StorageError::Conflict(format!(
                "policy-approved intent must start as {}, got {}",
                OrderIntentStatus::ApprovedByPolicy.as_str(),
                intent.status.as_str()
            )));
        }
        quant_order_intent::Entity::insert(intent.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn approve(
        &self,
        intent_id: &OrderIntentId,
        approval: ApproveOrderIntent,
    ) -> Result<OrderIntentInfo, StorageError> {
        let Some(row) = quant_order_intent::Entity::find_by_id(intent_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "order intent not found: {intent_id}"
            )));
        };
        if row.status != OrderIntentStatus::PendingApproval {
            return Err(StorageError::Conflict(format!(
                "cannot approve intent {intent_id} from status {}",
                row.status.as_str()
            )));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Approved);
        active.approval_status = ActiveValue::Set(ApprovalStatus::Approved);
        active.approved_by = ActiveValue::Set(Some(approval.approved_by));
        active.approval_reason = ActiveValue::Set(Some(approval.approval_reason));
        active.approved_at = ActiveValue::Set(Some(approval.approved_at));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn transition(
        &self,
        intent_id: &OrderIntentId,
        next: OrderIntentStatus,
    ) -> Result<OrderIntentInfo, StorageError> {
        let Some(row) = quant_order_intent::Entity::find_by_id(intent_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "order intent not found: {intent_id}"
            )));
        };
        validate_intent_transition(row.status, next, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(next);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn get_for_update(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError> {
        quant_order_intent::Entity::find_by_id(intent_id.clone())
            .lock_exclusive()
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn mark_admission_rejected(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError> {
        let row = load_intent(&self.db, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::AdmissionRejected, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::AdmissionRejected);
        active.status_reason = ActiveValue::Set(Some(status_reason));
        active.admission_trace_ref = ActiveValue::Set(admission_trace_ref);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn invalidate(
        &self,
        intent_id: &OrderIntentId,
        reason: ApprovalInvalidation,
    ) -> Result<OrderIntentInfo, StorageError> {
        let row = load_intent(&self.db, intent_id).await?;
        validate_intent_transition(row.status, OrderIntentStatus::Invalidated, intent_id)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(OrderIntentStatus::Invalidated);
        active.status_reason = ActiveValue::Set(Some(reason.as_str().to_owned()));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_expired(&self, now: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError> {
        quant_order_intent::Entity::find()
            .filter(quant_order_intent::Column::ExpiresAt.lte(now))
            .filter(quant_order_intent::Column::Status.is_in(EXPIRABLE_STATUSES))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

const EXPIRABLE_STATUSES: [OrderIntentStatus; 5] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
    OrderIntentStatus::AdmissionPending,
    OrderIntentStatus::AdmissionRejected,
];

async fn load_intent(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
) -> Result<quant_order_intent::Model, StorageError> {
    let Some(row) = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(StorageError::Conflict(format!(
            "order intent not found: {intent_id}"
        )));
    };
    Ok(row)
}

fn validate_intent_transition(
    current: OrderIntentStatus,
    next: OrderIntentStatus,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let valid = matches!(
        (current, next),
        (
            OrderIntentStatus::Draft,
            OrderIntentStatus::PendingApproval | OrderIntentStatus::ApprovedByPolicy,
        ) | (
            OrderIntentStatus::PendingApproval,
            OrderIntentStatus::Approved
                | OrderIntentStatus::Rejected
                | OrderIntentStatus::Invalidated,
        ) | (
            OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy,
            OrderIntentStatus::AdmissionPending,
        ) | (
            OrderIntentStatus::AdmissionPending,
            OrderIntentStatus::Submitted
                | OrderIntentStatus::AdmissionRejected
                | OrderIntentStatus::Invalidated,
        ) | (
            OrderIntentStatus::Submitted,
            OrderIntentStatus::PartiallyFilled
                | OrderIntentStatus::Filled
                | OrderIntentStatus::Failed
                | OrderIntentStatus::Cancelled,
        ) | (
            OrderIntentStatus::PartiallyFilled,
            OrderIntentStatus::Filled | OrderIntentStatus::Failed | OrderIntentStatus::Cancelled
        ) | (
            _,
            OrderIntentStatus::Cancelled
                | OrderIntentStatus::Failed
                | OrderIntentStatus::Expired
                | OrderIntentStatus::Invalidated,
        )
    );
    if valid {
        return Ok(());
    }
    Err(StorageError::Conflict(format!(
        "invalid order intent transition for {intent_id}: {} -> {}",
        current.as_str(),
        next.as_str()
    )))
}
