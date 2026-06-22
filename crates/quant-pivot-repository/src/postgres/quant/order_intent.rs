//! Postgres-backed order intent repository.

use crate::traits::OrderIntentRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ApproveOrderIntent, NewOrderIntent, OrderIntentInfo},
    entities::quant_order_intent,
    enums::quant::{ApprovalStatus, OrderIntentStatus},
    types::OrderIntentId,
};
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};

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
}

fn validate_intent_transition(
    current: OrderIntentStatus,
    next: OrderIntentStatus,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let valid = matches!(
        (current, next),
        (OrderIntentStatus::Draft, OrderIntentStatus::PendingApproval)
            | (
                OrderIntentStatus::PendingApproval,
                OrderIntentStatus::Approved | OrderIntentStatus::Rejected,
            )
            | (
                OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy,
                OrderIntentStatus::Submitted,
            )
            | (
                OrderIntentStatus::Submitted,
                OrderIntentStatus::PartiallyFilled | OrderIntentStatus::Filled,
            )
            | (
                OrderIntentStatus::PartiallyFilled,
                OrderIntentStatus::Filled
            )
            | (
                _,
                OrderIntentStatus::Cancelled
                    | OrderIntentStatus::Failed
                    | OrderIntentStatus::Expired,
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
