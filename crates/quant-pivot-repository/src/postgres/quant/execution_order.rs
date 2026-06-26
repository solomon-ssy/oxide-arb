//! Postgres-backed execution-order repository.

use crate::traits::ExecutionOrderRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExecutionOrderPatch, ExecutionOrderSubmissionResult, NewExecutionOrder,
    },
    entities::quant_execution_order,
    enums::quant::ExecutionOrderState,
    types::{ExecutionOrderId, OrderIntentId},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    IntoActiveValue, QueryFilter,
};

/// Postgres-backed execution-order repository.
pub struct PgExecutionOrderRepository {
    db: DatabaseConnection,
}

impl PgExecutionOrderRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExecutionOrderRepository for PgExecutionOrderRepository {
    async fn create(&self, order: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError> {
        quant_execution_order::Entity::insert(order.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::OrderIntentId.eq(order_intent_id.clone()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn transition(
        &self,
        execution_order_id: &ExecutionOrderId,
        patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let Some(row) = quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "execution order not found: {execution_order_id}"
            )));
        };

        if let Some(next) = patch.state.into_option() {
            validate_execution_order_transition(row.state, next, execution_order_id)?;
        }

        let mut active = row.into_active_model();
        active.state = patch.state.into_active_value();
        active.venue_order_id = patch.venue_order_id.into_active_value();
        active.venue_status = patch.venue_status.into_active_value();
        active.submitted_at = patch.submitted_at.into_active_value();
        active.filled_at = patch.filled_at.into_active_value();
        active.cancelled_at = patch.cancelled_at.into_active_value();
        active.gtd_expiration_at = patch.gtd_expiration_at.into_active_value();
        active.error_message = patch.error_message.into_active_value();
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn record_submission_result(
        &self,
        execution_order_id: &ExecutionOrderId,
        result: ExecutionOrderSubmissionResult,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let _ = (execution_order_id, result);
        Err(StorageError::Conflict(
            "record_submission_result is implemented in Phase 05.4".to_owned(),
        ))
    }
}

fn validate_execution_order_transition(
    current: ExecutionOrderState,
    next: ExecutionOrderState,
    execution_order_id: &ExecutionOrderId,
) -> Result<(), StorageError> {
    if current == next {
        return Ok(());
    }
    let valid = matches!(
        (current, next),
        (
            ExecutionOrderState::Planned,
            ExecutionOrderState::Accepted
                | ExecutionOrderState::Submitted
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        ) | (
            ExecutionOrderState::Accepted,
            ExecutionOrderState::Submitted
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        ) | (
            ExecutionOrderState::Submitted,
            ExecutionOrderState::PartiallyFilled
                | ExecutionOrderState::Filled
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::PartiallyFilled,
            ExecutionOrderState::Filled
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::CancelRequested,
            ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::Ambiguous,
            ExecutionOrderState::PartiallyFilled
                | ExecutionOrderState::Filled
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        )
    );
    if valid {
        return Ok(());
    }
    Err(StorageError::Conflict(format!(
        "invalid execution order transition for {execution_order_id}: {} -> {}",
        current.as_str(),
        next.as_str()
    )))
}
