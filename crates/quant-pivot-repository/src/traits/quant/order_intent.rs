use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ApproveOrderIntent, NewOrderIntent, OrderIntentInfo},
    enums::execution::ApprovalInvalidation,
    enums::quant::OrderIntentStatus,
    types::OrderIntentId,
};

#[async_trait::async_trait]
pub trait OrderIntentRepository: Send + Sync {
    async fn create_pending(&self, intent: NewOrderIntent)
    -> Result<OrderIntentInfo, StorageError>;

    async fn create_policy_approved(
        &self,
        intent: NewOrderIntent,
    ) -> Result<OrderIntentInfo, StorageError>;

    async fn approve(
        &self,
        intent_id: &OrderIntentId,
        approval: ApproveOrderIntent,
    ) -> Result<OrderIntentInfo, StorageError>;

    async fn transition(
        &self,
        intent_id: &OrderIntentId,
        next: OrderIntentStatus,
    ) -> Result<OrderIntentInfo, StorageError>;

    async fn get_for_update(
        &self,
        intent_id: &OrderIntentId,
    ) -> Result<Option<OrderIntentInfo>, StorageError>;

    async fn mark_admission_rejected(
        &self,
        intent_id: &OrderIntentId,
        status_reason: String,
        admission_trace_ref: Option<String>,
    ) -> Result<OrderIntentInfo, StorageError>;

    async fn invalidate(
        &self,
        intent_id: &OrderIntentId,
        reason: ApprovalInvalidation,
    ) -> Result<OrderIntentInfo, StorageError>;

    async fn find_expired(&self, now: DateTime<Utc>) -> Result<Vec<OrderIntentInfo>, StorageError>;
}
