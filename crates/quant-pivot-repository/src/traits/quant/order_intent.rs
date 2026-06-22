use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ApproveOrderIntent, NewOrderIntent, OrderIntentInfo},
    enums::quant::OrderIntentStatus,
    types::OrderIntentId,
};

#[async_trait::async_trait]
pub trait OrderIntentRepository: Send + Sync {
    async fn create_pending(&self, intent: NewOrderIntent)
    -> Result<OrderIntentInfo, StorageError>;

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
}
