use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{ExecutionOrderInfo, NewExecutionOrder};
use quant_pivot_models::types::{ExecutionOrderId, OrderIntentId};

/// Execution order persistence port.
#[async_trait::async_trait]
pub trait ExecutionOrderRepository: Send + Sync {
    async fn create(&self, order: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError>;

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError>;

    async fn find_by_id(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError>;
}
