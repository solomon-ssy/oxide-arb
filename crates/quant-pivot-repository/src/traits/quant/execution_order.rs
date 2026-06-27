use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{ExecutionOrderInfo, ExecutionOrderPatch, NewExecutionOrder};
use quant_pivot_models::types::{ExecutionOrderId, OrderIntentId};

/// Execution order persistence port.
///
/// Single-row reads and the generic [`transition`](Self::transition) primitive
/// (exit / cancel paths in later sub-phases). The cross-table submission write
/// path (entry create, capital lock, venue result settlement) lives on
/// [`ExecutionSubmissionRepository`](crate::traits::ExecutionSubmissionRepository).
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

    /// Whether any execution order is in the `Ambiguous` state (submitted but
    /// unconfirmed — capital held, venue truth unknown). This is the fail-closed
    /// gate for new auto-execution entries: truth-unknown in-flight exposure must
    /// be reconciled (05.5) before opening more. Resting `Submitted` (open limit)
    /// orders are *not* blocking.
    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError>;

    async fn transition(
        &self,
        execution_order_id: &ExecutionOrderId,
        patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError>;
}
