use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{
    ExecutionOrderInfo, ExecutionOrderListQuery, ExecutionOrderPatch, NewExecutionOrder, Paginated,
};
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

    async fn page(
        &self,
        query: ExecutionOrderListQuery,
    ) -> Result<Paginated<ExecutionOrderInfo>, StorageError> {
        let query = query.normalized();
        Ok(Paginated::from_request(Vec::new(), 0, &query.page))
    }

    /// Whether any execution order is in the `Ambiguous` state (submitted but
    /// unconfirmed — capital held, venue truth unknown). This is the fail-closed
    /// gate for new auto-execution entries: truth-unknown in-flight exposure must
    /// be reconciled (05.5) before opening more. Resting `Submitted` (open limit)
    /// orders are *not* blocking.
    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError>;

    /// Orders whose venue truth is still unknown and whose capital is held —
    /// `Submitted` (resting open) and `Ambiguous` (unconfirmed). These are the
    /// inputs the reconciliation worker (05.5) resolves to a terminal verdict;
    /// already-terminal orders (`Filled`/`Cancelled`/`Failed`) and synchronous
    /// `PartiallyFilled` (capital + position already applied at submit) are
    /// excluded so reconciliation never double-applies a fill. Ordered oldest
    /// first, bounded by `limit`.
    async fn find_reconcilable(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError>;

    async fn transition(
        &self,
        execution_order_id: &ExecutionOrderId,
        patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError>;
}
