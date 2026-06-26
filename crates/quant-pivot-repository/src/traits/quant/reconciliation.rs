use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, NewReconciliation, ReconciliationInfo, ReconciliationPatch,
    },
    types::{ExecutionOrderId, ReconciliationId},
};

/// Execution-order reconciliation summary persistence port.
#[async_trait::async_trait]
pub trait ReconciliationRepository: Send + Sync {
    async fn create(
        &self,
        reconciliation: NewReconciliation,
    ) -> Result<ReconciliationInfo, StorageError>;

    async fn append_evidence(
        &self,
        reconciliation_id: &ReconciliationId,
        evidence: AppendReconciliationEvidence,
    ) -> Result<ReconciliationInfo, StorageError>;

    async fn resolve(
        &self,
        reconciliation_id: &ReconciliationId,
        patch: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError>;

    async fn find_by_execution_order(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ReconciliationInfo>, StorageError>;

    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError>;

    async fn has_unresolvable(&self) -> Result<bool, StorageError>;
}
