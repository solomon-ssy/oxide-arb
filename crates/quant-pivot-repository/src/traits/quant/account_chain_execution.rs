use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AccountChainEventCursor, AccountChainExecutionInsertOutcome, NewAccountChainExecution,
    },
    types::ExecutionAccountId,
};

/// Append-only repository for finalized account-scoped exchange executions.
#[async_trait::async_trait]
pub trait AccountChainExecutionRepository: Send + Sync {
    async fn latest_cursor(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<AccountChainEventCursor>, StorageError>;

    async fn append(
        &self,
        executions: Vec<NewAccountChainExecution>,
    ) -> Result<AccountChainExecutionInsertOutcome, StorageError>;
}
