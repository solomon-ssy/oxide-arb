use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{ExecutionAccountInfo, NewExecutionAccount},
    types::ExecutionAccountId,
};

#[async_trait::async_trait]
pub trait ExecutionAccountRepository: Send + Sync {
    /// Idempotently persist one content-addressed boot-verified account.
    async fn ensure(
        &self,
        account: NewExecutionAccount,
    ) -> Result<ExecutionAccountInfo, StorageError>;

    async fn find_by_id(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<ExecutionAccountInfo>, StorageError>;
}
