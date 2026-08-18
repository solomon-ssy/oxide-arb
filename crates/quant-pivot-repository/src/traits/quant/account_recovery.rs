use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{AccountExecutionAssociationOutcome, AccountRecoveryIncidentInfo},
    types::{AccountChainExecutionId, ExecutionAccountId},
};

#[async_trait::async_trait]
pub trait AccountRecoveryRepository: Send + Sync {
    async fn associate_execution(
        &self,
        execution_id: &AccountChainExecutionId,
        associated_at: DateTime<Utc>,
    ) -> Result<AccountExecutionAssociationOutcome, StorageError>;

    async fn active_incident(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Option<AccountRecoveryIncidentInfo>, StorageError>;
}
