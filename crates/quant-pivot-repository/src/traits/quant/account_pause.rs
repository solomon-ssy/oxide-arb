use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseOperationInfo,
        NewAccountPauseOperation,
    },
    enums::execution::AccountPauseOperationKind,
    types::{AccountPauseOperationId, AccountRecoveryIncidentId},
};

#[async_trait::async_trait]
pub trait AccountPauseOperationRepository: Send + Sync {
    async fn insert_prepared(
        &self,
        submission: NewAccountPauseOperation,
    ) -> Result<AccountPauseOperationInfo, StorageError>;
    async fn recoverable(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        operation_kind: AccountPauseOperationKind,
    ) -> Result<Vec<AccountPauseOperationInfo>, StorageError>;
    async fn for_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        operation_kind: AccountPauseOperationKind,
    ) -> Result<Vec<AccountPauseOperationInfo>, StorageError>;
    async fn record_dispatch(
        &self,
        submission_id: &AccountPauseOperationId,
        dispatch: AccountPauseDispatch,
        dispatched_at: DateTime<Utc>,
    ) -> Result<AccountPauseOperationInfo, StorageError>;
    async fn confirm(
        &self,
        submission_id: &AccountPauseOperationId,
        confirmation: AccountPauseConfirmation,
    ) -> Result<AccountPauseOperationInfo, StorageError>;
}
