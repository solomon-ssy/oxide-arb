use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AccountPauseConfirmation, AccountPauseDispatch, AccountPauseSubmissionInfo,
        NewAccountPauseSubmission,
    },
    types::{AccountPauseSubmissionId, AccountRecoveryIncidentId},
};

#[async_trait::async_trait]
pub trait AccountPauseRepository: Send + Sync {
    async fn insert_prepared(
        &self,
        submission: NewAccountPauseSubmission,
    ) -> Result<AccountPauseSubmissionInfo, StorageError>;
    async fn recoverable(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Vec<AccountPauseSubmissionInfo>, StorageError>;
    async fn for_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Vec<AccountPauseSubmissionInfo>, StorageError>;
    async fn record_dispatch(
        &self,
        submission_id: &AccountPauseSubmissionId,
        dispatch: AccountPauseDispatch,
        dispatched_at: DateTime<Utc>,
    ) -> Result<AccountPauseSubmissionInfo, StorageError>;
    async fn confirm(
        &self,
        submission_id: &AccountPauseSubmissionId,
        confirmation: AccountPauseConfirmation,
    ) -> Result<AccountPauseSubmissionInfo, StorageError>;
}
