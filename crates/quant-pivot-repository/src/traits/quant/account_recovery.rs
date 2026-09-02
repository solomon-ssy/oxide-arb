use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AccountChainExecutionInfo, AccountCleanFunderBlockerInfo,
        AccountExecutionAssociationOutcome, AccountRecoveryIncidentInfo,
        AccountRecoveryManifestDraft, AccountRecoveryManifestInfo, FinalizeAccountRecoveryIncident,
        SealAccountRecoveryIncident,
    },
    types::{AccountChainExecutionId, AccountRecoveryIncidentId, ExecutionAccountId},
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

    async fn find_incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountRecoveryIncidentInfo>, StorageError>;

    async fn incident_executions(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Vec<AccountChainExecutionInfo>, StorageError>;

    async fn append_manifest(
        &self,
        draft: AccountRecoveryManifestDraft,
    ) -> Result<AccountRecoveryManifestInfo, StorageError>;

    async fn latest_manifest(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountRecoveryManifestInfo>, StorageError>;

    async fn clean_funder_blocker(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> Result<Option<AccountCleanFunderBlockerInfo>, StorageError>;

    async fn seal_incident(
        &self,
        command: SealAccountRecoveryIncident,
    ) -> Result<AccountRecoveryIncidentInfo, StorageError>;

    async fn finalize_incident(
        &self,
        command: FinalizeAccountRecoveryIncident,
    ) -> Result<AccountRecoveryIncidentInfo, StorageError>;
}
