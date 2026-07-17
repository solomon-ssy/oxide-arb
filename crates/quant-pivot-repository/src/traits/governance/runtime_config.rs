use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigApproval, NewRuntimeConfigVersion,
        RuntimeConfigActivationInfo, RuntimeConfigApprovalInfo, RuntimeConfigVersionInfo,
    },
    types::{
        ContentHash, RuntimeConfigActivationId, RuntimeConfigApprovalId, RuntimeConfigVersionId,
    },
};

#[async_trait::async_trait]
pub trait RuntimeConfigVersionRepository: Send + Sync {
    async fn record_approval(
        &self,
        approval: NewRuntimeConfigApproval,
    ) -> Result<RuntimeConfigApprovalInfo, StorageError>;

    async fn load_approval(
        &self,
        approval_id: &RuntimeConfigApprovalId,
    ) -> Result<Option<RuntimeConfigApprovalInfo>, StorageError>;

    async fn list_valid_approvals(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigApprovalInfo>, StorageError>;

    async fn create_version(
        &self,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError>;

    async fn activate_version(
        &self,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError>;

    /// Activate an operator-selected version only after transactionally
    /// validating its exact approval evidence.
    async fn activate_approved_version(
        &self,
        activation: NewRuntimeConfigActivation,
        require_approver_activator_separation: bool,
    ) -> Result<RuntimeConfigActivationInfo, StorageError>;

    /// Append an activation only when the exact expected activation is still
    /// current. Production pointer switches use this CAS to avoid overwriting
    /// a concurrent governed config transition.
    async fn activate_version_if_current(
        &self,
        expected_current_activation_id: Option<&RuntimeConfigActivationId>,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError>;

    async fn load_current_activation(
        &self,
    ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError>;

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_by_hash(
        &self,
        config_hash: &ContentHash,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn list_versions(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError>;

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError>;
}
