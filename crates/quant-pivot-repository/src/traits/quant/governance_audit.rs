//! Model-governance audit ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{ModelGovernanceAuditInfo, NewModelGovernanceAudit},
    types::ModelVersionId,
};

/// Persistence port for the append-only (WORM) model-governance audit trail.
#[async_trait::async_trait]
pub trait ModelGovernanceAuditRepository: Send + Sync {
    /// Append a governance audit row, returning the persisted projection.
    async fn create(
        &self,
        audit: NewModelGovernanceAudit,
    ) -> Result<ModelGovernanceAuditInfo, StorageError>;

    /// Append once, returning the stored row for an exact identity retry.
    ///
    /// Reusing an audit id with any semantic field drift fails closed.
    async fn append_exact(
        &self,
        audit: NewModelGovernanceAudit,
    ) -> Result<ModelGovernanceAuditInfo, StorageError>;

    /// List the audit trail for a model version, most recent first.
    async fn list_by_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelGovernanceAuditInfo>, StorageError>;
}
