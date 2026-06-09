use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo,
        control_factor::{AuditedOutcome, NewControlFactorAuditEvent},
        evidence::EvidenceQueryResult,
    },
    types::RuntimeConfigVersionId,
};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait RuntimeConfigVersionRepository: Send + Sync {
    async fn create_version(
        &self,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError>;

    async fn activate_version(
        &self,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError>;

    /// Creates an immutable version and appends a chained governance audit event
    /// in one transaction.
    ///
    /// Returns the created version paired with the appended audit event id so the
    /// general operation log can hard-link the creation to the governance chain.
    async fn create_version_governed(
        &self,
        version: NewRuntimeConfigVersion,
        audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<RuntimeConfigVersionInfo>, StorageError>;

    /// Appends a chained governance audit event and records its `event_id` on the
    /// activation row, atomically, so activation lineage is traceable to the
    /// global audit chain.
    async fn activate_version_governed(
        &self,
        activation: NewRuntimeConfigActivation,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigActivationInfo, StorageError>;

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_version_evidence(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<EvidenceQueryResult<RuntimeConfigVersionInfo>, StorageError> {
        let rows = self.load_version(version_id).await?.into_iter().collect();
        evidence_query_result(
            "RuntimeConfigVersionRepository",
            "load_version",
            version_id,
            Vec::new(),
            Some(1),
            rows,
        )
    }

    async fn load_by_hash(
        &self,
        config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_by_hash_evidence(
        &self,
        config_hash: &str,
    ) -> Result<EvidenceQueryResult<RuntimeConfigVersionInfo>, StorageError> {
        let rows = self.load_by_hash(config_hash).await?.into_iter().collect();
        evidence_query_result(
            "RuntimeConfigVersionRepository",
            "load_by_hash",
            &config_hash,
            Vec::new(),
            Some(1),
            rows,
        )
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError>;

    async fn load_active_at_evidence(
        &self,
        at: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<RuntimeConfigVersionInfo>, StorageError> {
        let rows = self.load_active_at(at).await?.into_iter().collect();
        evidence_query_result(
            "RuntimeConfigVersionRepository",
            "load_active_at",
            &at,
            vec!["activated_at DESC".to_owned()],
            Some(1),
            rows,
        )
    }

    /// Lists immutable runtime-config versions, most recent first.
    async fn list_versions(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError>;

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError>;
}
