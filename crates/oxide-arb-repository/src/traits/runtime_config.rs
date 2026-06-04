use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo, evidence::EvidenceQueryResult,
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

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError>;
}
