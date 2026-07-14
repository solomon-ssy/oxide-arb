use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{
    ArchivePartitionDropAuditInfo, ArchivePartitionManifestInfo, NewArchivePartitionManifest,
};
use uuid::Uuid;

/// WORM ledger guarding destructive `ClickHouse` partition retention.
#[async_trait::async_trait]
pub trait ArchivePartitionRepository: Send + Sync {
    async fn find_manifest(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Option<ArchivePartitionManifestInfo>, StorageError>;

    async fn seal_manifest(
        &self,
        manifest: NewArchivePartitionManifest,
    ) -> Result<ArchivePartitionManifestInfo, StorageError>;

    async fn claim_pending_drop(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ArchivePartitionManifestInfo>, StorageError>;

    async fn find_drop_audit(
        &self,
        manifest_id: Uuid,
    ) -> Result<Option<ArchivePartitionDropAuditInfo>, StorageError>;

    async fn complete_drop(
        &self,
        manifest_id: Uuid,
        worker_id: Uuid,
        dropped_at: DateTime<Utc>,
    ) -> Result<ArchivePartitionDropAuditInfo, StorageError>;

    async fn mark_drop_failed(
        &self,
        manifest_id: Uuid,
        worker_id: Uuid,
        detail: String,
    ) -> Result<(), StorageError>;
}
