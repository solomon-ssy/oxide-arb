use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewUniverseMember, NewUniverseSnapshot, UniverseMemberInfo, UniverseSnapshotInfo},
    types::UniverseSnapshotId,
};

/// Universe snapshot persistence port.
#[async_trait::async_trait]
pub trait UniverseRepository: Send + Sync {
    async fn create_snapshot(
        &self,
        snapshot: NewUniverseSnapshot,
        members: Vec<NewUniverseMember>,
    ) -> Result<UniverseSnapshotInfo, StorageError>;

    async fn find_by_id(
        &self,
        snapshot_id: &UniverseSnapshotId,
    ) -> Result<Option<UniverseSnapshotInfo>, StorageError>;

    async fn list_members(
        &self,
        snapshot_id: &UniverseSnapshotId,
    ) -> Result<Vec<UniverseMemberInfo>, StorageError>;
}
