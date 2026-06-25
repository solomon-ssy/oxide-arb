use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AccountSnapshotInfo, NewAccountSnapshot},
    types::AccountSnapshotId,
};

/// Decision-time account capital snapshot persistence.
#[async_trait::async_trait]
pub trait AccountSnapshotRepository: Send + Sync {
    async fn create(
        &self,
        snapshot: NewAccountSnapshot,
    ) -> Result<AccountSnapshotInfo, StorageError>;

    async fn find_by_id(
        &self,
        account_snapshot_id: &AccountSnapshotId,
    ) -> Result<Option<AccountSnapshotInfo>, StorageError>;
}
