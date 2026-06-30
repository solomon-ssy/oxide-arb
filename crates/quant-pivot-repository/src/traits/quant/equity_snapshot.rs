use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{EquitySnapshotInfo, EquitySnapshotQuery, NewEquitySnapshot, Paginated},
    types::EquitySnapshotId,
};

/// Strategy-capital equity history persistence port.
#[async_trait::async_trait]
pub trait EquitySnapshotRepository: Send + Sync {
    async fn create(&self, snapshot: NewEquitySnapshot)
    -> Result<EquitySnapshotInfo, StorageError>;

    async fn find_by_id(
        &self,
        id: &EquitySnapshotId,
    ) -> Result<Option<EquitySnapshotInfo>, StorageError>;

    async fn latest(&self) -> Result<Option<EquitySnapshotInfo>, StorageError>;

    async fn latest_at_or_before(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<Option<EquitySnapshotInfo>, StorageError>;

    async fn page(
        &self,
        query: EquitySnapshotQuery,
    ) -> Result<Paginated<EquitySnapshotInfo>, StorageError>;
}
