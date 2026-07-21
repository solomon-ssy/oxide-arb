use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        MarketSelectionInfo, MarketSelectionMemberInfo, NewMarketSelection,
        NewMarketSelectionMember,
    },
    types::MarketSelectionId,
};

/// Selection snapshot persistence port.
#[async_trait::async_trait]
pub trait MarketSelectionRepository: Send + Sync {
    async fn create_snapshot(
        &self,
        snapshot: NewMarketSelection,
        members: Vec<NewMarketSelectionMember>,
    ) -> Result<MarketSelectionInfo, StorageError>;

    async fn find_by_id(
        &self,
        snapshot_id: &MarketSelectionId,
    ) -> Result<Option<MarketSelectionInfo>, StorageError>;

    async fn list_members(
        &self,
        snapshot_id: &MarketSelectionId,
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError>;

    /// Batch-load members for immutable selection snapshots without N+1 reads.
    async fn list_members_by_snapshot_ids(
        &self,
        snapshot_ids: &[MarketSelectionId],
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError>;
}
