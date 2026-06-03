use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, MarketPitSnapshotInfo, UpsertMarket},
    types::MarketId,
};
use std::{collections::HashSet, sync::Arc};

#[async_trait::async_trait]
pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError>;
    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError>;
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;

    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError>;

    /// Insert new rows and update existing rows in one round-trip (`ON CONFLICT DO UPDATE`).
    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError>;

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
