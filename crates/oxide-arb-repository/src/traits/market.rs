use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, UpsertMarket},
    types::MarketId,
};
use std::collections::HashSet;

pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<MarketInfo>, StorageError>;
    async fn find_active(&self) -> Result<Vec<MarketInfo>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<MarketInfo>, StorageError>;
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<MarketInfo>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;

    async fn upsert(&self, market: UpsertMarket) -> Result<MarketInfo, StorageError>;

    /// Insert new rows and update existing rows in one round-trip (`ON CONFLICT DO UPDATE`).
    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError>;

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
