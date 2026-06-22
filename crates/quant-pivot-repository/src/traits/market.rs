use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{MarketInfo, MarketPageQuery, Paginated, UpsertMarket},
    types::MarketId,
};
use std::{collections::HashSet, sync::Arc};

#[async_trait::async_trait]
pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError>;
    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError>;
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;
    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError>;
    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError>;
    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
