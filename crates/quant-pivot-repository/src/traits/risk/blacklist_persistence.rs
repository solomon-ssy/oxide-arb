use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BlacklistInfo, UpsertBlacklistEntry},
    types::MarketId,
};

#[async_trait::async_trait]
pub trait BlacklistPersistenceRepository: Send + Sync {
    async fn upsert(&self, entry: UpsertBlacklistEntry) -> Result<(), StorageError>;
    async fn remove(&self, market_id: &MarketId) -> Result<(), StorageError>;
    async fn load_active(&self) -> Result<Vec<BlacklistInfo>, StorageError>;
}
