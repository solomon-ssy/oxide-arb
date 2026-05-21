use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::market;
use oxide_arb_models::types::MarketId;
use std::collections::HashSet;

pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<market::Model>, StorageError>;
    async fn find_active(&self) -> Result<Vec<market::Model>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<market::Model>, StorageError>;
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<market::Model>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;
    async fn insert(&self, model: market::ActiveModel) -> Result<market::Model, StorageError>;
    async fn insert_batch(&self, models: Vec<market::ActiveModel>) -> Result<u64, StorageError>;
    async fn update(&self, model: market::ActiveModel) -> Result<market::Model, StorageError>;
    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
