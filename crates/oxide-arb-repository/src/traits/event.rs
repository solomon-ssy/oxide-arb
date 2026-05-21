use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::event;
use oxide_arb_models::types::EventId;
use std::collections::HashSet;

pub trait EventRepository: Send + Sync {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<event::Model>, StorageError>;
    async fn find_active(&self) -> Result<Vec<event::Model>, StorageError>;
    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError>;
    async fn insert(&self, model: event::ActiveModel) -> Result<event::Model, StorageError>;
    async fn insert_batch(&self, models: Vec<event::ActiveModel>) -> Result<u64, StorageError>;
    async fn update(&self, model: event::ActiveModel) -> Result<event::Model, StorageError>;
}
