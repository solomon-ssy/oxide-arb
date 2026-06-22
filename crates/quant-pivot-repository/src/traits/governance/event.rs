use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{EventInfo, UpsertEvent},
    types::EventId,
};
use std::collections::HashSet;

#[async_trait::async_trait]
pub trait EventRepository: Send + Sync {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<EventInfo>, StorageError>;
    async fn find_by_ids(&self, ids: &[EventId]) -> Result<Vec<EventInfo>, StorageError>;
    async fn find_active(&self) -> Result<Vec<EventInfo>, StorageError>;
    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError>;

    async fn upsert(&self, event: UpsertEvent) -> Result<EventInfo, StorageError>;
    async fn upsert_batch(&self, events: Vec<UpsertEvent>) -> Result<u64, StorageError>;
}
