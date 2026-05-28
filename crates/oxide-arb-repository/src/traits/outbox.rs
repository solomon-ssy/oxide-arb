use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewOutboxEventWithId, OutboxEventInfo, UpdateOutboxEvent},
    types::OutboxEventId,
};

#[async_trait::async_trait]
pub trait OutboxRepository: Send + Sync {
    async fn create(&self, event: NewOutboxEventWithId) -> Result<OutboxEventInfo, StorageError>;

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, StorageError>;

    async fn update(
        &self,
        event_id: &OutboxEventId,
        update: UpdateOutboxEvent,
    ) -> Result<(), StorageError>;

    async fn dead_letter_count(&self) -> Result<u64, StorageError>;
}
