use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::lifecycle_event;

pub trait LifecycleRepository: Send + Sync {
    async fn record(
        &self,
        phase: &str,
        stage: Option<&str>,
        message: &str,
        metadata: Option<&str>,
    ) -> Result<lifecycle_event::Model, StorageError>;

    async fn get_recent(&self, limit: u64) -> Result<Vec<lifecycle_event::Model>, StorageError>;
}
