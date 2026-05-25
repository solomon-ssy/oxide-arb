use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{LifecycleEventInfo, NewLifecycleEvent};

pub trait LifecycleRepository: Send + Sync {
    async fn create(&self, event: NewLifecycleEvent) -> Result<LifecycleEventInfo, StorageError>;

    async fn get_recent(&self, limit: u64) -> Result<Vec<LifecycleEventInfo>, StorageError>;
}
