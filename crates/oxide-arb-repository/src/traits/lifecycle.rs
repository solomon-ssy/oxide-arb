use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{entities::lifecycle_event, enums::lifecycle::LifecyclePhase};

pub trait LifecycleRepository: Send + Sync {
    async fn record(
        &self,
        phase: LifecyclePhase,
        stage: Option<&str>,
        message: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<lifecycle_event::Model, StorageError>;

    async fn get_recent(&self, limit: u64) -> Result<Vec<lifecycle_event::Model>, StorageError>;
}
