use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewEmergencySnapshot;

#[async_trait::async_trait]
pub trait EmergencyRepository: Send + Sync {
    async fn create(&self, snapshot: NewEmergencySnapshot) -> Result<(), StorageError>;
}
