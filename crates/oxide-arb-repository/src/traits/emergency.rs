use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewEmergencySnapshot;

pub trait EmergencyRepository: Send + Sync {
    async fn create(&self, snapshot: NewEmergencySnapshot) -> Result<(), StorageError>;
}
