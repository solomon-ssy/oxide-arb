use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::NewEmergencySnapshot;

#[async_trait::async_trait]
pub trait EmergencyRepository: Send + Sync {
    async fn create(&self, snapshot: NewEmergencySnapshot) -> Result<(), StorageError>;
}
