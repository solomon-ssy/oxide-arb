use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewRiskAuditEvent;

#[async_trait::async_trait]
pub trait RiskAuditRepository: Send + Sync {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError>;
    async fn create_batch(&self, events: Vec<NewRiskAuditEvent>) -> Result<(), StorageError>;
}
