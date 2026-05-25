use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewRiskAuditEvent;

pub trait RiskAuditRepository: Send + Sync {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError>;
}
