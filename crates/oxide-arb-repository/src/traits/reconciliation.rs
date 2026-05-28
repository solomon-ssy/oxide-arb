use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewReconciliationReport;

#[async_trait::async_trait]
pub trait ReconciliationRepository: Send + Sync {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError>;
}
