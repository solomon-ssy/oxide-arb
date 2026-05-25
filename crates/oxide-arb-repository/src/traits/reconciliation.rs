use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::NewReconciliationReport;

pub trait ReconciliationRepository: Send + Sync {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError>;
}
