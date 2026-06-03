use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{NewReconciliationReport, ReconciliationReportInfo};

#[async_trait::async_trait]
pub trait ReconciliationRepository: Send + Sync {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError>;

    async fn latest_before(
        &self,
        before: DateTime<Utc>,
    ) -> Result<Option<ReconciliationReportInfo>, StorageError>;

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ReconciliationReportInfo>, StorageError>;
}
