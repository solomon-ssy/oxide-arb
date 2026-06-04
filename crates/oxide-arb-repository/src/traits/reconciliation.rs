use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{
    NewReconciliationReport, ReconciliationReportInfo, evidence::EvidenceQueryResult,
};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait ReconciliationRepository: Send + Sync {
    async fn create(&self, report: NewReconciliationReport) -> Result<(), StorageError>;

    async fn latest_before(
        &self,
        before: DateTime<Utc>,
    ) -> Result<Option<ReconciliationReportInfo>, StorageError>;

    async fn latest_before_evidence(
        &self,
        before: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<ReconciliationReportInfo>, StorageError> {
        let rows = self.latest_before(before).await?.into_iter().collect();
        evidence_query_result(
            "ReconciliationRepository",
            "latest_before",
            &before,
            vec!["checked_at DESC".to_owned(), "id DESC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ReconciliationReportInfo>, StorageError>;

    async fn find_between_evidence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<ReconciliationReportInfo>, StorageError> {
        let rows = self.find_between(start, end).await?;
        evidence_query_result(
            "ReconciliationRepository",
            "find_between",
            &(start, end),
            vec!["checked_at ASC".to_owned(), "id ASC".to_owned()],
            Some(1),
            rows,
        )
    }
}
