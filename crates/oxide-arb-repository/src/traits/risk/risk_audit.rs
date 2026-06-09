use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{
    NewRiskAuditEvent, PageRequest, Paginated, RiskAuditEventInfo, TimeWindow,
    evidence::EvidenceQueryResult,
};

use chrono::{DateTime, Utc};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait RiskAuditRepository: Send + Sync {
    async fn create(&self, event: NewRiskAuditEvent) -> Result<(), StorageError>;
    async fn create_batch(&self, events: Vec<NewRiskAuditEvent>) -> Result<(), StorageError>;
    async fn find_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RiskAuditEventInfo>, StorageError>;

    /// Paginated risk-decision audit events in the window (newest first) for the
    /// trades-decisions dashboard. Returns a page plus the total match count.
    async fn find_between_page(
        &self,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<RiskAuditEventInfo>, StorageError>;

    async fn find_between_evidence(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<RiskAuditEventInfo>, StorageError> {
        let rows = self.find_between(from, to).await?;
        evidence_query_result(
            "RiskAuditRepository",
            "find_between",
            &(from, to),
            vec!["created_at ASC".to_owned(), "id ASC".to_owned()],
            Some(1),
            rows,
        )
    }
}
