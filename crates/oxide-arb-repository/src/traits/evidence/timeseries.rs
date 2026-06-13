use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::types::{OpportunityId, TokenId};
use oxide_arb_models::{
    clickhouse::{
        AuditStageCountRow, BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow,
        OpportunityDetectionRow, TickEventL2Row, TickEventRow,
    },
    domain::{
        MarketFilter, PageRequest, Paginated, TimeWindow,
        evidence::{EvidenceQueryResult, QueryContract},
    },
};
use serde::Serialize;

/// Aggregated funnel statistics for a time window.
///
/// `total_detected` is the scanner baseline from `opportunity_detection`;
/// `stages` carries per-stage distinct-opportunity counts from
/// `opportunity_audit` (which records rejection / terminal / settlement
/// stages only).
#[derive(Debug, Clone)]
pub struct AuditFunnelStats {
    /// Distinct opportunities detected inside the window.
    pub total_detected: u64,
    /// Distinct-opportunity count per audit stage (unordered).
    pub stages: Vec<AuditStageCountRow>,
}

pub fn evidence_query_result<T, P>(
    repository: &str,
    method: &str,
    params: &P,
    ordering: Vec<String>,
    schema_version: Option<u32>,
    rows: Vec<T>,
) -> Result<EvidenceQueryResult<T>, StorageError>
where
    P: Serialize,
{
    let params_bytes =
        serde_json::to_vec(params).map_err(|error| StorageError::Codec(error.to_string()))?;
    let params_hash = format!(
        "blake3:{}",
        hex::encode(blake3::hash(&params_bytes).as_bytes())
    );
    Ok(EvidenceQueryResult::from_rows(
        rows,
        QueryContract::new(repository, method, params_hash, ordering, schema_version),
    ))
}

#[async_trait::async_trait]
pub trait TimeseriesFactWriter: Send + Sync {
    async fn insert_tick_events(&self, rows: Vec<TickEventRow>) -> Result<(), StorageError>;

    async fn insert_l2_events(&self, rows: Vec<TickEventL2Row>) -> Result<(), StorageError>;

    async fn insert_book_snapshots(&self, rows: Vec<BookSnapshotRow>) -> Result<(), StorageError>;

    async fn insert_detections(
        &self,
        rows: Vec<OpportunityDetectionRow>,
    ) -> Result<(), StorageError>;

    async fn insert_audits(&self, rows: Vec<OpportunityAuditRow>) -> Result<(), StorageError>;

    async fn insert_calibration_snapshots(
        &self,
        rows: Vec<CalibrationSnapshotRow>,
    ) -> Result<(), StorageError>;
}

#[async_trait::async_trait]
pub trait EvidenceTimeseriesRepository: Send + Sync {
    async fn tick_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
        limit: u64,
    ) -> Result<EvidenceQueryResult<TickEventRow>, StorageError>;

    async fn l2_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<TickEventL2Row>, StorageError>;

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<EvidenceQueryResult<BookSnapshotRow>, StorageError>;

    async fn detections(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<OpportunityDetectionRow>, StorageError>;

    /// Bounded, paginated detections for the web dashboard (drops the evidence
    /// provenance contract the materialization path needs; returns a page + total).
    async fn detections_page(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<OpportunityDetectionRow>, StorageError>;

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError>;

    async fn terminal_audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError>;

    async fn audit_funnel(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError>;

    /// Bounded, paginated audit funnel for the web dashboard (page + total).
    async fn audit_funnel_page(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<OpportunityAuditRow>, StorageError>;

    /// Aggregated funnel statistics: detection baseline + per-stage
    /// distinct-opportunity counts for the window.
    async fn audit_funnel_stats(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<AuditFunnelStats, StorageError>;

    async fn calibration_snapshots(
        &self,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<CalibrationSnapshotRow>, StorageError>;
}
