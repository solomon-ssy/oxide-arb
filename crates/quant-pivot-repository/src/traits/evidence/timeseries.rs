use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::{OpportunityId, TokenId};
use quant_pivot_models::{
    clickhouse::{
        AuditStageCountRow, BookDecisionContextRow, BookL2ReplayRow, BookMicrostructureRow,
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventRow,
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

    async fn insert_book_l2_replay(&self, rows: Vec<BookL2ReplayRow>) -> Result<(), StorageError>;

    async fn insert_book_snapshots(&self, rows: Vec<BookSnapshotRow>) -> Result<(), StorageError>;

    async fn insert_book_decision_contexts(
        &self,
        rows: Vec<BookDecisionContextRow>,
    ) -> Result<(), StorageError>;

    async fn insert_book_microstructure_1s(
        &self,
        rows: Vec<BookMicrostructureRow>,
    ) -> Result<(), StorageError>;

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

    async fn book_l2_replay(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookL2ReplayRow>, StorageError>;

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<EvidenceQueryResult<BookSnapshotRow>, StorageError>;

    async fn book_decision_contexts(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookDecisionContextRow>, StorageError>;

    async fn book_microstructure_1m(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookMicrostructureRow>, StorageError>;

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
