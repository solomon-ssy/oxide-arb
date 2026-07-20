//! Append-only operational-readiness evidence index.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    enums::quant::ResearchReadinessEvidenceKind,
    types::{ContentHash, ResearchReadinessEvidenceId},
};

/// Real 24-hour shadow-plane latency observations derived from durable PG
/// ledgers. A missing percentile is distinct from a measured zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowLatencyObservation {
    pub decision_prepared_count: u64,
    pub decision_prepared_p95_ms: Option<u64>,
    pub endpoint_rtt_count: u64,
    pub endpoint_rtt_p95_ms: Option<u64>,
    pub market_delay_count: u64,
    pub market_delay_p95_ms: Option<u64>,
}

#[async_trait::async_trait]
pub trait ResearchReadinessEvidenceRepository: Send + Sync {
    /// Append or return the existing identical content-addressed observation.
    async fn append(
        &self,
        evidence: NewResearchReadinessEvidence,
    ) -> Result<ResearchReadinessEvidenceInfo, StorageError>;

    /// Latest evidence known and still valid at `as_of`.
    async fn latest_valid(
        &self,
        kind: ResearchReadinessEvidenceKind,
        scope_hash: &ContentHash,
        as_of: DateTime<Utc>,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError>;

    /// Exact immutable evidence row used by an already frozen research plan.
    async fn find_by_id(
        &self,
        evidence_id: &ResearchReadinessEvidenceId,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError>;

    /// Aggregate the three non-book shadow latency dimensions over one exact
    /// half-open window. Production preflight never substitutes config values
    /// when a durable observation family is absent.
    async fn observe_shadow_latency(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<ShadowLatencyObservation, StorageError>;
}
