use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        EconomicOutcomeReconciliationResult, EconomicOutcomeReplayContext,
        EconomicOutcomeTaskClaim, EconomicOutcomeTaskSettlement, NewRecommendationEconomicOutcome,
        RecommendationEconomicOutcomeInfo,
    },
    types::{RecommendationId, RecommendationReportId, WorkerId},
};

#[async_trait::async_trait]
pub trait RecommendationEconomicOutcomeRepository: Send + Sync {
    async fn insert(
        &self,
        outcome: NewRecommendationEconomicOutcome,
    ) -> Result<RecommendationEconomicOutcomeInfo, StorageError>;

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationEconomicOutcomeInfo>, StorageError>;

    /// Load exact immutable recommendation/policy/profile/latency lineage.
    async fn replay_context(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<EconomicOutcomeReplayContext, StorageError>;

    /// Create horizon tasks atomically with a report publication.
    async fn enqueue_report(&self, report_id: &RecommendationReportId)
    -> Result<u64, StorageError>;

    /// Lease horizon-due or canonically resolved tasks, freezing the replay boundary on first claim.
    async fn claim_due(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        source_lateness_secs: u64,
        limit: u64,
    ) -> Result<Vec<EconomicOutcomeTaskClaim>, StorageError>;

    /// Atomically publish the WORM outcome and complete the exact live lease attempt.
    async fn complete_task(
        &self,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
        outcome: NewRecommendationEconomicOutcome,
    ) -> Result<EconomicOutcomeReconciliationResult, StorageError>;

    /// Retry only the exact live lease attempt; stale workers may not alter durable state.
    async fn retry_task(
        &self,
        claim: EconomicOutcomeTaskClaim,
        worker_id: WorkerId,
        delay_secs: u64,
        error: String,
    ) -> Result<EconomicOutcomeTaskSettlement, StorageError>;
}
