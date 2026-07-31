use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::MarketResolutionRow,
    domain::quant::{
        InsertResolutionOutcomeResult, OutcomeTaskSettlement, RecommendationResolutionOutcomeInfo,
        RecommendationResolutionOutcomePage, RecommendationResolutionOutcomePageQuery,
        ResolutionOutcomeTaskClaim,
    },
    types::{RecommendationId, WorkerId},
};

/// Persistence port for immutable recommendation-resolution outcomes.
#[async_trait::async_trait]
pub trait RecommendationResolutionOutcomeRepository: Send + Sync {
    /// Seal one recommendation outcome from an already persisted canonical
    /// market-resolution fact.
    async fn reconcile_fact(
        &self,
        recommendation_id: &RecommendationId,
        fact: &MarketResolutionRow,
    ) -> Result<InsertResolutionOutcomeResult, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationResolutionOutcomeInfo>, StorageError>;

    /// Earliest recommendation visibility that the canonical resolution
    /// source cursor must cover at the frozen cutoff.
    async fn source_history_start(
        &self,
        available_through: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError>;

    /// Materialize and lease due terminal-market recommendations with
    /// `FOR UPDATE SKIP LOCKED`.
    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ResolutionOutcomeTaskClaim>, StorageError>;

    /// Complete or durably retry a task owned by `worker_id`.
    async fn settle_reconciliation(
        &self,
        recommendation_id: RecommendationId,
        worker_id: WorkerId,
        settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError>;

    async fn list_available_page(
        &self,
        query: RecommendationResolutionOutcomePageQuery,
    ) -> Result<RecommendationResolutionOutcomePage, StorageError>;
}
