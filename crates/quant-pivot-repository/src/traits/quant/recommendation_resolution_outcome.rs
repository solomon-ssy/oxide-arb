use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::MarketResolutionRow,
    domain::quant::{
        InsertResolutionOutcomeResult, RecommendationResolutionOutcomeInfo,
        RecommendationResolutionOutcomePage, RecommendationResolutionOutcomePageQuery,
        RecommendationResolutionReconciliationCandidate,
    },
    types::RecommendationId,
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

    /// Keyset page of terminal-market recommendations that still lack A03 truth.
    async fn list_reconciliation_candidates(
        &self,
        after: Option<RecommendationId>,
        limit: u64,
    ) -> Result<Vec<RecommendationResolutionReconciliationCandidate>, StorageError>;

    async fn list_available_page(
        &self,
        query: RecommendationResolutionOutcomePageQuery,
    ) -> Result<RecommendationResolutionOutcomePage, StorageError>;
}
