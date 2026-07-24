use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        ExecutionOutcomeReconciliationResult, RecommendationExecutionOutcomeInfo,
        RecommendationExecutionReconciliationCandidate,
    },
    types::{OrderIntentId, RecommendationId},
};

/// Persistence port for immutable recommendation-execution outcomes.
#[async_trait::async_trait]
pub trait RecommendationExecutionOutcomeRepository: Send + Sync {
    /// Lock, derive, and seal the complete execution source graph for one
    /// actually submitted intent.
    async fn reconcile_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<ExecutionOutcomeReconciliationResult, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationExecutionOutcomeInfo>, StorageError>;

    /// Keyset page of submitted terminal intents that still lack A04 truth.
    async fn list_reconciliation_candidates(
        &self,
        after: Option<OrderIntentId>,
        limit: u64,
    ) -> Result<Vec<RecommendationExecutionReconciliationCandidate>, StorageError>;
}
