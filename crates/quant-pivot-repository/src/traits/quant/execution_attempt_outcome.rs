use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        ExecutionAttemptBarrier, ExecutionAttemptOutcomeInfo, ExecutionAttemptReconciliationResult,
        ExecutionAttemptTaskClaim, OutcomeTaskSettlement,
    },
    types::{OrderIntentId, RecommendationId, WorkerId},
};

/// Persistence port for immutable execution-attempt outcomes.
#[async_trait::async_trait]
pub trait ExecutionAttemptOutcomeRepository: Send + Sync {
    /// Lock, derive, and seal the complete execution source graph for one
    /// actually submitted intent.
    async fn reconcile_intent(
        &self,
        order_intent_id: &OrderIntentId,
        available_through: DateTime<Utc>,
    ) -> Result<ExecutionAttemptReconciliationResult, StorageError>;

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<ExecutionAttemptOutcomeInfo>, StorageError>;

    /// Batch-load immutable attempt graphs as visible at a frozen cutoff,
    /// ordered by recommendation, terminal time, and intent identity.
    async fn list_by_recommendations(
        &self,
        recommendation_ids: &[RecommendationId],
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<ExecutionAttemptOutcomeInfo>, StorageError>;

    /// Materialize and claim due work with `SKIP LOCKED`.
    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ExecutionAttemptTaskClaim>, StorageError>;

    async fn settle_reconciliation(
        &self,
        order_intent_id: OrderIntentId,
        worker_id: WorkerId,
        settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError>;

    async fn barrier(&self, cutoff: DateTime<Utc>)
    -> Result<ExecutionAttemptBarrier, StorageError>;
}
