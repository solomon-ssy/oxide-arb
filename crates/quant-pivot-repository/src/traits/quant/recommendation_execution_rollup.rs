//! Final recommendation execution rollup persistence boundary.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        ExecutionRollupBarrier, ExecutionRollupReconciliationResult, ExecutionRollupTaskClaim,
        OutcomeTaskSettlement, RecommendationExecutionRollupInfo,
    },
    types::{RecommendationId, WorkerId},
};

/// Repository that seals recommendation truth only after every intent is terminal.
#[async_trait::async_trait]
pub trait RecommendationExecutionRollupRepository: Send + Sync {
    async fn reconcile_recommendation(
        &self,
        recommendation_id: RecommendationId,
        available_through: DateTime<Utc>,
    ) -> Result<ExecutionRollupReconciliationResult, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: RecommendationId,
    ) -> Result<Option<RecommendationExecutionRollupInfo>, StorageError>;

    async fn claim_reconciliation(
        &self,
        available_through: DateTime<Utc>,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ExecutionRollupTaskClaim>, StorageError>;

    async fn settle_reconciliation(
        &self,
        recommendation_id: RecommendationId,
        worker_id: WorkerId,
        settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError>;

    async fn barrier(&self, cutoff: DateTime<Utc>) -> Result<ExecutionRollupBarrier, StorageError>;
}
