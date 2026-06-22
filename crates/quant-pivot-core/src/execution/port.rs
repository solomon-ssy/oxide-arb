//! Execution port — trait boundary for [`ExecutionRunner`] dependency injection.

use super::execution_pipeline::ExecutionPipeline;
use async_trait::async_trait;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::domain::execution::ExecutionResult;
use oxide_arb_repository::traits::TradeRepository;
use std::sync::Arc;

/// Async execution boundary consumed by shard runners.
#[async_trait]
pub trait ExecutionPort: Send + Sync {
    async fn execute(&self, scored: Arc<ScoredOpportunity>) -> ExecutionResult;
}

#[async_trait]
impl<R> ExecutionPort for ExecutionPipeline<R>
where
    R: TradeRepository + Send + Sync + 'static,
{
    async fn execute(&self, scored: Arc<ScoredOpportunity>) -> ExecutionResult {
        Self::execute(self, scored).await
    }
}
