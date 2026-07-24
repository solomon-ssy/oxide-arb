use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::quant::{FeedbackCohortPage, FeedbackCohortPageQuery};

/// Aggregate read boundary for one PIT-frozen feedback cohort.
#[async_trait::async_trait]
pub trait FeedbackCohortRepository: Send + Sync {
    /// Read one exact-profile keyset page and only the truth plane consumed by
    /// the requested cohort.
    async fn list_page(
        &self,
        query: FeedbackCohortPageQuery,
    ) -> Result<FeedbackCohortPage, StorageError>;
}
