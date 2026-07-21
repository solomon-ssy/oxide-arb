//! Shadow-comparison ledger repository trait.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{NewShadowComparison, ShadowComparisonInfo, ShadowStabilitySummary},
    types::ModelVersionId,
};

/// Persistence port for the append-only, content-addressed shadow-comparison
/// ledger (signal/rank layer).
#[async_trait::async_trait]
pub trait ShadowComparisonRepository: Send + Sync {
    /// Insert a new shadow-comparison row, returning the persisted projection.
    async fn create(
        &self,
        comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonInfo, StorageError>;

    /// Aggregate the stability of a shadow version over comparisons at or after
    /// `since` (the publish-gate window): sample count, window bounds, mean `TopN`
    /// overlap, and whether any comparison flagged a hard divergence.
    async fn summary(
        &self,
        shadow_model_version_id: &ModelVersionId,
        since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError>;
}
