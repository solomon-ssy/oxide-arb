//! Shadow-comparison ledger repository trait.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        NewShadowComparison, ShadowComparisonInfo, ShadowObservationQuery, ShadowObservationWindow,
        ShadowStabilitySummary,
    },
    types::ModelVersionId,
};

/// Content-addressed append outcome for one shadow observation.
#[derive(Debug, Clone)]
pub enum ShadowComparisonWriteOutcome {
    /// A new semantic observation was appended.
    Inserted(ShadowComparisonInfo),
    /// The exact semantic observation was already durable.
    AlreadyPresent(ShadowComparisonInfo),
}

/// Persistence port for the append-only, content-addressed shadow-comparison
/// ledger (signal/rank layer).
#[async_trait::async_trait]
pub trait ShadowComparisonRepository: Send + Sync {
    /// Insert a new shadow-comparison row, returning the persisted projection.
    async fn create(
        &self,
        comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonWriteOutcome, StorageError>;

    /// Aggregate the stability of a shadow version over comparisons at or after
    /// `since` (the publish-gate window): sample count, window bounds, mean `TopN`
    /// overlap, and whether any comparison flagged a hard divergence.
    async fn summary(
        &self,
        candidate_model_version_id: &ModelVersionId,
        since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError>;

    /// Aggregate only rows carrying the exact published-generation identity
    /// inside the frozen half-open decision/creation window.
    async fn observation_window(
        &self,
        query: &ShadowObservationQuery,
    ) -> Result<ShadowObservationWindow, StorageError>;
}
