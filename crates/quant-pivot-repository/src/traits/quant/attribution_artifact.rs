//! Immutable attribution artifact index repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{AttributionArtifactInfo, NewAttributionArtifact},
    enums::quant::AttributionArtifactKind,
    types::{AttributionArtifactId, FeedbackCycleId, RecommendationId},
};

#[derive(Debug, Clone)]
pub enum AttributionArtifactWriteOutcome {
    Inserted(AttributionArtifactInfo),
    AlreadyPresent(AttributionArtifactInfo),
}

#[async_trait::async_trait]
pub trait AttributionArtifactRepository: Send + Sync {
    async fn insert(
        &self,
        artifact: NewAttributionArtifact,
    ) -> Result<AttributionArtifactWriteOutcome, StorageError>;

    async fn find_by_id(
        &self,
        artifact_id: &AttributionArtifactId,
    ) -> Result<Option<AttributionArtifactInfo>, StorageError>;

    async fn latest_for_recommendation(
        &self,
        recommendation_id: &RecommendationId,
        kind: AttributionArtifactKind,
    ) -> Result<Option<AttributionArtifactInfo>, StorageError>;

    /// Complete content-hash-ordered materialization manifest for one source
    /// feedback cycle.
    async fn list_for_cycle(
        &self,
        feedback_cycle_id: FeedbackCycleId,
    ) -> Result<Vec<AttributionArtifactInfo>, StorageError>;

    /// Return only evidence visible by the cycle cutoff and produced by a
    /// different cycle, in deterministic content-hash order.
    async fn list_available(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<AttributionArtifactInfo>, StorageError>;
}
