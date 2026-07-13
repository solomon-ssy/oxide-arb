use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{InsertFinalOutcome, NewRecommendationAttribution, RecommendationAttributionInfo},
    types::RecommendationId,
};

/// Recommendation attribution persistence port.
#[async_trait::async_trait]
pub trait AttributionRepository: Send + Sync {
    async fn insert_final_and_mark_attributed(
        &self,
        attribution: NewRecommendationAttribution,
    ) -> Result<InsertFinalOutcome, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationAttributionInfo>, StorageError>;

    async fn find_label_available_between(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationAttributionInfo>, StorageError> {
        let _ = (window_start, window_end, limit);
        Ok(Vec::new())
    }
}
