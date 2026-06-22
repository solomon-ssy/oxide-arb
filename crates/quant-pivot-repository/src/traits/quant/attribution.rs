use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{NewRecommendationAttribution, RecommendationAttributionInfo};
use quant_pivot_models::types::RecommendationId;

/// Recommendation attribution persistence port.
#[async_trait::async_trait]
pub trait AttributionRepository: Send + Sync {
    async fn create(
        &self,
        attribution: NewRecommendationAttribution,
    ) -> Result<RecommendationAttributionInfo, StorageError>;

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Vec<RecommendationAttributionInfo>, StorageError>;
}
