use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{NewRecommendation, RecommendationInfo};
use quant_pivot_models::types::{RecommendationId, RecommendationReportId};

/// Recommendation row persistence port.
#[async_trait::async_trait]
pub trait RecommendationRepository: Send + Sync {
    async fn create_batch(
        &self,
        recommendations: Vec<NewRecommendation>,
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    async fn find_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError>;
}
