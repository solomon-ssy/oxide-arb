use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::RecommendationInfo;
use quant_pivot_models::types::{RecommendationId, RecommendationReportId};

/// Read-only recommendation access.
///
/// Recommendations are written only as part of the report-creation transaction
/// ([`super::RecommendationReportRepository::create_report`]); there is no
/// standalone batch insert.
#[async_trait::async_trait]
pub trait RecommendationRepository: Send + Sync {
    async fn find_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError>;
}
