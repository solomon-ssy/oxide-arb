use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewRecommendation, NewRecommendationReport, RecommendationReportInfo},
    enums::quant::ReportKind,
    types::RecommendationReportId,
};

#[async_trait::async_trait]
pub trait RecommendationReportRepository: Send + Sync {
    async fn create_report(
        &self,
        report: NewRecommendationReport,
        recommendations: Vec<NewRecommendation>,
    ) -> Result<RecommendationReportInfo, StorageError>;

    async fn latest_published(
        &self,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
    ) -> Result<RecommendationReportInfo, StorageError>;
}
