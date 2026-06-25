use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewReportTransaction, RecommendationReportInfo},
    enums::quant::ReportKind,
    types::RecommendationReportId,
};

#[async_trait::async_trait]
pub trait RecommendationReportRepository: Send + Sync {
    /// Persist a report atomically: account snapshot → portfolio plan → report →
    /// recommendations, in one transaction.
    async fn create_report(
        &self,
        transaction: NewReportTransaction,
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
