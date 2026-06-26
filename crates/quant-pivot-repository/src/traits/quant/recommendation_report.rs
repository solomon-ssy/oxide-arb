use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        NewOperationLog, NewReportTransaction, Paginated, QuantReportListQuery,
        RecommendationReportInfo,
    },
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

    /// Load a single report by id.
    async fn find_by_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Paginated, filtered listing ordered by `published_at` then `created_at`
    /// (most recent first).
    async fn page(
        &self,
        query: QuantReportListQuery,
    ) -> Result<Paginated<RecommendationReportInfo>, StorageError>;

    async fn latest_published(
        &self,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    async fn find_by_trigger_key(
        &self,
        trigger_key: &str,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Ids of reports eligible for TTL expiry: `published` / `published_empty`
    /// whose `published_at` is at or before `published_before`, oldest first,
    /// capped at `limit`.
    async fn find_expirable(
        &self,
        published_before: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError>;

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<RecommendationReportInfo, StorageError>;

    async fn expire(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<RecommendationReportInfo, StorageError>;
}
