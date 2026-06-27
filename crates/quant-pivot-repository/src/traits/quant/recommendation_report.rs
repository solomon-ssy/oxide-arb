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

    /// Ids of `published` / `published_empty` reports whose roll-up
    /// `valid_until` deadline (`max(recommendation.valid_until)`) is at or before
    /// `now`, oldest first, capped — the report roll-up backstop sweep input.
    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError>;

    /// Roll a report up to `Expired` **iff** it is still `published` /
    /// `published_empty` and every one of its recommendations is terminal
    /// (`is_terminal`). Sets `expired_at` and writes the operation log in one
    /// transaction; does **not** touch recommendation rows (each already reached
    /// its own terminal state). Returns `None` when the report is not eligible
    /// (already terminal, or a recommendation is still actionable).
    async fn roll_up_to_expired(
        &self,
        report_id: &RecommendationReportId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Operator revoke of a whole report: report -> `Revoked` and every
    /// **non-terminal** recommendation -> `Revoked` (terminal recommendations are
    /// left intact), in one transaction.
    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<RecommendationReportInfo, StorageError>;
}
