use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        NewOperationLog, NewReportTransaction, OrderIntentInfo, Paginated, QuantReportListQuery,
        RecommendationReportInfo, ReportDataQualitySnapshotInfo,
    },
    enums::quant::ReportKind,
    types::{ModelRunId, RecommendationReportId},
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

    /// Report produced by an exact serving run. The schema enforces at most one
    /// report per non-null run id.
    async fn find_by_model_run_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Every committed report whose decision lies in `[from, to)`, including
    /// reports later revoked or expired. Runtime full parity audits what was
    /// served, not only what remains actionable.
    async fn list_committed_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportInfo>, StorageError>;

    /// Exact DQ snapshot bound by a report header. Its token rows freeze the
    /// immutable feature-vector ids for report-scoped pre-inference replay.
    async fn find_data_quality_snapshot(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportDataQualitySnapshotInfo>, StorageError>;

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

    /// Risk-bearing reports whose decision lies in `[from, to)`, used by
    /// deterministic parity containment when row-level evidence is unavailable.
    async fn find_actionable_ids_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportId>, StorageError>;

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
    ) -> Result<(RecommendationReportInfo, Vec<OrderIntentInfo>), StorageError>;
}
