use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::QuantReportListQuery,
        governance::NewOperationLog,
        pagination::Paginated,
        quant::{
            FactDeliverySettlement, NewReportTransaction, OrderIntentInfo, PreparedReportOutcome,
            PublishReportOutcome, RecommendationReportInfo, ReportDataQualitySnapshotInfo,
            ReportFactDeliveryInfo, ReportRunClaim,
        },
    },
    enums::quant::{ReportFactDeliveryStatus, ReportKind},
    types::{ModelRunId, RecommendationReportId, ResearchProfileId, WorkerId},
};

#[async_trait::async_trait]
pub trait RecommendationReportRepository: Send + Sync {
    /// Persist a complete Prepared artifact and complete its owned Running lease.
    async fn create_prepared_report(
        &self,
        run_claim: ReportRunClaim,
        transaction: NewReportTransaction,
    ) -> Result<PreparedReportOutcome, StorageError>;

    /// Load a single report by id.
    async fn find_by_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Direct report that was current immediately before this report.
    async fn find_predecessor_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<RecommendationReportId>, StorageError>;

    /// Load the report-scoped durable fact-delivery acknowledgement.
    async fn find_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError>;

    /// Lease one claimable report fact bundle using `FOR UPDATE SKIP LOCKED`.
    async fn claim_fact_delivery(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError>;

    /// Release a claimed delivery into an explicit retry or terminal failure.
    async fn fail_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
        status: ReportFactDeliveryStatus,
        error: &str,
    ) -> Result<FactDeliverySettlement<ReportFactDeliveryInfo>, StorageError>;

    /// Requeue a terminal failed delivery for the immutable Prepared artifact.
    async fn retry_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
        occurred_at: DateTime<Utc>,
    ) -> Result<ReportFactDeliveryInfo, StorageError>;

    /// Verify delivery and atomically publish or obsolete the candidate.
    async fn verify_and_publish_report(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
        occurred_at: DateTime<Utc>,
    ) -> Result<FactDeliverySettlement<PublishReportOutcome>, StorageError>;

    /// Lease one verified delivery whose committed event/notification has not
    /// yet been acknowledged.
    async fn claim_fact_announcement(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<ReportFactDeliveryInfo>, StorageError>;

    /// Acknowledge post-verification side effects under the exact lease owner.
    async fn acknowledge_fact_announcement(
        &self,
        report_id: &RecommendationReportId,
        worker_id: WorkerId,
    ) -> Result<ReportFactDeliveryInfo, StorageError>;

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

    async fn current(
        &self,
        profile_id: &ResearchProfileId,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError>;

    /// Risk-bearing reports whose decision lies in `[from, to)`, used by
    /// deterministic parity containment when row-level evidence is unavailable.
    async fn find_actionable_ids_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RecommendationReportId>, StorageError>;

    /// Ids of `Published` reports whose roll-up
    /// `valid_until` deadline (`max(recommendation.valid_until)`) is at or before
    /// `now`, oldest first, capped — the report roll-up backstop sweep input.
    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationReportId>, StorageError>;

    /// Roll a report up to `Expired` **iff** it is still `Published` and every
    /// recommendation satisfies `completes_report_rollup`. Sets `expired_at` and
    /// writes the operation log in one transaction; does **not** touch
    /// recommendation rows. Returns `None` when the report is not eligible
    /// (already closed, or a recommendation still blocks roll-up).
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
