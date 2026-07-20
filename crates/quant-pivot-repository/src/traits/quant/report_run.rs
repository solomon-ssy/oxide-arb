use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        EnqueueReportRunOutcome, MaterializeReportSchedule, MaterializeReportScheduleOutcome,
        NewReportRun, Paginated, ReconcileReportSchedule, ReconcileReportSchedulesOutcome,
        ReportRunClaimConfig, ReportRunInfo, ReportRunListQuery, ReportScheduleGapInfo,
        ReportScheduleGapListQuery, ReportScheduleHealthInfo, ReportScheduleStateInfo,
    },
    enums::quant::ReportRunTerminalReason,
    types::{
        DecisionPolicySnapshotId, RecommendationReportId, ReportRunId, ReportTriggerKey, WorkerId,
    },
};

/// Durable report build queue and lease ledger.
#[async_trait::async_trait]
pub trait ReportRunRepository: Send + Sync {
    /// Return the `PostgreSQL` clock used by scheduling and lease decisions.
    async fn database_time(&self) -> Result<DateTime<Utc>, StorageError>;

    /// Reconcile the complete active schedule set and invalidate changed queued specs.
    async fn reconcile_schedules(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
        schedules: Vec<ReconcileReportSchedule>,
    ) -> Result<ReconcileReportSchedulesOutcome, StorageError>;

    /// Load every derived schedule cursor for coordinator and health projection.
    async fn list_schedule_states(&self) -> Result<Vec<ReportScheduleStateInfo>, StorageError>;

    /// Atomically advance one due cursor, coalesce an older queued occurrence, and enqueue latest.
    async fn materialize_schedule(
        &self,
        command: MaterializeReportSchedule,
    ) -> Result<MaterializeReportScheduleOutcome, StorageError>;

    /// Read append-only schedule gaps in reverse detection order.
    async fn page_schedule_gaps(
        &self,
        query: ReportScheduleGapListQuery,
    ) -> Result<Paginated<ReportScheduleGapInfo>, StorageError>;

    /// Build one database-backed scheduler health snapshot.
    async fn schedule_health(&self) -> Result<ReportScheduleHealthInfo, StorageError>;

    /// Idempotently enqueue an ad-hoc run after expiring stale queued requests.
    async fn enqueue_ad_hoc(
        &self,
        run: NewReportRun,
        capacity: u64,
        ttl_secs: u64,
    ) -> Result<EnqueueReportRunOutcome, StorageError>;

    async fn find_by_id(&self, run_id: &ReportRunId)
    -> Result<Option<ReportRunInfo>, StorageError>;

    async fn find_by_trigger_key(
        &self,
        trigger_key: &ReportTriggerKey,
    ) -> Result<Option<ReportRunInfo>, StorageError>;

    async fn find_by_output_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportRunInfo>, StorageError>;

    async fn page(
        &self,
        query: ReportRunListQuery,
    ) -> Result<Paginated<ReportRunInfo>, StorageError>;

    /// Claim the oldest queued run while freezing decision time and config.
    async fn claim_next_run(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        ad_hoc_ttl_secs: u64,
        config: ReportRunClaimConfig,
    ) -> Result<Option<ReportRunInfo>, StorageError>;

    /// Extend a live lease under exact owner/status CAS.
    async fn heartbeat_run(
        &self,
        run_id: &ReportRunId,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<ReportRunInfo, StorageError>;

    /// Terminalize one owned running build before it creates an artifact.
    async fn fail_run(
        &self,
        run_id: &ReportRunId,
        worker_id: WorkerId,
        error_code: &str,
        error_summary: &str,
    ) -> Result<ReportRunInfo, StorageError>;

    /// Recover every expired Running lease as Abandoned.
    async fn abandon_expired_runs(&self) -> Result<Vec<ReportRunInfo>, StorageError>;

    /// Skip a queued run under an explicit durable reason.
    async fn skip_queued_run(
        &self,
        run_id: &ReportRunId,
        reason: ReportRunTerminalReason,
        occurred_at: DateTime<Utc>,
    ) -> Result<ReportRunInfo, StorageError>;
}
