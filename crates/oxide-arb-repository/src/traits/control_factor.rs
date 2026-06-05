use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, AuditActor, CancelMaterializationRunOutcome,
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo,
        ControlFactorPublicationInfo, ControlFactorStageReportInfo, ControlFactorValueInfo,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome, ExpireFactorsOutcome,
        MaterializationRunStatusPatch, NewControlFactorAuditEvent,
        NewControlFactorMaterializationRun, NewControlFactorPublication,
        NewControlFactorStageReport, NewControlFactorValue, PublishPublicationOutcome,
        RunTransitionOutcome,
    },
    enums::control_factor::{
        ControlFactorType, FactorStatus, MaterializationRunStatus, MaterializationStageName,
        PublicationMode, PublicationStatus,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId},
};

/// Authoritative persistence for the control-factor registry and its append-only
/// governance audit chain.
///
/// All state-changing governance operations (`create_factor`, `reject_factor`,
/// `expire_factors`, `publish_publication`, `rollback_publication`) are atomic:
/// they mutate registry state and append the chained audit event(s) in a single
/// transaction. There is intentionally **no** bare `transition_factor`: every
/// factor status change flows through a governed, audited operation.
#[async_trait::async_trait]
pub trait ControlFactorRepository: Send + Sync {
    // ── Materialization runs ────────────────────────────────────────────
    async fn enqueue_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
        options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError>;

    async fn load_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError>;

    async fn find_materialization_run_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError>;

    /// Returns the most recent run associated with `schedule_id` (matched on
    /// `trigger_ref`), optionally filtered to the given statuses. An empty
    /// `statuses` slice matches any status. Ordered by `created_at DESC`.
    ///
    /// Used by the offline materialization scheduler to decide whether a
    /// scheduled cadence is due, has an active run in flight, or is overdue /
    /// stale. It never publishes; it only reads.
    async fn latest_run_for_schedule(
        &self,
        schedule_id: &str,
        statuses: &[MaterializationRunStatus],
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError>;

    async fn try_acquire_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError>;

    async fn retry_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError>;

    async fn transition_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        expected_from: MaterializationRunStatus,
        target: MaterializationRunStatus,
        patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError>;

    async fn cancel_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        reason: &str,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError>;

    // ── Stage reports ───────────────────────────────────────────────────
    async fn upsert_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError>;

    async fn load_stage_report(
        &self,
        run_id: &MaterializationRunId,
        stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError>;

    async fn list_stage_reports(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError>;

    // ── Factor values ───────────────────────────────────────────────────
    /// Persists a materialization draft factor and appends its `FactorCreated`
    /// chained audit event in one transaction.
    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
        audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorValueInfo, StorageError>;

    async fn load_factor(
        &self,
        factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError>;

    async fn list_factors_by_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError>;

    async fn list_factors_by_status(
        &self,
        status: FactorStatus,
        factor_type: Option<ControlFactorType>,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError>;

    /// Marks a factor `Rejected` with a reason and appends a chained audit event.
    async fn reject_factor(
        &self,
        factor_id: &ControlFactorId,
        status_reason: &str,
        audit: NewControlFactorAuditEvent,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError>;

    /// Expires TTL-due factors, appending one `FactorExpired` chained audit event
    /// per affected factor.
    async fn expire_factors(
        &self,
        now: DateTime<Utc>,
        actor: AuditActor,
    ) -> Result<ExpireFactorsOutcome, StorageError>;

    // ── Publications ────────────────────────────────────────────────────
    /// Idempotently creates and activates a publication in one transaction:
    /// validates members, serializes on the publication mode, supersedes the
    /// current active publication, transitions member factors, and appends the
    /// chained audit. Retries with the same `idempotency_key` return the existing
    /// publication.
    async fn publish_publication(
        &self,
        publication: NewControlFactorPublication,
        audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError>;

    async fn load_publication(
        &self,
        publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError>;

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError>;

    async fn list_publications(
        &self,
        mode: PublicationMode,
        status: Option<PublicationStatus>,
        limit: u64,
    ) -> Result<Vec<ControlFactorPublicationInfo>, StorageError>;

    /// Rolls the active publication back to a known-good target in one
    /// transaction, transitioning member factors and appending the chained audit.
    async fn rollback_publication(
        &self,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorPublicationInfo, StorageError>;

    // ── Audit chain ─────────────────────────────────────────────────────
    /// Appends an event to the global audit hash chain under an advisory lock.
    /// Idempotent on `(request_id, event_type, resource_id)`.
    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError>;

    /// Loads a contiguous slice of the audit chain ordered by ascending sequence.
    async fn load_audit_chain(
        &self,
        from_sequence: i64,
        limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError>;
}
