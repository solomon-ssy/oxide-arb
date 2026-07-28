//! Durable feedback-cycle orchestration and immutable evidence port.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        DriftReportInfo, FeedbackCycleInfo, FeedbackCycleTerminal, FeedbackEvaluationUseInfo,
        FeedbackOutboxEntry, FeedbackQueueSnapshot, FeedbackStageEventInfo, NewDriftReport,
        NewFeedbackCycle, NewFeedbackEvaluationUse, NewFeedbackStageEvent,
    },
    types::{FeedbackCycleId, WorkerId},
};

/// Outcome of an idempotent cycle-identity insert.
#[derive(Debug, Clone)]
pub enum FeedbackCycleWriteOutcome {
    Inserted(FeedbackCycleInfo),
    AlreadyPresent(FeedbackCycleInfo),
}

/// Outcome of an idempotent stage-event append.
#[derive(Debug, Clone)]
pub enum FeedbackStageWriteOutcome {
    Inserted(FeedbackStageEventInfo),
    AlreadyPresent(FeedbackStageEventInfo),
}

/// Outcome of an idempotent drift-report append.
#[derive(Debug, Clone)]
pub enum DriftReportWriteOutcome {
    Inserted(DriftReportInfo),
    AlreadyPresent(DriftReportInfo),
}

/// Outcome of an idempotent evaluation-use append.
#[derive(Debug, Clone)]
pub enum FeedbackEvaluationWriteOutcome {
    Inserted(FeedbackEvaluationUseInfo),
    AlreadyPresent(FeedbackEvaluationUseInfo),
}

/// Outcome of a generation-CAS lifecycle mutation.
#[derive(Debug, Clone)]
pub enum FeedbackCycleCasOutcome {
    Applied(FeedbackCycleInfo),
    AlreadyApplied(FeedbackCycleInfo),
}

/// Whether a claim started a queued cycle or recovered an expired lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackCycleClaimMode {
    Started,
    LeaseRecovered,
}

/// Unowned generation precondition used by governed cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackCycleGeneration {
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_generation: i64,
}

impl From<&FeedbackCycleInfo> for FeedbackCycleGeneration {
    fn from(cycle: &FeedbackCycleInfo) -> Self {
        Self {
            feedback_cycle_id: cycle.feedback_cycle_id,
            expected_generation: cycle.generation,
        }
    }
}

/// Exact lease owner and generation required for worker mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackCycleLeaseGuard {
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_generation: i64,
    pub worker_id: WorkerId,
}

impl FeedbackCycleLeaseGuard {
    #[must_use]
    pub const fn with_generation(self, expected_generation: i64) -> Self {
        Self {
            expected_generation,
            ..self
        }
    }
}

/// One atomically acquired feedback-cycle lease.
#[derive(Debug, Clone)]
pub struct FeedbackCycleClaim {
    pub cycle: FeedbackCycleInfo,
    pub mode: FeedbackCycleClaimMode,
    pub lease: FeedbackCycleLeaseGuard,
}

/// Persistence owner for cycle identity, leases, CAS, and WORM evidence.
#[async_trait::async_trait]
pub trait FeedbackCycleRepository: Send + Sync {
    /// Return the same `PostgreSQL` clock used for every lease decision.
    async fn database_time(&self) -> Result<DateTime<Utc>, StorageError>;

    /// Atomically persist one exact cycle identity and its trigger evidence.
    ///
    /// Concurrent exact retries read back and validate the winning immutable
    /// rows. A natural-key collision with different content fails closed.
    async fn record_trigger(
        &self,
        cycle: NewFeedbackCycle,
        trigger: NewFeedbackStageEvent,
    ) -> Result<(FeedbackCycleWriteOutcome, FeedbackStageWriteOutcome), StorageError>;

    async fn find_cycle(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Option<FeedbackCycleInfo>, StorageError>;

    /// Claim the oldest queued or expired-running cycle without waiting on a
    /// row another worker currently owns.
    async fn claim_cycle(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<FeedbackCycleClaim>, StorageError>;

    /// Renew one live lease under exact owner and generation CAS.
    async fn renew_cycle_lease(
        &self,
        lease: FeedbackCycleLeaseGuard,
        lease_secs: u64,
    ) -> Result<FeedbackCycleInfo, StorageError>;

    /// Make one owned running lease immediately reclaimable during shutdown.
    async fn release_cycle_lease(
        &self,
        lease: FeedbackCycleLeaseGuard,
    ) -> Result<FeedbackCycleInfo, StorageError>;

    /// Atomically record governed cancellation evidence and mutate the cycle.
    ///
    /// Queued cycles become terminal `Cancelled`; running cycles retain their
    /// lease and set `cancel_requested_at` for the next stage boundary.
    async fn request_cancel(
        &self,
        generation: FeedbackCycleGeneration,
        event: NewFeedbackStageEvent,
    ) -> Result<(FeedbackCycleCasOutcome, FeedbackStageWriteOutcome), StorageError>;

    /// Terminalize one live owned cycle under exact generation CAS.
    async fn finalize_cycle(
        &self,
        lease: FeedbackCycleLeaseGuard,
        terminal: FeedbackCycleTerminal,
    ) -> Result<FeedbackCycleCasOutcome, StorageError>;

    /// Append worker stage evidence under a live owner/generation lease.
    async fn append_stage(
        &self,
        lease: FeedbackCycleLeaseGuard,
        event: NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError>;

    /// Append one typed drift header under a live owner/generation lease.
    async fn append_drift(
        &self,
        lease: FeedbackCycleLeaseGuard,
        report: NewDriftReport,
    ) -> Result<DriftReportWriteOutcome, StorageError>;

    /// Irreversibly consume one promotion holdout under a live lease.
    async fn append_evaluation(
        &self,
        lease: FeedbackCycleLeaseGuard,
        evaluation: NewFeedbackEvaluationUse,
    ) -> Result<FeedbackEvaluationWriteOutcome, StorageError>;

    async fn list_stage_events(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<FeedbackStageEventInfo>, StorageError>;

    async fn list_drift_reports(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<DriftReportInfo>, StorageError>;

    async fn list_evaluation_uses(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<FeedbackEvaluationUseInfo>, StorageError>;

    /// Read bounded scheduler and feedback-publication backlog state.
    async fn queue_snapshot(&self) -> Result<FeedbackQueueSnapshot, StorageError>;

    /// Claim globally ordered unpublished feedback events with `SKIP LOCKED`.
    async fn claim_outbox(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<FeedbackOutboxEntry>, StorageError>;

    /// Mark one exact claimed revision published using the database clock.
    async fn publish_outbox(&self, revision: i64, worker_id: WorkerId) -> Result<(), StorageError>;

    /// Release one failed claim with a bounded diagnostic for retry.
    async fn fail_outbox(
        &self,
        revision: i64,
        worker_id: WorkerId,
        detail: String,
    ) -> Result<(), StorageError>;

    /// Replay immutable events strictly after one global revision.
    async fn list_outbox(
        &self,
        after_revision: i64,
        limit: u64,
    ) -> Result<Vec<FeedbackOutboxEntry>, StorageError>;
}
