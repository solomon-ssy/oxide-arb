//! Durable research-job ledger repository trait.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::ResearchJobListQuery,
        pagination::Paginated,
        quant::{NewResearchJob, ResearchJobFinalization, ResearchJobInfo},
    },
    enums::quant::ResearchJobKind,
    types::{ResearchJobError, ResearchJobId, ResearchJobProgress, WorkerId},
};

/// Number of running jobs of one kind (concurrency-cap accounting).
#[derive(Debug, Clone, Copy)]
pub struct KindRunningCount {
    pub kind: ResearchJobKind,
    pub running: i64,
}

/// Result of a crash-recovery sweep over orphaned `running` rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimOutcome {
    /// Orphans re-queued for another attempt.
    pub requeued: u64,
    /// Orphans quarantined to `failed` (exceeded `max_recovery_attempts`).
    pub quarantined: u64,
}

/// Exact-retry outcome when enqueueing one immutable research-job contract.
#[derive(Debug, Clone)]
pub enum ResearchJobEnqueueOutcome {
    Inserted(ResearchJobInfo),
    AlreadyPresent(ResearchJobInfo),
}

/// Persistence port for the durable async research-job ledger.
#[async_trait::async_trait]
pub trait ResearchJobRepository: Send + Sync {
    /// Insert a newly-enqueued job or return the exact existing row.
    ///
    /// A conflicting immutable contract under the same deterministic identity
    /// fails closed; a mutable status/progress change does not invalidate an
    /// exact retry of the original enqueue request.
    async fn enqueue(&self, job: NewResearchJob)
    -> Result<ResearchJobEnqueueOutcome, StorageError>;

    /// Look up a job by id.
    async fn find_by_id(
        &self,
        job_id: &ResearchJobId,
    ) -> Result<Option<ResearchJobInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: ResearchJobListQuery,
    ) -> Result<Paginated<ResearchJobInfo>, StorageError>;

    /// Count currently-`running` jobs grouped by kind (per-kind concurrency caps).
    async fn running_counts(&self) -> Result<Vec<KindRunningCount>, StorageError>;

    /// Atomically lease the oldest `queued` job whose kind is in `eligible`,
    /// transitioning it to `running` with the given lease owner/expiry.
    ///
    /// Uses `FOR UPDATE SKIP LOCKED` so concurrent workers never contend on the
    /// same row. Returns `None` when no eligible job is queued.
    async fn lease_next(
        &self,
        eligible: &[ResearchJobKind],
        owner: &WorkerId,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ResearchJobInfo>, StorageError>;

    /// Renew a running job's lease + optionally update its progress snapshot.
    ///
    /// Returns `false` when the job is no longer `running` under this owner (it
    /// was cancelled, reclaimed, or finalized) — the worker treats that as a
    /// cooperative stop signal.
    async fn heartbeat(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        lease_expires_at: DateTime<Utc>,
        progress: Option<ResearchJobProgress>,
    ) -> Result<bool, StorageError>;

    /// Move a running job to a terminal state, recording its result/error/coverage
    /// and clearing the lease.
    ///
    /// The transition is conditional on `status = running` **and**
    /// `lease_owner = owner` so a stale worker cannot overwrite a row that was
    /// reclaimed and re-leased by another epoch. Returns
    /// [`StorageError::StateConflict`] when the guard fails.
    async fn finalize(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        finalization: ResearchJobFinalization,
    ) -> Result<ResearchJobInfo, StorageError>;

    /// Cancel a still-`queued` job atomically (never touches a `running` one).
    /// Returns `true` iff a queued row was transitioned to `cancelled`.
    async fn cancel_if_queued(
        &self,
        job_id: &ResearchJobId,
        error: ResearchJobError,
    ) -> Result<bool, StorageError>;

    /// Reclaim orphaned `running` rows (lease expired or owned by a dead epoch):
    /// re-queue those under the recovery cap, quarantine the rest to `failed`.
    async fn reclaim_orphaned(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
    ) -> Result<ReclaimOutcome, StorageError>;

    /// Graceful-shutdown drain: re-queue **this owner's own** still-`running`
    /// rows so a new epoch picks them up immediately, without waiting for the
    /// lease to expire (unlike [`Self::reclaim_orphaned`], which keys on an
    /// expired / dead-epoch lease). Rows under the recovery cap are re-queued
    /// with `recovery_attempt += 1`; rows at the cap are quarantined to `failed`
    /// (a job that keeps interrupting must not re-queue forever).
    async fn requeue_inflight(&self, owner: &WorkerId) -> Result<ReclaimOutcome, StorageError>;
}
