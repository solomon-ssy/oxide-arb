//! Durable research-job ledger repository trait.

use std::time::Duration;

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

/// Result of atomically handling one typed transient execution failure.
#[derive(Debug, Clone)]
pub enum ResearchJobRetryOutcome {
    /// The same immutable job identity is waiting for its DB-clock deadline.
    Scheduled(ResearchJobInfo),
    /// The governed automatic retry cap was reached and the job was failed.
    Exhausted(ResearchJobInfo),
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

    /// Load the requested immutable jobs in canonical identity order.
    async fn find_by_ids(
        &self,
        job_ids: &[ResearchJobId],
    ) -> Result<Vec<ResearchJobInfo>, StorageError>;

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

    /// Persist a normal wait for external evidence and release the current
    /// lease without consuming crash-recovery or transient-retry budget.
    ///
    /// The repository derives `next_attempt_at` from the database clock and
    /// accepts the transition only from the current `running` owner.
    async fn await_evidence(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        progress: ResearchJobProgress,
        retry_after: Duration,
    ) -> Result<ResearchJobInfo, StorageError>;

    /// Cancel a job that does not currently hold a lease atomically (never
    /// touches a `running` one). Returns `true` iff a pending row was
    /// transitioned to `cancelled`.
    async fn cancel_if_pending(
        &self,
        job_id: &ResearchJobId,
        error: ResearchJobError,
    ) -> Result<bool, StorageError>;

    /// Atomically schedule a typed transient execution retry or fail the job
    /// when its automatic recovery cap is exhausted. The repository derives
    /// `next_attempt_at` from the database clock and applies the running-owner
    /// compare-and-set, so process clock skew and stale workers cannot reopen a
    /// job.
    async fn retry_transient(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        detail: String,
        retry_after: Duration,
    ) -> Result<ResearchJobRetryOutcome, StorageError>;

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
