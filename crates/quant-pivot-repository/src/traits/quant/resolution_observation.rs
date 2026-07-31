//! Durable resolution observation inbox and projection queue.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        data_plane::UpsertDomainSourceCursor,
        quant::{
            NewResolutionObservationInbox, ResolutionObservationProjectionInfo,
            ResolutionProjectionBarrier, ResolutionProjectionClaim, ResolutionProjectionSettlement,
            ResolutionScanCommitOutcome,
        },
    },
    types::{ContentHash, ResolutionObservationId, WorkerId},
};

/// `PostgreSQL` canonical intake and lease boundary for resolution truth.
#[async_trait::async_trait]
pub trait ResolutionObservationRepository: Send + Sync {
    /// Atomically persist every page observation, create projection work, and advance the cursor.
    async fn commit_scan(
        &self,
        expected_cursor_hash: ContentHash,
        cursor: UpsertDomainSourceCursor,
        observations: Vec<NewResolutionObservationInbox>,
    ) -> Result<ResolutionScanCommitOutcome, StorageError>;

    /// Claim due projection work using database time and skip-locked leases.
    async fn claim_pending(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ResolutionProjectionClaim>, StorageError>;

    /// Settle an owned projection attempt using a typed lifecycle outcome.
    async fn settle(
        &self,
        observation_id: ResolutionObservationId,
        worker_id: WorkerId,
        settlement: ResolutionProjectionSettlement,
    ) -> Result<ResolutionObservationProjectionInfo, StorageError>;

    /// Resolve an inbox identity for recovery and deterministic replay.
    async fn find_by_checkpoint(
        &self,
        checkpoint_hash: ContentHash,
    ) -> Result<Option<ResolutionProjectionClaim>, StorageError>;

    /// Compute point-in-time canonical coverage for `TruthFreeze`.
    async fn barrier(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<ResolutionProjectionBarrier, StorageError>;
}
