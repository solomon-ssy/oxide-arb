use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        FeedbackSchedulerClaim, FeedbackSchedulerControl, FeedbackSchedulerLease,
        FeedbackSchedulerRetry, FeedbackSchedulerStateInfo, FeedbackSchedulerSuccess,
        NewFeedbackSchedulerState,
    },
    types::{ResearchProfileId, WorkerId},
};

/// PostgreSQL-authoritative feedback scheduler state and lease operations.
#[async_trait::async_trait]
pub trait FeedbackSchedulerRepository: Send + Sync {
    /// Reconcile one governed profile version into durable scheduler state.
    async fn sync_state(
        &self,
        state: NewFeedbackSchedulerState,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError>;

    async fn find_state(
        &self,
        research_profile_id: &ResearchProfileId,
    ) -> Result<Option<FeedbackSchedulerStateInfo>, StorageError>;

    async fn list_states(&self) -> Result<Vec<FeedbackSchedulerStateInfo>, StorageError>;

    /// Claim the oldest eligible profile using the `PostgreSQL` statement clock.
    async fn claim_due(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<FeedbackSchedulerClaim>, StorageError>;

    async fn renew_lease(
        &self,
        lease: FeedbackSchedulerLease,
        lease_secs: u64,
    ) -> Result<FeedbackSchedulerLease, StorageError>;

    async fn settle_success(
        &self,
        lease: FeedbackSchedulerLease,
        success: FeedbackSchedulerSuccess,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError>;

    async fn settle_retry(
        &self,
        lease: FeedbackSchedulerLease,
        retry: FeedbackSchedulerRetry,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError>;

    /// Apply an operator pause/resume mutation with pause-revision CAS.
    async fn apply_control(
        &self,
        control: FeedbackSchedulerControl,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError>;
}
