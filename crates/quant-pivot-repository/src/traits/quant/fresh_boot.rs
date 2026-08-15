use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AdvanceFreshBootRun, BlockFreshBootRun, DelayFreshBootRun, FreshBootRunEventInfo,
        FreshBootRunInfo, NewFreshBootRun, SupersedeFreshBootRun,
    },
    types::{FreshBootRunId, PolicyIdempotencyKey, WorkerId},
};

/// Durable compare-and-swap owner of fresh-boot orchestration state.
#[async_trait::async_trait]
pub trait FreshBootRepository: Send + Sync {
    async fn create_or_load(&self, run: NewFreshBootRun) -> Result<FreshBootRunInfo, StorageError>;

    async fn find(&self, run_id: &FreshBootRunId)
    -> Result<Option<FreshBootRunInfo>, StorageError>;

    async fn find_by_key(
        &self,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> Result<Option<FreshBootRunInfo>, StorageError>;

    async fn list_latest(&self) -> Result<Vec<FreshBootRunInfo>, StorageError>;

    async fn list_events(
        &self,
        run_id: FreshBootRunId,
    ) -> Result<Vec<FreshBootRunEventInfo>, StorageError>;

    async fn claim_due(
        &self,
        worker_id: WorkerId,
        claimed_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<FreshBootRunInfo>, StorageError>;

    async fn advance(&self, command: AdvanceFreshBootRun)
    -> Result<FreshBootRunInfo, StorageError>;

    async fn delay(&self, command: DelayFreshBootRun) -> Result<FreshBootRunInfo, StorageError>;

    async fn block_terminal(
        &self,
        command: BlockFreshBootRun,
    ) -> Result<FreshBootRunInfo, StorageError>;

    async fn retry_now(
        &self,
        run_id: FreshBootRunId,
        expected_revision: i64,
        actor: String,
        reason: String,
        occurred_at: DateTime<Utc>,
    ) -> Result<FreshBootRunInfo, StorageError>;

    async fn supersede(
        &self,
        command: SupersedeFreshBootRun,
        replacement: NewFreshBootRun,
    ) -> Result<FreshBootRunInfo, StorageError>;
}
