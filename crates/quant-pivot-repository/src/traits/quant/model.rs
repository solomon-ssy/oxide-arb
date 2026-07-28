use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{ModelRunInfo, NewModelRun},
    enums::quant::ModelRunErrorCode,
    types::{ContentHash, ModelRunId, ModelVersionId},
};

/// Model run persistence port (distinct from registry spec/version lifecycle).
///
/// The online round creates a `Running` run up front (so factor values can take
/// its foreign key) and finalizes it after inference via [`Self::succeed`],
/// [`Self::fail`], or [`Self::cancel`]. Every finalizer is a guarded transition:
/// only a `Running` run may move to a terminal state.
#[async_trait::async_trait]
pub trait ModelRunRepository: Send + Sync {
    /// Insert immutable run lineage. `PostgreSQL` assigns the initial `Running`
    /// state and lifecycle start timestamp in the insert statement.
    async fn create(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError>;

    /// Start one pre-assigned durable run or return its exact Running/Succeeded
    /// replay. Identity drift and failed/cancelled terminal reuse fail closed.
    async fn start_exact(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError>;

    /// Look up a run by id.
    async fn find_by_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<ModelRunInfo>, StorageError>;

    /// Successful live serving runs whose decision time is in `[from, until)`.
    /// Ordered by decision time and stable id for deterministic parity sampling.
    async fn list_succeeded_live_between(
        &self,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<ModelRunInfo>, StorageError>;

    /// Finalize a `Running` run as `Succeeded`, recording its output hash and
    /// a database-owned lifecycle timestamp. Rejects the transition if the run
    /// is not currently `Running`.
    ///
    /// When `model_version_id` is `Some`, sets the FK on the run row (training
    /// backfill after version registration, or explicit finalization for runs
    /// that already carried the version at create time). When `None`, the column
    /// is left unchanged.
    async fn succeed(
        &self,
        model_run_id: &ModelRunId,
        output_hash: ContentHash,
        model_version_id: Option<ModelVersionId>,
    ) -> Result<ModelRunInfo, StorageError>;

    /// Finalize success or return the exact already-succeeded terminal.
    ///
    /// This is the response-loss/restart boundary for durable jobs with a
    /// preassigned run id. A different output, subject, or terminal state is
    /// rejected rather than treated as an idempotent replay.
    async fn succeed_exact(
        &self,
        model_run_id: &ModelRunId,
        output_hash: ContentHash,
        model_version_id: Option<ModelVersionId>,
    ) -> Result<ModelRunInfo, StorageError>;

    /// Finalize a `Running` run as `Failed`, recording the error code + message.
    /// The terminal timestamp is database-owned. Rejects the transition if the
    /// run is not currently `Running`.
    async fn fail(
        &self,
        model_run_id: &ModelRunId,
        error_code: ModelRunErrorCode,
        error_message: String,
    ) -> Result<ModelRunInfo, StorageError>;

    /// Finalize a cooperatively cancelled `Running` run as `Cancelled` with a
    /// database-owned lifecycle timestamp.
    async fn cancel(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> Result<ModelRunInfo, StorageError>;
}
