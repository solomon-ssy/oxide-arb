use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelRunInfo, NewModelRun},
    enums::quant::ModelRunErrorCode,
    types::{ContentHash, ModelRunId, ModelVersionId},
};

/// Model run persistence port (distinct from registry spec/version lifecycle).
///
/// The online round creates a `Running` run up front (so factor values can take
/// its foreign key) and finalizes it after inference via [`Self::succeed`] /
/// [`Self::fail`]. Both finalizers are guarded transitions: only a `Running` run
/// may move to a terminal state.
#[async_trait::async_trait]
pub trait ModelRunRepository: Send + Sync {
    /// Insert a freshly-minted run (status `Running`).
    async fn create(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError>;

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
    /// metrics. Rejects the transition if the run is not currently `Running`.
    ///
    /// When `model_version_id` is `Some`, sets the FK on the run row (training
    /// backfill after version registration, or explicit finalization for runs
    /// that already carried the version at create time). When `None`, the column
    /// is left unchanged.
    async fn succeed(
        &self,
        model_run_id: &ModelRunId,
        output_hash: ContentHash,
        finished_at: DateTime<Utc>,
        model_version_id: Option<ModelVersionId>,
    ) -> Result<ModelRunInfo, StorageError>;

    /// Finalize a `Running` run as `Failed`, recording the error code + message.
    /// Rejects the transition if the run is not currently `Running`.
    async fn fail(
        &self,
        model_run_id: &ModelRunId,
        error_code: ModelRunErrorCode,
        error_message: String,
        finished_at: DateTime<Utc>,
    ) -> Result<ModelRunInfo, StorageError>;
}
