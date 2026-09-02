//! Admin port for offline model training.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        api::{ModelTrainJobParams, TrainedModelView},
        quant::{JobProgressSink, ModelVersionInfo},
    },
    types::{ModelRunId, ModelVersionId},
};

/// Result of an explicit owner-driven terminalization of a training run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrainingRunFinalization {
    /// The run was absent, newly terminalized, or already in the requested
    /// terminal state.
    Terminalized,
    /// The atomic model/version commit won the race and must be surfaced as a
    /// successful research-job result after exact version/parity readback.
    CommitWon { model_version_id: ModelVersionId },
}

/// Dependency-inversion boundary between the HTTP layer and the core trainer.
///
/// Implemented in `quant-pivot-core` and injected into `quant_pivot_web::state::AppState`.
#[async_trait]
pub trait ModelTrainingPort: Send + Sync {
    /// Train a model from a frozen dataset and register a Candidate version.
    ///
    /// `progress` receives phase-level snapshots (`load → train → finalize`);
    /// `cancel` is polled at phase boundaries so a cancelled train unwinds
    /// promptly ([`quant_pivot_error::research::ResearchError::Cancelled`]).
    async fn train(
        &self,
        params: ModelTrainJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView>;

    /// Terminalize a still-running training run after an explicit operator
    /// cancellation. Lease loss and shutdown must never call this operation.
    async fn cancel_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<TrainingRunFinalization>;

    /// Terminalize a still-running training run after its bounded transient
    /// retries are exhausted. Scheduled retries and lease loss must not call it.
    async fn fail_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<TrainingRunFinalization>;

    /// Load a registered model version (UI poll target after train).
    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>>;
}
