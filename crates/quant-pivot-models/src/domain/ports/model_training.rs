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
    types::ModelVersionId,
};

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

    /// Load a registered model version (UI poll target after train).
    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>>;
}
