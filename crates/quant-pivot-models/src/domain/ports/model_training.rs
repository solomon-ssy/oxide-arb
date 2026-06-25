//! Admin port for offline model training (Phase 3.6).

use async_trait::async_trait;

use crate::{
    domain::{ModelVersionInfo, TrainModelRequest, TrainedModelView},
    types::ModelVersionId,
};
use quant_pivot_error::QuantResult;

/// Dependency-inversion boundary between the HTTP layer and the core trainer.
///
/// Implemented in `quant-pivot-core` and injected into `quant_pivot_web::AppState`.
#[async_trait]
pub trait ModelTrainingPort: Send + Sync {
    /// Train a model from a frozen dataset and register a Candidate version.
    async fn train(&self, request: TrainModelRequest) -> QuantResult<TrainedModelView>;

    /// Load a registered model version (UI poll target after train).
    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>>;
}
