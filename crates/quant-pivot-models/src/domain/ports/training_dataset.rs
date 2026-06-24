//! Admin port for offline training-dataset plan/build (Phase 3.5).

use async_trait::async_trait;

use crate::{
    domain::{
        BuildTrainingDatasetRequest, TrainingDatasetInfo, TrainingDatasetPlanView,
        TrainingDatasetView,
    },
    types::TrainingDatasetId,
};
use quant_pivot_error::QuantResult;

/// Dependency-inversion boundary between the HTTP layer and core research services.
///
/// Implemented in `quant-pivot-core` and injected into [`quant_pivot_web::AppState`].
#[async_trait]
pub trait TrainingDatasetPort: Send + Sync {
    /// Load a persisted ledger row (UI poll target after build).
    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<Option<TrainingDatasetInfo>>;

    /// Compute the deterministic sample grid without writing artifacts.
    async fn plan(
        &self,
        request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetPlanView>;

    /// Plan + materialize Parquet + persist ledger row (may take minutes).
    async fn build(&self, request: BuildTrainingDatasetRequest)
    -> QuantResult<TrainingDatasetView>;
}
