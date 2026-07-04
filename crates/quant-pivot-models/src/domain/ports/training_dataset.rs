//! Admin port for offline training-dataset plan/build (Phase 3.5).

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        BuildTrainingDatasetRequest, JobProgressSink, TrainingDatasetInfo, TrainingDatasetPlanView,
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
    ///
    /// `progress` receives per-cross-section snapshots as the historical spine is
    /// materialized (the durable worker throttles + surfaces them). `cancel` is
    /// polled at each cross-section boundary: a cancelled build unwinds within
    /// ~one section and never persists a partial artifact (returns
    /// [`quant_pivot_error::research::ResearchError::Cancelled`]).
    async fn build(
        &self,
        request: BuildTrainingDatasetRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView>;
}
