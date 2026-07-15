//! Admin port for offline training-dataset plan/build (Phase 3.5).

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        BuildTrainingDatasetRequest, JobProgressSink, TrainingDatasetInfo, TrainingDatasetPlanView,
        TrainingDatasetView,
    },
    types::{ContentHash, ResearchEvaluationTrack, TrainingDatasetId},
};
use quant_pivot_error::QuantResult;

/// Server-frozen input for the internal `PolicyFit` Dataset build.
///
/// The HTTP Dataset API can never construct this authority: the trade-policy
/// workflow supplies the exact preflight program hash and evaluation track so
/// Source Slice identity cannot drift between preflight and materialization.
#[derive(Debug, Clone)]
pub struct PolicyFitDatasetBuildRequest {
    pub dataset: BuildTrainingDatasetRequest,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
}

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

    /// Materialize the Dataset owned by one trade-policy fit.
    ///
    /// This is intentionally absent from the HTTP route surface. Implementers
    /// must preserve the frozen program/track supplied by preflight and must
    /// reject `PolicyFit` through the generic [`Self::plan`] / [`Self::build`]
    /// methods.
    async fn build_policy_fit(
        &self,
        request: PolicyFitDatasetBuildRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView>;
}
