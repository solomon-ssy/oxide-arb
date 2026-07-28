//! Admin port for CPCV + governed trial-grid validation.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        api::{BacktestPathSetView, CpcvBacktestJobParams},
        quant::JobProgressSink,
    },
    types::{BacktestPathSetId, ModelVersionId},
};

/// Dependency-inversion boundary between the HTTP layer and the core CPCV
/// orchestrator ([`crate::runtime_config`] is frozen at request time).
///
/// Implemented in `quant-pivot-core` and injected into `quant_pivot_web::state::AppState`.
#[async_trait]
pub trait CpcvBacktestPort: Send + Sync {
    /// Run Combinatorial Purged Cross-Validation + the governed trial grid
    /// for `model_version_id` and persist a report.
    async fn run(
        &self,
        params: CpcvBacktestJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView>;

    /// Load a persisted CPCV path-set result.
    async fn find_path_set(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> QuantResult<Option<BacktestPathSetView>>;

    /// Load the most recent path set for a model version (UI history /
    /// catalog). Publish gates read `ModelVersion.publish_path_set_id`, not
    /// this latest-by-`created_at` helper.
    async fn latest_path_set(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<BacktestPathSetView>>;
}
