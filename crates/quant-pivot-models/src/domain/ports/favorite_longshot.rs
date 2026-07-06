//! Admin port for favorite-longshot bias-table fitting + read (Phase 11.2.1).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        BiasTableListQuery, FavoriteLongshotBiasTableInfo, FitBiasTableRequest, JobProgressSink,
        Paginated,
    },
    types::{FavoriteLongshotBiasTableId, RuntimeConfigVersionId},
};
use quant_pivot_error::QuantResult;

/// Frozen params for a durable `BiasTableFit` research job.
///
/// The runtime-config version is frozen at enqueue so the fit reads the exact
/// `factors.structural.favorite_longshot` parameters (bins, gates, lead) that
/// were active when the operator requested it — deterministic on replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiasTableFitJobParams {
    /// The operator's fit request (window + reason).
    pub request: FitBiasTableRequest,
    /// Frozen runtime-config version governing the fit parameters.
    pub runtime_config_version_id: RuntimeConfigVersionId,
}

/// Terminal outcome of a bias-table fit.
///
/// `bias_table_id` is `None` when the fit was **fail-closed**: no category
/// cleared its sample gate, so no artifact was minted (the job still succeeds).
pub struct BiasTableFitOutcome {
    /// The persisted artifact id, or `None` when the fit produced no table.
    pub bias_table_id: Option<FavoriteLongshotBiasTableId>,
    /// Number of qualifying categories in the fitted table (0 when none).
    pub category_count: u64,
    /// Total samples the fit drew from the settlement spine.
    pub total_sample_count: u64,
}

/// Dependency-inversion boundary between the HTTP / job layer and the core
/// favorite-longshot bias-table fitter.
#[async_trait]
pub trait FavoriteLongshotFitPort: Send + Sync {
    /// Fit a bias table over the request window, persisting it when any category
    /// qualifies (fail-closed otherwise).
    async fn fit(
        &self,
        params: BiasTableFitJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BiasTableFitOutcome>;

    /// Load a persisted bias table by id.
    async fn find(
        &self,
        bias_table_id: &FavoriteLongshotBiasTableId,
    ) -> QuantResult<Option<FavoriteLongshotBiasTableInfo>>;

    /// Page the bias-table catalog, newest first.
    async fn page(
        &self,
        query: BiasTableListQuery,
    ) -> QuantResult<Paginated<FavoriteLongshotBiasTableInfo>>;
}
