//! Admin ports for the unified calibration-artifact family (Phase 11.3 §3.4).
//!
//! Covers favorite-longshot bias-table fitting (Phase 11.2.1, kind =
//! `market_price_bias`) and model-score probability-calibrator fitting (kind =
//! `model_score`).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        CalibrationArtifactInfo, CalibrationArtifactListQuery, FitBiasTableRequest,
        FitModelCalibratorRequest, JobProgressSink, Paginated,
    },
    types::{CalibrationArtifactId, RuntimeConfigVersionId},
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
    pub bias_table_id: Option<CalibrationArtifactId>,
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

    /// Load a persisted calibration artifact by id (any kind).
    async fn find(
        &self,
        bias_table_id: &CalibrationArtifactId,
    ) -> QuantResult<Option<CalibrationArtifactInfo>>;

    /// Page the unified calibration-artifact catalog, newest first.
    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> QuantResult<Paginated<CalibrationArtifactInfo>>;
}

/// Frozen params for a durable `ModelCalibrationFit` research job (Phase 11.3 §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCalibrationFitJobParams {
    /// The operator's fit request (target model + calibration dataset + method).
    pub request: FitModelCalibratorRequest,
    /// Frozen runtime-config version the backtest-replay harness runs under
    /// (feature/factor recomputation + portfolio caps) — deterministic on
    /// replay, mirrors `BiasTableFitJobParams`.
    pub runtime_config_version_id: RuntimeConfigVersionId,
}

/// Terminal outcome of a model-score calibrator fit.
///
/// `artifact_id` is `None` when the fit was **fail-closed** (e.g. the isotonic
/// sample-count floor was not met and no fallback is silently substituted).
pub struct ModelCalibrationFitOutcome {
    pub artifact_id: Option<CalibrationArtifactId>,
    pub sample_count: u64,
}

/// Dependency-inversion boundary between the HTTP / job layer and the core
/// model-score probability-calibrator fitter.
#[async_trait]
pub trait ModelCalibrationFitPort: Send + Sync {
    /// Fit a `ProbabilityCalibrator` for `request.model_version_id` on the
    /// independent held-out `request.calibration_dataset_id`, persisting the
    /// artifact fail-closed (never a silent fallback across methods).
    async fn fit(
        &self,
        params: ModelCalibrationFitJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<ModelCalibrationFitOutcome>;
}
