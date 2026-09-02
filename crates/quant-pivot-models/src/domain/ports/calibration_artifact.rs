//! Admin ports for the unified calibration-artifact family.
//!
//! Covers favorite-longshot bias-table fitting (kind = `market_price_bias`)
//! and model-score probability-calibrator fitting (kind =
//! `model_score`).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        api::{
            CalibrationArtifactListQuery, FitBiasTableRequest, FitModelCalibratorRequest,
            ModelCalibrationFitPreflightView,
        },
        pagination::Paginated,
        ports::GovernanceActor,
        quant::{CalibrationArtifactInfo, JobProgressSink},
    },
    enums::quant::DownsideSource,
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        TrainingDatasetId,
    },
};

/// Frozen params for a durable `BiasTableFit` research job.
///
/// The runtime-config version is frozen at enqueue so the fit reads the exact
/// `factors.structural.favorite_longshot` parameters (bins, gates, lead) that
/// were active when the operator requested it — deterministic on replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BiasTableFitJobParams {
    /// The operator's fit request (window + reason).
    pub request: FitBiasTableRequest,
    /// Frozen runtime-config version governing the fit parameters.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

/// Terminal outcome of a bias-table fit.
///
/// `artifact_id` is `None` when the fit was **fail-closed**: no category
/// cleared its sample gate, so no artifact was minted (the job still succeeds).
pub struct BiasTableFitOutcome {
    /// The persisted artifact id, or `None` when the fit produced no table.
    pub artifact_id: Option<CalibrationArtifactId>,
    /// Number of qualifying categories in the fitted table (0 when none).
    pub category_count: u64,
    /// Total samples the fit drew from the settlement spine.
    pub total_sample_count: u64,
}

/// Dependency-inversion boundary between the HTTP / job layer and the unified
/// calibration-artifact ledger.
///
/// Combines the favorite-longshot bias-table fitter *plus* the generic
/// catalog reads/activation shared by every artifact kind (`model_score` and
/// `market_price_bias` alike) — the web/job layer's only calibration-artifact
/// dependency, so it is scoped to the whole family, not just the
/// favorite-longshot fit.
#[async_trait]
pub trait CalibrationArtifactFitPort: Send + Sync {
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
        artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<Option<CalibrationArtifactInfo>>;

    /// Page the unified calibration-artifact catalog, newest first.
    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> QuantResult<Paginated<CalibrationArtifactInfo>>;

    /// Mark an artifact `active` (any kind); `market_price_bias` is
    /// single-active (deactivates every other bias table), `model_score` has
    /// no cross-model exclusivity (see the repository-layer `mark_active`).
    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<CalibrationArtifactInfo>;
}

/// Frozen params for a durable `ModelCalibrationFit` research job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCalibrationFitJobParams {
    /// Pre-assigned run id frozen in the durable outer job for exact lease recovery.
    pub model_run_id: ModelRunId,
    /// The operator's fit request (target model + calibration dataset + method).
    pub request: FitModelCalibratorRequest,
    /// Frozen runtime-config version the backtest-replay harness runs under
    /// (feature/factor recomputation + portfolio caps) — deterministic on
    /// replay, mirrors `BiasTableFitJobParams`.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Immutable downside semantics for the derived calibrated model.
    pub downside_source: DownsideSource,
    /// Exact initiating actor frozen into the durable job and derivation audit.
    pub actor: GovernanceActor,
}

/// Terminal outcome of a model-score calibrator fit.
///
/// An underpowered split is a successful, reproducible computation with no
/// calibrator artifact. It carries its own exact output commitment so durable
/// retries cannot confuse “insufficient evidence” with an execution failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelCalibrationFitOutcome {
    Calibrated {
        artifact_id: CalibrationArtifactId,
        sample_count: u64,
    },
    Insufficient {
        sample_count: u64,
        total_sample_count: u64,
        minimum_sample_count: u64,
        outcome_hash: ContentHash,
    },
}

/// Result of explicitly terminalizing a calibration run after operator
/// cancellation or exhausted retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationRunFinalization {
    /// The run was absent, newly terminalized, or already in the requested state.
    Terminalized,
    /// The atomic calibration commit won and exact outcome recovery must finish.
    CommitWon,
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

    async fn cancel_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<CalibrationRunFinalization>;

    async fn fail_run(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> QuantResult<CalibrationRunFinalization>;

    /// Read-only disjoint + embargo preflight check — the
    /// same purge/embargo primitive `fit` enforces fail-closed, without
    /// enqueueing a job or mutating any state.
    async fn preflight(
        &self,
        model_version_id: &ModelVersionId,
        calibration_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<ModelCalibrationFitPreflightView>;
}
