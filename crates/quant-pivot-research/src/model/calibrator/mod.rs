//! `ProbabilityCalibrator`: maps raw model scores to calibrated win probabilities.
//!
//! Fit **only** on an independent held-out calibration split (Phase 11.3 §3.2).
//!
//! Two methods, matching production guidance (scikit-learn `CalibratedClassifierCV`):
//! [`isotonic::IsotonicCalibrator`] (non-parametric monotone regression;
//! preferred with `>= min_samples_isotonic` samples) and
//! [`platt::PlattCalibrator`] (two-parameter sigmoid; data-efficient for small
//! samples or near-sigmoid miscalibration). Both live outside any optional
//! feature gate: calibration is a fail-closed, money-critical production path
//! and must never depend on an opt-in build feature (e.g. `optimize`/`argmin`).

pub mod isotonic;
pub mod platt;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::CalibrationMethod,
    types::{CalibrationArtifactId, Price, Probability},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    model::{artifact::ReturnEstimate, reliability::ReliabilityReport},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// The fitted monotone mapping a `ProbabilityCalibrator` produces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "method")]
pub enum MonotoneMapping {
    /// Isotonic step function: ascending `(score, probability)` knots.
    Isotonic { knots: Vec<IsotonicKnot> },
    /// Platt sigmoid: `P(win) = 1 / (1 + exp(a * score + b))`.
    Platt { a: Decimal, b: Decimal },
}

/// One isotonic-regression knot (score → calibrated probability, non-decreasing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IsotonicKnot {
    pub score: Decimal,
    pub probability: Decimal,
}

/// Maps raw model scores to empirically calibrated win probabilities.
///
/// Implementations are pure (no I/O); the caller is responsible for sourcing
/// `scores`/`outcomes` from an independent held-out calibration split (Phase
/// 11.3 §4) — this trait has no opinion on data provenance.
pub trait ProbabilityCalibrator: Send + Sync {
    /// The method this calibrator implements.
    fn method(&self) -> CalibrationMethod;

    /// Fit the monotone mapping from paired `(score, outcome)` observations.
    ///
    /// # Errors
    ///
    /// Returns an error when the sample count is insufficient for the method
    /// (fail-closed — never silently falls back to a different method).
    fn fit(&self, scores: &[Decimal], outcomes: &[bool]) -> QuantResult<MonotoneMapping>;

    /// Map one raw score to a calibrated win probability, clamped to `[0, 1]`.
    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> Probability;
}

/// Apply a fitted [`MonotoneMapping`] to one score (shared by every caller —
/// runtime scoring, reliability recomputation, and tests — so "how a mapping
/// is applied" has exactly one implementation).
#[must_use]
pub fn apply_mapping(mapping: &MonotoneMapping, score: Decimal) -> Probability {
    let raw = match mapping {
        MonotoneMapping::Isotonic { knots } => isotonic::interpolate(knots, score),
        MonotoneMapping::Platt { a, b } => platt::sigmoid(*a, *b, score),
    };
    Probability::new(raw.clamp(Decimal::ZERO, Decimal::ONE))
}

/// A `model_score` calibration artifact resolved for runtime scoring.
///
/// Carries the fitted [`MonotoneMapping`] plus its [`ReliabilityReport`] (the
/// `DownsideSource::MfeMae` per-score-bucket lookup). Bound once at model-runtime
/// load time (mirrors [`crate::model::overlay::WeightOverlay`]) — never
/// re-fetched per candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCalibration {
    pub artifact_id: CalibrationArtifactId,
    pub mapping: MonotoneMapping,
    pub reliability: ReliabilityReport,
}

impl ResolvedCalibration {
    /// Calibrated `P(win)` for one candidate's composite score.
    #[must_use]
    pub fn calibrate(&self, composite_score: Decimal) -> Probability {
        apply_mapping(&self.mapping, composite_score)
    }

    /// Derive the expected return / downside (bps) per Phase 11.3 §3.3:
    /// `E[r] = P(win)·(1-p)/p − (1−P(win))`, `downside` = the calibration
    /// split's mean `max_adverse_excursion_bps` in the candidate's score
    /// bucket. An invalid market price (outside `(0, 1)`) or a score bucket
    /// with no MAE evidence yields a zero estimate — never fabricated —
    /// which downstream Kelly sizing rejects via `InvalidEdgeInputs`.
    #[must_use]
    pub fn estimate_return(&self, composite_score: Decimal, market_price: Price) -> ReturnEstimate {
        let p = market_price.inner();
        if p <= Decimal::ZERO || p >= Decimal::ONE {
            return ReturnEstimate {
                expected_return_bps: Decimal::ZERO,
                downside_bps: Decimal::ZERO,
                calibrated: true,
            };
        }
        let p_win = self.calibrate(composite_score).inner();
        let bps = Decimal::from(10_000);
        let expected_return_bps = ((p_win * (Decimal::ONE - p) / p - (Decimal::ONE - p_win)) * bps)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        let downside_bps = self
            .reliability
            .bin_for(composite_score)
            .and_then(|bin| bin.mean_adverse_excursion_bps)
            .map_or(Decimal::ZERO, |mae| {
                mae.abs().round_dp(RESEARCH_DECIMAL_SCALE)
            });
        ReturnEstimate {
            expected_return_bps,
            downside_bps,
            calibrated: true,
        }
    }
}

/// Dependency-inversion boundary for resolving a `model_score`
/// [`CalibrationArtifactId`] into its compute-domain [`ResolvedCalibration`].
///
/// Defined here (not `quant-pivot-repository`) so `quant-pivot-research`
/// never depends on the persistence crate — mirrors [`crate::artifact::ArtifactStore`].
/// `quant-pivot-core` implements this over its `CalibrationArtifactRepository`.
#[async_trait]
pub trait CalibrationArtifactLoader: Send + Sync {
    /// Load and validate (kind = `ModelScore`) the calibration artifact.
    ///
    /// # Errors
    ///
    /// Fails closed when the artifact is absent, is the wrong `kind`, or its
    /// payload cannot be deserialized — a `Calibrated` return model must never
    /// silently fall back to an unresolved / stale calibrator.
    async fn load(&self, artifact_id: &CalibrationArtifactId) -> QuantResult<ResolvedCalibration>;
}
