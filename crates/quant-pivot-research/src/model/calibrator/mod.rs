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
use quant_pivot_error::{QuantResult, research::ResearchError};
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
    ///
    /// # Errors
    ///
    /// Returns a typed inference error when the fitted mapping cannot be
    /// represented by the runtime numeric boundary.
    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> QuantResult<Probability>;
}

/// Apply a fitted [`MonotoneMapping`] to one score (shared by every caller —
/// runtime scoring, reliability recomputation, and tests — so "how a mapping
/// is applied" has exactly one implementation).
pub fn apply_mapping(mapping: &MonotoneMapping, score: Decimal) -> QuantResult<Probability> {
    validate_mapping(mapping)?;
    let raw = match mapping {
        MonotoneMapping::Isotonic { knots } => isotonic::interpolate(knots, score)?,
        MonotoneMapping::Platt { a, b } => platt::sigmoid(*a, *b, score)?,
    };
    Ok(Probability::new(raw.clamp(Decimal::ZERO, Decimal::ONE)))
}

/// Validate the frozen transform graph of a probability calibrator.
///
/// # Errors
///
/// Rejects empty, unordered, duplicate, out-of-range, or non-monotone
/// isotonic knots. Both artifact loading and inference call this boundary so a
/// malformed artifact cannot silently emit a plausible probability.
pub fn validate_mapping(mapping: &MonotoneMapping) -> QuantResult<()> {
    let MonotoneMapping::Isotonic { knots } = mapping else {
        return Ok(());
    };
    if knots.is_empty() {
        return Err(ResearchError::Inference {
            detail: "isotonic calibration mapping has no fitted knots".to_owned(),
        }
        .into());
    }
    for knot in knots {
        if knot.probability < Decimal::ZERO || knot.probability > Decimal::ONE {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic calibration probability {} at score {} is outside [0, 1]",
                    knot.probability, knot.score
                ),
            }
            .into());
        }
    }
    for pair in knots.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        if left.score >= right.score {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic calibration scores must be strictly increasing, got {} then {}",
                    left.score, right.score
                ),
            }
            .into());
        }
        if left.probability > right.probability {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic calibration probabilities must be non-decreasing, got {} then {}",
                    left.probability, right.probability
                ),
            }
            .into());
        }
    }
    Ok(())
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
    pub fn calibrate(&self, composite_score: Decimal) -> QuantResult<Probability> {
        apply_mapping(&self.mapping, composite_score)
    }

    /// Derive the expected return / downside (bps) per Phase 11.3 §3.3:
    /// `E[r] = P(win)·(1-p)/p − (1−P(win))`, `downside` = the calibration
    /// split's mean `max_adverse_excursion_bps` in the candidate's
    /// **calibrated-probability** bucket (matching the reliability report's
    /// ECE bucketing — never the raw pre-calibration score bucket). An
    /// invalid market price (outside `(0, 1)`) or a probability bucket with
    /// no MAE evidence rejects inference. Neither condition is a business zero.
    ///
    /// `win_probability` on the returned [`ReturnEstimate`] is the *same*
    /// `P(win)` used to derive `expected_return_bps` — Kelly sizing (Phase
    /// 11.3 §4 redesign) consumes it directly as `q`, so the sizing decision
    /// and the return estimate always share one probability, never two
    /// independently-derived numbers.
    pub fn estimate_return(
        &self,
        composite_score: Decimal,
        market_price: Price,
    ) -> QuantResult<ReturnEstimate> {
        let p = market_price.inner();
        if p <= Decimal::ZERO || p >= Decimal::ONE {
            return Err(ResearchError::Inference {
                detail: format!(
                    "calibrated return requires an executable market price in (0, 1), got {p}"
                ),
            }
            .into());
        }
        let p_win = self.calibrate(composite_score)?;
        let p_win_inner = p_win.inner();
        let bps = Decimal::from(10_000);
        let expected_return_bps =
            ((p_win_inner * (Decimal::ONE - p) / p - (Decimal::ONE - p_win_inner)) * bps)
                .round_dp(RESEARCH_DECIMAL_SCALE);
        let downside_bps = self
            .reliability
            .bin_for(p_win_inner)
            .and_then(|bin| bin.mean_adverse_excursion_bps)
            .ok_or_else(|| ResearchError::Inference {
                detail: format!(
                    "calibrated probability {p_win_inner} has no frozen MAE downside evidence"
                ),
            })?
            .abs()
            .round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(ReturnEstimate {
            expected_return_bps,
            downside_bps,
            calibrated: true,
            win_probability: Some(p_win),
        })
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

#[cfg(test)]
mod tests {
    use super::{IsotonicKnot, MonotoneMapping, ResolvedCalibration};
    use crate::model::reliability::{ReliabilityBin, ReliabilityReport};
    use quant_pivot_models::types::{CalibrationArtifactId, Price, Probability};
    use rust_decimal_macros::dec;

    fn resolved() -> ResolvedCalibration {
        ResolvedCalibration {
            artifact_id: CalibrationArtifactId::from_v7(),
            mapping: MonotoneMapping::Isotonic {
                knots: vec![
                    IsotonicKnot {
                        score: dec!(0),
                        probability: dec!(0.5),
                    },
                    IsotonicKnot {
                        score: dec!(1),
                        probability: dec!(0.5),
                    },
                ],
            },
            reliability: ReliabilityReport {
                bins: vec![ReliabilityBin {
                    predicted_lo: dec!(0),
                    predicted_hi: dec!(1),
                    sample_count: 100,
                    mean_predicted: Probability::new(dec!(0.5)),
                    empirical_frequency: Probability::new(dec!(0.5)),
                    wilson_ci: (Probability::new(dec!(0.4)), Probability::new(dec!(0.6))),
                    mean_adverse_excursion_bps: Some(dec!(-500)),
                }],
                brier_score: dec!(0.1),
                log_loss: dec!(0.3),
                ece: dec!(0.05),
                n_samples: 100,
            },
        }
    }

    #[test]
    fn estimate_return_rejects_boundary_market_prices() {
        // `market_price` outside the *open* interval `(0, 1)` — a price of
        // exactly 0 or 1 is not an executable YES-leg price on a real
        // Polymarket order book, and `p=0` would divide by zero in
        // `(1-p)/p` if not rejected first — must be a typed inference failure,
        // never a zero-valued business prediction.
        let resolved = resolved();
        for boundary in [dec!(0), dec!(1)] {
            assert!(
                resolved
                    .estimate_return(dec!(0.5), Price::new(boundary))
                    .is_err(),
                "boundary price {boundary} must fail closed"
            );
        }
    }

    #[test]
    fn estimate_return_accepts_interior_market_prices() {
        let resolved = resolved();
        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.01)))
            .expect("interior estimate");
        assert!(estimate.win_probability.is_some());
        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.99)))
            .expect("interior estimate");
        assert!(estimate.win_probability.is_some());
    }

    #[test]
    fn estimate_return_rejects_missing_downside_evidence() {
        let mut resolved = resolved();
        resolved.reliability.bins[0].mean_adverse_excursion_bps = None;
        assert!(
            resolved
                .estimate_return(dec!(0.5), Price::new(dec!(0.5)))
                .is_err()
        );
    }

    #[test]
    fn empty_isotonic_mapping_fails_closed() {
        let mapping = MonotoneMapping::Isotonic { knots: Vec::new() };
        assert!(super::apply_mapping(&mapping, dec!(0.5)).is_err());
    }
}
