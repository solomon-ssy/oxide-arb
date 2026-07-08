//! Isotonic probability calibration via pool-adjacent-violators (PAVA) regression.
//!
//! Same algorithm as scikit-learn's `IsotonicRegression`, applied to `(score,
//! outcome)` pairs sorted ascending by score.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{enums::quant::CalibrationMethod, types::Probability};
use rust_decimal::Decimal;

use crate::stats::pava_non_decreasing;

use super::{IsotonicKnot, MonotoneMapping, ProbabilityCalibrator};

/// Minimum paired samples to attempt an isotonic fit at all (below this even
/// a single knot is statistically meaningless).
const MIN_SAMPLES_FLOOR: usize = 10;

/// Non-parametric monotone probability calibrator.
///
/// `min_samples` is the governed floor (`model.calibration.min_samples_isotonic`)
/// below which [`ProbabilityCalibrator::fit`] fails closed rather than fitting
/// an unreliable curve on too few points.
pub struct IsotonicCalibrator {
    min_samples: usize,
}

impl IsotonicCalibrator {
    #[must_use]
    pub const fn new(min_samples: usize) -> Self {
        Self { min_samples }
    }
}

impl ProbabilityCalibrator for IsotonicCalibrator {
    fn method(&self) -> CalibrationMethod {
        CalibrationMethod::Isotonic
    }

    fn fit(&self, scores: &[Decimal], outcomes: &[bool]) -> QuantResult<MonotoneMapping> {
        if scores.len() != outcomes.len() || scores.len() < self.min_samples.max(MIN_SAMPLES_FLOOR)
        {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "isotonic calibration requires >= {} paired samples, got {} \
                     (governed `model.calibration.min_samples_isotonic`; use Platt for \
                     smaller samples — never silently switched here)",
                    self.min_samples.max(MIN_SAMPLES_FLOOR),
                    scores.len()
                ),
            }));
        }
        let mut order: Vec<usize> = (0..scores.len()).collect();
        order.sort_by(|&a, &b| scores[a].cmp(&scores[b]));
        let sorted_scores: Vec<Decimal> = order.iter().map(|&i| scores[i]).collect();
        let binary: Vec<Decimal> = order
            .iter()
            .map(|&i| {
                if outcomes[i] {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                }
            })
            .collect();
        let calibrated = pava_non_decreasing(&binary);

        // Collapse ties in `sorted_scores` to their (already-pooled, equal)
        // calibrated value, keeping one knot per distinct score.
        let mut knots: Vec<IsotonicKnot> = Vec::new();
        for (score, probability) in sorted_scores.iter().zip(&calibrated) {
            match knots.last_mut() {
                Some(last) if last.score == *score => last.probability = *probability,
                _ => knots.push(IsotonicKnot {
                    score: *score,
                    probability: *probability,
                }),
            }
        }
        Ok(MonotoneMapping::Isotonic { knots })
    }

    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> Probability {
        super::apply_mapping(mapping, score)
    }
}

/// Piecewise-constant interpolation over ascending isotonic knots.
///
/// The calibrated probability at `score` is the value of the last knot at or below
/// it (step function), clamped to the first/last knot outside the fitted range.
/// Empty knots yield `0` (never fabricated).
#[must_use]
pub fn interpolate(knots: &[IsotonicKnot], score: Decimal) -> Decimal {
    if knots.is_empty() {
        return Decimal::ZERO;
    }
    if score <= knots[0].score {
        return knots[0].probability;
    }
    let mut value = knots[0].probability;
    for knot in knots {
        if knot.score > score {
            break;
        }
        value = knot.probability;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{IsotonicCalibrator, MonotoneMapping};
    use crate::model::calibrator::ProbabilityCalibrator;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn fit_fails_closed_below_min_samples() {
        let calibrator = IsotonicCalibrator::new(1000);
        let scores = vec![dec!(0.1), dec!(0.9)];
        let outcomes = vec![false, true];
        assert!(calibrator.fit(&scores, &outcomes).is_err());
    }

    #[test]
    fn fit_produces_monotone_calibrated_probabilities() {
        let calibrator = IsotonicCalibrator::new(10);
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..100 {
            let score = Decimal::from(i) / dec!(100);
            scores.push(score);
            // Higher score -> higher win rate: mostly losses below the midpoint,
            // mostly wins above it, with a little noise (every 7th sample flips).
            let base = i >= 50;
            outcomes.push(if i % 7 == 0 { !base } else { base });
        }
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let MonotoneMapping::Isotonic { knots } = &mapping else {
            panic!("expected isotonic mapping");
        };
        for window in knots.windows(2) {
            assert!(window[0].score < window[1].score);
            assert!(window[0].probability <= window[1].probability);
        }
        let low = calibrator.calibrate(&mapping, dec!(0.01));
        let high = calibrator.calibrate(&mapping, dec!(0.99));
        assert!(low.inner() <= high.inner());
    }
}
