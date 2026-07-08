//! Isotonic probability calibration via pool-adjacent-violators (PAVA) regression.
//!
//! Same algorithm as scikit-learn's `IsotonicRegression`, applied to `(score,
//! outcome)` pairs sorted ascending by score.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{enums::quant::CalibrationMethod, types::Probability};
use rust_decimal::Decimal;

use crate::stats::pava_non_decreasing_grouped;

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

        // Aggregate tied scores into one (score, mean outcome, weight) group
        // *before* PAVA — matching scikit-learn's `_make_unique` preprocessing.
        // Running PAVA on the raw per-sample sequence instead (with ties only
        // collapsed after the fact) pools duplicated x-positions as if they
        // were independent unit-weight points, which yields a different —
        // incorrect — fitted probability whenever a score repeats.
        let mut group_scores: Vec<Decimal> = Vec::new();
        let mut group_means: Vec<Decimal> = Vec::new();
        let mut group_weights: Vec<u64> = Vec::new();
        for &idx in &order {
            let score = scores[idx];
            let outcome = if outcomes[idx] {
                Decimal::ONE
            } else {
                Decimal::ZERO
            };
            match group_scores.last() {
                Some(&last) if last == score => {
                    let last_idx = group_means.len() - 1;
                    let weight = group_weights[last_idx];
                    group_means[last_idx] = (group_means[last_idx] * Decimal::from(weight)
                        + outcome)
                        / Decimal::from(weight + 1);
                    group_weights[last_idx] = weight + 1;
                }
                _ => {
                    group_scores.push(score);
                    group_means.push(outcome);
                    group_weights.push(1);
                }
            }
        }
        let pooled = pava_non_decreasing_grouped(&group_means, &group_weights);

        let knots: Vec<IsotonicKnot> = group_scores
            .into_iter()
            .zip(pooled)
            .map(|(score, probability)| IsotonicKnot { score, probability })
            .collect();
        Ok(MonotoneMapping::Isotonic { knots })
    }

    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> Probability {
        super::apply_mapping(mapping, score)
    }
}

/// Piecewise-linear interpolation over ascending isotonic knots — the same
/// `predict` semantics scikit-learn's `IsotonicRegression` uses between fitted
/// points (`out_of_bounds='clip'` at the edges).
///
/// The calibrated probability at `score` linearly interpolates between the two
/// bracketing knots; outside the fitted range it clamps to the nearest knot's
/// probability. Empty knots yield `0` (never fabricated).
#[must_use]
pub fn interpolate(knots: &[IsotonicKnot], score: Decimal) -> Decimal {
    let Some(first) = knots.first() else {
        return Decimal::ZERO;
    };
    if score <= first.score {
        return first.probability;
    }
    let last = knots[knots.len() - 1];
    if score >= last.score {
        return last.probability;
    }
    for window in knots.windows(2) {
        let (lo, hi) = (window[0], window[1]);
        if score >= lo.score && score <= hi.score {
            if hi.score == lo.score {
                return lo.probability;
            }
            let t = (score - lo.score) / (hi.score - lo.score);
            return lo.probability + t * (hi.probability - lo.probability);
        }
    }
    last.probability
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

    fn tied_scores_dataset() -> (Vec<Decimal>, Vec<bool>) {
        (
            vec![
                dec!(0.1),
                dec!(0.2),
                dec!(0.3),
                dec!(0.5),
                dec!(0.5),
                dec!(0.5),
                dec!(0.7),
                dec!(0.8),
                dec!(0.9),
                dec!(1.0),
            ],
            vec![
                false, false, false, true, false, false, true, true, true, true,
            ],
        )
    }

    #[test]
    fn isotonic_tied_scores_match_grouped_pava() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let MonotoneMapping::Isotonic { knots } = &mapping else {
            panic!("expected isotonic mapping");
        };
        // One knot per distinct score: ties are pooled by their grouped mean
        // *before* PAVA (sklearn `_make_unique` semantics), not duplicated or
        // treated as independent unit-weight points.
        assert_eq!(knots.len(), 8, "{knots:?}");
        let tied_knot = knots
            .iter()
            .find(|k| k.score == dec!(0.5))
            .expect("knot at tied score 0.5");
        // Grouped mean of [true, false, false] at score=0.5 is 1/3 — the
        // regression this guards against pooled the raw per-sample sequence
        // (weighting every duplicate as an independent point), which is only
        // equivalent to grouped pooling when the tied group's neighbors never
        // need merging; this dataset's group means (0,0,0,1/3,1,1,1,1) are
        // already non-decreasing so it isolates the grouping step itself.
        assert_eq!(tied_knot.probability, dec!(0.333333333333));
    }

    #[test]
    fn isotonic_interpolate_linear_between_knots() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        // score=0.6 sits exactly halfway between the score=0.5 knot (p=1/3)
        // and the score=0.7 knot (p=1). Linear interpolation (scikit-learn
        // `predict` semantics) must land strictly between them, not repeat
        // one endpoint's value as a step function would.
        let at_tie = calibrator.calibrate(&mapping, dec!(0.5));
        let at_next = calibrator.calibrate(&mapping, dec!(0.7));
        let mid = calibrator.calibrate(&mapping, dec!(0.6));
        assert!(mid.inner() > at_tie.inner());
        assert!(mid.inner() < at_next.inner());
        assert_eq!(mid.inner(), (at_tie.inner() + at_next.inner()) / dec!(2));
    }

    #[test]
    fn isotonic_interpolate_clamps_outside_fitted_range() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let below = calibrator.calibrate(&mapping, dec!(-5));
        let above = calibrator.calibrate(&mapping, dec!(5));
        let first = calibrator.calibrate(&mapping, dec!(0.1));
        let last = calibrator.calibrate(&mapping, dec!(1.0));
        assert_eq!(below, first);
        assert_eq!(above, last);
    }
}
