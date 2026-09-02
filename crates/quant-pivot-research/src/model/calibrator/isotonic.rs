//! Isotonic probability calibration via pool-adjacent-violators (PAVA) regression.
//!
//! Same algorithm as scikit-learn's `IsotonicRegression`, applied to `(score,
//! outcome)` pairs sorted ascending by score.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{enums::quant::CalibrationMethod, types::Probability};
use rust_decimal::Decimal;

use super::{IsotonicKnot, MonotoneMapping, ProbabilityCalibrator};
use crate::{
    model::{CancellationProbe, apply_mapping},
    stats::pava_non_decreasing_grouped,
};

/// Minimum paired samples to attempt an isotonic fit at all (below this even
/// a single knot is statistically meaningless).
const MIN_SAMPLES_FLOOR: usize = 10;
const CANCELLATION_INTERVAL: usize = 1_024;
const SORT_RUN_LENGTH: usize = 4_096;

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

    fn fit(
        &self,
        scores: &[Decimal],
        outcomes: &[bool],
        cancellation: &CancellationProbe,
    ) -> QuantResult<MonotoneMapping> {
        cancellation.check("isotonic fit start")?;
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
        // Fail-closed (matches `PlattCalibrator`): a calibration split with a
        // single outcome class carries no discriminative signal — PAVA would
        // still "fit" a degenerate constant-probability mapping without
        // error, silently producing an uninformative calibrator rather than
        // rejecting the split.
        let mut won_count = 0_usize;
        for (index, &won) in outcomes.iter().enumerate() {
            if index % CANCELLATION_INTERVAL == 0 {
                cancellation.check("isotonic outcome scan")?;
            }
            won_count += usize::from(won);
        }
        if won_count == 0 || won_count == outcomes.len() {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: "isotonic calibration requires both won and lost outcomes in the split"
                    .to_owned(),
            }));
        }
        let order = cancellable_score_order(scores, cancellation)?;

        // Aggregate tied scores into one (score, mean outcome, weight) group
        // *before* PAVA — matching scikit-learn's `_make_unique` preprocessing.
        // Running PAVA on the raw per-sample sequence instead (with ties only
        // collapsed after the fact) pools duplicated x-positions as if they
        // were independent unit-weight points, which yields a different —
        // incorrect — fitted probability whenever a score repeats.
        let mut group_scores: Vec<Decimal> = Vec::new();
        let mut group_wins: Vec<u64> = Vec::new();
        let mut group_weights: Vec<u64> = Vec::new();
        for (position, &idx) in order.iter().enumerate() {
            if position % CANCELLATION_INTERVAL == 0 {
                cancellation.check("isotonic tie aggregation")?;
            }
            let score = scores[idx];
            match group_scores.last() {
                Some(&last) if last == score => {
                    let last_idx = group_weights.len() - 1;
                    group_weights[last_idx] =
                        group_weights[last_idx].checked_add(1).ok_or_else(|| {
                            ResearchError::DatasetBuild {
                                detail: "isotonic tied-score weight overflowed".to_owned(),
                            }
                        })?;
                    if outcomes[idx] {
                        group_wins[last_idx] =
                            group_wins[last_idx].checked_add(1).ok_or_else(|| {
                                ResearchError::DatasetBuild {
                                    detail: "isotonic tied-score win count overflowed".to_owned(),
                                }
                            })?;
                    }
                }
                _ => {
                    group_scores.push(score);
                    group_wins.push(u64::from(outcomes[idx]));
                    group_weights.push(1);
                }
            }
        }
        let mut group_means = Vec::with_capacity(group_wins.len());
        for (index, (&wins, &weight)) in group_wins.iter().zip(&group_weights).enumerate() {
            if index % CANCELLATION_INTERVAL == 0 {
                cancellation.check("isotonic tied-score means")?;
            }
            group_means.push(Decimal::from(wins) / Decimal::from(weight));
        }
        let pooled = pava_non_decreasing_grouped(&group_means, &group_weights, cancellation)?;

        let mut knots = Vec::with_capacity(group_scores.len());
        for (index, (score, probability)) in group_scores.into_iter().zip(pooled).enumerate() {
            if index % CANCELLATION_INTERVAL == 0 {
                cancellation.check("isotonic knot construction")?;
            }
            knots.push(IsotonicKnot { score, probability });
        }
        cancellation.check("isotonic fit completion")?;
        Ok(MonotoneMapping::Isotonic { knots })
    }

    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> QuantResult<Probability> {
        apply_mapping(mapping, score)
    }
}

/// Piecewise-linear interpolation over ascending isotonic knots — the same
/// `predict` semantics scikit-learn's `IsotonicRegression` uses between fitted
/// points (`out_of_bounds='clip'` at the edges).
///
/// The calibrated probability at `score` linearly interpolates between the two
/// bracketing knots; outside the fitted range it clamps to the nearest knot's
/// probability.
///
/// # Errors
///
/// Defensively rejects an empty mapping. The sole callers first validate or
/// construct an immutable [`MonotoneMapping`], so the binary search may rely on
/// strictly increasing knots without repeating an O(k) proof per lookup.
pub(super) fn interpolate(knots: &[IsotonicKnot], score: Decimal) -> QuantResult<Decimal> {
    let Some(first) = knots.first() else {
        return Err(ResearchError::Inference {
            detail: "isotonic calibration mapping has no fitted knots".to_owned(),
        }
        .into());
    };
    match knots.binary_search_by(|knot| knot.score.cmp(&score)) {
        Ok(index) => Ok(knots[index].probability),
        Err(0) => Ok(first.probability),
        Err(index) if index == knots.len() => Ok(knots[knots.len() - 1].probability),
        Err(index) => {
            let lo = knots[index - 1];
            let hi = knots[index];
            let t = (score - lo.score) / (hi.score - lo.score);
            Ok(lo.probability + t * (hi.probability - lo.probability))
        }
    }
}

fn cancellable_score_order(
    scores: &[Decimal],
    cancellation: &CancellationProbe,
) -> QuantResult<Vec<usize>> {
    let mut order = (0..scores.len()).collect::<Vec<_>>();
    for (run_index, run) in order.chunks_mut(SORT_RUN_LENGTH).enumerate() {
        cancellation.check("isotonic score-run sort")?;
        run.sort_unstable_by(|&left, &right| {
            scores[left]
                .cmp(&scores[right])
                .then_with(|| left.cmp(&right))
        });
        if run_index % CANCELLATION_INTERVAL == 0 {
            cancellation.check("isotonic score-run completion")?;
        }
    }
    let mut width = SORT_RUN_LENGTH;
    while width < order.len() {
        cancellation.check("isotonic score merge pass")?;
        let span = width.saturating_mul(2);
        let mut merged = Vec::with_capacity(order.len());
        for start in (0..order.len()).step_by(span) {
            let middle = start.saturating_add(width).min(order.len());
            let end = start.saturating_add(span).min(order.len());
            let mut left = start;
            let mut right = middle;
            while left < middle || right < end {
                if merged.len() % CANCELLATION_INTERVAL == 0 {
                    cancellation.check("isotonic score merge")?;
                }
                let take_left = right == end
                    || (left < middle
                        && scores[order[left]]
                            .cmp(&scores[order[right]])
                            .then_with(|| order[left].cmp(&order[right]))
                            .is_le());
                if take_left {
                    merged.push(order[left]);
                    left += 1;
                } else {
                    merged.push(order[right]);
                    right += 1;
                }
            }
        }
        order = merged;
        width = span.min(order.len());
    }
    cancellation.check("isotonic score ordering completion")?;
    Ok(order)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{IsotonicCalibrator, MonotoneMapping};
    use crate::model::{CancellationProbe, calibrator::ProbabilityCalibrator};

    #[test]
    fn fit_rejects_below_samples() {
        let calibrator = IsotonicCalibrator::new(1000);
        let scores = vec![dec!(0.1), dec!(0.9)];
        let outcomes = vec![false, true];
        assert!(
            calibrator
                .fit(&scores, &outcomes, &CancellationProbe::default())
                .is_err()
        );
    }

    #[test]
    fn fit_rejects_without_classes() {
        // All-win split: no discriminative signal, PAVA would otherwise
        // silently fit a degenerate constant-1.0 mapping (matches Platt's
        // equivalent guard).
        let calibrator = IsotonicCalibrator::new(10);
        let scores: Vec<Decimal> = (0..20).map(|i| Decimal::from(i) / dec!(20)).collect();
        let outcomes = vec![true; 20];
        assert!(
            calibrator
                .fit(&scores, &outcomes, &CancellationProbe::default())
                .is_err(),
            "an all-win calibration split must be rejected, not silently fit"
        );

        let outcomes = vec![false; 20];
        assert!(
            calibrator
                .fit(&scores, &outcomes, &CancellationProbe::default())
                .is_err(),
            "an all-loss calibration split must be rejected, not silently fit"
        );
    }

    #[test]
    fn fit_produces_monotone_probabilities() {
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
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
        let MonotoneMapping::Isotonic { knots } = &mapping else {
            panic!("expected isotonic mapping");
        };
        for window in knots.windows(2) {
            assert!(window[0].score < window[1].score);
            assert!(window[0].probability <= window[1].probability);
        }
        let low = calibrator
            .calibrate(&mapping, dec!(0.01))
            .expect("calibrate low");
        let high = calibrator
            .calibrate(&mapping, dec!(0.99))
            .expect("calibrate high");
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
    fn isotonic_tied_scores_pava() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
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
    fn tied_order_is_deterministic() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let first = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("first tied-score fit");
        let mut reordered = outcomes;
        reordered[3..6].reverse();
        let second = calibrator
            .fit(&scores, &reordered, &CancellationProbe::default())
            .expect("reordered tied-score fit");
        assert_eq!(first, second);
    }

    #[test]
    fn isotonic_interpolate_linear_knots() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
        // score=0.6 sits exactly halfway between the score=0.5 knot (p=1/3)
        // and the score=0.7 knot (p=1). Linear interpolation (scikit-learn
        // `predict` semantics) must land strictly between them, not repeat
        // one endpoint's value as a step function would.
        let at_tie = calibrator
            .calibrate(&mapping, dec!(0.5))
            .expect("calibrate tie");
        let at_next = calibrator
            .calibrate(&mapping, dec!(0.7))
            .expect("calibrate next");
        let mid = calibrator
            .calibrate(&mapping, dec!(0.6))
            .expect("calibrate midpoint");
        assert!(mid.inner() > at_tie.inner());
        assert!(mid.inner() < at_next.inner());
        assert_eq!(mid.inner(), (at_tie.inner() + at_next.inner()) / dec!(2));
    }

    #[test]
    fn isotonic_interpolate_clamps_range() {
        let calibrator = IsotonicCalibrator::new(10);
        let (scores, outcomes) = tied_scores_dataset();
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
        let below = calibrator
            .calibrate(&mapping, dec!(-5))
            .expect("calibrate below");
        let above = calibrator
            .calibrate(&mapping, dec!(5))
            .expect("calibrate above");
        let first = calibrator
            .calibrate(&mapping, dec!(0.1))
            .expect("calibrate first");
        let last = calibrator
            .calibrate(&mapping, dec!(1.0))
            .expect("calibrate last");
        assert_eq!(below, first);
        assert_eq!(above, last);
    }

    #[test]
    fn fit_observes_cancellation() {
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&checks);
        let cancellation =
            CancellationProbe::new(move || observed.fetch_add(1, Ordering::Relaxed) >= 4);
        let scores = (0..20_000)
            .map(|index| Decimal::from(index) / Decimal::from(20_000))
            .collect::<Vec<_>>();
        let outcomes = (0..20_000).map(|index| index % 2 == 0).collect::<Vec<_>>();
        let result = IsotonicCalibrator::new(10).fit(&scores, &outcomes, &cancellation);
        assert!(
            result.is_err(),
            "running isotonic fit must observe cancellation"
        );
        assert!(checks.load(Ordering::Relaxed) > 4);
    }
}
