//! Deterministic, `Decimal`-domain statistics shared by the trainer and backtest.
//!
//! Also hosts Wilson-interval / isotonic-regression primitives used by both
//! calibration artifact families (`ProbabilityCalibrator` reliability bins and
//! `FavoriteLongshotBiasTable` price bins).
//!
//! The rank/correlation functions are pure and platform-deterministic (no
//! `f64`). [`wilson_z`] / [`wilson_interval`] cross an `f64` boundary (the
//! normal-quantile inverse CDF has no closed Decimal form) — the same
//! established boundary already used throughout the research plane's
//! statistical fits; results are quantized back to `Decimal` immediately.

use quant_pivot_error::{QuantResult, research::ResearchError};
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};
use statrs::distribution::{ContinuousCDF, Normal};

use crate::precision::RESEARCH_DECIMAL_SCALE;

/// Spearman rank correlation between two equal-length series (tie-aware).
///
/// Returns `0` for series shorter than two or with zero rank variance.
#[must_use]
pub fn spearman(xs: &[Decimal], ys: &[Decimal]) -> Decimal {
    if xs.len() != ys.len() || xs.len() < 2 {
        return Decimal::ZERO;
    }
    let rx = average_ranks(xs);
    let ry = average_ranks(ys);
    pearson(&rx, &ry)
}

/// Pearson correlation; `0` when either series has zero variance.
#[must_use]
pub fn pearson(xs: &[Decimal], ys: &[Decimal]) -> Decimal {
    let n = Decimal::from(xs.len() as u64);
    if n.is_zero() || xs.len() != ys.len() {
        return Decimal::ZERO;
    }
    let mean_x: Decimal = xs.iter().sum::<Decimal>() / n;
    let mean_y: Decimal = ys.iter().sum::<Decimal>() / n;
    let mut cov = Decimal::ZERO;
    let mut var_x = Decimal::ZERO;
    let mut var_y = Decimal::ZERO;
    for (x, y) in xs.iter().zip(ys) {
        let dx = *x - mean_x;
        let dy = *y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x.is_zero() || var_y.is_zero() {
        return Decimal::ZERO;
    }
    let denom = sqrt(var_x * var_y);
    if denom.is_zero() {
        return Decimal::ZERO;
    }
    (cov / denom).clamp(-Decimal::ONE, Decimal::ONE)
}

/// Average ranks (1-based), assigning the mean rank to tied values.
#[must_use]
pub fn average_ranks(values: &[Decimal]) -> Vec<Decimal> {
    let n = values.len();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| values[a].cmp(&values[b]).then(a.cmp(&b)));
    let mut ranks = vec![Decimal::ZERO; n];
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && values[order[j]] == values[order[i]] {
            j += 1;
        }
        let count = Decimal::from((j - i) as u64);
        let sum: Decimal = (i..j).map(|k| Decimal::from((k + 1) as u64)).sum();
        let avg = sum / count;
        for &idx in &order[i..j] {
            ranks[idx] = avg;
        }
        i = j;
    }
    ranks
}

/// Arithmetic mean (0 when empty).
#[must_use]
pub fn mean(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    values.iter().sum::<Decimal>() / Decimal::from(values.len() as u64)
}

/// Population variance (`0` for fewer than two values or zero-variance input).
#[must_use]
pub fn variance(values: &[Decimal]) -> Decimal {
    let n = values.len();
    if n < 2 {
        return Decimal::ZERO;
    }
    let m = mean(values);
    let sum_sq: Decimal = values.iter().map(|v| (*v - m) * (*v - m)).sum();
    sum_sq / Decimal::from(n as u64)
}

/// Sample standard deviation (population variance's square root; `0` for
/// fewer than two values).
#[must_use]
pub fn stddev(values: &[Decimal]) -> Decimal {
    sqrt(variance(values))
}

/// Pearson (population) skewness `γ3 = E[(X-μ)³] / σ³`.
///
/// `0` when σ is zero or fewer than two values. Used by the
/// Probabilistic/Deflated Sharpe Ratio formulas (Bailey & López de Prado
/// 2012/2014), which correct for non-normal return distributions.
#[must_use]
pub fn skewness(values: &[Decimal]) -> Decimal {
    let n = values.len();
    if n < 2 {
        return Decimal::ZERO;
    }
    let m = mean(values);
    let sigma = stddev(values);
    if sigma.is_zero() {
        return Decimal::ZERO;
    }
    let sum_cubed: Decimal = values.iter().map(|v| (*v - m) * (*v - m) * (*v - m)).sum();
    (sum_cubed / Decimal::from(n as u64)) / (sigma * sigma * sigma)
}

/// Pearson (population, non-excess) kurtosis `γ4 = E[(X-μ)⁴] / σ⁴`.
///
/// A normal distribution has `γ4 = 3`; `0` when σ is zero or fewer than two
/// values — callers must not confuse a genuinely degenerate series with a
/// normal one.
#[must_use]
pub fn kurtosis(values: &[Decimal]) -> Decimal {
    let n = values.len();
    if n < 2 {
        return Decimal::ZERO;
    }
    let m = mean(values);
    let sigma = stddev(values);
    if sigma.is_zero() {
        return Decimal::ZERO;
    }
    let variance_sq = sigma * sigma * sigma * sigma;
    let sum_fourth: Decimal = values
        .iter()
        .map(|v| {
            let d = *v - m;
            d * d * d * d
        })
        .sum();
    (sum_fourth / Decimal::from(n as u64)) / variance_sq
}

/// Deterministic decimal square root (Newton's method, fixed iterations).
#[must_use]
pub fn sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut guess = value;
    let two = Decimal::from(2);
    for _ in 0..40 {
        let next = (guess + value / guess) / two;
        if (next - guess).abs() < Decimal::new(1, RESEARCH_DECIMAL_SCALE) {
            return next;
        }
        guess = next;
    }
    guess
}

/// A count converted to `f64` through `Decimal` (no lossy `as` cast).
pub fn count_f64(n: u64) -> QuantResult<f64> {
    Decimal::from(n)
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!("count {n} is not representable as finite f64"),
            }
            .into()
        })
}

/// The two-sided normal quantile for a confidence level (e.g. `0.95 → 1.96`).
pub fn wilson_z(confidence: Decimal) -> QuantResult<f64> {
    let level = confidence
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!("Wilson confidence {confidence} is not representable as finite f64"),
        })?
        .clamp(0.5, 0.999_999);
    let upper = 1.0 - (1.0 - level) / 2.0;
    let distribution =
        Normal::new(0.0, 1.0).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("standard normal construction failed: {error}"),
        })?;
    Ok(distribution.inverse_cdf(upper))
}

/// Standard normal CDF `Φ(z)`. Used by the Probabilistic/Deflated Sharpe
/// Ratio (Bailey & López de Prado 2012/2014) to convert a standardized
/// Sharpe-significance statistic into a probability.
#[must_use]
pub fn normal_cdf(z: f64) -> f64 {
    Normal::new(0.0, 1.0).map_or(0.5, |dist| dist.cdf(z))
}

/// Standard normal inverse CDF (quantile function) `Φ⁻¹(p)`, `p` clamped to `(0, 1)`.
///
/// Used by the Deflated Sharpe Ratio's expected-maximum-Sharpe benchmark
/// (`SR*`) and by the CSCV/PBO logit transform's rank quantiles.
#[must_use]
pub fn normal_inverse_cdf(p: f64) -> f64 {
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    Normal::new(0.0, 1.0).map_or(0.0, |dist| dist.inverse_cdf(p))
}

/// Wilson score interval for a binomial proportion, quantized to `scale`.
pub fn wilson_interval(p_hat: f64, n: u64, z: f64, scale: u32) -> QuantResult<(Decimal, Decimal)> {
    if n == 0 {
        return Ok((Decimal::ZERO, Decimal::ZERO));
    }
    let n = count_f64(n)?;
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = (p_hat + z2 / (2.0 * n)) / denom;
    let margin = z * ((p_hat * (1.0 - p_hat) / n) + z2 / (4.0 * n * n)).sqrt() / denom;
    let lo = Decimal::from_f64((center - margin).clamp(0.0, 1.0))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "Wilson lower bound is not representable as Decimal".to_owned(),
        })?
        .round_dp(scale);
    let hi = Decimal::from_f64((center + margin).clamp(0.0, 1.0))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "Wilson upper bound is not representable as Decimal".to_owned(),
        })?
        .round_dp(scale);
    Ok((lo, hi))
}

/// Pool-adjacent-violators core: `group_means[i]` (weight `group_weights[i]`)
/// are already-aggregated observations sharing one x-position (e.g. tied
/// isotonic-calibration scores, or one observation per group when every
/// weight is `1`). Merges backward on any monotonicity violation, tracking
/// both the pooled weight (for the running mean) and the number of original
/// groups folded into each pool (so the caller can expand back to one output
/// per input group, not per unit of weight).
fn pava_pool_groups(group_means: &[Decimal], group_weights: &[u64]) -> Vec<(Decimal, u64, usize)> {
    // Each pool: (weighted sum, total weight, number of original groups merged in).
    let mut pools: Vec<(Decimal, u64, usize)> = Vec::with_capacity(group_means.len());
    for (&mean, &weight) in group_means.iter().zip(group_weights) {
        pools.push((mean * Decimal::from(weight), weight, 1));
        // Merge while the last pool's mean violates monotonicity.
        while pools.len() >= 2 {
            let (sum_b, n_b, _) = pools[pools.len() - 1];
            let (sum_a, n_a, _) = pools[pools.len() - 2];
            let mean_a = sum_a / Decimal::from(n_a);
            let mean_b = sum_b / Decimal::from(n_b);
            if mean_a <= mean_b {
                break;
            }
            let (sum_b, n_b, groups_b) = pools.pop().expect("checked len >= 2 above");
            let (sum_a, n_a, groups_a) = pools.pop().expect("checked len >= 2 above");
            pools.push((sum_a + sum_b, n_a + n_b, groups_a + groups_b));
        }
    }
    pools
}

/// Expand merged PAVA pools back into one pooled mean per original input
/// group (repeating a merged pool's mean for every group folded into it).
fn expand_pava_pools(pools: &[(Decimal, u64, usize)]) -> Vec<Decimal> {
    let mut out = Vec::with_capacity(pools.iter().map(|&(_, _, groups)| groups).sum());
    for &(sum, weight, groups) in pools {
        let mean = (sum / Decimal::from(weight)).round_dp(RESEARCH_DECIMAL_SCALE);
        for _ in 0..groups {
            out.push(mean);
        }
    }
    out
}

/// Pool-adjacent-violators isotonic regression producing a non-decreasing
/// series (unweighted: one input value per group).
///
/// Used by the `ProbabilityCalibrator` isotonic method
/// ([`crate::model::calibrator::isotonic`]) and available generically to any
/// other monotone-regression need in this crate — one shared implementation
/// rather than a per-caller PAVA.
#[must_use]
pub fn pava_non_decreasing(values: &[Decimal]) -> Vec<Decimal> {
    let weights = vec![1_u64; values.len()];
    expand_pava_pools(&pava_pool_groups(values, &weights))
}

/// Isotonic regression producing a non-increasing series (PAVA on the reverse).
#[must_use]
pub fn pava_non_increasing(values: &[Decimal]) -> Vec<Decimal> {
    let reversed: Vec<Decimal> = values.iter().rev().copied().collect();
    let mut out = pava_non_decreasing(&reversed);
    out.reverse();
    out
}

/// Weighted, grouped pool-adjacent-violators regression.
///
/// `group_means[i]` (weight `group_weights[i]`) are observations already
/// aggregated per distinct x-position — the sklearn `_make_unique`-style
/// aggregation that must happen **before** PAVA when the input carries ties,
/// so tied x-positions are pooled by their true weighted mean rather than by
/// PAVA treating each tied sample as an independent unit-weight point.
/// Returns one pooled, non-decreasing mean per input group (same
/// length/order as `group_means`).
#[must_use]
pub fn pava_non_decreasing_grouped(group_means: &[Decimal], group_weights: &[u64]) -> Vec<Decimal> {
    expand_pava_pools(&pava_pool_groups(group_means, group_weights))
}

#[cfg(test)]
mod calibration_stat_tests {
    use super::{
        kurtosis, normal_cdf, normal_inverse_cdf, pava_non_decreasing, pava_non_decreasing_grouped,
        pava_non_increasing, skewness, stddev, variance, wilson_interval, wilson_z,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn pava_enforces_non_decreasing() {
        let values = vec![dec!(1), dec!(3), dec!(2), dec!(5), dec!(4)];
        let out = pava_non_decreasing(&values);
        for window in out.windows(2) {
            assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn pava_non_increasing_reverses_order() {
        let values = vec![dec!(5), dec!(2), dec!(3), dec!(1)];
        let out = pava_non_increasing(&values);
        for window in out.windows(2) {
            assert!(window[0] >= window[1]);
        }
    }

    #[test]
    fn wilson_interval_contains_point_estimate() {
        let z = wilson_z(Decimal::new(95, 2)).expect("Wilson z");
        let (lo, hi) = wilson_interval(0.5, 100, z, 6).expect("Wilson interval");
        assert!(lo < Decimal::new(5, 1));
        assert!(hi > Decimal::new(5, 1));
    }

    #[test]
    fn wilson_z_matches_known_95_percent_quantile() {
        // The standard two-sided 95% normal quantile, to float precision.
        let z = wilson_z(Decimal::new(95, 2)).expect("Wilson z");
        assert!(
            (z - 1.959_963_984_540_054_5).abs() < 1e-9,
            "z={z}, expected ~1.959963984540054"
        );
    }

    #[test]
    fn wilson_interval_matches_known_closed_form() {
        // p_hat=0.2, n=50, z=1.9599639845400545 (95%) — golden (lo, hi)
        // independently computed in Python from the standard Wilson score
        // interval closed form (Wilson, 1927), rounded to the same 6-dp scale
        // `wilson_interval` quantizes to.
        let z = wilson_z(Decimal::new(95, 2)).expect("Wilson z");
        let (lo, hi) = wilson_interval(0.2, 50, z, 6).expect("Wilson interval");
        assert_eq!(lo, Decimal::new(112_438, 6), "lo={lo}");
        assert_eq!(hi, Decimal::new(330_371, 6), "hi={hi}");
    }

    #[test]
    fn wilson_interval_zero_samples_yields_zero_width() {
        let (lo, hi) = wilson_interval(0.5, 0, 1.96, 6).expect("Wilson interval");
        assert_eq!(lo, Decimal::ZERO);
        assert_eq!(hi, Decimal::ZERO);
    }

    #[test]
    fn pava_non_decreasing_grouped_pools_ties_before_pava() {
        // scores [1, 2, 2, 3], outcomes [0, 1, 0, 1] grouped as
        // (x=1, mean=0, w=1), (x=2, mean=0.5, w=2), (x=3, mean=1, w=1).
        // Group means (0, 0.5, 1) are already non-decreasing, so the
        // grouped-before-pooling x=2 mean of 0.5 must survive unmerged --
        // the value a per-sample PAVA run (treating the two x=2 samples as
        // independent unit-weight points interleaved with x=1/x=3) would
        // instead pool into something else entirely.
        let group_means = vec![dec!(0), dec!(0.5), dec!(1)];
        let group_weights = vec![1_u64, 2, 1];
        let pooled = pava_non_decreasing_grouped(&group_means, &group_weights);
        assert_eq!(pooled, vec![dec!(0), dec!(0.5), dec!(1)]);
    }

    #[test]
    fn pava_non_decreasing_grouped_merges_violating_groups_by_weight() {
        // Group means [0, 1, 0.4] with weights [1, 1, 3]: the middle group
        // (mean=1, weight=1) violates monotonicity against the heavier last
        // group (mean=0.4, weight=3) and must merge into a weighted mean of
        // (1*1 + 0.4*3) / 4 = 0.55, applied to both merged groups.
        let group_means = vec![dec!(0), dec!(1), dec!(0.4)];
        let group_weights = vec![1_u64, 1, 3];
        let pooled = pava_non_decreasing_grouped(&group_means, &group_weights);
        assert_eq!(pooled, vec![dec!(0), dec!(0.55), dec!(0.55)]);
    }

    #[test]
    fn pava_non_decreasing_grouped_matches_unweighted_pava_at_unit_weight() {
        let values = vec![dec!(1), dec!(3), dec!(2), dec!(5), dec!(4)];
        let weights = vec![1_u64; values.len()];
        assert_eq!(
            pava_non_decreasing_grouped(&values, &weights),
            pava_non_decreasing(&values)
        );
    }

    #[test]
    fn variance_and_stddev_of_known_series() {
        // [2, 4, 4, 4, 5, 5, 7, 9]: population variance = 4, stddev = 2 (textbook example).
        let values = vec![
            dec!(2),
            dec!(4),
            dec!(4),
            dec!(4),
            dec!(5),
            dec!(5),
            dec!(7),
            dec!(9),
        ];
        assert_eq!(variance(&values), dec!(4));
        assert_eq!(stddev(&values), dec!(2));
    }

    #[test]
    fn skewness_is_zero_for_symmetric_series() {
        let symmetric = vec![dec!(-2), dec!(-1), dec!(0), dec!(1), dec!(2)];
        assert_eq!(skewness(&symmetric), Decimal::ZERO);
    }

    #[test]
    fn skewness_is_positive_for_right_tailed_series() {
        let right_tailed = vec![dec!(1), dec!(1), dec!(1), dec!(1), dec!(10)];
        assert!(skewness(&right_tailed) > Decimal::ZERO);
    }

    #[test]
    fn kurtosis_of_degenerate_series_is_zero_not_normal() {
        // Fewer than two distinct values ⇒ zero variance ⇒ must report 0, never
        // silently claim the normal distribution's kurtosis of 3.
        let degenerate = vec![dec!(5), dec!(5), dec!(5)];
        assert_eq!(kurtosis(&degenerate), Decimal::ZERO);
    }

    #[test]
    fn normal_cdf_matches_known_quantiles() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-9);
        assert!((normal_cdf(1.959_963_984_540_054_5) - 0.975).abs() < 1e-6);
    }

    #[test]
    fn normal_inverse_cdf_is_the_inverse_of_normal_cdf() {
        let p = 0.841_344_746_068_542_9; // Phi(1.0)
        assert!((normal_inverse_cdf(p) - 1.0).abs() < 1e-6);
    }
}
