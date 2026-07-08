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
#[must_use]
pub fn count_f64(n: u64) -> f64 {
    Decimal::from(n).to_f64().unwrap_or(0.0)
}

/// The two-sided normal quantile for a confidence level (e.g. `0.95 → 1.96`).
#[must_use]
pub fn wilson_z(confidence: Decimal) -> f64 {
    let level = confidence.to_f64().unwrap_or(0.95).clamp(0.5, 0.999_999);
    let upper = 1.0 - (1.0 - level) / 2.0;
    Normal::new(0.0, 1.0).map_or(1.96, |dist| dist.inverse_cdf(upper))
}

/// Wilson score interval for a binomial proportion, quantized to `scale`.
#[must_use]
pub fn wilson_interval(p_hat: f64, n: u64, z: f64, scale: u32) -> (Decimal, Decimal) {
    if n == 0 {
        return (Decimal::ZERO, Decimal::ZERO);
    }
    let n = count_f64(n);
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = (p_hat + z2 / (2.0 * n)) / denom;
    let margin = z * ((p_hat * (1.0 - p_hat) / n) + z2 / (4.0 * n * n)).sqrt() / denom;
    let lo = Decimal::from_f64((center - margin).clamp(0.0, 1.0))
        .unwrap_or(Decimal::ZERO)
        .round_dp(scale);
    let hi = Decimal::from_f64((center + margin).clamp(0.0, 1.0))
        .unwrap_or(Decimal::ONE)
        .round_dp(scale);
    (lo, hi)
}

/// Pool-adjacent-violators isotonic regression producing a non-decreasing
/// series (the shared PAVA core: sorted-input mean-pooling with backward
/// merge on any monotonicity violation).
///
/// Shared by the governed return-curve/multiplier tightening
/// ([`crate::model::calibration`]) and the `ProbabilityCalibrator` isotonic
/// method ([`crate::model::calibrator::isotonic`]) — same algorithm, same
/// numerical behavior, one implementation.
#[must_use]
pub fn pava_non_decreasing(values: &[Decimal]) -> Vec<Decimal> {
    // Each pool: (weighted sum, count).
    let mut pools: Vec<(Decimal, u64)> = Vec::with_capacity(values.len());
    for &value in values {
        pools.push((value, 1));
        // Merge while the last pool's mean violates monotonicity.
        while pools.len() >= 2 {
            let (sum_b, n_b) = pools[pools.len() - 1];
            let (sum_a, n_a) = pools[pools.len() - 2];
            let mean_a = sum_a / Decimal::from(n_a);
            let mean_b = sum_b / Decimal::from(n_b);
            if mean_a <= mean_b {
                break;
            }
            pools.pop();
            pools.pop();
            pools.push((sum_a + sum_b, n_a + n_b));
        }
    }
    expand_pava_pools(&pools, values.len())
}

/// Isotonic regression producing a non-increasing series (PAVA on the reverse).
#[must_use]
pub fn pava_non_increasing(values: &[Decimal]) -> Vec<Decimal> {
    let reversed: Vec<Decimal> = values.iter().rev().copied().collect();
    let mut out = pava_non_decreasing(&reversed);
    out.reverse();
    out
}

/// Expand merged PAVA pools back into a per-knot series of pool means.
fn expand_pava_pools(pools: &[(Decimal, u64)], len: usize) -> Vec<Decimal> {
    let mut out = Vec::with_capacity(len);
    for &(sum, count) in pools {
        let mean = (sum / Decimal::from(count)).round_dp(RESEARCH_DECIMAL_SCALE);
        for _ in 0..count {
            out.push(mean);
        }
    }
    out
}

#[cfg(test)]
mod calibration_stat_tests {
    use super::{pava_non_decreasing, pava_non_increasing, wilson_interval, wilson_z};
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
        let z = wilson_z(Decimal::new(95, 2));
        let (lo, hi) = wilson_interval(0.5, 100, z, 6);
        assert!(lo < Decimal::new(5, 1));
        assert!(hi > Decimal::new(5, 1));
    }
}
