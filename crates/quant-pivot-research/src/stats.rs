//! Deterministic, `Decimal`-domain statistics shared by the trainer and the
//! backtest metrics.
//!
//! All functions are pure and platform-deterministic (no `f64`): correlations
//! drive money-adjacent ranking quality, so they must reproduce bit-for-bit.

use rust_decimal::Decimal;

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
