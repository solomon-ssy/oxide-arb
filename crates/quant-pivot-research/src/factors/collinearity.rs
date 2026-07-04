//! Factor collinearity analysis: the pairwise **Spearman** rank-correlation
//! matrix of a factor-value panel and the pairs that breach a tolerance.
//!
//! Two momentum factors that are really the same signal (audit #2) show up here
//! as a high `|ρ|`. In 11.1 this is an **analyzer + offline/CI lint** (the
//! acceptance suite asserts the default factor set is not collinear); wiring it
//! as a hard model-publish gate is 11.5.
//!
//! Spearman is Pearson correlation of average ranks, computed over the
//! observations where **both** factors are present. The `f64` reductions are
//! quantized to a fixed decimal scale so the report is deterministic.

use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde::{Deserialize, Serialize};

use crate::factors::value::FactorName;

/// Decimal places correlation values are quantized to (deterministic reports).
const CORR_SCALE: u32 = 6;

/// Minimum overlapping observations for a defined pairwise correlation.
const MIN_OVERLAP: usize = 3;

/// A tall panel of factor observations: `rows[obs][factor]`, index-aligned with
/// `factors`. A `None` cell is a missing observation for that factor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactorObservationMatrix {
    /// Factor names, index-aligned with each row's columns.
    pub factors: Vec<FactorName>,
    /// Observation rows; each row is aligned with `factors`.
    pub rows: Vec<Vec<Option<Decimal>>>,
}

/// A pair of factors whose absolute Spearman correlation breaches the tolerance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollinearPair {
    /// The first factor.
    pub left: FactorName,
    /// The second factor.
    pub right: FactorName,
    /// Their Spearman rank correlation in `[-1, 1]`.
    pub correlation: Decimal,
}

/// The collinearity report: the full correlation matrix plus the breaching pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorCollinearityReport {
    /// Factor names, index-aligned with `matrix` rows/columns.
    pub factors: Vec<FactorName>,
    /// The symmetric Spearman correlation matrix (`matrix[i][j]`), diagonal `1`.
    pub matrix: Vec<Vec<Decimal>>,
    /// Pairs with `|ρ| > threshold` (`left` before `right` in registry order).
    pub violations: Vec<CollinearPair>,
    /// The absolute-correlation tolerance used.
    pub threshold: Decimal,
}

impl FactorCollinearityReport {
    /// Whether any pair breached the tolerance.
    #[must_use]
    pub const fn is_collinear(&self) -> bool {
        !self.violations.is_empty()
    }
}

/// Computes the Spearman collinearity report for a factor panel.
pub struct FactorCollinearityAnalyzer;

impl FactorCollinearityAnalyzer {
    /// Build the Spearman correlation matrix and flag pairs above `threshold`.
    #[must_use]
    pub fn analyze(
        panel: &FactorObservationMatrix,
        threshold: Decimal,
    ) -> FactorCollinearityReport {
        let factor_count = panel.factors.len();
        let columns: Vec<Vec<Option<f64>>> = (0..factor_count)
            .map(|index| column(panel, index))
            .collect();

        let mut matrix = vec![vec![Decimal::ZERO; factor_count]; factor_count];
        let mut violations = Vec::new();
        for i in 0..factor_count {
            matrix[i][i] = Decimal::ONE;
            for j in (i + 1)..factor_count {
                let rho = spearman(&columns[i], &columns[j]);
                let rho_decimal = Decimal::from_f64(rho)
                    .unwrap_or(Decimal::ZERO)
                    .round_dp(CORR_SCALE);
                matrix[i][j] = rho_decimal;
                matrix[j][i] = rho_decimal;
                if rho_decimal.abs() > threshold {
                    violations.push(CollinearPair {
                        left: panel.factors[i].clone(),
                        right: panel.factors[j].clone(),
                        correlation: rho_decimal,
                    });
                }
            }
        }
        FactorCollinearityReport {
            factors: panel.factors.clone(),
            matrix,
            violations,
            threshold,
        }
    }
}

/// Extract one factor's column as optional `f64`s (index-aligned with rows).
fn column(panel: &FactorObservationMatrix, index: usize) -> Vec<Option<f64>> {
    use rust_decimal::prelude::ToPrimitive;
    panel
        .rows
        .iter()
        .map(|row| row.get(index).copied().flatten().and_then(|v| v.to_f64()))
        .collect()
}

/// Spearman rank correlation over the observations where both columns present.
fn spearman(left: &[Option<f64>], right: &[Option<f64>]) -> f64 {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        if let (Some(l), Some(r)) = (left_value, right_value) {
            a.push(*l);
            b.push(*r);
        }
    }
    if a.len() < MIN_OVERLAP {
        return 0.0;
    }
    pearson(&average_ranks(&a), &average_ranks(&b))
}

/// Average (tie-corrected) ranks of a slice, in the slice's own order.
fn average_ranks(values: &[f64]) -> Vec<f64> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|left, right| {
        values[*left]
            .partial_cmp(&values[*right])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut ranks = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start;
        while end + 1 < order.len()
            && (values[order[end + 1]] - values[order[start]]).abs() < f64::EPSILON
        {
            end += 1;
        }
        // Average rank (1-based) of the tie group `[start, end]`.
        let sum: usize = (start..=end).map(|position| position + 1).sum();
        let average = f64::from(u32::try_from(sum).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(end - start + 1).unwrap_or(1));
        for position in start..=end {
            ranks[order[position]] = average;
        }
        start = end + 1;
    }
    ranks
}

/// Pearson correlation of two equal-length slices; `0` for zero variance.
fn pearson(left: &[f64], right: &[f64]) -> f64 {
    let n = f64::from(u32::try_from(left.len()).unwrap_or(u32::MAX));
    if n == 0.0 {
        return 0.0;
    }
    let mean_left = left.iter().sum::<f64>() / n;
    let mean_right = right.iter().sum::<f64>() / n;
    let mut covariance = 0.0;
    let mut var_left = 0.0;
    let mut var_right = 0.0;
    for (l, r) in left.iter().zip(right.iter()) {
        let dl = l - mean_left;
        let dr = r - mean_right;
        covariance = dl.mul_add(dr, covariance);
        var_left = dl.mul_add(dl, var_left);
        var_right = dr.mul_add(dr, var_right);
    }
    let denominator = (var_left * var_right).sqrt();
    if denominator == 0.0 {
        return 0.0;
    }
    (covariance / denominator).clamp(-1.0, 1.0)
}
