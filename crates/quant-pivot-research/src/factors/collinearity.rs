//! Factor collinearity analysis: the pairwise **Spearman** rank-correlation
//! matrix of a factor-value panel and the pairs that breach a tolerance.
//!
//! Two momentum factors that are really the same signal (audit #2) show up here
//! as a high `|ρ|`. In 11.1 this is an **analyzer + offline/CI lint**: the
//! acceptance suite (`default_momentum_estimators_not_mutually_collinear`)
//! asserts the four momentum estimators and the simple return stay below the
//! configured tolerance on a heterogeneous synthetic panel. Wiring `|ρ|` as a
//! hard model-publish gate is 11.5.
//!
//! Spearman is Pearson correlation of average ranks, computed over the
//! observations where **both** factors are present. The `f64` reductions are
//! quantized to a fixed decimal scale so the report is deterministic.

use std::collections::HashMap;

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

/// Residualize every factor column against a categorical grouping.
///
/// OLS on group dummies, which for a pure one-hot grouping reduces to subtracting
/// the within-group mean (`residual = value − mean_group`).
///
/// This is sector/category-neutral collinearity (the `factors.orthogonalize.
/// neutralize_by = [category]` operator): it removes correlation that is merely
/// two factors both tracking the same category composition, so what remains is
/// genuine same-signal redundancy. `groups` is row-aligned with `panel.rows`; a
/// `None` group is its own bucket (unknown-category observations neutralize among
/// themselves, never against a fabricated group). Missing cells stay missing.
#[must_use]
pub fn neutralize_by_group(
    panel: &FactorObservationMatrix,
    groups: &[Option<i64>],
) -> FactorObservationMatrix {
    let factor_count = panel.factors.len();
    let mut rows = panel.rows.clone();
    for factor in 0..factor_count {
        // Within-group running sum/count over the present values of this factor.
        let mut aggregate: HashMap<Option<i64>, (Decimal, u64)> = HashMap::new();
        for (row_index, row) in panel.rows.iter().enumerate() {
            if let Some(Some(value)) = row.get(factor).copied() {
                let key = groups.get(row_index).copied().unwrap_or(None);
                let entry = aggregate.entry(key).or_insert((Decimal::ZERO, 0));
                entry.0 += value;
                entry.1 += 1;
            }
        }
        for (row_index, row) in rows.iter_mut().enumerate() {
            let Some(cell) = row.get_mut(factor) else {
                continue;
            };
            if let Some(value) = *cell {
                let key = groups.get(row_index).copied().unwrap_or(None);
                if let Some((sum, count)) = aggregate.get(&key)
                    && *count > 0
                {
                    let mean = *sum / Decimal::from(*count);
                    *cell = Some(value - mean);
                }
            }
        }
    }
    FactorObservationMatrix {
        factors: panel.factors.clone(),
        rows,
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{FactorCollinearityAnalyzer, FactorObservationMatrix, neutralize_by_group};
    use crate::factors::value::FactorName;

    fn cell(value: i64, scale: u32) -> Decimal {
        Decimal::new(value, scale)
    }

    #[test]
    fn neutralize_by_category_removes_shared_category_level() {
        // Two factors both step up with the category level (so their raw Spearman
        // is inflated), but within each category their fine structure is not the
        // same. Category-neutralizing strips the shared level, so the residual
        // correlation is materially smaller — the category composition no longer
        // masquerades as genuine factor redundancy.
        let alpha = FactorName::from_static("alpha");
        let beta = FactorName::from_static("beta");
        let mut rows = Vec::new();
        for category in 0..3_i64 {
            let base = category * 100;
            // Within-group: alpha rises 0,1,2; beta is a rotation (1,2,0).
            rows.push(vec![Some(cell(base, 0)), Some(cell(base + 1, 0))]);
            rows.push(vec![Some(cell(base + 1, 0)), Some(cell(base + 2, 0))]);
            rows.push(vec![Some(cell(base + 2, 0)), Some(cell(base, 0))]);
        }
        let panel = FactorObservationMatrix {
            factors: vec![alpha, beta],
            rows,
        };
        let threshold = Decimal::new(9, 1);

        let raw = FactorCollinearityAnalyzer::analyze(&panel, threshold);
        assert!(
            raw.matrix[0][1] > Decimal::new(5, 1),
            "raw factors are inflated by the shared category level (ρ={})",
            raw.matrix[0][1]
        );

        let groups: Vec<Option<i64>> = (0..3_i64)
            .flat_map(|category| [Some(category); 3])
            .collect();
        let neutralized = neutralize_by_group(&panel, &groups);
        let report = FactorCollinearityAnalyzer::analyze(&neutralized, threshold);
        assert!(
            report.matrix[0][1].abs() < raw.matrix[0][1].abs(),
            "neutralizing the category level shrinks the correlation (raw={}, neutralized={})",
            raw.matrix[0][1],
            report.matrix[0][1]
        );
    }

    #[test]
    fn neutralize_treats_missing_group_as_its_own_bucket() {
        // A `None` group must not borrow another group's mean; it neutralizes
        // among the other `None` rows only.
        let alpha = FactorName::from_static("alpha");
        let rows = vec![
            vec![Some(cell(10, 0))],
            vec![Some(cell(20, 0))],
            vec![Some(cell(100, 0))],
            vec![Some(cell(140, 0))],
        ];
        let panel = FactorObservationMatrix {
            factors: vec![alpha],
            rows,
        };
        let groups = [Some(0_i64), Some(0), None, None];
        let neutralized = neutralize_by_group(&panel, &groups);
        // Group 0 mean = 15 ⇒ residuals ±5; None group mean = 120 ⇒ residuals ±20.
        assert_eq!(neutralized.rows[0][0], Some(Decimal::new(-5, 0)));
        assert_eq!(neutralized.rows[1][0], Some(Decimal::new(5, 0)));
        assert_eq!(neutralized.rows[2][0], Some(Decimal::new(-20, 0)));
        assert_eq!(neutralized.rows[3][0], Some(Decimal::new(20, 0)));
    }
}
