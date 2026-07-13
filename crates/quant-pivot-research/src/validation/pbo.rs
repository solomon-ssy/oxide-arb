//! Probability of Backtest Overfitting via Combinatorially Symmetric CV.
//!
//! Cross-Validation (CSCV) — Phase 11.5 §3.5, Bailey, Borwein, López de Prado
//! & Zhu (2014/2017), *The Probability of Backtest Overfitting*, Algorithm 2.3.
//!
//! PBO answers: "if a researcher tried `N` independently governed strategy
//! configurations and reported whichever looked best in-sample, how often
//! would that champion actually underperform the out-of-sample median?" It
//! needs a `T`-period × `N`-trial performance matrix (Phase 11.5 §3.5's
//! trial grid supplies the columns — every governed hyperparameter
//! configuration gets one full-window train+backtest, producing one column
//! of per-period returns); the algorithm itself is a **pure, model-free**
//! resampling procedure over that matrix, with no further training required.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use rayon::prelude::*;
use rust_decimal::Decimal;

use crate::{precision::RESEARCH_DECIMAL_SCALE, stats, validation::combinatorics::combinations};

/// A `T`-period × `N`-trial matrix of per-period returns.
///
/// `returns[t][k]` is trial `k`'s return in period `t`. Every column must
/// share the same period axis (built from full-window backtests over the
/// identical window).
#[derive(Debug, Clone)]
pub struct TrialPerformanceMatrix {
    /// Ascending period timestamps (`T` rows).
    pub periods: Vec<DateTime<Utc>>,
    /// `T` rows × `N` columns; `returns[t].len()` must equal `N` for every `t`.
    pub returns: Vec<Vec<Decimal>>,
}

impl TrialPerformanceMatrix {
    /// Number of periods (`T`).
    #[must_use]
    pub const fn period_count(&self) -> usize {
        self.periods.len()
    }

    /// Number of trials (`N`), or `0` for an empty matrix.
    #[must_use]
    pub fn trial_count(&self) -> usize {
        self.returns.first().map_or(0, Vec::len)
    }
}

/// CSCV configuration: `block_count` (`S`) must be even and `>= 4`, and the
/// matrix must have at least `block_count` periods (each block non-empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PboInput {
    /// Number of equal-length time blocks (`S`) the CSCV procedure partitions
    /// the timeline into. `C(S, S/2)` combinations are enumerated.
    pub block_count: u32,
}

/// Per-block, per-trial pre-aggregated sufficient statistics (sum, sum of
/// squares, count) so a combination's IS/OOS Sharpe can be computed in
/// `O(blocks_in_combination)` rather than re-scanning every period.
struct BlockStats {
    sum: Decimal,
    sum_sq: Decimal,
    count: u64,
}

/// Aggregate `matrix` into `block_count` contiguous blocks, dropping the
/// earliest `T mod block_count` periods so every block is exactly
/// `T / block_count` periods long (CSCV requires equal-length blocks; the
/// alternative — uneven blocks — would bias later blocks' Sharpe estimates).
fn aggregate_blocks(
    matrix: &TrialPerformanceMatrix,
    block_count: usize,
) -> QuantResult<Vec<Vec<BlockStats>>> {
    let n_trials = matrix.trial_count();
    let block_len = matrix.period_count() / block_count;
    let drop = matrix.period_count() % block_count;
    (0..block_count)
        .map(|block| {
            let start = block
                .checked_mul(block_len)
                .and_then(|offset| drop.checked_add(offset))
                .ok_or_else(|| {
                    methodology("PBO block boundary calculation overflowed usize".to_owned())
                })?;
            let end = start.checked_add(block_len).ok_or_else(|| {
                methodology("PBO block end calculation overflowed usize".to_owned())
            })?;
            (0..n_trials)
                .map(|trial| {
                    let mut sum = Decimal::ZERO;
                    let mut sum_sq = Decimal::ZERO;
                    for row in &matrix.returns[start..end] {
                        let value = row[trial];
                        sum += value;
                        sum_sq += value * value;
                    }
                    Ok(BlockStats {
                        sum,
                        sum_sq,
                        count: u64::try_from(block_len).map_err(|error| {
                            methodology(format!("PBO block length does not fit u64: {error}"))
                        })?,
                    })
                })
                .collect::<QuantResult<Vec<_>>>()
        })
        .collect()
}

/// Sharpe ratio (unannualized: `mean / stddev`) from pre-aggregated
/// sufficient statistics over a set of blocks for one trial. Annualization is
/// a constant per-matrix scalar that cancels out under ranking, so CSCV
/// (a purely rank-based procedure) never needs it.
fn sharpe_from_blocks(blocks: &[&BlockStats]) -> Decimal {
    let count: u64 = blocks.iter().map(|b| b.count).sum();
    if count == 0 {
        return Decimal::ZERO;
    }
    let sum: Decimal = blocks.iter().map(|b| b.sum).sum();
    let sum_sq: Decimal = blocks.iter().map(|b| b.sum_sq).sum();
    let n = Decimal::from(count);
    let mean = sum / n;
    let mean_squared = mean * mean;
    let variance = sum_sq / n - mean_squared;
    if variance <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    mean / stats::sqrt(variance)
}

/// Evaluate the Probability of Backtest Overfitting for `matrix` (Bailey,
/// Borwein, López de Prado & Zhu 2014/2017, Algorithm 2.3):
///
/// 1. Partition the `T` periods into `input.block_count` equal blocks.
/// 2. Enumerate every way to split the blocks into two equal halves
///    (`C(S, S/2)` combinations); one half is in-sample (IS), the other
///    out-of-sample (OOS).
/// 3. For each combination, find the trial with the highest IS Sharpe (the
///    "IS champion"), then find that champion's relative rank
///    `ω = rank_OOS / (N + 1)` among all trials' OOS Sharpes.
/// 4. Transform to the logit `λ = ln(ω / (1 - ω))`.
/// 5. `PBO` = the fraction of combinations with `λ < 0` (the IS champion
///    finished below the OOS median).
///
/// # Errors
///
/// Returns [`ResearchError::ValidationMethodology`] when `block_count` is odd, `< 4`,
/// exceeds the period count, or the matrix has fewer than two trials (PBO is
/// only meaningful as a comparison across trials).
pub fn probability_of_backtest_overfitting(
    matrix: &TrialPerformanceMatrix,
    input: &PboInput,
) -> QuantResult<Decimal> {
    if input.block_count < 4 || !input.block_count.is_multiple_of(2) {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "pbo.block_count must be even and >= 4, got {}",
                input.block_count
            ),
        }
        .into());
    }
    let block_count = usize::try_from(input.block_count)
        .map_err(|error| methodology(format!("pbo.block_count does not fit usize: {error}")))?;
    validate_matrix(matrix)?;
    if matrix.period_count() < block_count {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "pbo requires at least block_count={} periods, got {}",
                block_count,
                matrix.period_count()
            ),
        }
        .into());
    }
    let trial_columns = matrix.trial_count();
    if trial_columns < 2 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("pbo requires at least 2 trials to compare, got {trial_columns}"),
        }
        .into());
    }
    let trial_count = u32::try_from(trial_columns).map_err(|error| {
        methodology(format!(
            "pbo trial count {trial_columns} exceeds the supported u32 range: {error}"
        ))
    })?;

    let blocks = aggregate_blocks(matrix, block_count)?;
    let half = block_count / 2;
    let is_combinations = combinations(block_count, half);

    let negative_flags: Vec<QuantResult<bool>> = is_combinations
        .par_iter()
        .map(|is_blocks| negative_logit(is_blocks, block_count, trial_count, &blocks))
        .collect();
    let mut negative_logits = 0_usize;
    for flag in negative_flags {
        if flag? {
            negative_logits = negative_logits.checked_add(1).ok_or_else(|| {
                methodology("PBO negative-logit count overflowed usize".to_owned())
            })?;
        }
    }

    let negative_logits = u64::try_from(negative_logits).map_err(|error| {
        methodology(format!(
            "PBO negative-logit count does not fit u64: {error}"
        ))
    })?;
    let combination_count = u64::try_from(is_combinations.len())
        .map_err(|error| methodology(format!("PBO combination count does not fit u64: {error}")))?;
    if combination_count == 0 {
        return Err(methodology(
            "PBO generated zero in-sample combinations".to_owned(),
        ));
    }
    let pbo = Decimal::from(negative_logits) / Decimal::from(combination_count);
    Ok(pbo.round_dp(RESEARCH_DECIMAL_SCALE))
}

fn negative_logit(
    is_blocks: &[usize],
    block_count: usize,
    trial_count: u32,
    blocks: &[Vec<BlockStats>],
) -> QuantResult<bool> {
    let column_count = usize::try_from(trial_count)
        .map_err(|error| methodology(format!("PBO trial count does not fit usize: {error}")))?;
    let is_set: HashSet<usize> = is_blocks.iter().copied().collect();
    let (is_idx, oos_idx): (Vec<usize>, Vec<usize>) =
        (0..block_count).partition(|index| is_set.contains(index));

    let is_sharpes: Vec<Decimal> = (0..column_count)
        .map(|trial| {
            let refs: Vec<&BlockStats> =
                is_idx.iter().map(|&block| &blocks[block][trial]).collect();
            sharpe_from_blocks(&refs)
        })
        .collect();
    let oos_sharpes: Vec<Decimal> = (0..column_count)
        .map(|trial| {
            let refs: Vec<&BlockStats> =
                oos_idx.iter().map(|&block| &blocks[block][trial]).collect();
            sharpe_from_blocks(&refs)
        })
        .collect();
    let champion = is_sharpes
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.cmp(right))
        .map(|(index, _)| index)
        .ok_or_else(|| methodology("PBO produced no in-sample Sharpe values".to_owned()))?;

    // Competition rank with average ties: count strictly worse + half of ties.
    // Strict `<` alone inflated ω (and understated PBO) when Sharpes tied.
    let champion_sharpe = oos_sharpes[champion];
    let worse = oos_sharpes
        .iter()
        .filter(|&&sharpe| sharpe < champion_sharpe)
        .count();
    let ties = oos_sharpes
        .iter()
        .filter(|&&sharpe| sharpe == champion_sharpe)
        .count();
    let ties = u32::try_from(ties)
        .map_err(|error| methodology(format!("PBO tie count does not fit u32: {error}")))?;
    let worse = u32::try_from(worse)
        .map_err(|error| methodology(format!("PBO worse-rank count does not fit u32: {error}")))?;
    let rank = 0.5f64.mul_add(f64::from(ties), f64::from(worse));
    let omega = rank / f64::from(trial_count).mul_add(1.0, 1.0);
    if !(0.0..1.0).contains(&omega) {
        return Err(methodology(format!(
            "PBO relative rank omega must be in (0, 1), got {omega}"
        )));
    }
    let logit = (omega / (1.0 - omega)).ln();
    if !logit.is_finite() {
        return Err(methodology("PBO logit is non-finite".to_owned()));
    }
    Ok(logit < 0.0)
}

fn validate_matrix(matrix: &TrialPerformanceMatrix) -> QuantResult<()> {
    if matrix.periods.len() != matrix.returns.len() {
        return Err(methodology(format!(
            "PBO period/return row count mismatch: periods={} returns={}",
            matrix.periods.len(),
            matrix.returns.len()
        )));
    }
    let trial_count = matrix.trial_count();
    for (row_index, row) in matrix.returns.iter().enumerate() {
        if row.len() != trial_count {
            return Err(methodology(format!(
                "PBO return row {row_index} has {} trials, expected {trial_count}",
                row.len()
            )));
        }
    }
    Ok(())
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use super::{PboInput, TrialPerformanceMatrix, probability_of_backtest_overfitting};
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn matrix(periods: usize, columns: Vec<Vec<Decimal>>) -> TrialPerformanceMatrix {
        let period_times: Vec<_> = (0..periods)
            .map(|i| {
                let offset = i64::try_from(i).unwrap_or(i64::MAX) * 3_600;
                Utc.timestamp_opt(1_700_000_000 + offset, 0).unwrap()
            })
            .collect();
        TrialPerformanceMatrix {
            periods: period_times,
            returns: columns,
        }
    }

    /// A "good" trial with a consistently high, low-variance return, and a
    /// "noise" trial whose apparent IS outperformance is pure luck (its
    /// returns alternate sign with no persistent edge) — the noise trial
    /// should rarely be the reported IS champion in a way that also holds up
    /// OOS, so PBO should be well below 1 when the good trial dominates and
    /// well above 0 when the "good" trial's edge doesn't persist.
    #[test]
    fn pbo_is_a_valid_probability_in_unit_interval() {
        let rows: Vec<Vec<Decimal>> = (0..64)
            .map(|i| {
                let base = if i % 2 == 0 { dec!(0.01) } else { dec!(-0.005) };
                vec![dec!(0.01), base, dec!(0.005) * Decimal::from(i % 3 - 1)]
            })
            .collect();
        let m = matrix(64, rows);
        let pbo =
            probability_of_backtest_overfitting(&m, &PboInput { block_count: 8 }).expect("pbo");
        assert!(pbo >= Decimal::ZERO && pbo <= Decimal::ONE, "pbo={pbo}");
    }

    #[test]
    fn pbo_rejects_odd_block_count() {
        let m = matrix(16, vec![vec![dec!(0.01), dec!(0.02)]; 16]);
        assert!(probability_of_backtest_overfitting(&m, &PboInput { block_count: 5 }).is_err());
    }

    #[test]
    fn pbo_rejects_single_trial() {
        let m = matrix(16, vec![vec![dec!(0.01)]; 16]);
        assert!(probability_of_backtest_overfitting(&m, &PboInput { block_count: 4 }).is_err());
    }

    #[test]
    fn pbo_rejects_non_rectangular_matrix() {
        let mut m = matrix(16, vec![vec![dec!(0.01), dec!(0.02)]; 16]);
        m.returns[7].pop();
        assert!(probability_of_backtest_overfitting(&m, &PboInput { block_count: 4 }).is_err());
    }

    #[test]
    fn pbo_rejects_period_axis_mismatch() {
        let mut m = matrix(16, vec![vec![dec!(0.01), dec!(0.02)]; 16]);
        m.periods.pop();
        assert!(probability_of_backtest_overfitting(&m, &PboInput { block_count: 4 }).is_err());
    }

    #[test]
    fn pbo_is_low_when_one_trial_dominates_every_block() {
        // Trial 1 is a constant `-0.06` shift of trial 0 in *every* period —
        // translation-invariant variance means trial 0's Sharpe exceeds
        // trial 1's Sharpe over any subset of periods, so trial 0 must win
        // the IS champion race and its OOS rank must never disagree: PBO = 0.
        // (Two literally constant, zero-variance series would make every
        // trial's Sharpe collapse to 0 — a degenerate tie, not "dominance" —
        // hence the oscillating-but-still-dominant construction here.)
        let rows: Vec<Vec<Decimal>> = (0..32)
            .map(|i| {
                let wobble = Decimal::from(i % 3 - 1) * dec!(0.005);
                vec![dec!(0.05) + wobble, dec!(-0.01) + wobble]
            })
            .collect();
        let m = matrix(32, rows);
        let pbo =
            probability_of_backtest_overfitting(&m, &PboInput { block_count: 8 }).expect("pbo");
        assert_eq!(pbo, Decimal::ZERO);
    }
}
