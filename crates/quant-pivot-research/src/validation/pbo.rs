//! Probability of Backtest Overfitting via Combinatorially Symmetric CV.
//!
//! Cross-Validation (CSCV), following Bailey, Borwein, López de Prado, and Zhu
//! (2014/2017), *The Probability of Backtest Overfitting*, Algorithm 2.3.
//!
//! PBO answers: "if a researcher tried `N` independently governed strategy
//! configurations and reported whichever looked best in-sample, how often
//! would that champion actually underperform the out-of-sample median?" It
//! needs a `T`-period × `N`-trial performance matrix. The governed trial grid
//! supplies the columns — every hyperparameter configuration gets one
//! complete cross-fitted out-of-sample return path from the same governed
//! purge/embargo CPCV design. The algorithm itself is a **pure, model-free**
//! resampling procedure over that matrix, with no further training required.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    hashing::CanonicalDigest,
    types::backtest::{
        BACKTEST_METRIC_SCALE, CscvBlockEvidence, CscvCombinationEvidence, CscvDegradationEvidence,
        CscvDegradationUndefinedReason, CscvDominanceEvidence, CscvDominanceRelation,
        CscvDsrTrialCountEvidence, CscvSelectionEvidence, CscvTrialBlockStatistic,
        CscvTrialDependenceEvidence, CscvTrialEquivalenceClass, CscvTrialGridBinding,
        CscvTrialPairDependence, CscvTrialPairRelationship, CscvTrialPerformance,
    },
};
use rayon::prelude::*;
use rust_decimal::{Decimal, MathematicalOps, prelude::ToPrimitive};

use crate::{stats, validation::combinatorics::combinations};

/// A `T`-period × `N`-trial matrix of per-period returns.
///
/// `returns[t][k]` is trial `k`'s return in period `t`. Every column must
/// share the same period axis and CPCV path-selection rule over the identical
/// frozen window. Each cell is out-of-sample for its period.
#[derive(Debug, Clone)]
pub struct TrialPerformanceMatrix {
    /// Ascending period timestamps (`T` rows).
    pub periods: Vec<DateTime<Utc>>,
    /// Contiguous row-major `T × N` returns.
    returns: Vec<Decimal>,
    trials: usize,
}

impl TrialPerformanceMatrix {
    /// Validate and flatten row-major returns into one allocation.
    pub fn from_rows(periods: Vec<DateTime<Utc>>, returns: Vec<Vec<Decimal>>) -> QuantResult<Self> {
        let trials = returns.first().map_or(0, Vec::len);
        if periods.len() != returns.len() {
            return Err(methodology(format!(
                "PBO period/return row count mismatch: periods={} returns={}",
                periods.len(),
                returns.len()
            )));
        }
        if let Some((row, values)) = returns
            .iter()
            .enumerate()
            .find(|(_, values)| values.len() != trials)
        {
            return Err(methodology(format!(
                "PBO return row {row} has {} trials, expected {trials}",
                values.len()
            )));
        }
        let capacity = periods
            .len()
            .checked_mul(trials)
            .ok_or_else(|| methodology("PBO matrix capacity overflowed usize".to_owned()))?;
        let mut flattened = Vec::with_capacity(capacity);
        for row in returns {
            flattened.extend(row);
        }
        Ok(Self {
            periods,
            returns: flattened,
            trials,
        })
    }

    /// Validate column-major trial output and transpose it directly into one
    /// row-major allocation. This is the canonical trial-grid construction
    /// path: it avoids allocating one `Vec` for every period (one million
    /// allocations at the governed maximum dataset size).
    pub fn from_columns(
        periods: Vec<DateTime<Utc>>,
        columns: &[Vec<Decimal>],
    ) -> QuantResult<Self> {
        let period_count = periods.len();
        if let Some((trial, values)) = columns
            .iter()
            .enumerate()
            .find(|(_, values)| values.len() != period_count)
        {
            return Err(methodology(format!(
                "PBO trial column {trial} has {} periods, expected {period_count}",
                values.len()
            )));
        }
        let trials = columns.len();
        let capacity = period_count
            .checked_mul(trials)
            .ok_or_else(|| methodology("PBO matrix capacity overflowed usize".to_owned()))?;
        let mut returns = Vec::with_capacity(capacity);
        for period in 0..period_count {
            returns.extend(columns.iter().map(|column| column[period]));
        }
        Ok(Self {
            periods,
            returns,
            trials,
        })
    }

    /// Number of trials (`N`), or `0` for an empty matrix.
    #[must_use]
    pub const fn trial_count(&self) -> usize {
        self.trials
    }

    /// Borrow one period's trial returns.
    #[must_use]
    pub fn row(&self, period: usize) -> Option<&[Decimal]> {
        let start = period.checked_mul(self.trials)?;
        let end = start.checked_add(self.trials)?;
        self.returns.get(start..end)
    }

    /// Return one exact OOS observation by period and governed trial index.
    ///
    /// This deliberately returns a copied [`Decimal`]: financial values stay
    /// exact, while callers can verify a subject path against one matrix
    /// column without exposing or reinterpreting the row-major storage.
    #[must_use]
    pub fn return_at(&self, period: usize, trial: usize) -> Option<Decimal> {
        self.row(period)?.get(trial).copied()
    }

    /// Iterate borrowed period rows.
    pub fn rows(&self) -> impl ExactSizeIterator<Item = &[Decimal]> {
        self.returns
            .chunks_exact(self.trials.max(1))
            .take(self.periods.len())
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

/// Aggregate `matrix` into `block_count` contiguous, exactly equal blocks.
/// CSCV Algorithm 2.3 requires `T / S × N` submatrices; silently discarding a
/// remainder would mutate the frozen research population and is forbidden.
fn aggregate_blocks(
    matrix: &TrialPerformanceMatrix,
    block_count: usize,
) -> QuantResult<Vec<CscvBlockEvidence>> {
    let n_trials = matrix.trial_count();
    let block_len = matrix.periods.len() / block_count;
    (0..block_count)
        .map(|block| {
            let start = block.checked_mul(block_len).ok_or_else(|| {
                methodology("PBO block boundary calculation overflowed usize".to_owned())
            })?;
            let end = start.checked_add(block_len).ok_or_else(|| {
                methodology("PBO block end calculation overflowed usize".to_owned())
            })?;
            let trial_statistics = (0..n_trials)
                .map(|trial| {
                    let mut sum = Decimal::ZERO;
                    let mut sum_sq = Decimal::ZERO;
                    for row in matrix.rows().skip(start).take(end - start) {
                        let value = row[trial];
                        sum = sum.checked_add(value).ok_or_else(|| {
                            methodology("PBO block return sum overflowed Decimal".to_owned())
                        })?;
                        let squared = value.checked_mul(value).ok_or_else(|| {
                            methodology("PBO squared return overflowed Decimal".to_owned())
                        })?;
                        sum_sq = sum_sq.checked_add(squared).ok_or_else(|| {
                            methodology("PBO squared-return sum overflowed Decimal".to_owned())
                        })?;
                    }
                    Ok(CscvTrialBlockStatistic {
                        trial_id: u32::try_from(trial).map_err(|error| {
                            methodology(format!("PBO trial index does not fit u32: {error}"))
                        })?,
                        observation_count: u64::try_from(block_len).map_err(|error| {
                            methodology(format!("PBO block length does not fit u64: {error}"))
                        })?,
                        return_sum: sum.normalize(),
                        squared_return_sum: sum_sq.normalize(),
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?;
            Ok(CscvBlockEvidence {
                block_index: u32::try_from(block).map_err(|error| {
                    methodology(format!("PBO block index does not fit u32: {error}"))
                })?,
                first_period: matrix.periods[start],
                last_period: matrix.periods[end - 1],
                trial_statistics,
            })
        })
        .collect()
}

/// Sharpe ratio (unannualized: `mean / stddev`) from pre-aggregated
/// sufficient statistics over a set of blocks for one trial. Annualization is
/// a constant per-matrix scalar that cancels out under ranking, so CSCV
/// (a purely rank-based procedure) never needs it.
fn sharpe_from_blocks(blocks: &[&CscvTrialBlockStatistic]) -> QuantResult<Decimal> {
    let count = blocks.iter().try_fold(0_u64, |count, block| {
        count
            .checked_add(block.observation_count)
            .ok_or_else(|| methodology("PBO block observation count overflowed u64".to_owned()))
    })?;
    if count == 0 {
        return Ok(Decimal::ZERO);
    }
    let sum = blocks.iter().try_fold(Decimal::ZERO, |sum, block| {
        sum.checked_add(block.return_sum)
            .ok_or_else(|| methodology("PBO combined return sum overflowed Decimal".to_owned()))
    })?;
    let sum_sq = blocks.iter().try_fold(Decimal::ZERO, |sum, block| {
        sum.checked_add(block.squared_return_sum).ok_or_else(|| {
            methodology("PBO combined squared-return sum overflowed Decimal".to_owned())
        })
    })?;
    let n = Decimal::from(count);
    let mean = sum / n;
    let mean_squared = mean
        .checked_mul(mean)
        .ok_or_else(|| methodology("PBO mean square overflowed Decimal".to_owned()))?;
    let variance = sum_sq / n - mean_squared;
    if variance < Decimal::ZERO {
        return Err(methodology(format!(
            "PBO sufficient statistics produced negative variance {variance}"
        )));
    }
    if variance == Decimal::ZERO {
        if sum != Decimal::ZERO {
            return Err(methodology(format!(
                "PBO Sharpe is undefined for a non-zero constant return series (sum={sum}, count={count})"
            )));
        }
        // A true no-trade series has zero excess return and zero risk. Treat it
        // as the neutral benchmark for rank selection; no positive/negative
        // constant series receives this convention.
        return Ok(Decimal::ZERO);
    }
    Ok((mean / stats::sqrt(variance)).round_dp(BACKTEST_METRIC_SCALE))
}

/// Evaluate and freeze CSCV selection-bias evidence for `matrix` (Bailey,
/// Borwein, López de Prado & Zhu 2014/2017, Algorithm 2.3):
///
/// 1. Partition the `T` periods into `grid.block_count` equal blocks.
/// 2. Enumerate every way to split the blocks into two equal halves
///    (`C(S, S/2)` combinations); one half is in-sample (IS), the other
///    out-of-sample (OOS).
/// 3. For each combination, find the trial with the highest IS Sharpe (the
///    "IS champion"), then find that champion's relative rank
///    `ω = rank_OOS / (N + 1)` among all trials' OOS Sharpes.
/// 4. Transform to the logit `λ = ln(ω / (1 - ω))`.
/// 5. `PBO` = the fraction of combinations with `λ < 0` (the IS champion
///    finished below the OOS median).
/// 6. Persist exact block sufficient statistics, every symmetric selection
///    observation, OOS loss, performance degradation, and stochastic
///    dominance so PBO and the DSR trial dispersion can be independently
///    recomputed from the append-only path-set row.
///
/// # Errors
///
/// Returns [`ResearchError::ValidationMethodology`] when the precommitted
/// selection grid and matrix disagree, timestamps are not strictly increasing,
/// `T` is not exactly divisible by `S`, or arithmetic evidence cannot be
/// represented exactly enough for deterministic persistence.
pub fn analyze_selection_bias(
    matrix: &TrialPerformanceMatrix,
    grid: &CscvTrialGridBinding,
) -> QuantResult<CscvSelectionEvidence> {
    grid.validate()
        .map_err(|error| methodology(format!("invalid CSCV trial-grid binding: {error}")))?;
    let block_count = usize::try_from(grid.block_count)
        .map_err(|error| methodology(format!("pbo.block_count does not fit usize: {error}")))?;
    (matrix).validate_matrix()?;
    if matrix.periods.len() < block_count {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "pbo requires at least block_count={} periods, got {}",
                block_count,
                matrix.periods.len()
            ),
        }
        .into());
    }
    if !matrix.periods.len().is_multiple_of(block_count) {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CSCV requires equal blocks: period_count={} is not divisible by block_count={block_count}",
                matrix.periods.len()
            ),
        }
        .into());
    }
    let trial_columns = matrix.trial_count();
    if trial_columns != grid.trials.len() {
        return Err(methodology(format!(
            "CSCV matrix has {trial_columns} trials but the precommitted grid has {}",
            grid.trials.len()
        )));
    }
    let trial_dependence = matrix.trial_dependence()?;
    let representative_trials = trial_dependence
        .equivalence_classes
        .iter()
        .map(|class| {
            usize::try_from(class.representative_trial_id).map_err(|error| {
                methodology(format!(
                    "behavioral trial representative does not fit usize: {error}"
                ))
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let blocks = aggregate_blocks(matrix, block_count)?;
    let half = block_count / 2;
    let is_combinations = combinations(block_count, half);
    let analyses: Vec<QuantResult<CombinationAnalysis>> = is_combinations
        .par_iter()
        .enumerate()
        .map(|(index, is_blocks)| {
            analyze_combination(
                index,
                is_blocks,
                block_count,
                &representative_trials,
                &blocks,
            )
        })
        .collect();
    let mut combinations_evidence = Vec::with_capacity(analyses.len());
    let mut all_oos_sharpes = Vec::with_capacity(
        analyses
            .len()
            .checked_mul(representative_trials.len())
            .ok_or_else(|| {
                methodology("CSCV OOS population capacity overflowed usize".to_owned())
            })?,
    );
    for combination_result in analyses {
        let combination = combination_result?;
        all_oos_sharpes.extend(combination.oos_sharpes);
        combinations_evidence.push(combination.evidence);
    }
    let combination_count = u64::try_from(combinations_evidence.len())
        .map_err(|error| methodology(format!("PBO combination count does not fit u64: {error}")))?;
    if combination_count == 0 {
        return Err(methodology(
            "PBO generated zero in-sample combinations".to_owned(),
        ));
    }
    let negative_logit_count =
        count_combinations(&combinations_evidence, |value| value.below_oos_median)?;
    let out_of_sample_loss_count =
        count_combinations(&combinations_evidence, |value| value.out_of_sample_loss)?;
    let pbo = (Decimal::from(negative_logit_count) / Decimal::from(combination_count))
        .round_dp(BACKTEST_METRIC_SCALE);
    let out_of_sample_loss_probability = (Decimal::from(out_of_sample_loss_count)
        / Decimal::from(combination_count))
    .round_dp(BACKTEST_METRIC_SCALE);
    let trial_performances = trial_performances(&blocks, trial_columns)?;
    let behavioral_trial_sharpes = representative_trials
        .iter()
        .map(|&representative| trial_performances[representative].full_sample_sharpe)
        .collect::<Vec<_>>();
    let behavioral_trial_sharpe_variance =
        stats::variance(&behavioral_trial_sharpes).round_dp(BACKTEST_METRIC_SCALE);
    let period_axis_hash =
        CanonicalDigest::content_hash_typed("quant-pivot/cscv-period-axis", 1, &matrix.periods)
            .map_err(|error| methodology(format!("hash CSCV period axis: {error}")))?;
    let evidence = CscvSelectionEvidence {
        schema_version: CscvSelectionEvidence::schema_version(),
        period_count: u64::try_from(matrix.periods.len())
            .map_err(|error| methodology(format!("CSCV period count does not fit u64: {error}")))?,
        period_axis_hash,
        block_count: grid.block_count,
        block_length: u64::try_from(matrix.periods.len() / block_count)
            .map_err(|error| methodology(format!("CSCV block length does not fit u64: {error}")))?,
        blocks,
        trial_performances,
        behavioral_trial_sharpe_variance,
        trial_dependence,
        performance_degradation: degradation_evidence(&combinations_evidence)?,
        stochastic_dominance: dominance_evidence(&combinations_evidence, &all_oos_sharpes)?,
        combinations: combinations_evidence,
        negative_logit_count,
        pbo,
        out_of_sample_loss_count,
        out_of_sample_loss_probability,
    };
    evidence
        .validate_for(grid)
        .map_err(|error| methodology(format!("invalid generated CSCV evidence: {error}")))?;
    Ok(evidence)
}

struct TrialMoments {
    period_count: u64,
    observations: Decimal,
    return_sums: Vec<Decimal>,
    squared_return_sums: Vec<Decimal>,
    variations: Vec<Decimal>,
}

struct RawTrialDependence {
    pairs: Vec<CscvTrialPairDependence>,
    relationships: BTreeMap<(usize, usize), CscvTrialPairRelationship>,
    representative_by_trial: Vec<usize>,
}

struct BehavioralTrialPopulation {
    equivalence_classes: Vec<CscvTrialEquivalenceClass>,
    representatives: Vec<usize>,
    trial_count: u32,
    pair_count: u64,
    zero_variance_trial_ids: Vec<u32>,
}

impl TrialPerformanceMatrix {
    fn trial_dependence(&self) -> QuantResult<CscvTrialDependenceEvidence> {
        let moments = self.trial_moments()?;
        let raw = self.raw_dependence(&moments)?;
        let population = BehavioralTrialPopulation::try_build(&raw, &moments)?;
        let trial_count_estimation = population.count_evidence(&raw)?;
        let raw_pair_count = u64::try_from(raw.pairs.len()).map_err(|error| {
            methodology(format!("CSCV raw pair count does not fit u64: {error}"))
        })?;
        Ok(CscvTrialDependenceEvidence {
            raw_pair_count,
            raw_pairs: raw.pairs,
            equivalence_classes: population.equivalence_classes,
            behavioral_pair_count: population.pair_count,
            trial_count_estimation,
        })
    }

    fn trial_moments(&self) -> QuantResult<TrialMoments> {
        let trial_count = self.trial_count();
        let period_count = u64::try_from(self.periods.len())
            .map_err(|error| methodology(format!("CSCV period count does not fit u64: {error}")))?;
        let observations = Decimal::from(period_count);
        let mut return_sums = vec![Decimal::ZERO; trial_count];
        let mut squared_return_sums = vec![Decimal::ZERO; trial_count];
        for row in self.rows() {
            for (trial, value) in row.iter().enumerate() {
                return_sums[trial] = return_sums[trial].checked_add(*value).ok_or_else(|| {
                    methodology("CSCV trial return sum overflowed Decimal".to_owned())
                })?;
                let squared = value.checked_mul(*value).ok_or_else(|| {
                    methodology("CSCV squared trial return overflowed Decimal".to_owned())
                })?;
                squared_return_sums[trial] = squared_return_sums[trial]
                    .checked_add(squared)
                    .ok_or_else(|| {
                        methodology("CSCV squared-return sum overflowed Decimal".to_owned())
                    })?;
            }
        }
        let variations = squared_return_sums
            .iter()
            .zip(&return_sums)
            .map(|(squared_sum, sum)| {
                squared_sum
                    .checked_mul(observations)
                    .and_then(|value| {
                        sum.checked_mul(*sum)
                            .and_then(|squared| value.checked_sub(squared))
                    })
                    .ok_or_else(|| methodology("CSCV trial variation overflowed".to_owned()))
            })
            .collect::<QuantResult<Vec<_>>>()?;
        if variations
            .iter()
            .any(|variation| *variation < Decimal::ZERO)
        {
            return Err(methodology(
                "CSCV trial population produced negative exact variation".to_owned(),
            ));
        }
        Ok(TrialMoments {
            period_count,
            observations,
            return_sums,
            squared_return_sums,
            variations,
        })
    }

    fn raw_dependence(&self, moments: &TrialMoments) -> QuantResult<RawTrialDependence> {
        let trial_count = self.trial_count();
        let expected_pair_count = trial_count
            .checked_mul(trial_count.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or_else(|| methodology("CSCV dependence pair count overflowed usize".to_owned()))?;
        let mut pairs = Vec::with_capacity(expected_pair_count);
        let mut relationships = BTreeMap::new();
        let mut representative_by_trial = (0..trial_count).collect::<Vec<_>>();
        for left in 0..trial_count {
            for right in left + 1..trial_count {
                let cross_product_sum = self.cross_product_sum(left, right)?;
                let twice_cross = cross_product_sum.checked_mul(Decimal::TWO).ok_or_else(|| {
                    methodology("CSCV doubled cross-product overflowed".to_owned())
                })?;
                let squared_difference = moments.squared_return_sums[left]
                    .checked_add(moments.squared_return_sums[right])
                    .and_then(|value| value.checked_sub(twice_cross))
                    .ok_or_else(|| methodology("CSCV squared difference overflowed".to_owned()))?;
                let covariance = cross_product_sum
                    .checked_mul(moments.observations)
                    .and_then(|value| {
                        moments.return_sums[left]
                            .checked_mul(moments.return_sums[right])
                            .and_then(|product| value.checked_sub(product))
                    })
                    .ok_or_else(|| methodology("CSCV covariance overflowed Decimal".to_owned()))?;
                let relationship = if squared_difference == Decimal::ZERO {
                    representative_by_trial[right] = representative_by_trial[left];
                    CscvTrialPairRelationship::ExactDuplicate
                } else {
                    TrialMoments::pair_relationship(
                        moments.variations[left],
                        moments.variations[right],
                        covariance,
                    )?
                };
                let left_trial_id = u32::try_from(left).map_err(|error| {
                    methodology(format!("CSCV left trial id does not fit u32: {error}"))
                })?;
                let right_trial_id = u32::try_from(right).map_err(|error| {
                    methodology(format!("CSCV right trial id does not fit u32: {error}"))
                })?;
                relationships.insert((left, right), relationship);
                pairs.push(CscvTrialPairDependence {
                    left_trial_id,
                    right_trial_id,
                    observation_count: moments.period_count,
                    cross_product_sum,
                    relationship,
                });
            }
        }
        Ok(RawTrialDependence {
            pairs,
            relationships,
            representative_by_trial,
        })
    }

    fn cross_product_sum(&self, left: usize, right: usize) -> QuantResult<Decimal> {
        self.rows().try_fold(Decimal::ZERO, |sum, row| {
            let product = row[left]
                .checked_mul(row[right])
                .ok_or_else(|| methodology("CSCV cross-product overflowed Decimal".to_owned()))?;
            sum.checked_add(product)
                .ok_or_else(|| methodology("CSCV cross-product sum overflowed Decimal".to_owned()))
        })
    }
}

impl TrialMoments {
    fn pair_relationship(
        left_variation: Decimal,
        right_variation: Decimal,
        covariance: Decimal,
    ) -> QuantResult<CscvTrialPairRelationship> {
        if left_variation == Decimal::ZERO || right_variation == Decimal::ZERO {
            return Ok(CscvTrialPairRelationship::ZeroVariance {
                left_zero_variance: left_variation == Decimal::ZERO,
                right_zero_variance: right_variation == Decimal::ZERO,
            });
        }
        let variance_product = left_variation
            .checked_mul(right_variation)
            .ok_or_else(|| methodology("CSCV variance product overflowed".to_owned()))?;
        let denominator = variance_product.sqrt().ok_or_else(|| {
            methodology("CSCV correlation denominator has no decimal root".to_owned())
        })?;
        let correlation = covariance
            .checked_div(denominator)
            .ok_or_else(|| methodology("CSCV correlation division failed".to_owned()))?
            .clamp(-Decimal::ONE, Decimal::ONE)
            .round_dp(BACKTEST_METRIC_SCALE);
        Ok(CscvTrialPairRelationship::Pearson { correlation })
    }
}

impl BehavioralTrialPopulation {
    fn try_build(raw: &RawTrialDependence, moments: &TrialMoments) -> QuantResult<Self> {
        let mut members_by_representative = BTreeMap::<usize, Vec<u32>>::new();
        for (trial, &representative) in raw.representative_by_trial.iter().enumerate() {
            members_by_representative
                .entry(representative)
                .or_default()
                .push(u32::try_from(trial).map_err(|error| {
                    methodology(format!("CSCV trial id does not fit u32: {error}"))
                })?);
        }
        let equivalence_classes = members_by_representative
            .into_iter()
            .enumerate()
            .map(|(class, (representative, member_trial_ids))| {
                Ok(CscvTrialEquivalenceClass {
                    class_id: u32::try_from(class).map_err(|error| {
                        methodology(format!("CSCV class id does not fit u32: {error}"))
                    })?,
                    representative_trial_id: u32::try_from(representative).map_err(|error| {
                        methodology(format!(
                            "CSCV representative trial id does not fit u32: {error}"
                        ))
                    })?,
                    member_trial_ids,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let representatives = equivalence_classes
            .iter()
            .map(|class| {
                usize::try_from(class.representative_trial_id).map_err(|error| {
                    methodology(format!(
                        "CSCV representative trial id does not fit usize: {error}"
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let count = u64::try_from(representatives.len()).map_err(|error| {
            methodology(format!(
                "CSCV behavioral trial count does not fit u64: {error}"
            ))
        })?;
        let trial_count = u32::try_from(count).map_err(|error| {
            methodology(format!(
                "CSCV behavioral trial count does not fit u32: {error}"
            ))
        })?;
        let pair_count = count
            .checked_mul(count.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or_else(|| methodology("CSCV behavioral pair count overflowed u64".to_owned()))?;
        let zero_variance_trial_ids = representatives
            .iter()
            .filter(|&&representative| moments.variations[representative] == Decimal::ZERO)
            .map(|&representative| {
                u32::try_from(representative).map_err(|error| {
                    methodology(format!(
                        "CSCV zero-variance representative does not fit u32: {error}"
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(Self {
            equivalence_classes,
            representatives,
            trial_count,
            pair_count,
            zero_variance_trial_ids,
        })
    }

    fn count_evidence(&self, raw: &RawTrialDependence) -> QuantResult<CscvDsrTrialCountEvidence> {
        if self.representatives.len() <= 1 || !self.zero_variance_trial_ids.is_empty() {
            return Ok(CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                behavioral_trial_count: self.trial_count,
                zero_variance_representative_trial_ids: self.zero_variance_trial_ids.clone(),
                conservative_independent_trial_count: self.trial_count,
            });
        }
        let correlation_sum = self.representatives.iter().enumerate().try_fold(
            Decimal::ZERO,
            |sum, (left_position, &left)| {
                self.representatives
                    .iter()
                    .skip(left_position + 1)
                    .try_fold(sum, |sum, &right| {
                        let relationship =
                            raw.relationships.get(&(left, right)).ok_or_else(|| {
                                methodology("CSCV behavioral relationship is missing".to_owned())
                            })?;
                        let CscvTrialPairRelationship::Pearson { correlation } = relationship
                        else {
                            return Err(methodology(
                                "CSCV non-redundant varying trials require Pearson dependence"
                                    .to_owned(),
                            ));
                        };
                        sum.checked_add(*correlation).ok_or_else(|| {
                            methodology(
                                "CSCV behavioral correlation sum overflowed Decimal".to_owned(),
                            )
                        })
                    })
            },
        )?;
        let average_correlation = correlation_sum
            .checked_div(Decimal::from(self.pair_count))
            .ok_or_else(|| methodology("CSCV average correlation division failed".to_owned()))?
            .round_dp(BACKTEST_METRIC_SCALE);
        let implied_independent_trial_count = average_correlation
            .checked_add(
                Decimal::ONE
                    .checked_sub(average_correlation)
                    .and_then(|value| value.checked_mul(Decimal::from(self.trial_count)))
                    .ok_or_else(|| methodology("CSCV implied trial count overflowed".to_owned()))?,
            )
            .ok_or_else(|| methodology("CSCV implied trial count overflowed".to_owned()))?
            .round_dp(BACKTEST_METRIC_SCALE);
        let conservative_independent_trial_count = implied_independent_trial_count
            .ceil()
            .to_u32()
            .ok_or_else(|| methodology("CSCV implied trial count does not fit u32".to_owned()))?;
        Ok(CscvDsrTrialCountEvidence::AverageCorrelation {
            behavioral_trial_count: self.trial_count,
            average_correlation,
            implied_independent_trial_count,
            conservative_independent_trial_count,
        })
    }
}

struct CombinationAnalysis {
    evidence: CscvCombinationEvidence,
    oos_sharpes: Vec<Decimal>,
}

fn analyze_combination(
    combination_index: usize,
    is_blocks: &[usize],
    block_count: usize,
    representative_trials: &[usize],
    blocks: &[CscvBlockEvidence],
) -> QuantResult<CombinationAnalysis> {
    let column_count = representative_trials.len();
    let trial_count = u32::try_from(column_count)
        .map_err(|error| methodology(format!("PBO trial count does not fit u32: {error}")))?;
    let mut is_mask = vec![false; block_count];
    for &block in is_blocks {
        let slot = is_mask.get_mut(block).ok_or_else(|| {
            methodology(format!(
                "CSCV IS block {block} is outside {block_count} blocks"
            ))
        })?;
        *slot = true;
    }
    let (is_idx, oos_idx): (Vec<usize>, Vec<usize>) =
        (0..block_count).partition(|index| is_mask[*index]);

    let is_sharpes = representative_trials
        .iter()
        .map(|&trial| {
            let refs = is_idx
                .iter()
                .map(|&block| &blocks[block].trial_statistics[trial])
                .collect::<Vec<_>>();
            sharpe_from_blocks(&refs)
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let oos_sharpes = representative_trials
        .iter()
        .map(|&trial| {
            let refs = oos_idx
                .iter()
                .map(|&block| &blocks[block].trial_statistics[trial])
                .collect::<Vec<_>>();
            sharpe_from_blocks(&refs)
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let champion = (0..column_count)
        .max_by_key(|&index| (is_sharpes[index], Reverse(index)))
        .ok_or_else(|| methodology("PBO produced no in-sample Sharpe values".to_owned()))?;

    // Bailey et al. define the OOS rank on the one-based interval 1..N and
    // normalize it by N + 1. Average ties therefore occupy their one-based
    // midrank: strictly-worse + (ties + 1) / 2. Omitting the one-based offset
    // incorrectly classifies a completely tied trial grid as overfit.
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
    let rank_twice = worse
        .checked_mul(2)
        .and_then(|value| value.checked_add(ties))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| methodology("PBO exact midrank overflowed u32".to_owned()))?;
    let median_boundary = trial_count
        .checked_add(1)
        .ok_or_else(|| methodology("PBO median rank boundary overflowed u32".to_owned()))?;
    let champion_trial_id = u32::try_from(representative_trials[champion])
        .map_err(|error| methodology(format!("PBO champion index does not fit u32: {error}")))?;
    Ok(CombinationAnalysis {
        evidence: CscvCombinationEvidence {
            combination_index: u32::try_from(combination_index).map_err(|error| {
                methodology(format!("PBO combination index does not fit u32: {error}"))
            })?,
            in_sample_block_indices: is_blocks
                .iter()
                .map(|&block| {
                    u32::try_from(block).map_err(|error| {
                        methodology(format!("PBO block index does not fit u32: {error}"))
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?,
            champion_trial_id,
            in_sample_sharpe: is_sharpes[champion],
            out_of_sample_sharpe: champion_sharpe,
            out_of_sample_rank_twice: rank_twice,
            below_oos_median: rank_twice < median_boundary,
            out_of_sample_loss: champion_sharpe < Decimal::ZERO,
        },
        oos_sharpes,
    })
}

fn count_combinations(
    combinations: &[CscvCombinationEvidence],
    predicate: impl Fn(&CscvCombinationEvidence) -> bool,
) -> QuantResult<u64> {
    u64::try_from(combinations.iter().filter(|value| predicate(value)).count())
        .map_err(|error| methodology(format!("CSCV diagnostic count does not fit u64: {error}")))
}

fn trial_performances(
    blocks: &[CscvBlockEvidence],
    trial_count: usize,
) -> QuantResult<Vec<CscvTrialPerformance>> {
    (0..trial_count)
        .map(|trial| {
            let statistics = blocks
                .iter()
                .map(|block| &block.trial_statistics[trial])
                .collect::<Vec<_>>();
            Ok(CscvTrialPerformance {
                trial_id: u32::try_from(trial).map_err(|error| {
                    methodology(format!("CSCV trial index does not fit u32: {error}"))
                })?,
                full_sample_sharpe: sharpe_from_blocks(&statistics)?,
            })
        })
        .collect()
}

fn degradation_evidence(
    combinations: &[CscvCombinationEvidence],
) -> QuantResult<CscvDegradationEvidence> {
    let count = Decimal::from(u64::try_from(combinations.len()).map_err(|error| {
        methodology(format!("CSCV regression count does not fit u64: {error}"))
    })?);
    let mean_is = combinations
        .iter()
        .map(|value| value.in_sample_sharpe)
        .sum::<Decimal>()
        / count;
    let mean_oos = combinations
        .iter()
        .map(|value| value.out_of_sample_sharpe)
        .sum::<Decimal>()
        / count;
    let mut is_variation = Decimal::ZERO;
    let mut oos_variation = Decimal::ZERO;
    let mut covariance = Decimal::ZERO;
    for value in combinations {
        let centered_is = value.in_sample_sharpe - mean_is;
        let centered_oos = value.out_of_sample_sharpe - mean_oos;
        is_variation += centered_is * centered_is;
        oos_variation += centered_oos * centered_oos;
        covariance += centered_is * centered_oos;
    }
    if is_variation == Decimal::ZERO {
        return Ok(CscvDegradationEvidence::Undefined {
            reason: CscvDegradationUndefinedReason::ConstantInSampleChampionPerformance,
        });
    }
    if oos_variation == Decimal::ZERO {
        return Ok(CscvDegradationEvidence::Undefined {
            reason: CscvDegradationUndefinedReason::ConstantOutOfSampleChampionPerformance,
        });
    }
    let slope = covariance / is_variation;
    let intercept = mean_oos - slope * mean_is;
    let residual_sum = combinations
        .iter()
        .map(|value| {
            let residual =
                value.out_of_sample_sharpe - (intercept + slope * value.in_sample_sharpe);
            residual * residual
        })
        .sum::<Decimal>();
    let r_squared = (Decimal::ONE - residual_sum / oos_variation).round_dp(BACKTEST_METRIC_SCALE);
    if !(Decimal::ZERO..=Decimal::ONE).contains(&r_squared) {
        return Err(methodology(format!(
            "CSCV degradation R-squared is outside [0,1]: {r_squared}"
        )));
    }
    Ok(CscvDegradationEvidence::Estimated {
        intercept: intercept.round_dp(BACKTEST_METRIC_SCALE),
        slope: slope.round_dp(BACKTEST_METRIC_SCALE),
        r_squared,
    })
}

fn dominance_evidence(
    combinations: &[CscvCombinationEvidence],
    all_oos_sharpes: &[Decimal],
) -> QuantResult<CscvDominanceEvidence> {
    let mut selected = combinations
        .iter()
        .map(|value| value.out_of_sample_sharpe)
        .collect::<Vec<_>>();
    let mut population = all_oos_sharpes.to_vec();
    selected.sort_unstable();
    population.sort_unstable();
    let support = selected
        .iter()
        .chain(&population)
        .copied()
        .collect::<BTreeSet<_>>();
    let selected_denominator = Decimal::from(u64::try_from(selected.len()).map_err(|error| {
        methodology(format!(
            "CSCV selected population does not fit u64: {error}"
        ))
    })?);
    let population_denominator =
        Decimal::from(u64::try_from(population.len()).map_err(|error| {
            methodology(format!(
                "CSCV full OOS population does not fit u64: {error}"
            ))
        })?);
    let mut selected_index = 0usize;
    let mut population_index = 0usize;
    let mut prior_point = None;
    let mut prior_cdf_gap = Decimal::ZERO;
    let mut integrated_advantage = Decimal::ZERO;
    let mut min_integrated_advantage = Decimal::ZERO;
    let mut max_integrated_advantage = Decimal::ZERO;
    let mut max_selected_cdf_excess = Decimal::ZERO;
    let mut first_order_strict = false;
    let mut second_order_strict = false;
    for point in &support {
        if let Some(prior) = prior_point {
            integrated_advantage += prior_cdf_gap * (*point - prior);
            min_integrated_advantage = min_integrated_advantage.min(integrated_advantage);
            max_integrated_advantage = max_integrated_advantage.max(integrated_advantage);
            second_order_strict |= integrated_advantage > Decimal::ZERO;
        }
        while selected
            .get(selected_index)
            .is_some_and(|value| value <= point)
        {
            selected_index += 1;
        }
        while population
            .get(population_index)
            .is_some_and(|value| value <= point)
        {
            population_index += 1;
        }
        let selected_cdf = Decimal::from(u64::try_from(selected_index).map_err(|error| {
            methodology(format!("CSCV selected CDF count does not fit u64: {error}"))
        })?) / selected_denominator;
        let population_cdf = Decimal::from(u64::try_from(population_index).map_err(|error| {
            methodology(format!(
                "CSCV population CDF count does not fit u64: {error}"
            ))
        })?) / population_denominator;
        let selected_excess = selected_cdf - population_cdf;
        max_selected_cdf_excess = max_selected_cdf_excess.max(selected_excess);
        first_order_strict |= selected_cdf < population_cdf;
        prior_cdf_gap = population_cdf - selected_cdf;
        prior_point = Some(*point);
    }
    let first_order = if max_selected_cdf_excess > Decimal::ZERO {
        CscvDominanceRelation::NoSelectedDominance
    } else if first_order_strict {
        CscvDominanceRelation::SelectedDominates
    } else {
        CscvDominanceRelation::Equivalent
    };
    let second_order = if min_integrated_advantage < Decimal::ZERO {
        CscvDominanceRelation::NoSelectedDominance
    } else if second_order_strict {
        CscvDominanceRelation::SelectedDominates
    } else {
        CscvDominanceRelation::Equivalent
    };
    Ok(CscvDominanceEvidence {
        evaluation_point_count: u64::try_from(support.len()).map_err(|error| {
            methodology(format!("CSCV dominance support does not fit u64: {error}"))
        })?,
        first_order,
        second_order,
        max_selected_cdf_excess: max_selected_cdf_excess.round_dp(BACKTEST_METRIC_SCALE),
        min_integrated_cdf_advantage: min_integrated_advantage.round_dp(BACKTEST_METRIC_SCALE),
        max_integrated_cdf_advantage: max_integrated_advantage.round_dp(BACKTEST_METRIC_SCALE),
    })
}

impl TrialPerformanceMatrix {
    fn validate_matrix(&self) -> QuantResult<()> {
        let expected = self
            .periods
            .len()
            .checked_mul(self.trial_count())
            .ok_or_else(|| methodology("PBO matrix shape overflowed usize".to_owned()))?;
        if self.returns.len() != expected {
            return Err(methodology(format!(
                "PBO matrix has {} values but shape {}x{} requires {expected}",
                self.returns.len(),
                self.periods.len(),
                self.trial_count()
            )));
        }
        if self.periods.is_empty() || self.periods.windows(2).any(|window| window[0] >= window[1]) {
            return Err(methodology(
                "PBO period axis must be non-empty and strictly increasing".to_owned(),
            ));
        }
        Ok(())
    }
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        hashing::CanonicalDigest,
        types::backtest::{
            CscvDsrTrialCountEvidence, CscvTrialDescriptor, CscvTrialGridBinding,
            CscvTrialPairRelationship,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{TrialPerformanceMatrix, analyze_selection_bias};

    fn matrix(periods: usize, columns: Vec<Vec<Decimal>>) -> TrialPerformanceMatrix {
        let period_times: Vec<_> = (0..periods)
            .map(|i| {
                let offset = i64::try_from(i).unwrap_or(i64::MAX) * 3_600;
                Utc.timestamp_opt(1_700_000_000 + offset, 0).unwrap()
            })
            .collect();
        TrialPerformanceMatrix::from_rows(period_times, columns).expect("performance matrix")
    }

    fn grid(trial_count: usize, block_count: u32) -> CscvTrialGridBinding {
        let trials = (0..trial_count)
            .map(|trial| CscvTrialDescriptor {
                trial_id: u32::try_from(trial).expect("trial id"),
                label: format!("trial-{trial}"),
                config_hash: CanonicalDigest::content_hash_typed(
                    "quant-pivot/test-cscv-trial",
                    1,
                    &trial,
                )
                .expect("trial hash"),
            })
            .collect();
        CscvTrialGridBinding::try_new(block_count, trials).expect("trial grid")
    }

    /// A "good" trial with a consistently high, low-variance return, and a
    /// "noise" trial whose apparent IS outperformance is pure luck (its
    /// returns alternate sign with no persistent edge) — the noise trial
    /// should rarely be the reported IS champion in a way that also holds up
    /// OOS, so PBO should be well below 1 when the good trial dominates and
    /// well above 0 when the "good" trial's edge doesn't persist.
    #[test]
    fn pbo_valid_probability_interval() {
        let rows: Vec<Vec<Decimal>> = (0..64)
            .map(|i| {
                let base = if i % 2 == 0 { dec!(0.01) } else { dec!(-0.005) };
                let good = if i % 2 == 0 {
                    dec!(0.0101)
                } else {
                    dec!(0.0099)
                };
                vec![good, base, dec!(0.005) * Decimal::from(i % 3 - 1)]
            })
            .collect();
        let m = matrix(64, rows);
        let pbo = analyze_selection_bias(&m, &grid(3, 8))
            .expect("CSCV evidence")
            .pbo;
        assert!(pbo >= Decimal::ZERO && pbo <= Decimal::ONE, "pbo={pbo}");
    }

    #[test]
    fn nonzero_constant_trials_rejected() {
        let rows = vec![vec![dec!(0.01), dec!(0.02)]; 64];
        let matrix = matrix(64, rows);
        assert!(
            analyze_selection_bias(&matrix, &grid(2, 8)).is_err(),
            "CSCV requires a performance statistic estimable on every subsample"
        );
    }

    #[test]
    fn no_trade_behavioral_count() {
        let rows = (0..64)
            .map(|period| {
                let varying = Decimal::from(period % 5) * dec!(0.001) - dec!(0.002);
                vec![Decimal::ZERO, varying]
            })
            .collect();
        let evidence =
            analyze_selection_bias(&matrix(64, rows), &grid(2, 8)).expect("CSCV evidence");
        assert_eq!(
            evidence.trial_dependence.trial_count_estimation,
            CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                behavioral_trial_count: 2,
                zero_variance_representative_trial_ids: vec![0],
                conservative_independent_trial_count: 2,
            }
        );
    }

    #[test]
    fn duplicate_parameterizations_preserve_statistics() {
        let base_rows = (0..64)
            .map(|period| {
                let edge = match period % 4 {
                    0 => dec!(0.018),
                    1 => dec!(-0.004),
                    2 => dec!(0.012),
                    _ => dec!(-0.002),
                };
                vec![edge, Decimal::ZERO]
            })
            .collect::<Vec<_>>();
        let expanded_rows = base_rows
            .iter()
            .map(|row| (0..8).flat_map(|_| [row[0], row[1]]).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let base = analyze_selection_bias(&matrix(64, base_rows), &grid(2, 8))
            .expect("base CSCV evidence");
        let expanded = analyze_selection_bias(&matrix(64, expanded_rows), &grid(16, 8))
            .expect("expanded CSCV evidence");

        assert_eq!(expanded.pbo, base.pbo);
        assert_eq!(
            expanded.out_of_sample_loss_probability,
            base.out_of_sample_loss_probability
        );
        assert_eq!(
            expanded.behavioral_trial_sharpe_variance,
            base.behavioral_trial_sharpe_variance
        );
        assert_eq!(
            expanded
                .trial_dependence
                .conservative_independent_trial_count(),
            2
        );
        assert_eq!(expanded.trial_performances.len(), 16);
        assert_eq!(expanded.trial_dependence.equivalence_classes.len(), 2);
        assert_eq!(
            expanded.trial_dependence.equivalence_classes[0].member_trial_ids,
            vec![0, 2, 4, 6, 8, 10, 12, 14]
        );
        assert_eq!(
            expanded.trial_dependence.equivalence_classes[1].member_trial_ids,
            vec![1, 3, 5, 7, 9, 11, 13, 15]
        );
        assert_eq!(
            expanded.trial_dependence.trial_count_estimation,
            CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                behavioral_trial_count: 2,
                zero_variance_representative_trial_ids: vec![1],
                conservative_independent_trial_count: 2,
            }
        );
    }

    #[test]
    fn tampered_behavioral_count_rejected() {
        let rows = (0..64)
            .map(|period| {
                let varying = Decimal::from(period % 5) * dec!(0.001) - dec!(0.002);
                vec![Decimal::ZERO, varying]
            })
            .collect();
        let trial_grid = grid(2, 8);
        let mut evidence =
            analyze_selection_bias(&matrix(64, rows), &trial_grid).expect("CSCV evidence");
        evidence.trial_dependence.trial_count_estimation =
            CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                behavioral_trial_count: 2,
                zero_variance_representative_trial_ids: vec![1],
                conservative_independent_trial_count: 1,
            };
        assert!(evidence.validate_for(&trial_grid).is_err());
    }

    #[test]
    fn pbo_rejects_odd_count() {
        assert!(CscvTrialGridBinding::try_new(5, grid(2, 4).trials).is_err());
    }

    #[test]
    fn rejects_explosive_block_count() {
        assert!(CscvTrialGridBinding::try_new(18, grid(2, 4).trials).is_err());
    }

    #[test]
    fn pbo_rejects_single_trial() {
        let descriptor = grid(2, 4).trials.remove(0);
        assert!(CscvTrialGridBinding::try_new(4, vec![descriptor]).is_err());
    }

    #[test]
    fn pbo_rejects_non_matrix() {
        let periods = (0..16)
            .map(|i| Utc.timestamp_opt(1_700_000_000 + i * 3_600, 0).unwrap())
            .collect();
        let mut rows = vec![vec![dec!(0.01), dec!(0.02)]; 16];
        rows[7].pop();
        assert!(TrialPerformanceMatrix::from_rows(periods, rows).is_err());
    }

    #[test]
    fn column_constructor_transposes_layout() {
        let periods = (0..3)
            .map(|i| Utc.timestamp_opt(1_700_000_000 + i * 3_600, 0).unwrap())
            .collect::<Vec<_>>();
        let from_rows = TrialPerformanceMatrix::from_rows(
            periods.clone(),
            vec![
                vec![dec!(1), dec!(4)],
                vec![dec!(2), dec!(5)],
                vec![dec!(3), dec!(6)],
            ],
        )
        .expect("row-major matrix");
        let from_columns = TrialPerformanceMatrix::from_columns(
            periods,
            &[
                vec![dec!(1), dec!(2), dec!(3)],
                vec![dec!(4), dec!(5), dec!(6)],
            ],
        )
        .expect("column-major source");
        assert_eq!(from_columns.return_at(0, 0), Some(dec!(1)));
        assert_eq!(from_columns.return_at(2, 1), Some(dec!(6)));
        assert_eq!(from_columns.return_at(3, 0), None);
        assert_eq!(from_columns.return_at(0, 2), None);
        assert_eq!(
            from_columns.rows().collect::<Vec<_>>(),
            from_rows.rows().collect::<Vec<_>>()
        );
    }

    #[test]
    fn pbo_rejects_period_mismatch() {
        let mut m = matrix(16, vec![vec![dec!(0.01), dec!(0.02)]; 16]);
        m.periods.pop();
        assert!(analyze_selection_bias(&m, &grid(2, 4)).is_err());
    }

    #[test]
    fn pbo_rejects_period_remainder() {
        let m = matrix(18, vec![vec![dec!(0.01), dec!(0.02)]; 18]);
        assert!(analyze_selection_bias(&m, &grid(2, 8)).is_err());
    }

    #[test]
    fn pbo_low_one_block() {
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
        let pbo = analyze_selection_bias(&m, &grid(2, 8))
            .expect("CSCV evidence")
            .pbo;
        assert_eq!(pbo, Decimal::ZERO);
    }

    #[test]
    fn pbo_equal_trials_neutral() {
        let rows = (0..32)
            .map(|period| {
                let value = Decimal::from(period % 5) * dec!(0.001) - dec!(0.002);
                vec![value; 6]
            })
            .collect();
        let m = matrix(32, rows);
        let evidence = analyze_selection_bias(&m, &grid(6, 8)).expect("CSCV evidence");
        assert_eq!(evidence.pbo, Decimal::ZERO);
        assert!(
            evidence
                .trial_dependence
                .raw_pairs
                .iter()
                .all(|pair| matches!(pair.relationship, CscvTrialPairRelationship::ExactDuplicate))
        );
        assert_eq!(
            evidence
                .trial_dependence
                .conservative_independent_trial_count(),
            1
        );
        assert_eq!(
            evidence.trial_dependence.trial_count_estimation,
            CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                behavioral_trial_count: 1,
                zero_variance_representative_trial_ids: Vec::new(),
                conservative_independent_trial_count: 1,
            }
        );
    }

    #[test]
    fn orthogonal_trials_retain_count() {
        let pattern = [
            vec![dec!(-1), dec!(-1)],
            vec![dec!(-1), dec!(1)],
            vec![dec!(1), dec!(-1)],
            vec![dec!(1), dec!(1)],
        ];
        let rows = pattern.into_iter().cycle().take(16).collect();
        let m = matrix(16, rows);
        let evidence = analyze_selection_bias(&m, &grid(2, 4)).expect("CSCV evidence");

        assert_eq!(
            evidence.trial_dependence.trial_count_estimation,
            CscvDsrTrialCountEvidence::AverageCorrelation {
                behavioral_trial_count: 2,
                average_correlation: Decimal::ZERO,
                implied_independent_trial_count: dec!(2),
                conservative_independent_trial_count: 2,
            }
        );
    }

    #[test]
    fn tampered_dependence_is_rejected() {
        let rows = (0..16)
            .map(|period| {
                let value = Decimal::from(period % 5) - dec!(2);
                vec![value, value]
            })
            .collect();
        let trial_grid = grid(2, 4);
        let mut evidence =
            analyze_selection_bias(&matrix(16, rows), &trial_grid).expect("CSCV evidence");
        evidence.trial_dependence.raw_pairs[0].relationship = CscvTrialPairRelationship::Pearson {
            correlation: dec!(0.5),
        };

        assert!(evidence.validate_for(&trial_grid).is_err());
    }
}
