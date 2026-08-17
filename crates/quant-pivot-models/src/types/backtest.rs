//! Canonical strongly typed backtest persistence documents.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
    vec::IntoIter,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::{Decimal, MathematicalOps, prelude::ToPrimitive};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::common::MarketCategory,
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        ModelVersionId, PortfolioRejectionReason, Probability, TrainingDatasetId,
    },
};

const CPCV_EVIDENCE_SCHEMA_VERSION: u32 = 10;
/// Smallest symmetric CSCV partition count with non-trivial complementary
/// train/test selections.
pub const CSCV_MIN_BLOCK_COUNT: u32 = 4;
/// Governed combinatorial ceiling. `S=16` already yields 12,870 symmetric
/// selections; larger values create an unbounded evidence and compute surface.
pub const CSCV_MAX_BLOCK_COUNT: u32 = 16;
const BACKTEST_PORTFOLIO_FUNNEL_SCHEMA_VERSION: u32 = 1;
/// Fixed decimal precision used by deterministic backtest/comparison metrics.
pub const BACKTEST_METRIC_SCALE: u32 = 12;

/// Deeply verified serving subject bound into one persisted CPCV path set.
///
/// Relational subject IDs remain native columns on `quant_backtest_path_set`.
/// These hashes bind the exact immutable bytes and semantic contracts behind
/// those IDs so a cached path set can be reverified without trusting scalar
/// foreign keys alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct CpcvPathSetSubject {
    pub schema_version: u32,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_dataset_hash: ContentHash,
    pub dataset_manifest_hash: ContentHash,
    pub dataset_artifact_bytes_hash: ContentHash,
    pub policy_snapshot_hash: ContentHash,
}

impl CpcvPathSetSubject {
    #[must_use]
    pub const fn new(
        model_artifact_hash: ContentHash,
        serving_contract_hash: ContentHash,
        training_dataset_hash: ContentHash,
        dataset_manifest_hash: ContentHash,
        dataset_artifact_bytes_hash: ContentHash,
        policy_snapshot_hash: ContentHash,
    ) -> Self {
        Self {
            schema_version: CPCV_EVIDENCE_SCHEMA_VERSION,
            model_artifact_hash,
            serving_contract_hash,
            training_dataset_hash,
            dataset_manifest_hash,
            dataset_artifact_bytes_hash,
            policy_snapshot_hash,
        }
    }

    pub const fn validate(&self) -> Result<(), CpcvEvidenceError> {
        validate_evidence_version(self.schema_version)
    }
}

/// Explicit policy used when a weighted CPCV fold must be resealed without the
/// subject model's production calibration dependency.
///
/// Fold estimators are newly trained and therefore cannot inherit a calibrator
/// fitted against the subject estimator. A calibrated subject must resolve its
/// verified uncalibrated parent and bind that exact parent's heuristic return
/// model; clearing calibration without this lineage is forbidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CpcvFoldCalibrationPolicy {
    /// Classical model families do not carry the weighted return-model layer.
    NotApplicable,
    /// The subject is already an uncalibrated root; preserve its exact
    /// heuristic return model.
    SubjectHeuristic { return_model_hash: ContentHash },
    /// The subject is a calibrated child. Fold training uses the exact
    /// heuristic return model from its deeply verified parent.
    CalibratedSubjectParentHeuristic {
        calibration_artifact_id: CalibrationArtifactId,
        calibration_hash: ContentHash,
        parent_model_version_id: ModelVersionId,
        parent_artifact_hash: ContentHash,
        parent_serving_contract_hash: ContentHash,
        parent_return_model_hash: ContentHash,
    },
}

/// Exact precommitted complete OOS path used by the serving subject and every
/// governed trial column for DSR/CSCV selection statistics.
///
/// `combination_indices` is the strictly increasing set of unique subject
/// `C(N,k)` folds needed to reconstruct `path_index`. Keeping this binding in
/// the methodology makes sparse exact path projection auditable and forces
/// the observed subject statistic and its multiple-testing population onto
/// one identical OOS functional. The complete subject CPCV path distribution
/// remains the source for distributional robustness gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CpcvTrialPathBinding {
    pub path_index: u32,
    pub combination_indices: Vec<u32>,
}

/// One predeclared strategy configuration in the CSCV selection population.
///
/// The label is operator-facing evidence, while `config_hash` is the canonical
/// identity of the complete executable configuration. Trial identifiers are
/// positional and must be contiguous from zero so deterministic tie-breaking
/// cannot change when callers reorder an input collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialDescriptor {
    pub trial_id: u32,
    pub label: String,
    pub config_hash: ContentHash,
}

/// Pre-run commitment to the exact CSCV strategy-selection population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialGridBinding {
    pub block_count: u32,
    pub trials: Vec<CscvTrialDescriptor>,
}

impl CscvTrialGridBinding {
    pub fn try_new(
        block_count: u32,
        trials: Vec<CscvTrialDescriptor>,
    ) -> Result<Self, CpcvEvidenceError> {
        let value = Self {
            block_count,
            trials,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), CpcvEvidenceError> {
        if self.block_count < CSCV_MIN_BLOCK_COUNT
            || self.block_count > CSCV_MAX_BLOCK_COUNT
            || !self.block_count.is_multiple_of(2)
        {
            return Err(CpcvEvidenceError::InvalidCscvBlockCount {
                block_count: self.block_count,
            });
        }
        if self.trials.len() < 2 {
            return Err(CpcvEvidenceError::InsufficientCscvTrials {
                trial_count: self.trials.len(),
            });
        }
        for (position, trial) in self.trials.iter().enumerate() {
            let expected = u32::try_from(position)
                .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV trial" })?;
            if trial.trial_id != expected {
                return Err(CpcvEvidenceError::NonCanonicalTrialId {
                    expected,
                    actual: trial.trial_id,
                });
            }
            if trial.label.trim().is_empty() {
                return Err(CpcvEvidenceError::EmptyCscvTrialLabel {
                    trial_id: trial.trial_id,
                });
            }
            if self.trials[..position]
                .iter()
                .any(|prior| prior.label == trial.label || prior.config_hash == trial.config_hash)
            {
                return Err(CpcvEvidenceError::DuplicateCscvTrial {
                    trial_id: trial.trial_id,
                });
            }
        }
        Ok(())
    }
}

/// Exact sufficient statistics for one trial within one equal-length CSCV
/// time block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialBlockStatistic {
    pub trial_id: u32,
    pub observation_count: u64,
    pub return_sum: Decimal,
    pub squared_return_sum: Decimal,
}

/// One contiguous equal-length CSCV time block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvBlockEvidence {
    pub block_index: u32,
    pub first_period: DateTime<Utc>,
    pub last_period: DateTime<Utc>,
    pub trial_statistics: Vec<CscvTrialBlockStatistic>,
}

/// Full-window performance used to calculate the DSR trial dispersion from
/// the same selection population used by CSCV/PBO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialPerformance {
    pub trial_id: u32,
    pub full_sample_sharpe: Decimal,
}

/// One non-redundant executable strategy induced by the precommitted trial grid.
///
/// Hyperparameter configurations remain separate governed trials, but exact
/// equality of their complete OOS return columns means they are the same
/// random variable for PBO/DSR. `representative_trial_id` is the smallest raw
/// trial identifier in the class and is the only member admitted to the CSCV
/// rank population. Every raw trial remains present in exactly one class so
/// unfavorable and no-trade results cannot be discarded after observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialEquivalenceClass {
    pub class_id: u32,
    pub representative_trial_id: u32,
    pub member_trial_ids: Vec<u32>,
}

/// Mathematically identified relationship between two governed trial-return
/// columns.
///
/// Pearson correlation is not defined when either non-duplicate column has
/// zero variance. That case is represented explicitly; it is never assigned a
/// synthetic zero correlation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CscvTrialPairRelationship {
    /// The two complete return columns are byte-for-byte economically equal.
    /// They are fully redundant even when both columns are constant.
    ExactDuplicate,
    /// Both columns have positive variance and a defined Pearson correlation.
    Pearson { correlation: Decimal },
    /// At least one non-duplicate column has exactly zero variance.
    ZeroVariance {
        left_zero_variance: bool,
        right_zero_variance: bool,
    },
}

/// Full-window pairwise dependence sufficient statistic for two governed
/// trial-return columns.
///
/// `cross_product_sum = Σ r_left,t · r_right,t`; together with the per-trial
/// sums and squared sums already frozen in [`CscvBlockEvidence`], it permits
/// independent recomputation of covariance, relationship classification, and
/// exact duplicate detection without retaining the process-sized trial matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialPairDependence {
    pub left_trial_id: u32,
    pub right_trial_id: u32,
    pub observation_count: u64,
    pub cross_product_sum: Decimal,
    pub relationship: CscvTrialPairRelationship,
}

/// Exact method used to derive the DSR multiple-testing count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "method")]
pub enum CscvDsrTrialCountEvidence {
    /// Every pair has an identified correlation, so Bailey and López de
    /// Prado's equal-weight interpolation is applicable.
    AverageCorrelation {
        /// Number of exact OOS-return equivalence classes before dependence
        /// adjustment. This is never the raw hyperparameter-grid cardinality.
        behavioral_trial_count: u32,
        average_correlation: Decimal,
        /// Fractional `N_hat = rho_bar + (1 - rho_bar) · M` before
        /// conservative integer quantization.
        implied_independent_trial_count: Decimal,
        /// Ceiling of `implied_independent_trial_count`; this is the DSR N.
        conservative_independent_trial_count: u32,
    },
    /// A single behavioral class or a zero-variance behavioral class makes
    /// average-correlation interpolation unidentified. Use the complete count
    /// of non-redundant OOS-return classes directly. This remains conservative
    /// with respect to unknown dependence without multiplying an identical
    /// strategy merely because several configurations produced it.
    DirectBehavioralClassCount {
        behavioral_trial_count: u32,
        zero_variance_representative_trial_ids: Vec<u32>,
        conservative_independent_trial_count: u32,
    },
}

/// Dependence-aware DSR trial-count evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvTrialDependenceEvidence {
    /// Exhaustive pair ledger over the raw precommitted configuration grid.
    pub raw_pair_count: u64,
    pub raw_pairs: Vec<CscvTrialPairDependence>,
    /// Canonical partition of every raw trial into exact OOS-return classes.
    pub equivalence_classes: Vec<CscvTrialEquivalenceClass>,
    /// Pair count over class representatives used by dependence adjustment.
    pub behavioral_pair_count: u64,
    pub trial_count_estimation: CscvDsrTrialCountEvidence,
}

impl CscvTrialDependenceEvidence {
    /// DSR N frozen by the identified estimation branch.
    #[must_use]
    pub const fn conservative_independent_trial_count(&self) -> u32 {
        match &self.trial_count_estimation {
            CscvDsrTrialCountEvidence::AverageCorrelation {
                conservative_independent_trial_count,
                ..
            }
            | CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                conservative_independent_trial_count,
                ..
            } => *conservative_independent_trial_count,
        }
    }
}

#[derive(Clone, Copy)]
struct CscvTrialAggregate {
    return_sum: Decimal,
    squared_return_sum: Decimal,
}

impl CscvTrialAggregate {
    fn variation(self, observations: Decimal) -> Result<Decimal, CpcvEvidenceError> {
        self.squared_return_sum
            .checked_mul(observations)
            .and_then(|value| {
                self.return_sum
                    .checked_mul(self.return_sum)
                    .and_then(|squared| value.checked_sub(squared))
            })
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)
    }
}

impl CscvTrialPairDependence {
    fn recompute(
        &self,
        left: CscvTrialAggregate,
        right: CscvTrialAggregate,
        observations: Decimal,
    ) -> Result<CscvTrialPairRelationship, CpcvEvidenceError> {
        let twice_cross = self
            .cross_product_sum
            .checked_mul(Decimal::TWO)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let squared_difference = left
            .squared_return_sum
            .checked_add(right.squared_return_sum)
            .and_then(|value| value.checked_sub(twice_cross))
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let exact_duplicate_returns = squared_difference == Decimal::ZERO;
        if exact_duplicate_returns {
            return Ok(CscvTrialPairRelationship::ExactDuplicate);
        }
        let left_variation = left.variation(observations)?;
        let right_variation = right.variation(observations)?;
        if left_variation < Decimal::ZERO || right_variation < Decimal::ZERO {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        if left_variation == Decimal::ZERO || right_variation == Decimal::ZERO {
            return Ok(CscvTrialPairRelationship::ZeroVariance {
                left_zero_variance: left_variation == Decimal::ZERO,
                right_zero_variance: right_variation == Decimal::ZERO,
            });
        }
        let covariance = self
            .cross_product_sum
            .checked_mul(observations)
            .and_then(|value| {
                left.return_sum
                    .checked_mul(right.return_sum)
                    .and_then(|product| value.checked_sub(product))
            })
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let variance_product = left_variation
            .checked_mul(right_variation)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let denominator = variance_product
            .sqrt()
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let correlation = covariance
            .checked_div(denominator)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?
            .clamp(-Decimal::ONE, Decimal::ONE)
            .round_dp(BACKTEST_METRIC_SCALE);
        Ok(CscvTrialPairRelationship::Pearson { correlation })
    }
}

/// One symmetric IS/OOS CSCV selection observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvCombinationEvidence {
    pub combination_index: u32,
    pub in_sample_block_indices: Vec<u32>,
    pub champion_trial_id: u32,
    pub in_sample_sharpe: Decimal,
    pub out_of_sample_sharpe: Decimal,
    /// Twice the one-based OOS midrank. This preserves tied ranks exactly
    /// without persisting a binary floating-point logit.
    pub out_of_sample_rank_twice: u32,
    pub below_oos_median: bool,
    pub out_of_sample_loss: bool,
}

/// Explicit reason why the IS-to-OOS degradation regression is undefined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CscvDegradationUndefinedReason {
    ConstantInSampleChampionPerformance,
    ConstantOutOfSampleChampionPerformance,
}

/// Bailey-style performance-degradation regression for IS champions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "status")]
pub enum CscvDegradationEvidence {
    Estimated {
        intercept: Decimal,
        slope: Decimal,
        r_squared: Decimal,
    },
    Undefined {
        reason: CscvDegradationUndefinedReason,
    },
}

/// Empirical stochastic-dominance relation of IS-selected OOS performance
/// against the complete OOS trial population.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CscvDominanceRelation {
    SelectedDominates,
    Equivalent,
    NoSelectedDominance,
}

/// First- and second-order stochastic-dominance diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CscvDominanceEvidence {
    pub evaluation_point_count: u64,
    pub first_order: CscvDominanceRelation,
    pub second_order: CscvDominanceRelation,
    /// Maximum amount by which the selected-performance CDF exceeds the
    /// complete-population CDF. Positive values violate first-order dominance.
    pub max_selected_cdf_excess: Decimal,
    /// Minimum integrated `(population CDF - selected CDF)` value. Negative
    /// values violate second-order dominance.
    pub min_integrated_cdf_advantage: Decimal,
    pub max_integrated_cdf_advantage: Decimal,
}

/// Durable, independently recomputable CSCV/PBO and DSR-dispersion evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct CscvSelectionEvidence {
    pub schema_version: u32,
    pub period_count: u64,
    pub period_axis_hash: ContentHash,
    pub block_count: u32,
    pub block_length: u64,
    pub blocks: Vec<CscvBlockEvidence>,
    pub trial_performances: Vec<CscvTrialPerformance>,
    /// Population variance of full-sample Sharpe across exact OOS-return
    /// equivalence-class representatives, never across duplicated raw trials.
    pub behavioral_trial_sharpe_variance: Decimal,
    pub trial_dependence: CscvTrialDependenceEvidence,
    pub combinations: Vec<CscvCombinationEvidence>,
    pub negative_logit_count: u64,
    pub pbo: Decimal,
    pub out_of_sample_loss_count: u64,
    pub out_of_sample_loss_probability: Decimal,
    pub performance_degradation: CscvDegradationEvidence,
    pub stochastic_dominance: CscvDominanceEvidence,
}

impl CscvSelectionEvidence {
    #[must_use]
    pub const fn schema_version() -> u32 {
        CPCV_EVIDENCE_SCHEMA_VERSION
    }

    pub fn validate_for(&self, grid: &CscvTrialGridBinding) -> Result<(), CpcvEvidenceError> {
        validate_evidence_version(self.schema_version)?;
        grid.validate()?;
        if self.block_count != grid.block_count {
            return Err(CpcvEvidenceError::CscvBlockCountMismatch {
                expected: grid.block_count,
                actual: self.block_count,
            });
        }
        let trial_count = grid.trials.len();
        let block_count = usize::try_from(self.block_count)
            .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV block" })?;
        let period_count = usize::try_from(self.period_count).map_err(|_| {
            CpcvEvidenceError::FoldIdentityOverflow {
                kind: "CSCV period",
            }
        })?;
        let block_length = usize::try_from(self.block_length).map_err(|_| {
            CpcvEvidenceError::FoldIdentityOverflow {
                kind: "CSCV block length",
            }
        })?;
        if block_length == 0
            || period_count % block_count != 0
            || block_length.checked_mul(block_count) != Some(period_count)
        {
            return Err(CpcvEvidenceError::InvalidCscvPeriodShape {
                period_count: self.period_count,
                block_count: self.block_count,
                block_length: self.block_length,
            });
        }
        if self.blocks.len() != block_count
            || self.trial_performances.len() != trial_count
            || self.behavioral_trial_sharpe_variance < Decimal::ZERO
        {
            return Err(CpcvEvidenceError::InvalidCscvEvidenceShape);
        }
        for (position, block) in self.blocks.iter().enumerate() {
            let expected = u32::try_from(position)
                .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV block" })?;
            if block.block_index != expected
                || block.first_period > block.last_period
                || block.trial_statistics.len() != trial_count
                || position > 0 && self.blocks[position - 1].last_period >= block.first_period
            {
                return Err(CpcvEvidenceError::InvalidCscvBlock {
                    block_index: block.block_index,
                });
            }
            for (trial_position, statistic) in block.trial_statistics.iter().enumerate() {
                let expected_trial = u32::try_from(trial_position).map_err(|_| {
                    CpcvEvidenceError::FoldIdentityOverflow {
                        kind: "CSCV block trial",
                    }
                })?;
                if statistic.trial_id != expected_trial
                    || statistic.observation_count != self.block_length
                    || statistic.squared_return_sum < Decimal::ZERO
                {
                    return Err(CpcvEvidenceError::InvalidCscvBlockTrial {
                        block_index: block.block_index,
                        trial_id: statistic.trial_id,
                    });
                }
            }
        }
        for (position, performance) in self.trial_performances.iter().enumerate() {
            let expected =
                u32::try_from(position).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
                    kind: "CSCV trial performance",
                })?;
            if performance.trial_id != expected {
                return Err(CpcvEvidenceError::NonCanonicalTrialId {
                    expected,
                    actual: performance.trial_id,
                });
            }
        }
        let behavioral_trials = self.validate_dependence(trial_count)?;
        self.validate_behavioral_variance(&behavioral_trials)?;
        self.validate_combinations(&behavioral_trials, block_count)?;
        self.validate_probabilities()?;
        if self.stochastic_dominance.evaluation_point_count == 0
            || self.stochastic_dominance.max_selected_cdf_excess < Decimal::ZERO
        {
            return Err(CpcvEvidenceError::InvalidCscvDominance);
        }
        if let CscvDegradationEvidence::Estimated { r_squared, .. } = &self.performance_degradation
            && !(Decimal::ZERO..=Decimal::ONE).contains(r_squared)
        {
            return Err(CpcvEvidenceError::InvalidCscvDegradation);
        }
        Ok(())
    }

    fn validate_dependence(&self, trial_count: usize) -> Result<Vec<usize>, CpcvEvidenceError> {
        let (expected_pairs, expected_pair_count) = Self::dependence_pair_count(trial_count)?;
        if self.trial_dependence.raw_pair_count != expected_pair_count
            || self.trial_dependence.raw_pairs.len() != expected_pairs
        {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        let aggregates = self.trial_aggregates(trial_count)?;
        let observations = Decimal::from(self.period_count);
        let variations = aggregates
            .iter()
            .map(|aggregate| aggregate.variation(observations))
            .collect::<Result<Vec<_>, _>>()?;
        if variations
            .iter()
            .any(|variation| *variation < Decimal::ZERO)
        {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }

        let mut pair_position = 0_usize;
        let mut relationships = BTreeMap::new();
        for left in 0..trial_count {
            for right in left + 1..trial_count {
                let relationship =
                    self.validate_pair(pair_position, left, right, observations, &aggregates)?;
                let left_id =
                    u32::try_from(left).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
                let right_id =
                    u32::try_from(right).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
                if relationships
                    .insert((left_id, right_id), relationship)
                    .is_some()
                {
                    return Err(CpcvEvidenceError::InvalidCscvDependence);
                }
                pair_position = pair_position
                    .checked_add(1)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
            }
        }

        let expected_classes = Self::equivalence_classes(trial_count, &relationships)?;
        if self.trial_dependence.equivalence_classes != expected_classes {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        let representatives = expected_classes
            .iter()
            .map(|class| {
                usize::try_from(class.representative_trial_id)
                    .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (behavioral_pairs, behavioral_pair_count) =
            Self::behavioral_pair_count(representatives.len())?;
        if self.trial_dependence.behavioral_pair_count != behavioral_pair_count {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        let zero_variance_representative_trial_ids = representatives
            .iter()
            .filter(|&&representative| variations[representative] == Decimal::ZERO)
            .map(|&representative| {
                u32::try_from(representative).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)
            })
            .collect::<Result<Vec<_>, _>>()?;

        if representatives.len() == 1 || !zero_variance_representative_trial_ids.is_empty() {
            let behavioral_trial_count = u32::try_from(representatives.len())
                .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
            return match &self.trial_dependence.trial_count_estimation {
                CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                    behavioral_trial_count: actual_behavioral_count,
                    zero_variance_representative_trial_ids: actual_zero_variance_ids,
                    conservative_independent_trial_count,
                } if *actual_behavioral_count == behavioral_trial_count
                    && *actual_zero_variance_ids == zero_variance_representative_trial_ids
                    && *conservative_independent_trial_count == behavioral_trial_count =>
                {
                    Ok(representatives)
                }
                CscvDsrTrialCountEvidence::DirectBehavioralClassCount { .. }
                | CscvDsrTrialCountEvidence::AverageCorrelation { .. } => {
                    Err(CpcvEvidenceError::InvalidCscvDependence)
                }
            };
        }

        let mut correlation_sum = Decimal::ZERO;
        for (left_position, &left) in representatives.iter().enumerate() {
            for &right in representatives.iter().skip(left_position + 1) {
                let left_id =
                    u32::try_from(left).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
                let right_id =
                    u32::try_from(right).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
                let relationship = relationships
                    .get(&(left_id, right_id))
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                let CscvTrialPairRelationship::Pearson { correlation } = relationship else {
                    return Err(CpcvEvidenceError::InvalidCscvDependence);
                };
                correlation_sum = correlation_sum
                    .checked_add(*correlation)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
            }
        }
        self.validate_dependence_summary(
            representatives,
            behavioral_pairs,
            behavioral_pair_count,
            correlation_sum,
        )
    }

    fn dependence_pair_count(trial_count: usize) -> Result<(usize, u64), CpcvEvidenceError> {
        let expected_pairs = trial_count
            .checked_mul(trial_count.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .filter(|count| *count > 0)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let expected_pair_count =
            u64::try_from(expected_pairs).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
        Ok((expected_pairs, expected_pair_count))
    }

    fn behavioral_pair_count(trial_count: usize) -> Result<(usize, u64), CpcvEvidenceError> {
        if trial_count < 2 {
            return Ok((0, 0));
        }
        Self::dependence_pair_count(trial_count)
    }

    fn equivalence_classes(
        trial_count: usize,
        relationships: &BTreeMap<(u32, u32), CscvTrialPairRelationship>,
    ) -> Result<Vec<CscvTrialEquivalenceClass>, CpcvEvidenceError> {
        let mut representative_by_trial = Vec::with_capacity(trial_count);
        for trial in 0..trial_count {
            let trial_id =
                u32::try_from(trial).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
            let mut representative = trial_id;
            for prior in 0..trial {
                let prior_id =
                    u32::try_from(prior).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
                if relationships.get(&(prior_id, trial_id))
                    == Some(&CscvTrialPairRelationship::ExactDuplicate)
                {
                    representative = *representative_by_trial
                        .get(prior)
                        .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                    break;
                }
            }
            representative_by_trial.push(representative);
        }
        let mut members_by_representative = BTreeMap::<u32, Vec<u32>>::new();
        for (trial, representative) in representative_by_trial.into_iter().enumerate() {
            members_by_representative
                .entry(representative)
                .or_default()
                .push(u32::try_from(trial).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?);
        }
        members_by_representative
            .into_iter()
            .enumerate()
            .map(|(class, (representative_trial_id, member_trial_ids))| {
                if member_trial_ids.first().copied() != Some(representative_trial_id) {
                    return Err(CpcvEvidenceError::InvalidCscvDependence);
                }
                Ok(CscvTrialEquivalenceClass {
                    class_id: u32::try_from(class)
                        .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?,
                    representative_trial_id,
                    member_trial_ids,
                })
            })
            .collect()
    }

    fn trial_aggregates(
        &self,
        trial_count: usize,
    ) -> Result<Vec<CscvTrialAggregate>, CpcvEvidenceError> {
        let mut aggregates = Vec::with_capacity(trial_count);
        for trial in 0..trial_count {
            let mut observation_count = 0_u64;
            let mut return_sum = Decimal::ZERO;
            let mut squared_return_sum = Decimal::ZERO;
            for block in &self.blocks {
                let statistic = block
                    .trial_statistics
                    .get(trial)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                observation_count = observation_count
                    .checked_add(statistic.observation_count)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                return_sum = return_sum
                    .checked_add(statistic.return_sum)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                squared_return_sum = squared_return_sum
                    .checked_add(statistic.squared_return_sum)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
            }
            if observation_count != self.period_count {
                return Err(CpcvEvidenceError::InvalidCscvDependence);
            }
            aggregates.push(CscvTrialAggregate {
                return_sum,
                squared_return_sum,
            });
        }
        Ok(aggregates)
    }

    fn validate_pair(
        &self,
        pair_position: usize,
        left: usize,
        right: usize,
        observations: Decimal,
        aggregates: &[CscvTrialAggregate],
    ) -> Result<CscvTrialPairRelationship, CpcvEvidenceError> {
        let pair = self
            .trial_dependence
            .raw_pairs
            .get(pair_position)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let expected_left =
            u32::try_from(left).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
        let expected_right =
            u32::try_from(right).map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
        if pair.left_trial_id != expected_left
            || pair.right_trial_id != expected_right
            || pair.observation_count != self.period_count
        {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        let left_aggregate = aggregates
            .get(left)
            .copied()
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let right_aggregate = aggregates
            .get(right)
            .copied()
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let expected_relationship =
            pair.recompute(left_aggregate, right_aggregate, observations)?;
        if pair.relationship != expected_relationship {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        Ok(pair.relationship)
    }

    fn validate_dependence_summary(
        &self,
        representatives: Vec<usize>,
        expected_pairs: usize,
        expected_pair_count: u64,
        correlation_sum: Decimal,
    ) -> Result<Vec<usize>, CpcvEvidenceError> {
        if expected_pairs == 0 || expected_pair_count == 0 {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        let average_correlation = correlation_sum
            .checked_div(Decimal::from(expected_pair_count))
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?
            .round_dp(BACKTEST_METRIC_SCALE);
        let behavioral_trial_count = u32::try_from(representatives.len())
            .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
        let behavioral_trial_count_decimal = Decimal::from(
            u64::try_from(representatives.len())
                .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?,
        );
        let implied_independent_trial_count = average_correlation
            .checked_add(
                Decimal::ONE
                    .checked_sub(average_correlation)
                    .and_then(|value| value.checked_mul(behavioral_trial_count_decimal))
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)?,
            )
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?
            .round_dp(BACKTEST_METRIC_SCALE);
        let conservative_independent_trial_count = implied_independent_trial_count
            .ceil()
            .to_u32()
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let minimum_average = -Decimal::ONE
            .checked_div(behavioral_trial_count_decimal - Decimal::ONE)
            .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
        let quantization_tolerance = Decimal::new(1, BACKTEST_METRIC_SCALE);
        if average_correlation < minimum_average - quantization_tolerance
            || average_correlation > Decimal::ONE
        {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        match &self.trial_dependence.trial_count_estimation {
            CscvDsrTrialCountEvidence::AverageCorrelation {
                behavioral_trial_count: actual_behavioral_count,
                average_correlation: actual_average,
                implied_independent_trial_count: actual_implied,
                conservative_independent_trial_count: actual_count,
            } if *actual_behavioral_count == behavioral_trial_count
                && *actual_average == average_correlation
                && *actual_implied == implied_independent_trial_count
                && *actual_count == conservative_independent_trial_count =>
            {
                Ok(representatives)
            }
            CscvDsrTrialCountEvidence::AverageCorrelation { .. }
            | CscvDsrTrialCountEvidence::DirectBehavioralClassCount { .. } => {
                Err(CpcvEvidenceError::InvalidCscvDependence)
            }
        }
    }

    fn validate_behavioral_variance(
        &self,
        representatives: &[usize],
    ) -> Result<(), CpcvEvidenceError> {
        let sharpes = representatives
            .iter()
            .map(|&representative| {
                self.trial_performances
                    .get(representative)
                    .map(|performance| performance.full_sample_sharpe)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected_variance = if sharpes.len() < 2 {
            Decimal::ZERO
        } else {
            let count = Decimal::from(
                u64::try_from(sharpes.len())
                    .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?,
            );
            let sum = sharpes.iter().try_fold(Decimal::ZERO, |sum, value| {
                sum.checked_add(*value)
                    .ok_or(CpcvEvidenceError::InvalidCscvDependence)
            })?;
            let mean = sum
                .checked_div(count)
                .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
            sharpes
                .iter()
                .try_fold(Decimal::ZERO, |sum, value| {
                    let centered = value
                        .checked_sub(mean)
                        .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                    let squared = centered
                        .checked_mul(centered)
                        .ok_or(CpcvEvidenceError::InvalidCscvDependence)?;
                    sum.checked_add(squared)
                        .ok_or(CpcvEvidenceError::InvalidCscvDependence)
                })?
                .checked_div(count)
                .ok_or(CpcvEvidenceError::InvalidCscvDependence)?
                .round_dp(BACKTEST_METRIC_SCALE)
        };
        if self.behavioral_trial_sharpe_variance != expected_variance {
            return Err(CpcvEvidenceError::InvalidCscvDependence);
        }
        Ok(())
    }

    fn validate_combinations(
        &self,
        behavioral_trials: &[usize],
        block_count: usize,
    ) -> Result<(), CpcvEvidenceError> {
        let trial_count = behavioral_trials.len();
        let behavioral_trial_ids = behavioral_trials
            .iter()
            .map(|trial| u32::try_from(*trial))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|_| CpcvEvidenceError::InvalidCscvDependence)?;
        let half = block_count / 2;
        let expected_count = Self::expected_combination_count(block_count, half)?;
        if self.combinations.len() != expected_count {
            return Err(CpcvEvidenceError::InvalidCscvEvidenceShape);
        }
        let mut prior_mask = None;
        for (position, combination) in self.combinations.iter().enumerate() {
            let expected =
                u32::try_from(position).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
                    kind: "CSCV combination",
                })?;
            if combination.combination_index != expected
                || combination.in_sample_block_indices.len() != half
                || combination
                    .in_sample_block_indices
                    .windows(2)
                    .any(|window| window[0] >= window[1])
                || combination
                    .in_sample_block_indices
                    .last()
                    .is_none_or(|index| {
                        usize::try_from(*index).map_or(true, |value| value >= block_count)
                    })
                || !behavioral_trial_ids.contains(&combination.champion_trial_id)
            {
                return Err(CpcvEvidenceError::InvalidCscvCombination {
                    combination_index: combination.combination_index,
                });
            }
            let mask =
                combination
                    .in_sample_block_indices
                    .iter()
                    .try_fold(0_u32, |mask, index| {
                        let bit = 1_u32.checked_shl(*index).ok_or(
                            CpcvEvidenceError::InvalidCscvCombination {
                                combination_index: combination.combination_index,
                            },
                        )?;
                        Ok::<u32, CpcvEvidenceError>(mask | bit)
                    })?;
            if prior_mask.is_some_and(|prior| prior >= mask) {
                return Err(CpcvEvidenceError::InvalidCscvCombination {
                    combination_index: combination.combination_index,
                });
            }
            prior_mask = Some(mask);
            let rank_boundary = u32::try_from(trial_count)
                .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV trial" })?
                .checked_add(1)
                .ok_or(CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV rank" })?;
            let max_rank = u32::try_from(trial_count)
                .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV trial" })?
                .checked_mul(2)
                .ok_or(CpcvEvidenceError::FoldIdentityOverflow { kind: "CSCV rank" })?;
            if combination.out_of_sample_rank_twice < 2
                || combination.out_of_sample_rank_twice > max_rank
                || combination.below_oos_median
                    != (combination.out_of_sample_rank_twice < rank_boundary)
                || combination.out_of_sample_loss
                    != (combination.out_of_sample_sharpe < Decimal::ZERO)
            {
                return Err(CpcvEvidenceError::InvalidCscvCombination {
                    combination_index: combination.combination_index,
                });
            }
        }
        Ok(())
    }

    fn expected_combination_count(
        block_count: usize,
        selected_count: usize,
    ) -> Result<usize, CpcvEvidenceError> {
        let selected_count = selected_count.min(block_count - selected_count);
        let mut result = 1_u128;
        for index in 0..selected_count {
            let factor = u128::try_from(block_count - index).map_err(|_| {
                CpcvEvidenceError::FoldIdentityOverflow {
                    kind: "CSCV combination count",
                }
            })?;
            let divisor =
                u128::try_from(index + 1).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
                    kind: "CSCV combination count",
                })?;
            result = result
                .checked_mul(factor)
                .ok_or(CpcvEvidenceError::FoldIdentityOverflow {
                    kind: "CSCV combination count",
                })?
                / divisor;
        }
        usize::try_from(result).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
            kind: "CSCV combination count",
        })
    }

    fn validate_probabilities(&self) -> Result<(), CpcvEvidenceError> {
        let combination_count = u64::try_from(self.combinations.len()).map_err(|_| {
            CpcvEvidenceError::FoldIdentityOverflow {
                kind: "CSCV combination",
            }
        })?;
        let negative_count = self
            .combinations
            .iter()
            .filter(|combination| combination.below_oos_median)
            .count();
        let loss_count = self
            .combinations
            .iter()
            .filter(|combination| combination.out_of_sample_loss)
            .count();
        let negative_count =
            u64::try_from(negative_count).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
                kind: "CSCV negative logit",
            })?;
        let loss_count =
            u64::try_from(loss_count).map_err(|_| CpcvEvidenceError::FoldIdentityOverflow {
                kind: "CSCV OOS loss",
            })?;
        let expected_pbo = (Decimal::from(negative_count) / Decimal::from(combination_count))
            .round_dp(BACKTEST_METRIC_SCALE);
        let expected_loss = (Decimal::from(loss_count) / Decimal::from(combination_count))
            .round_dp(BACKTEST_METRIC_SCALE);
        if self.negative_logit_count != negative_count
            || self.pbo != expected_pbo
            || self.out_of_sample_loss_count != loss_count
            || self.out_of_sample_loss_probability != expected_loss
        {
            return Err(CpcvEvidenceError::InvalidCscvProbabilities);
        }
        Ok(())
    }
}

impl CpcvTrialPathBinding {
    pub fn try_new(
        path_index: u32,
        combination_indices: Vec<u32>,
    ) -> Result<Self, CpcvEvidenceError> {
        let value = Self {
            path_index,
            combination_indices,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), CpcvEvidenceError> {
        if self.combination_indices.is_empty() {
            return Err(CpcvEvidenceError::EmptyTrialPathBinding {
                path_index: self.path_index,
            });
        }
        if self
            .combination_indices
            .windows(2)
            .any(|window| window[0] >= window[1])
        {
            return Err(CpcvEvidenceError::NonCanonicalTrialPathBinding {
                path_index: self.path_index,
            });
        }
        Ok(())
    }
}

/// Complete governed methodology commitments for one CPCV run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct CpcvMethodologyBinding {
    pub schema_version: u32,
    pub config_hash: ContentHash,
    pub portfolio_caps_hash: ContentHash,
    pub replay_config_hash: ContentHash,
    pub fold_calibration: CpcvFoldCalibrationPolicy,
    pub trial_path: CpcvTrialPathBinding,
    pub trial_grid: CscvTrialGridBinding,
}

impl CpcvMethodologyBinding {
    #[must_use]
    pub const fn new(
        config_hash: ContentHash,
        portfolio_caps_hash: ContentHash,
        replay_config_hash: ContentHash,
        fold_calibration: CpcvFoldCalibrationPolicy,
        trial_path: CpcvTrialPathBinding,
        trial_grid: CscvTrialGridBinding,
    ) -> Self {
        Self {
            schema_version: CPCV_EVIDENCE_SCHEMA_VERSION,
            config_hash,
            portfolio_caps_hash,
            replay_config_hash,
            fold_calibration,
            trial_path,
            trial_grid,
        }
    }

    pub fn validate(&self) -> Result<(), CpcvEvidenceError> {
        validate_evidence_version(self.schema_version)?;
        self.trial_path.validate()?;
        self.trial_grid.validate()
    }
}

/// Semantic identity of one trained estimator in the frozen CPCV evidence
/// ledger.
///
/// A validation combination is not identified by its training-row hash:
/// purge/embargo can legitimately produce equal training sets for distinct
/// held-out combinations. Its immutable identity therefore binds the
/// deterministic combination index and exact held-out partition/group sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CpcvEstimatorIdentity {
    /// One purged/embargoed CPCV combination.
    Validation {
        combination_index: u32,
        test_partitions_hash: ContentHash,
        test_partition_count: u64,
        test_groups_hash: ContentHash,
        test_group_count: u64,
    },
    /// One purge/embargo CPCV combination for a governed hyperparameter trial.
    /// Trial performance is therefore out-of-sample at every period; a
    /// full-window fit evaluated on its own training window is forbidden.
    TrialPathValidation {
        trial_id: u32,
        path_index: u32,
        combination_index: u32,
        test_partitions_hash: ContentHash,
        test_partition_count: u64,
        test_groups_hash: ContentHash,
        test_group_count: u64,
    },
}

/// Immutable evidence for one ephemeral fold/trial estimator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CpcvFoldValidationRegime {
    PredictiveUtility,
    PortfolioEconomics,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CpcvFoldArtifact {
    pub identity: CpcvEstimatorIdentity,
    pub validation_regime: CpcvFoldValidationRegime,
    pub training_groups_hash: ContentHash,
    pub training_group_count: u64,
    pub calibration_fit_groups_hash: ContentHash,
    pub calibration_fit_group_count: u64,
    pub scenario_fit_groups_hash: ContentHash,
    pub scenario_fit_group_count: u64,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub model_payload_hash: ContentHash,
    pub calibration_function_hash: ContentHash,
    pub scenario_economic_function_hash: ContentHash,
    pub calibration_artifact_hash: ContentHash,
    pub scenario_model_hash: ContentHash,
}

/// Canonically ordered, duplicate-free fold/trial artifact ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CpcvFoldArtifacts(Vec<CpcvFoldArtifact>);

impl CpcvFoldArtifacts {
    pub fn try_new(mut artifacts: Vec<CpcvFoldArtifact>) -> Result<Self, CpcvEvidenceError> {
        if artifacts.is_empty() {
            return Err(CpcvEvidenceError::EmptyFoldArtifacts);
        }
        artifacts.sort_by_key(|artifact| artifact.identity);
        let value = Self(artifacts);
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), CpcvEvidenceError> {
        if self.0.is_empty() {
            return Err(CpcvEvidenceError::EmptyFoldArtifacts);
        }
        if self
            .0
            .windows(2)
            .any(|window| window[0].identity >= window[1].identity)
        {
            return Err(CpcvEvidenceError::NonCanonicalFoldArtifacts);
        }
        let validation_regime = self.0[0].validation_regime;
        if self
            .0
            .iter()
            .any(|artifact| artifact.validation_regime != validation_regime)
        {
            return Err(CpcvEvidenceError::MixedValidationRegimes);
        }
        let mut expected_validation_index = 0u32;
        let mut validation_holdouts = Vec::new();
        let mut trials = BTreeMap::<u32, (u32, Vec<u32>)>::new();
        for artifact in &self.0 {
            Self::validate_artifact(artifact)?;
            match artifact.identity {
                CpcvEstimatorIdentity::Validation {
                    combination_index,
                    test_partitions_hash,
                    test_partition_count,
                    test_groups_hash,
                    test_group_count,
                } => {
                    if test_partition_count == 0 || test_group_count == 0 {
                        return Err(CpcvEvidenceError::EmptyTestGroups {
                            identity: artifact.identity,
                        });
                    }
                    if combination_index != expected_validation_index {
                        return Err(CpcvEvidenceError::NonCanonicalValidationIndex {
                            expected: expected_validation_index,
                            actual: combination_index,
                        });
                    }
                    expected_validation_index = expected_validation_index
                        .checked_add(1)
                        .ok_or(CpcvEvidenceError::FoldIdentityOverflow { kind: "validation" })?;
                    validation_holdouts.push((
                        test_partitions_hash,
                        test_partition_count,
                        test_groups_hash,
                        test_group_count,
                    ));
                }
                CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id,
                    path_index,
                    combination_index,
                    test_partitions_hash,
                    test_partition_count,
                    test_groups_hash,
                    test_group_count,
                } => {
                    if test_partition_count == 0 || test_group_count == 0 {
                        return Err(CpcvEvidenceError::EmptyTestGroups {
                            identity: artifact.identity,
                        });
                    }
                    let combination = usize::try_from(combination_index).map_err(|_| {
                        CpcvEvidenceError::FoldIdentityOverflow {
                            kind: "trial combination",
                        }
                    })?;
                    let expected_holdout = validation_holdouts.get(combination).ok_or(
                        CpcvEvidenceError::TrialCombinationOutsideSubject {
                            trial_id,
                            combination_index,
                        },
                    )?;
                    if *expected_holdout
                        != (
                            test_partitions_hash,
                            test_partition_count,
                            test_groups_hash,
                            test_group_count,
                        )
                    {
                        return Err(CpcvEvidenceError::TrialHoldoutMismatch {
                            trial_id,
                            combination_index,
                        });
                    }
                    let entry = trials
                        .entry(trial_id)
                        .or_insert_with(|| (path_index, Vec::new()));
                    if entry.0 != path_index {
                        return Err(CpcvEvidenceError::TrialPathIndexMismatch {
                            trial_id,
                            expected: entry.0,
                            actual: path_index,
                        });
                    }
                    entry.1.push(combination_index);
                }
            }
        }

        let mut expected_path_index = None;
        let mut expected_combinations: Option<&[u32]> = None;
        for (position, (trial_id, (path_index, combinations))) in trials.iter().enumerate() {
            let expected_trial_id = u32::try_from(position)
                .map_err(|_| CpcvEvidenceError::FoldIdentityOverflow { kind: "trial" })?;
            if *trial_id != expected_trial_id {
                return Err(CpcvEvidenceError::NonCanonicalTrialId {
                    expected: expected_trial_id,
                    actual: *trial_id,
                });
            }
            if combinations.is_empty()
                || combinations.windows(2).any(|window| window[0] >= window[1])
            {
                return Err(CpcvEvidenceError::NonCanonicalTrialCombinations {
                    trial_id: *trial_id,
                });
            }
            match expected_path_index {
                None => expected_path_index = Some(*path_index),
                Some(expected) if expected == *path_index => {}
                Some(expected) => {
                    return Err(CpcvEvidenceError::TrialPathIndexMismatch {
                        trial_id: *trial_id,
                        expected,
                        actual: *path_index,
                    });
                }
            }
            match expected_combinations {
                None => expected_combinations = Some(combinations),
                Some(expected) if expected == combinations => {}
                Some(_) => {
                    return Err(CpcvEvidenceError::TrialCombinationSetMismatch {
                        trial_id: *trial_id,
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_artifact(artifact: &CpcvFoldArtifact) -> Result<(), CpcvEvidenceError> {
        if artifact.training_group_count == 0 {
            return Err(CpcvEvidenceError::EmptyTrainingGroups {
                identity: artifact.identity,
            });
        }
        if artifact.validation_regime == CpcvFoldValidationRegime::PortfolioEconomics
            && artifact.calibration_fit_group_count == 0
        {
            return Err(CpcvEvidenceError::EmptyCalibrationGroups {
                identity: artifact.identity,
            });
        }
        if artifact.validation_regime == CpcvFoldValidationRegime::PortfolioEconomics
            && artifact.scenario_fit_group_count == 0
        {
            return Err(CpcvEvidenceError::EmptyScenarioGroups {
                identity: artifact.identity,
            });
        }
        if artifact.validation_regime == CpcvFoldValidationRegime::PredictiveUtility
            && (artifact.calibration_fit_group_count != 0 || artifact.scenario_fit_group_count != 0)
        {
            return Err(CpcvEvidenceError::NonCanonicalFoldArtifacts);
        }
        Ok(())
    }

    /// Return the single validation semantics shared by every fold and trial.
    pub fn validation_regime(&self) -> Result<CpcvFoldValidationRegime, CpcvEvidenceError> {
        self.validate()?;
        Ok(self.0[0].validation_regime)
    }

    pub fn validate_for(&self, binding: &CpcvTrialPathBinding) -> Result<(), CpcvEvidenceError> {
        self.validate()?;
        binding.validate()?;
        let Some((actual_path_index, actual_combinations)) =
            self.0.iter().find_map(|artifact| match artifact.identity {
                CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 0,
                    path_index,
                    ..
                } => Some((
                    path_index,
                    self.0
                        .iter()
                        .filter_map(|candidate| match candidate.identity {
                            CpcvEstimatorIdentity::TrialPathValidation {
                                trial_id: 0,
                                combination_index,
                                ..
                            } => Some(combination_index),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                _ => None,
            })
        else {
            return Err(CpcvEvidenceError::MissingTrialPathArtifacts);
        };
        if actual_path_index != binding.path_index {
            return Err(CpcvEvidenceError::TrialPathBindingIndexMismatch {
                expected: binding.path_index,
                actual: actual_path_index,
            });
        }
        if actual_combinations != binding.combination_indices {
            return Err(CpcvEvidenceError::TrialPathBindingFoldMismatch {
                path_index: binding.path_index,
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn validation_count(&self) -> usize {
        self.0
            .iter()
            .filter(|artifact| {
                matches!(artifact.identity, CpcvEstimatorIdentity::Validation { .. })
            })
            .count()
    }

    #[must_use]
    pub fn trial_count(&self) -> usize {
        let mut count = 0usize;
        let mut previous = None;
        for trial_id in self
            .0
            .iter()
            .filter_map(|artifact| match artifact.identity {
                CpcvEstimatorIdentity::TrialPathValidation { trial_id, .. } => Some(trial_id),
                CpcvEstimatorIdentity::Validation { .. } => None,
            })
        {
            if previous != Some(trial_id) {
                count += 1;
                previous = Some(trial_id);
            }
        }
        count
    }
}

impl Deref for CpcvFoldArtifacts {
    type Target = [CpcvFoldArtifact];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for CpcvFoldArtifacts {
    type IntoIter = IntoIter<CpcvFoldArtifact>;
    type Item = CpcvFoldArtifact;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CpcvEvidenceError {
    #[error("unsupported CPCV evidence schema version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("CPCV fold-artifact ledger must not be empty")]
    EmptyFoldArtifacts,
    #[error("CPCV fold-artifact ledger mixes predictive and portfolio validation regimes")]
    MixedValidationRegimes,
    #[error("CPCV {identity:?} artifact has no training groups")]
    EmptyTrainingGroups { identity: CpcvEstimatorIdentity },
    #[error("CPCV {identity:?} artifact has no nested calibration-fit groups")]
    EmptyCalibrationGroups { identity: CpcvEstimatorIdentity },
    #[error("CPCV {identity:?} artifact has no nested scenario-fit groups")]
    EmptyScenarioGroups { identity: CpcvEstimatorIdentity },
    #[error("CPCV {identity:?} validation artifact has no held-out partitions or groups")]
    EmptyTestGroups { identity: CpcvEstimatorIdentity },
    #[error("CPCV fold-artifact ledger must be strictly sorted and duplicate-free")]
    NonCanonicalFoldArtifacts,
    #[error("CPCV validation identities must be contiguous: expected {expected}, got {actual}")]
    NonCanonicalValidationIndex { expected: u32, actual: u32 },
    #[error(
        "CPCV trial identities must be contiguous from zero: expected {expected}, got {actual}"
    )]
    NonCanonicalTrialId { expected: u32, actual: u32 },
    #[error("CSCV block count must be even and within 4..=16, got {block_count}")]
    InvalidCscvBlockCount { block_count: u32 },
    #[error("CSCV requires at least two distinct governed trials, got {trial_count}")]
    InsufficientCscvTrials { trial_count: usize },
    #[error("CSCV trial {trial_id} has an empty audit label")]
    EmptyCscvTrialLabel { trial_id: u32 },
    #[error("CSCV trial {trial_id} duplicates an earlier label or configuration hash")]
    DuplicateCscvTrial { trial_id: u32 },
    #[error(
        "CSCV evidence block count differs from methodology: expected {expected}, got {actual}"
    )]
    CscvBlockCountMismatch { expected: u32, actual: u32 },
    #[error(
        "CSCV periods do not form equal non-empty blocks: periods={period_count}, blocks={block_count}, block_length={block_length}"
    )]
    InvalidCscvPeriodShape {
        period_count: u64,
        block_count: u32,
        block_length: u64,
    },
    #[error("CSCV evidence collections or trial variance have an invalid shape")]
    InvalidCscvEvidenceShape,
    #[error("CSCV block {block_index} is not canonical, equal-length, or time ordered")]
    InvalidCscvBlock { block_index: u32 },
    #[error("CSCV block {block_index} trial statistic {trial_id} is invalid")]
    InvalidCscvBlockTrial { block_index: u32, trial_id: u32 },
    #[error("CSCV combination {combination_index} is not canonical or internally consistent")]
    InvalidCscvCombination { combination_index: u32 },
    #[error("CSCV PBO or OOS-loss probability disagrees with its combination ledger")]
    InvalidCscvProbabilities,
    #[error("CSCV performance-degradation evidence is outside its valid domain")]
    InvalidCscvDegradation,
    #[error("CSCV stochastic-dominance evidence is outside its valid domain")]
    InvalidCscvDominance,
    #[error("CSCV trial-dependence evidence is incomplete or not independently recomputable")]
    InvalidCscvDependence,
    #[error(
        "CPCV trial {trial_id} projected combination indices must be non-empty, strictly increasing, and duplicate-free"
    )]
    NonCanonicalTrialCombinations { trial_id: u32 },
    #[error(
        "CPCV trial {trial_id} path index differs inside the ledger: expected {expected}, got {actual}"
    )]
    TrialPathIndexMismatch {
        trial_id: u32,
        actual: u32,
        expected: u32,
    },
    #[error("CPCV trial {trial_id} uses a different projected fold set")]
    TrialCombinationSetMismatch { trial_id: u32 },
    #[error(
        "CPCV trial {trial_id} combination {combination_index} is outside the subject combination ledger"
    )]
    TrialCombinationOutsideSubject {
        trial_id: u32,
        combination_index: u32,
    },
    #[error(
        "CPCV trial {trial_id} combination {combination_index} does not use the subject validation holdout"
    )]
    TrialHoldoutMismatch {
        trial_id: u32,
        combination_index: u32,
    },
    #[error("CPCV trial path {path_index} binding must contain a non-empty fold set")]
    EmptyTrialPathBinding { path_index: u32 },
    #[error(
        "CPCV trial path {path_index} binding fold indices must be strictly increasing and duplicate-free"
    )]
    NonCanonicalTrialPathBinding { path_index: u32 },
    #[error("CPCV fold ledger has no trial-path artifacts")]
    MissingTrialPathArtifacts,
    #[error(
        "CPCV fold ledger trial path index differs from methodology: expected {expected}, got {actual}"
    )]
    TrialPathBindingIndexMismatch { expected: u32, actual: u32 },
    #[error("CPCV fold ledger does not exactly cover methodology trial path {path_index}")]
    TrialPathBindingFoldMismatch { path_index: u32 },
    #[error("CPCV {kind} identity sequence overflowed u32")]
    FoldIdentityOverflow { kind: &'static str },
}

const fn validate_evidence_version(actual: u32) -> Result<(), CpcvEvidenceError> {
    if actual != CPCV_EVIDENCE_SCHEMA_VERSION {
        return Err(CpcvEvidenceError::UnsupportedVersion {
            expected: CPCV_EVIDENCE_SCHEMA_VERSION,
            actual,
        });
    }
    Ok(())
}

/// Expected-versus-realized agreement summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExpectedVsRealized {
    pub mean_expected_bps: Decimal,
    pub mean_realized_bps: Decimal,
    pub correlation: Decimal,
    pub bias_bps: Decimal,
}

/// One category's backtest metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryMetric {
    pub category: MarketCategory,
    pub sample_count: u64,
    pub realized_return_rank_correlation: Decimal,
    pub hit_rate: Probability,
    pub mean_realized_bps: Decimal,
}

/// Fixed-schema category metrics persisted as one JSONB value object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryMetrics(Vec<CategoryMetric>);

impl From<Vec<CategoryMetric>> for CategoryMetrics {
    fn from(values: Vec<CategoryMetric>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryMetrics {
    type Target = [CategoryMetric];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CategoryMetrics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for CategoryMetrics {
    type IntoIter = IntoIter<CategoryMetric>;
    type Item = CategoryMetric;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One cumulative realized-PnL point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PnlCurvePoint {
    pub decision_at: DateTime<Utc>,
    pub cumulative_realized_pnl_usd: Decimal,
}

/// Portfolio-level `PnL` simulation summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PnlSimulation {
    pub total_allocated_usd: Decimal,
    pub realized_pnl_usd: Decimal,
    pub gross_return: Decimal,
    pub pnl_curve: Vec<PnlCurvePoint>,
}

/// Canonical count of executable tiers excluded for one stable economic reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestTierExclusionCount {
    pub reason: PortfolioRejectionReason,
    pub count: u64,
}

/// Complete replay funnel from model emission through exact venue execution.
///
/// Candidate preparation and economic-tier selection use different counting
/// units, so a candidate that cannot produce an executable L2 tier is retained
/// in its own scalar instead of being mixed into tier-level rejection tallies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct BacktestPortfolioFunnel {
    pub schema_version: u32,
    pub decision_tick_count: u64,
    pub emitted_candidate_count: u64,
    pub candidate_without_executable_tier_count: u64,
    pub executable_tier_count: u64,
    pub admission_rejected_tier_count: u64,
    pub admitted_tier_count: u64,
    pub selected_tier_count: u64,
    pub executed_entry_count: u64,
    pub resolved_allocation_count: u64,
    pub no_candidate_tick_count: u64,
    pub no_executable_tier_tick_count: u64,
    pub no_selection_tick_count: u64,
    pub selected_tick_count: u64,
    pub tier_exclusion_reasons: Vec<BacktestTierExclusionCount>,
}

impl BacktestPortfolioFunnel {
    /// Construct a canonical empty funnel for a replay with no decision ticks.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            schema_version: BACKTEST_PORTFOLIO_FUNNEL_SCHEMA_VERSION,
            decision_tick_count: 0,
            emitted_candidate_count: 0,
            candidate_without_executable_tier_count: 0,
            executable_tier_count: 0,
            admission_rejected_tier_count: 0,
            admitted_tier_count: 0,
            selected_tier_count: 0,
            executed_entry_count: 0,
            resolved_allocation_count: 0,
            no_candidate_tick_count: 0,
            no_executable_tier_tick_count: 0,
            no_selection_tick_count: 0,
            selected_tick_count: 0,
            tier_exclusion_reasons: Vec::new(),
        }
    }

    /// Verify count conservation and canonical reason ordering.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != BACKTEST_PORTFOLIO_FUNNEL_SCHEMA_VERSION {
            return Err(format!(
                "backtest portfolio funnel schema must be {}, got {}",
                BACKTEST_PORTFOLIO_FUNNEL_SCHEMA_VERSION, self.schema_version
            ));
        }
        let prepared_tiers = self
            .admission_rejected_tier_count
            .checked_add(self.admitted_tier_count)
            .ok_or_else(|| "backtest portfolio funnel tier count overflowed".to_owned())?;
        if self.executable_tier_count != prepared_tiers {
            return Err(format!(
                "backtest portfolio funnel executable tiers {} != rejected {} + admitted {}",
                self.executable_tier_count,
                self.admission_rejected_tier_count,
                self.admitted_tier_count
            ));
        }
        if self.selected_tier_count > self.admitted_tier_count {
            return Err(
                "backtest portfolio funnel selected tiers exceed admitted tiers".to_owned(),
            );
        }
        if self.executed_entry_count != self.selected_tier_count {
            return Err(format!(
                "backtest portfolio funnel executed entries {} != selected tiers {}",
                self.executed_entry_count, self.selected_tier_count
            ));
        }
        if self.resolved_allocation_count > self.executed_entry_count {
            return Err(
                "backtest portfolio funnel resolved allocations exceed executed entries".to_owned(),
            );
        }
        if self.candidate_without_executable_tier_count > self.emitted_candidate_count {
            return Err(
                "backtest portfolio funnel non-executable candidates exceed emitted candidates"
                    .to_owned(),
            );
        }
        let classified_ticks = self
            .no_candidate_tick_count
            .checked_add(self.no_executable_tier_tick_count)
            .and_then(|count| count.checked_add(self.no_selection_tick_count))
            .and_then(|count| count.checked_add(self.selected_tick_count))
            .ok_or_else(|| "backtest portfolio funnel tick count overflowed".to_owned())?;
        if classified_ticks != self.decision_tick_count {
            return Err(format!(
                "backtest portfolio funnel classified ticks {classified_ticks} != decision ticks {}",
                self.decision_tick_count
            ));
        }
        if self.no_executable_tier_tick_count > self.candidate_without_executable_tier_count {
            return Err(
                "backtest portfolio funnel no-tier ticks exceed non-executable candidates"
                    .to_owned(),
            );
        }
        if self.selected_tick_count > self.selected_tier_count {
            return Err(
                "backtest portfolio funnel selected ticks exceed selected tiers".to_owned(),
            );
        }

        let mut previous = None;
        let mut excluded_tiers = 0_u64;
        for reason in &self.tier_exclusion_reasons {
            if reason.count == 0 {
                return Err(
                    "backtest portfolio funnel exclusion reasons must have positive counts"
                        .to_owned(),
                );
            }
            if previous.is_some_and(|prior| prior >= reason.reason) {
                return Err(
                    "backtest portfolio funnel exclusion reasons are not strictly ordered"
                        .to_owned(),
                );
            }
            previous = Some(reason.reason);
            excluded_tiers = excluded_tiers
                .checked_add(reason.count)
                .ok_or_else(|| "backtest portfolio exclusion count overflowed".to_owned())?;
        }
        let not_selected = self
            .admitted_tier_count
            .checked_sub(self.selected_tier_count)
            .ok_or_else(|| "backtest selected tier count exceeds admitted tiers".to_owned())?;
        let expected_excluded = self
            .admission_rejected_tier_count
            .checked_add(not_selected)
            .ok_or_else(|| "backtest expected exclusion count overflowed".to_owned())?;
        if excluded_tiers != expected_excluded {
            return Err(format!(
                "backtest portfolio funnel exclusion tally {excluded_tiers} != expected {expected_excluded}"
            ));
        }
        Ok(())
    }
}

/// Canonical hash preimage of every immutable backtest-report semantic field.
///
/// Database-only routing fields (`model_run_id`, `parquet_uri`) are deliberately
/// absent. The persisted report hash is the exact compute artifact identity and
/// therefore matches the research producer byte-for-byte.
#[derive(Debug, Serialize)]
pub struct BacktestReportHashInput<'a> {
    pub backtest_report_id: &'a BacktestReportId,
    pub model_version_id: &'a ModelVersionId,
    pub dataset_id: &'a TrainingDatasetId,
    pub decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: u64,
    pub missing_feature_count: u64,
    pub realized_return_rank_correlation: Decimal,
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: &'a ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: &'a [CategoryMetric],
    pub tail_loss: Decimal,
    pub report_pnl_simulation: &'a PnlSimulation,
    pub portfolio_funnel: &'a BacktestPortfolioFunnel,
}

impl BacktestReportHashInput<'_> {
    /// Hash the exact canonical JSON projection used by every producer and
    /// persistence verifier.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(self)
    }
}

/// Sharpe distribution across reconstructed CPCV paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SharpeDistribution {
    pub min: Decimal,
    pub p25: Decimal,
    pub median: Decimal,
    pub p75: Decimal,
    pub max: Decimal,
    pub median_max_drawdown: Option<Decimal>,
    pub median_tail_loss: Option<Decimal>,
    pub median_turnover: Option<Decimal>,
    pub baseline_uplift: Option<Decimal>,
}

/// One complete full-timeline reconstructed CPCV path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestPath {
    pub path_index: u32,
    /// Exact PIT decision clock for every reconstructed return observation.
    ///
    /// Scenario-model fitting aligns Routes on governed time buckets. Keeping
    /// this clock beside the return series prevents ordinal alignment from
    /// masquerading as contemporaneous cross-Route dependence.
    pub decision_times: Vec<DateTime<Utc>>,
    /// Expected return series used for selection and Sharpe statistics.
    pub group_returns: Vec<Decimal>,
    /// Incentive-excluded return series used for drawdown and tail-loss gates.
    pub risk_group_returns: Vec<Decimal>,
    /// Allocation-independent realized payout minus calibrated expected payout
    /// for each decision group. `None` is valid only for a validation family
    /// that does not emit Buy probability forecasts. Scenario fitting rejects
    /// every path containing `None`; optimizer `PnL` is never substituted.
    pub scenario_residuals: Vec<Option<Decimal>>,
    pub sharpe: Decimal,
    pub target_rank_ic: Decimal,
    pub max_drawdown: Decimal,
    pub tail_loss: Decimal,
    pub turnover: Option<Decimal>,
}

/// Complete reconstructed CPCV paths persisted atomically.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct BacktestPaths(Vec<BacktestPath>);

impl From<Vec<BacktestPath>> for BacktestPaths {
    fn from(paths: Vec<BacktestPath>) -> Self {
        Self(paths)
    }
}

impl Deref for BacktestPaths {
    type Target = [BacktestPath];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for BacktestPaths {
    type IntoIter = IntoIter<BacktestPath>;
    type Item = BacktestPath;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One category's candidate-versus-baseline rank-IC delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryRealizedReturnRankCorrelationDelta {
    pub category: MarketCategory,
    pub baseline_realized_return_rank_correlation: Decimal,
    pub candidate_realized_return_rank_correlation: Decimal,
    pub realized_return_rank_correlation_delta: Decimal,
}

/// Typed category comparison collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryRealizedReturnRankCorrelationDeltas(
    Vec<CategoryRealizedReturnRankCorrelationDelta>,
);

impl From<Vec<CategoryRealizedReturnRankCorrelationDelta>>
    for CategoryRealizedReturnRankCorrelationDeltas
{
    fn from(values: Vec<CategoryRealizedReturnRankCorrelationDelta>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryRealizedReturnRankCorrelationDeltas {
    type Target = [CategoryRealizedReturnRankCorrelationDelta];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for CategoryRealizedReturnRankCorrelationDeltas {
    type IntoIter = IntoIter<CategoryRealizedReturnRankCorrelationDelta>;
    type Item = CategoryRealizedReturnRankCorrelationDelta;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Canonical hash preimage of one candidate-versus-baseline comparison.
#[derive(Debug, Serialize)]
pub struct ModelComparisonHashInput<'a> {
    pub baseline_model_version_id: &'a ModelVersionId,
    pub candidate_model_version_id: &'a ModelVersionId,
    pub baseline_report_hash: &'a ContentHash,
    pub candidate_report_hash: &'a ContentHash,
    pub realized_return_rank_correlation_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: u64,
    pub category_breakdown_diff: &'a [CategoryRealizedReturnRankCorrelationDelta],
}

impl ModelComparisonHashInput<'_> {
    /// Hash the exact canonical JSON projection used by every producer and
    /// persistence verifier.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(self)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::{
        BacktestPortfolioFunnel, BacktestTierExclusionCount, CpcvEstimatorIdentity,
        CpcvEvidenceError, CpcvFoldArtifact, CpcvFoldArtifacts, CpcvFoldValidationRegime,
        CpcvTrialPathBinding, ExpectedVsRealized,
    };
    use crate::types::{ContentHash, PortfolioRejectionReason};

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    #[test]
    fn fixed_rejects_unknown_missing() {
        let valid = json!({
            "mean_expected_bps": "1",
            "mean_realized_bps": "2",
            "correlation": "0.5",
            "bias_bps": "-1"
        });
        let decoded: ExpectedVsRealized =
            serde_json::from_value(valid.clone()).expect("fixed document");
        assert_eq!(decoded.correlation, dec!(0.5));

        let mut unknown = valid.clone();
        unknown["extra"] = json!(true);
        assert!(serde_json::from_value::<ExpectedVsRealized>(unknown).is_err());

        let mut missing = valid;
        missing.as_object_mut().expect("object").remove("bias_bps");
        assert!(serde_json::from_value::<ExpectedVsRealized>(missing).is_err());
    }

    #[test]
    fn portfolio_funnel_conserves_counts() {
        let funnel = BacktestPortfolioFunnel {
            schema_version: 1,
            decision_tick_count: 2,
            emitted_candidate_count: 3,
            candidate_without_executable_tier_count: 1,
            executable_tier_count: 4,
            admission_rejected_tier_count: 1,
            admitted_tier_count: 3,
            selected_tier_count: 2,
            executed_entry_count: 2,
            resolved_allocation_count: 1,
            no_candidate_tick_count: 0,
            no_executable_tier_tick_count: 0,
            no_selection_tick_count: 0,
            selected_tick_count: 2,
            tier_exclusion_reasons: vec![
                BacktestTierExclusionCount {
                    reason: PortfolioRejectionReason::RobustExpectedNetFloor,
                    count: 1,
                },
                BacktestTierExclusionCount {
                    reason: PortfolioRejectionReason::NotSelectedByGlobalOptimum,
                    count: 1,
                },
            ],
        };
        funnel.validate().expect("count-conserving funnel");

        let mut drifted = funnel;
        drifted.executed_entry_count = 1;
        assert!(
            drifted.validate().is_err(),
            "selected/entry drift must fail closed"
        );
    }

    fn validation_identity(combination_index: u32, held_out_seed: char) -> CpcvEstimatorIdentity {
        CpcvEstimatorIdentity::Validation {
            combination_index,
            test_partitions_hash: hash(held_out_seed),
            test_partition_count: 2,
            test_groups_hash: hash(held_out_seed),
            test_group_count: 16,
        }
    }

    fn trial_identity(
        trial_id: u32,
        combination_index: u32,
        held_out_seed: char,
    ) -> CpcvEstimatorIdentity {
        trial_identity_on_path(trial_id, 0, combination_index, held_out_seed)
    }

    fn trial_identity_on_path(
        trial_id: u32,
        path_index: u32,
        combination_index: u32,
        held_out_seed: char,
    ) -> CpcvEstimatorIdentity {
        CpcvEstimatorIdentity::TrialPathValidation {
            trial_id,
            path_index,
            combination_index,
            test_partitions_hash: hash(held_out_seed),
            test_partition_count: 2,
            test_groups_hash: hash(held_out_seed),
            test_group_count: 16,
        }
    }

    fn fold_artifact(
        identity: CpcvEstimatorIdentity,
        model_artifact_hash: ContentHash,
    ) -> CpcvFoldArtifact {
        CpcvFoldArtifact {
            identity,
            validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
            training_groups_hash: hash('a'),
            training_group_count: 2,
            calibration_fit_groups_hash: hash('b'),
            calibration_fit_group_count: 1,
            scenario_fit_groups_hash: hash('c'),
            scenario_fit_group_count: 1,
            model_artifact_hash,
            serving_contract_hash: hash('d'),
            model_payload_hash: hash('e'),
            calibration_function_hash: hash('1'),
            scenario_economic_function_hash: hash('2'),
            calibration_artifact_hash: hash('f'),
            scenario_model_hash: hash('0'),
        }
    }

    #[test]
    fn ledger_accepts_equal_models() {
        let model_hash = hash('e');
        let ledger = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), model_hash),
            fold_artifact(validation_identity(1, '2'), model_hash),
            fold_artifact(trial_identity(0, 0, '1'), model_hash),
            fold_artifact(trial_identity(0, 1, '2'), model_hash),
        ])
        .expect("distinct fold identities may produce equal model bytes");
        ledger
            .validate_for(&CpcvTrialPathBinding::try_new(0, vec![0, 1]).expect("binding"))
            .expect("ledger matches bound complete path");
        assert_eq!(ledger.validation_count(), 2);
        assert_eq!(ledger.trial_count(), 1);
    }

    #[test]
    fn accepts_exact_sparse_path() {
        let ledger = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), hash('a')),
            fold_artifact(validation_identity(1, '2'), hash('b')),
            fold_artifact(validation_identity(2, '3'), hash('c')),
            fold_artifact(validation_identity(3, '4'), hash('d')),
            fold_artifact(validation_identity(4, '5'), hash('e')),
            fold_artifact(trial_identity(0, 0, '1'), hash('0')),
            fold_artifact(trial_identity(0, 1, '2'), hash('1')),
            fold_artifact(trial_identity(0, 4, '5'), hash('4')),
            fold_artifact(trial_identity(1, 0, '1'), hash('5')),
            fold_artifact(trial_identity(1, 1, '2'), hash('6')),
            fold_artifact(trial_identity(1, 4, '5'), hash('7')),
        ])
        .expect("two governed trials may share one exact sparse CPCV path");
        ledger
            .validate_for(&CpcvTrialPathBinding::try_new(0, vec![0, 1, 4]).expect("binding"))
            .expect("ledger exactly matches the precommitted sparse path");
        assert_eq!(ledger.validation_count(), 5);
        assert_eq!(ledger.trial_count(), 2);
    }

    #[test]
    fn ledger_rejects_duplicate_identity() {
        assert!(
            CpcvFoldArtifacts::try_new(vec![
                fold_artifact(validation_identity(0, '1'), hash('e')),
                fold_artifact(validation_identity(0, '1'), hash('f')),
            ])
            .is_err()
        );
    }

    #[test]
    fn ledger_rejects_identity_gap() {
        assert!(
            CpcvFoldArtifacts::try_new(vec![
                fold_artifact(validation_identity(1, '2'), hash('e'),)
            ])
            .is_err()
        );
    }

    #[test]
    fn ledger_rejects_trial_gap() {
        let ledger = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), hash('e')),
            fold_artifact(validation_identity(1, '2'), hash('f')),
            fold_artifact(trial_identity(0, 0, '1'), hash('0')),
        ])
        .expect("structurally valid projected ledger");
        assert!(
            ledger
                .validate_for(&CpcvTrialPathBinding::try_new(0, vec![0, 1]).expect("binding"))
                .is_err()
        );
    }

    #[test]
    fn ledger_rejects_trial_holdout() {
        assert!(
            CpcvFoldArtifacts::try_new(vec![
                fold_artifact(validation_identity(0, '1'), hash('e')),
                fold_artifact(trial_identity(0, 0, '2'), hash('f')),
            ])
            .is_err()
        );
    }

    #[test]
    fn rejects_binding_path_drift() {
        let ledger = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), hash('e')),
            fold_artifact(trial_identity(0, 0, '1'), hash('f')),
        ])
        .expect("structurally valid ledger");
        assert!(matches!(
            ledger.validate_for(&CpcvTrialPathBinding::try_new(1, vec![0]).expect("binding")),
            Err(CpcvEvidenceError::TrialPathBindingIndexMismatch {
                expected: 1,
                actual: 0
            })
        ));
    }

    #[test]
    fn rejects_trial_path_drift() {
        let result = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), hash('a')),
            fold_artifact(validation_identity(1, '2'), hash('b')),
            fold_artifact(trial_identity(0, 0, '1'), hash('c')),
            fold_artifact(trial_identity(0, 1, '2'), hash('d')),
            fold_artifact(trial_identity(1, 0, '1'), hash('e')),
            fold_artifact(trial_identity_on_path(1, 1, 1, '2'), hash('f')),
        ]);
        assert!(matches!(
            result,
            Err(CpcvEvidenceError::TrialPathIndexMismatch {
                trial_id: 1,
                expected: 0,
                actual: 1
            })
        ));
    }

    #[test]
    fn rejects_trial_combination_drift() {
        let result = CpcvFoldArtifacts::try_new(vec![
            fold_artifact(validation_identity(0, '1'), hash('a')),
            fold_artifact(validation_identity(1, '2'), hash('b')),
            fold_artifact(validation_identity(2, '3'), hash('c')),
            fold_artifact(trial_identity(0, 0, '1'), hash('d')),
            fold_artifact(trial_identity(0, 1, '2'), hash('e')),
            fold_artifact(trial_identity(1, 0, '1'), hash('f')),
            fold_artifact(trial_identity(1, 2, '3'), hash('0')),
        ]);
        assert!(matches!(
            result,
            Err(CpcvEvidenceError::TrialCombinationSetMismatch { trial_id: 1 })
        ));
    }
}
