//! Model training contract and the [`WeightedFactorTrainer`].
//!
//! The weighted-factor trainer optimizes only the **`OutcomeAlpha` simplex** in a
//! complete revision-bound [`FactorHeadSpec`]. Context penalties and the alpha
//! deadband are governed inputs and are never optimizer degrees of freedom.
//! It mirrors the alpha head's ranking kernel:
//!
//! ```text
//! yes_alpha = Σ applicable(weightᵢ · confidenceᵢ · signed_strengthᵢ)
//!             / Σ applicable(weightᵢ)
//! ```
//!
//! and searches the weight **simplex** (non-negative, sum to 1) to minimize the
//! governed LTR objective (`RankIC`-weighted `RankNet` or `RankNet` + `TopN`
//! tail/turnover + L2). The deterministic coordinate search is the **base**
//! optimizer (always linked); the `optimize` feature adds an `argmin` refinement
//! that is kept only when it strictly improves the training objective. Configuring
//! `argmin` without the feature **fails closed** (never silently degrades).
//!
//! Determinism is a hard, money-critical invariant: the same examples, plane,
//! seed head, label, and objective must yield byte-identical payload and
//! training-input commitments.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter, Result as FmtResult},
    mem,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::data_plane::DecisionBoundary,
    enums::{factor::NormalizationSource, model::ModelFamily},
    runtime_config::{FactorCrossSectionConfig, SmallCrossSectionPolicy, TrainingOptimizerKind},
    types::{
        ContentHash, FactorDefinitionId, MarketId, ModelInputContract, Probability, TokenId,
        factor::FactorServingPlane, model_training::TrainingObjectiveSpec,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[cfg(feature = "optimize")]
use crate::model::optimize;
use crate::{
    factors::{FactorName, FrozenReferenceCdf, FrozenReferenceQuantiles, NormalizedFactor},
    hashing::ResearchHasher,
    model::{
        artifact::{
            HorizonMultipliers, ReturnModelSpec, SubstitutionConfidenceRules,
            TrainingObjectiveReport, WeightedFactorModelPayload, model_input_contract_hash,
        },
        factor_heads::{AlphaFactorWeight, FactorHeadSpec},
        objective::{
            CrossSectionGroup, ObjectiveComponentReport, ObjectiveEvaluator, RankingDiagnostics,
            SampleRow,
        },
    },
    parallel::par_map_with_index,
    precision::RESEARCH_DECIMAL_SCALE,
    training::{LabelName, TrainingExample},
    validation::{DefaultPurgedSplitter, PurgeConfig, PurgedSplitter, TimelineGroup},
};
/// Which forward label a trainer targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelSelector {
    /// Label name (e.g. `token_payout_ratio`).
    pub name: LabelName,
    /// Label horizon in seconds (`0` for horizon-independent labels).
    pub horizon_secs: u64,
}

/// Rolling time-ordered validation split with label-horizon purge/embargo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationSpec {
    /// Number of contiguous, time-ordered folds (`>= 2`). The last fold is held
    /// out; earlier folds train after purge/embargo against the held-out
    /// interval, and each rolling split contributes one validation objective.
    pub folds: u32,
    /// Embargo fraction of the full timeline span (same knob as
    /// `research.validation.purge.embargo_pct`). Label-horizon purge is always on.
    #[serde(default = "default_embargo_pct")]
    pub embargo_pct: Decimal,
    /// Absolute embargo floor in seconds (typically max feature lookback).
    #[serde(default)]
    pub min_embargo_secs: u64,
}

fn default_embargo_pct() -> Decimal {
    Decimal::new(2, 2) // 0.02
}

impl Default for ValidationSpec {
    fn default() -> Self {
        Self {
            folds: 3,
            embargo_pct: default_embargo_pct(),
            min_embargo_secs: 0,
        }
    }
}

/// Cloneable, runtime-agnostic cooperative cancellation probe for pure model
/// kernels. The core binds it to the owning research job token.
#[derive(Clone)]
pub struct CancellationProbe {
    cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl Debug for CancellationProbe {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.debug_struct("CancellationProbe").finish()
    }
}

impl Default for CancellationProbe {
    fn default() -> Self {
        Self {
            cancelled: Arc::new(|| false),
        }
    }
}

impl CancellationProbe {
    pub fn new(cancelled: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            cancelled: Arc::new(cancelled),
        }
    }

    pub fn check(&self, phase: &str) -> QuantResult<()> {
        if (self.cancelled)() {
            return Err(ResearchError::Cancelled {
                detail: format!("model computation cancelled at `{phase}`"),
            }
            .into());
        }
        Ok(())
    }
}

/// Per-fold and aggregate validation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Held-out objective for the weighted trainer (last time block), or mean of
    /// rolling OOS fold objectives for classical pointwise trainers.
    pub held_out_objective: Decimal,
    /// Held-out component breakdown (weighted LTR path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_out_components: Option<ObjectiveComponentReport>,
    /// Ranking diagnostics on the held-out block (weighted LTR path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_out_diagnostics: Option<RankingDiagnostics>,
    /// Per-fold out-of-sample objective values in time order. Every entry was
    /// produced by an estimator and transform fit only on that fold's purged
    /// training partition.
    pub fold_objectives: Vec<Decimal>,
    /// Per-block component breakdowns (time-ordered).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fold_components: Vec<ObjectiveComponentReport>,
    /// Total resolved samples kept in cross-section groups (`decision_at` with ≥2 rows).
    pub sample_count: u64,
    /// Number of `decision_at` cross-sections dropped because they had fewer than 2 rows.
    #[serde(default)]
    pub dropped_singleton_groups: u64,
    /// Number of sample rows discarded with those singleton cross-sections.
    #[serde(default)]
    pub dropped_singleton_rows: u64,
    /// Effective independent trial count from `coordinate_search`, used as the
    /// Bailey multiple-testing correction input for DSR.
    #[serde(default)]
    pub coord_search_effective_n: u32,
}

/// Header-free request to fit a `WeightedFactor` family payload.
///
/// The complete factor plane and seed head are immutable training inputs. The
/// optimizer may change only the alpha simplex; context penalties and the alpha
/// deadband remain byte-for-byte equal to the seed head.
#[derive(Debug, Clone)]
pub struct TrainModelRequest {
    /// Cooperative cancellation observed at fold/trial/search boundaries.
    pub cancellation: CancellationProbe,
    /// Frozen, point-in-time training examples (decoded from the dataset Parquet).
    pub examples: Arc<[TrainingExample]>,
    /// Supervised target label.
    pub label: LabelSelector,
    /// Exact governed factor plane consumed by both training and serving.
    pub factor_plane: FactorServingPlane,
    /// Complete governed estimator head. Only `alpha_weights` are optimized.
    pub seed_head: FactorHeadSpec,
    /// Governed training objective snapshot.
    pub objective: TrainingObjectiveSpec,
    /// Rolling validation split.
    pub validation: ValidationSpec,
    /// Governed horizon multiplier.
    pub horizon_multipliers: HorizonMultipliers,
    /// Governed substitution confidence penalties.
    pub substitution_rules: SubstitutionConfidenceRules,
    /// Return model (heuristic until the independent calibration pipeline binds a calibrator).
    pub return_model: ReturnModelSpec,
    /// Exact ordered raw-input contract frozen by the owning model spec.
    pub input_contract: ModelInputContract,
    /// Small-cross-section transform policy/minimum fitted together with the
    /// weighted artifact.
    pub factor_cross_section: FactorCrossSectionConfig,
}

/// Verified header-free `WeightedFactor` training output.
///
/// The orchestration layer uses these commitments to build the immutable
/// [`ModelServingContract`](quant_pivot_models::types::model_serving::ModelServingContract),
/// then seals exactly one outer [`ModelArtifact`](crate::model::artifact::ModelArtifact).
#[derive(Debug, Clone)]
pub struct WeightedModelTrainingOutput {
    /// Executable family payload, already validated against `factor_plane`.
    pub payload: WeightedFactorModelPayload,
    /// Exact estimator-row commitment after fitted preprocessing.
    pub training_input_hash: ContentHash,
    /// Canonical ordered raw-input contract commitment.
    pub input_contract_hash: ContentHash,
    /// Canonical fitted transform commitment.
    pub input_transform_hash: ContentHash,
    /// In-sample (training-fold) objective.
    pub in_sample_metrics: TrainingObjectiveReport,
    /// Held-out validation objective.
    pub validation_metrics: ValidationReport,
}

/// Immutable, objective-independent estimator matrix for one purged CPCV model-fit split.
///
/// The preparation owns the fitted reference CDFs, transformed cross-sections and all output
/// contract inputs. A governed trial grid can therefore fit several objective specifications
/// against the exact same rows without rebuilding or re-normalizing the fold. Construction is
/// restricted to `folds == 1`; outer CPCV remains the sole OOS estimator.
pub struct PreparedWeightedFold {
    cancellation: CancellationProbe,
    factor_plane: FactorServingPlane,
    seed_head: FactorHeadSpec,
    seed_weights: Vec<OptimizerFactorWeight>,
    dataset: TrainingDataset,
    frozen_reference_quantiles: FrozenReferenceQuantiles,
    training_input_hash: ContentHash,
    input_contract: ModelInputContract,
    input_contract_hash: ContentHash,
    horizon_multipliers: HorizonMultipliers,
    substitution_rules: SubstitutionConfidenceRules,
    return_model: ReturnModelSpec,
    factor_cross_section: FactorCrossSectionConfig,
}

impl PreparedWeightedFold {
    /// Fit one governed objective against this exact prepared fold.
    pub fn train(
        &self,
        objective: &TrainingObjectiveSpec,
    ) -> QuantResult<WeightedModelTrainingOutput> {
        self.cancellation.check("prepared fold fit")?;
        ensure_optimizer_available(objective.optimizer)?;
        let evaluator = ObjectiveEvaluator::new(objective.clone());
        let seed = normalize_simplex(
            &self
                .seed_weights
                .iter()
                .map(|weight| weight.weight.max(Decimal::ZERO))
                .collect::<Vec<_>>(),
        );
        let (grid_weights, effective_n) =
            coordinate_search(&seed, &self.dataset.groups, &evaluator, &self.cancellation)?;
        let grid_report = evaluator.evaluate(&grid_weights, &self.dataset.groups)?;
        let (weights, train_report) = refine(
            &grid_weights,
            grid_report.objective_value(),
            &self.dataset.groups,
            &evaluator,
        )?;
        let fit = assemble_full_window_weights(
            &self.seed_weights,
            &weights,
            train_report.rounded(),
            &evaluator,
            &self.dataset,
            effective_n,
            self.frozen_reference_quantiles.clone(),
        )?;
        let mut factor_head = self.seed_head.clone();
        factor_head.alpha_weights = fit.alpha_weights;
        let payload = WeightedFactorModelPayload {
            factor_head,
            input_contract: self.input_contract.clone(),
            horizon_multipliers: self.horizon_multipliers.clone(),
            substitution_confidence_rules: self.substitution_rules.clone(),
            return_model: self.return_model.clone(),
            factor_cross_section: self.factor_cross_section.clone(),
            frozen_reference_quantiles: fit.frozen_reference_quantiles,
        };
        payload.validate_for_plane(&self.factor_plane)?;
        let input_transform_hash = payload.input_transform_hash()?;
        payload.model_payload_hash()?;
        Ok(WeightedModelTrainingOutput {
            payload,
            training_input_hash: self.training_input_hash,
            input_contract_hash: self.input_contract_hash,
            input_transform_hash,
            in_sample_metrics: fit.objective_report,
            validation_metrics: fit.validation,
        })
    }
}

/// Trains a family payload without constructing its serving contract or outer artifact.
#[async_trait]
pub trait ModelTrainer: Send + Sync {
    /// Family this trainer produces.
    fn model_family(&self) -> ModelFamily;

    /// Fit and verify a header-free family payload.
    async fn train(&self, request: TrainModelRequest) -> QuantResult<WeightedModelTrainingOutput>;
}

/// Coordinate-search step schedule for the simplex local search.
///
/// Each round shifts weight mass between factor pairs by every step in
/// descending order, accepting any strictly-improving move. The schedule is
/// fixed so the search is deterministic.
const SEARCH_STEPS: [i64; 4] = [16, 8, 4, 1]; // hundredths: 0.16, 0.08, 0.04, 0.01

/// Maximum coordinate-search rounds before declaring convergence.
const MAX_SEARCH_ROUNDS: usize = 64;

/// The weighted-factor trainer (deterministic coordinate search + optional
/// `argmin` refinement).
#[derive(Debug, Clone, Default)]
pub struct WeightedFactorTrainer;

impl WeightedFactorTrainer {
    /// Construct the trainer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ModelTrainer for WeightedFactorTrainer {
    fn model_family(&self) -> ModelFamily {
        ModelFamily::WeightedFactor
    }

    async fn train(&self, request: TrainModelRequest) -> QuantResult<WeightedModelTrainingOutput> {
        request.train_weighted()
    }
}

impl TrainModelRequest {
    /// Prepare the objective-independent matrix for a purged CPCV fold.
    pub fn prepare_fold(self) -> QuantResult<PreparedWeightedFold> {
        if self.validation.folds != 1 {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "prepared weighted fold requires validation.folds=1, got {}",
                    self.validation.folds
                ),
            }
            .into());
        }
        self.cancellation.check("prepared fold transform")?;
        self.seed_head.validate(&self.factor_plane)?;
        let seed_weights = self
            .seed_head
            .alpha_weights
            .iter()
            .map(OptimizerFactorWeight::from)
            .collect::<Vec<_>>();
        let reference_factors = cross_sectional_factor_names(&self.factor_plane);
        let factors = estimator_factor_names(&self.factor_plane);
        let frozen_reference_quantiles = fit_frozen_reference_quantiles(
            &self.examples,
            &self.label,
            &reference_factors,
            Some(&self.factor_cross_section),
        )?;
        let prepared = apply_reference_quantiles(
            &self.examples,
            &frozen_reference_quantiles,
            Some(&self.factor_cross_section),
        )?;
        let dataset = TrainingDataset::build(&prepared, &self.label, &factors)?;
        let training_input_hash = prepared_training_hash(&prepared, &self.label, &dataset)?;
        let input_contract_hash = model_input_contract_hash(&self.input_contract)?;
        Ok(PreparedWeightedFold {
            cancellation: self.cancellation,
            factor_plane: self.factor_plane,
            seed_head: self.seed_head,
            seed_weights,
            dataset,
            frozen_reference_quantiles,
            training_input_hash,
            input_contract: self.input_contract,
            input_contract_hash,
            horizon_multipliers: self.horizon_multipliers,
            substitution_rules: self.substitution_rules,
            return_model: self.return_model,
            factor_cross_section: self.factor_cross_section,
        })
    }

    /// The pure training routine (CPU-bound, deterministic).
    fn train_weighted(&self) -> QuantResult<WeightedModelTrainingOutput> {
        self.seed_head.validate(&self.factor_plane)?;
        let seed_weights = self
            .seed_head
            .alpha_weights
            .iter()
            .map(OptimizerFactorWeight::from)
            .collect::<Vec<_>>();
        let reference_factors = cross_sectional_factor_names(&self.factor_plane);
        let fit = SimplexFitInput {
            examples: &self.examples,
            label: &self.label,
            seed_weights: &seed_weights,
            reference_factors: &reference_factors,
            objective: &self.objective,
            validation: self.validation,
            factor_cross_section: Some(&self.factor_cross_section),
            cancellation: &self.cancellation,
        }
        .fit()?;

        let factors = estimator_factor_names(&self.factor_plane);
        let training_input_hash = weighted_training_input_hash(
            &self.examples,
            &self.label,
            &factors,
            &fit.frozen_reference_quantiles,
            Some(&self.factor_cross_section),
        )?;
        let input_contract_hash = model_input_contract_hash(&self.input_contract)?;
        let mut factor_head = self.seed_head.clone();
        factor_head.alpha_weights = fit.alpha_weights;
        let payload = WeightedFactorModelPayload {
            factor_head,
            input_contract: self.input_contract.clone(),
            horizon_multipliers: self.horizon_multipliers.clone(),
            substitution_confidence_rules: self.substitution_rules.clone(),
            return_model: self.return_model.clone(),
            factor_cross_section: self.factor_cross_section.clone(),
            frozen_reference_quantiles: fit.frozen_reference_quantiles,
        };
        payload.validate_for_plane(&self.factor_plane)?;
        let input_transform_hash = payload.input_transform_hash()?;
        payload.model_payload_hash()?;
        Ok(WeightedModelTrainingOutput {
            payload,
            training_input_hash,
            input_contract_hash,
            input_transform_hash,
            in_sample_metrics: fit.objective_report,
            validation_metrics: fit.validation,
        })
    }
}

fn cross_sectional_factor_names(plane: &FactorServingPlane) -> Vec<FactorName> {
    plane
        .definitions()
        .iter()
        .filter(|revision| revision.definition().normalization.is_cross_sectional())
        .map(|revision| revision.factor_name().clone())
        .collect()
}

fn estimator_factor_names(plane: &FactorServingPlane) -> Vec<FactorName> {
    plane
        .definitions()
        .iter()
        .filter(|revision| !revision.definition().is_diagnostic())
        .map(|revision| revision.factor_name().clone())
        .collect()
}

/// Fit one empirical raw-factor CDF per model input from the supplied training
/// partition.
///
/// The caller decides the partition: CV passes only purged train rows, while
/// the final model passes the complete frozen training set.
///
/// The trainer and the frozen model-parity verifier share this implementation
/// so a model cannot publish with a reference CDF different from the one its
/// frozen training rows deterministically imply.
pub fn fit_frozen_reference_quantiles(
    examples: &[TrainingExample],
    label: &LabelSelector,
    factors: &[FactorName],
    cross_section: Option<&FactorCrossSectionConfig>,
) -> QuantResult<FrozenReferenceQuantiles> {
    let Some(cross_section) = cross_section else {
        return Ok(FrozenReferenceQuantiles::empty());
    };
    if cross_section.small_cross_section_policy == SmallCrossSectionPolicy::Indeterminate {
        return Ok(FrozenReferenceQuantiles::empty());
    }
    let min_size = usize::try_from(cross_section.min_size).map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("factor cross-section min_size conversion failed: {error}"),
        })
    })?;
    let mut references = Vec::with_capacity(factors.len());
    for factor in factors {
        let values: Vec<Decimal> = examples
            .iter()
            .filter(|example| {
                example.labels.iter().any(|row| {
                    (&row.label_name, row.horizon_secs) == (&label.name, label.horizon_secs)
                })
            })
            .filter_map(|example| {
                example
                    .factor_values
                    .iter()
                    .find(|value| value.name == *factor)
                    .and_then(|value| value.raw_value)
                    .map(|value| value.abs())
            })
            .collect();
        if values.len() < min_size {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "factor `{factor}` has {} observed raw training values; frozen reference CDF \
                     requires at least {min_size}",
                    values.len()
                ),
            }
            .into());
        }
        references.push(FrozenReferenceCdf::fit(factor.clone(), values)?);
    }
    FrozenReferenceQuantiles::new(references)
}

/// Canonical commitment to the exact final weighted estimator input.
///
/// The reference transform is applied first, then the same reduced
/// `(decision interval, allocation key, signed factor columns, label)` groups
/// consumed by the optimizer are hashed in deterministic order. Dataset
/// semantic hash remains a separate artifact field and binds all source
/// evidence/per-source cutoffs, including rows excluded from the estimator.
pub fn weighted_training_input_hash(
    examples: &[TrainingExample],
    label: &LabelSelector,
    factors: &[FactorName],
    references: &FrozenReferenceQuantiles,
    cross_section: Option<&FactorCrossSectionConfig>,
) -> QuantResult<ContentHash> {
    let prepared = apply_reference_quantiles(examples, references, cross_section)?;
    let dataset = TrainingDataset::build(&prepared, label, factors)?;
    prepared_training_hash(&prepared, label, &dataset)
}

fn prepared_training_hash(
    prepared: &[TrainingExample],
    label: &LabelSelector,
    dataset: &TrainingDataset,
) -> QuantResult<ContentHash> {
    let mut labelled = prepared
        .iter()
        .filter(|example| {
            example.labels.iter().any(|row| {
                let matches_name = row.label_name == label.name;
                let matches_horizon = row.horizon_secs == label.horizon_secs;
                matches_name && matches_horizon
            })
        })
        .collect::<Vec<_>>();
    labelled.sort_by(|left, right| {
        left.decision_at()
            .cmp(&right.decision_at())
            .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
            .then_with(|| left.token_id.as_str().cmp(right.token_id.as_str()))
    });
    let identities = labelled
        .into_iter()
        .map(|example| WeightedInputIdentity {
            decision_boundary: &example.decision_boundary,
            market_id: &example.market_id,
            token_id: &example.token_id,
        })
        .collect();
    ResearchHasher::canonical(&WeightedTrainingInput {
        identities,
        groups: &dataset.groups,
    })
}

#[derive(Serialize)]
struct WeightedInputIdentity<'a> {
    decision_boundary: &'a DecisionBoundary,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
}

#[derive(Serialize)]
struct WeightedTrainingInput<'a> {
    identities: Vec<WeightedInputIdentity<'a>>,
    groups: &'a [CrossSectionGroup],
}

/// Apply a fitted training reference only to same-time columns below the frozen
/// minimum cross-section size. Larger columns retain their dataset's ordinary
/// same-cross-section normalization.
fn apply_reference_quantiles(
    examples: &[TrainingExample],
    references: &FrozenReferenceQuantiles,
    cross_section: Option<&FactorCrossSectionConfig>,
) -> QuantResult<Vec<TrainingExample>> {
    let Some(cross_section) = cross_section else {
        return Ok(examples.to_vec());
    };
    if cross_section.small_cross_section_policy == SmallCrossSectionPolicy::Indeterminate {
        return Ok(examples.to_vec());
    }
    references.validate()?;
    let min_size = usize::try_from(cross_section.min_size).map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("factor cross-section min_size conversion failed: {error}"),
        })
    })?;
    let mut present_counts = BTreeMap::new();
    for example in examples {
        for factor in &example.factor_values {
            if factor.raw_value.is_some() {
                let count = present_counts
                    .entry((example.decision_at(), factor.name.clone()))
                    .or_insert(0_usize);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "factor `{}` present-count overflow at decision_at {}",
                            factor.name,
                            example.decision_at()
                        ),
                    })?;
            }
        }
    }

    let mut prepared = examples.to_vec();
    for example in &mut prepared {
        let decision_at = example.decision_at();
        for factor in &mut example.factor_values {
            let present = present_counts
                .get(&(decision_at, factor.name.clone()))
                .copied()
                .unwrap_or(0);
            if present >= min_size {
                continue;
            }
            let Some(raw) = factor.raw_value else {
                continue;
            };
            let Some(reference) = references.get(&factor.name) else {
                continue;
            };
            factor.normalization = NormalizedFactor::Scored {
                score: if raw.is_zero() {
                    Probability::ZERO
                } else {
                    reference.percentile(raw.abs())?
                },
                source: NormalizationSource::FrozenReferenceQuantile,
                clamp: None,
            };
        }
    }
    Ok(prepared)
}

/// Private optimizer row. Public payloads always use revision-bound head rows.
#[derive(Debug, Clone)]
struct OptimizerFactorWeight {
    factor_definition_id: FactorDefinitionId,
    factor: FactorName,
    weight: Decimal,
}

impl From<&AlphaFactorWeight> for OptimizerFactorWeight {
    fn from(weight: &AlphaFactorWeight) -> Self {
        Self {
            factor_definition_id: weight.factor_definition_id,
            factor: weight.factor.clone(),
            weight: weight.weight,
        }
    }
}

/// The outcome of the private alpha-simplex fit.
#[derive(Debug, Clone)]
struct FittedWeights {
    /// Frozen, normalized per-factor weights in seed order.
    alpha_weights: Vec<AlphaFactorWeight>,
    /// In-sample (training-fold) objective report.
    pub objective_report: TrainingObjectiveReport,
    /// Held-out validation report.
    pub validation: ValidationReport,
    /// Final-training-partition reference CDFs frozen into the artifact.
    pub frozen_reference_quantiles: FrozenReferenceQuantiles,
}

struct SimplexFitContext<'a> {
    examples: &'a [TrainingExample],
    label: &'a LabelSelector,
    seed_weights: &'a [OptimizerFactorWeight],
    alpha_factors: &'a [FactorName],
    reference_factors: &'a [FactorName],
    seed: &'a [Decimal],
    evaluator: &'a ObjectiveEvaluator,
    factor_cross_section: Option<&'a FactorCrossSectionConfig>,
    cancellation: &'a CancellationProbe,
}

struct SimplexFitInput<'a> {
    examples: &'a [TrainingExample],
    label: &'a LabelSelector,
    seed_weights: &'a [OptimizerFactorWeight],
    reference_factors: &'a [FactorName],
    objective: &'a TrainingObjectiveSpec,
    validation: ValidationSpec,
    factor_cross_section: Option<&'a FactorCrossSectionConfig>,
    cancellation: &'a CancellationProbe,
}

impl SimplexFitInput<'_> {
    /// Fit only the governed alpha-head simplex.
    fn fit(self) -> QuantResult<FittedWeights> {
        let Self {
            examples,
            label,
            seed_weights,
            reference_factors,
            objective,
            validation,
            factor_cross_section,
            cancellation,
        } = self;
        cancellation.check("dataset matrix")?;
        if seed_weights.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "trainer requires a non-empty seed weight (candidate factor) set"
                    .to_owned(),
            }
            .into());
        }
        ensure_optimizer_available(objective.optimizer)?;

        // Deterministic factor order: the seed-weight order defines the columns.
        let factors = seed_weights
            .iter()
            .map(|weight| weight.factor.clone())
            .collect::<Vec<_>>();
        // Build the label/timeline skeleton before fitting any transform. Its group
        // boundaries are independent of factor scores and define the purged split.
        let timeline_dataset = TrainingDataset::build(examples, label, &factors)?;

        // `folds == 1` (or less): full-window fit — used by CPCV fold training where
        // the outer CPCV already supplies the OOS distribution. `folds >= 2`: purged
        // hold-out CV matching CPCV label-horizon semantics.
        let folds = validation.folds.max(1) as usize;
        if folds >= 2 && timeline_dataset.groups.len() < folds {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "insufficient resolved cross-section groups ({}) for {} validation folds",
                    timeline_dataset.groups.len(),
                    folds
                ),
            }
            .into());
        }

        let evaluator = ObjectiveEvaluator::new(objective.clone());
        let seed = normalize_simplex(
            &seed_weights
                .iter()
                .map(|weight| weight.weight.max(Decimal::ZERO))
                .collect::<Vec<_>>(),
        );

        let context = SimplexFitContext {
            examples,
            label,
            seed_weights,
            alpha_factors: &factors,
            reference_factors,
            seed: &seed,
            evaluator: &evaluator,
            factor_cross_section,
            cancellation,
        };
        if folds < 2 {
            return fit_full_window(&context);
        }
        let validation_report =
            fit_purged_validation(&context, &timeline_dataset, validation, folds)?;
        fit_final_full_window(&context, validation_report)
    }
}

fn fit_full_window(context: &SimplexFitContext<'_>) -> QuantResult<FittedWeights> {
    context.cancellation.check("full-window transform")?;
    let references = fit_frozen_reference_quantiles(
        context.examples,
        context.label,
        context.reference_factors,
        context.factor_cross_section,
    )?;
    let prepared =
        apply_reference_quantiles(context.examples, &references, context.factor_cross_section)?;
    let dataset = TrainingDataset::build(&prepared, context.label, context.alpha_factors)?;
    let (grid_weights, coord_search_effective_n) = coordinate_search(
        context.seed,
        &dataset.groups,
        context.evaluator,
        context.cancellation,
    )?;
    let grid_report = context.evaluator.evaluate(&grid_weights, &dataset.groups)?;
    let (weights, train_report) = refine(
        &grid_weights,
        grid_report.objective_value(),
        &dataset.groups,
        context.evaluator,
    )?;
    assemble_full_window_weights(
        context.seed_weights,
        &weights,
        train_report.rounded(),
        context.evaluator,
        &dataset,
        coord_search_effective_n,
        references,
    )
}

fn fit_purged_validation(
    context: &SimplexFitContext<'_>,
    timeline_dataset: &TrainingDataset,
    validation: ValidationSpec,
    folds: usize,
) -> QuantResult<ValidationReport> {
    let split = TimeSplit::new(timeline_dataset.groups.len(), folds)?;
    let purge = PurgeConfig {
        embargo_pct: validation.embargo_pct,
        min_embargo_secs: validation.min_embargo_secs,
    };
    let timeline: Vec<TimelineGroup> = timeline_dataset
        .groups
        .iter()
        .map(|group| TimelineGroup {
            decision_at: group.decision_at,
            label_horizon_end: group.label_horizon_end,
        })
        .collect();
    let validation_blocks = split.validation_blocks();
    if validation_blocks.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: "weighted rolling validation has no out-of-sample blocks".to_owned(),
        }
        .into());
    }

    let mut fold_components = Vec::with_capacity(validation_blocks.len());
    let mut fold_diagnostics = Vec::with_capacity(validation_blocks.len());
    let mut coord_search_effective_n = 0_u32;
    for (fold_index, block) in validation_blocks.iter().enumerate() {
        context.cancellation.check("validation fold")?;
        let fold = fit_validation_fold(
            context,
            timeline_dataset,
            &timeline,
            &purge,
            fold_index,
            block,
        )?;
        coord_search_effective_n = coord_search_effective_n
            .checked_add(fold.effective_n)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "weighted rolling validation effective trial count overflow".to_owned(),
            })?;
        fold_components.push(fold.components);
        fold_diagnostics.push(fold.diagnostics);
    }

    let held_out_components =
        fold_components
            .last()
            .cloned()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "weighted rolling validation produced no held-out component report"
                    .to_owned(),
            })?;
    let held_out_diagnostics =
        fold_diagnostics
            .last()
            .cloned()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "weighted rolling validation produced no held-out diagnostics".to_owned(),
            })?;
    let held_out_objective = held_out_components.objective_value();
    let fold_objectives = fold_components
        .iter()
        .map(ObjectiveComponentReport::objective_value)
        .map(|value| value.round_dp(RESEARCH_DECIMAL_SCALE))
        .collect();
    Ok(ValidationReport {
        held_out_objective: held_out_objective.round_dp(RESEARCH_DECIMAL_SCALE),
        held_out_components: Some(held_out_components),
        held_out_diagnostics: Some(held_out_diagnostics),
        fold_objectives,
        fold_components,
        sample_count: timeline_dataset.sample_count,
        dropped_singleton_groups: timeline_dataset.dropped_singleton_groups,
        dropped_singleton_rows: timeline_dataset.dropped_singleton_rows,
        coord_search_effective_n,
    })
}

struct ValidationFoldResult {
    components: ObjectiveComponentReport,
    diagnostics: RankingDiagnostics,
    effective_n: u32,
}

fn fit_validation_fold(
    context: &SimplexFitContext<'_>,
    timeline_dataset: &TrainingDataset,
    timeline: &[TimelineGroup],
    purge: &PurgeConfig,
    fold_index: usize,
    block: &TimeBlock,
) -> QuantResult<ValidationFoldResult> {
    let test_indices = (block.start..block.end).collect::<Vec<_>>();
    let purged = DefaultPurgedSplitter::new().split(timeline, &test_indices, purge)?;
    let train_indices = purged
        .train_indices
        .into_iter()
        .filter(|index| *index < block.start)
        .collect::<Vec<_>>();
    if train_indices.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "weighted rolling validation fold {} has no PIT-safe training groups after purge/embargo (purged={}, embargoed={})",
                fold_index + 1,
                purged.purged_indices.len(),
                purged.embargoed_indices.len()
            ),
        }
        .into());
    }
    let train_decision_at = train_indices
        .iter()
        .map(|index| timeline_dataset.groups[*index].decision_at)
        .collect::<BTreeSet<_>>();
    let reference_examples = context
        .examples
        .iter()
        .filter(|example| train_decision_at.contains(&example.decision_at()))
        .cloned()
        .collect::<Vec<_>>();
    let references = fit_frozen_reference_quantiles(
        &reference_examples,
        context.label,
        context.reference_factors,
        context.factor_cross_section,
    )?;
    let transformed_examples =
        apply_reference_quantiles(context.examples, &references, context.factor_cross_section)?;
    let fold_dataset =
        TrainingDataset::build(&transformed_examples, context.label, context.alpha_factors)?;
    timeline_dataset.ensure_fold_spine(&fold_dataset, fold_index)?;
    let train_groups = train_indices
        .iter()
        .map(|index| fold_dataset.groups[*index].clone())
        .collect::<Vec<_>>();
    let validation_groups = &fold_dataset.groups[block.start..block.end];
    let (grid_weights, effective_n) = coordinate_search(
        context.seed,
        &train_groups,
        context.evaluator,
        context.cancellation,
    )?;
    let grid_report = context.evaluator.evaluate(&grid_weights, &train_groups)?;
    let (weights, _) = refine(
        &grid_weights,
        grid_report.objective_value(),
        &train_groups,
        context.evaluator,
    )?;
    Ok(ValidationFoldResult {
        components: context
            .evaluator
            .evaluate(&weights, validation_groups)?
            .rounded(),
        diagnostics: context.evaluator.diagnostics(&weights, validation_groups)?,
        effective_n,
    })
}

impl TrainingDataset {
    fn ensure_fold_spine(&self, transformed: &Self, fold_index: usize) -> QuantResult<()> {
        let changed = transformed.groups.len() != self.groups.len()
            || transformed
                .groups
                .iter()
                .zip(&self.groups)
                .any(|(transformed, timeline)| transformed.decision_at != timeline.decision_at);
        if changed {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "weighted fold {} reference transform changed the timeline group spine",
                    fold_index + 1
                ),
            }
            .into());
        }
        Ok(())
    }
}

fn fit_final_full_window(
    context: &SimplexFitContext<'_>,
    validation: ValidationReport,
) -> QuantResult<FittedWeights> {
    context.cancellation.check("final full-window transform")?;
    // Refit both the reference CDFs and the estimator on the final full training
    // partition after CV. Held-out rows may assess the fold transform but cannot
    // influence it; only this explicit final fit may consume the full dataset.
    let final_references = fit_frozen_reference_quantiles(
        context.examples,
        context.label,
        context.reference_factors,
        context.factor_cross_section,
    )?;
    let final_examples = apply_reference_quantiles(
        context.examples,
        &final_references,
        context.factor_cross_section,
    )?;
    let final_dataset =
        TrainingDataset::build(&final_examples, context.label, context.alpha_factors)?;
    let (final_grid, final_effective_n) = coordinate_search(
        context.seed,
        &final_dataset.groups,
        context.evaluator,
        context.cancellation,
    )?;
    let final_grid_report = context
        .evaluator
        .evaluate(&final_grid, &final_dataset.groups)?;
    let (final_weights, final_report) = refine(
        &final_grid,
        final_grid_report.objective_value(),
        &final_dataset.groups,
        context.evaluator,
    )?;
    let mut final_fit = assemble_full_window_weights(
        context.seed_weights,
        &final_weights,
        final_report.rounded(),
        context.evaluator,
        &final_dataset,
        final_effective_n,
        final_references,
    )?;
    final_fit.validation = validation;
    final_fit
        .objective_report
        .summary
        .push_str("; purged held-out objective=");
    final_fit.objective_report.summary.push_str(
        &final_fit
            .validation
            .held_out_objective
            .round_dp(4)
            .to_string(),
    );
    Ok(final_fit)
}

/// Full-window fit path (`folds < 2`): no hold-out; held-out metrics mirror train.
fn assemble_full_window_weights(
    seed_weights: &[OptimizerFactorWeight],
    weights: &[Decimal],
    train_components: ObjectiveComponentReport,
    evaluator: &ObjectiveEvaluator,
    dataset: &TrainingDataset,
    coord_search_effective_n: u32,
    frozen_reference_quantiles: FrozenReferenceQuantiles,
) -> QuantResult<FittedWeights> {
    let train_objective = train_components.objective_value();
    let diagnostics = evaluator.diagnostics(weights, &dataset.groups)?;
    let weights_dp = weights
        .iter()
        .map(|w| w.round_dp(RESEARCH_DECIMAL_SCALE))
        .collect::<Vec<_>>();
    let frozen = normalize_simplex(&weights_dp);
    let alpha_weights = seed_weights
        .iter()
        .zip(&frozen)
        .map(|(seed, weight)| AlphaFactorWeight {
            factor_definition_id: seed.factor_definition_id,
            factor: seed.factor.clone(),
            weight: weight.round_dp(RESEARCH_DECIMAL_SCALE),
        })
        .collect::<Vec<_>>();
    Ok(FittedWeights {
        alpha_weights,
        objective_report: TrainingObjectiveReport {
            objective_value: train_objective.round_dp(RESEARCH_DECIMAL_SCALE),
            spec: evaluator.spec().clone(),
            components: train_components.clone(),
            diagnostics: Some(diagnostics.clone()),
            summary: format!(
                "ltr {:?} full-window train={} (final fit / CPCV fold), {} groups, {} samples \
                 (TopN pseudo=rank-equal)",
                evaluator.spec().rank_loss,
                train_objective.round_dp(4),
                dataset.groups.len(),
                dataset.sample_count,
            ),
        },
        validation: ValidationReport {
            held_out_objective: train_objective.round_dp(RESEARCH_DECIMAL_SCALE),
            held_out_components: Some(train_components),
            held_out_diagnostics: Some(diagnostics),
            fold_objectives: vec![train_objective.round_dp(RESEARCH_DECIMAL_SCALE)],
            fold_components: Vec::new(),
            sample_count: dataset.sample_count,
            dropped_singleton_groups: dataset.dropped_singleton_groups,
            dropped_singleton_rows: dataset.dropped_singleton_rows,
            coord_search_effective_n,
        },
        frozen_reference_quantiles,
    })
}

/// Reject `argmin` when the binary was built without the `optimize` feature.
fn ensure_optimizer_available(optimizer: TrainingOptimizerKind) -> QuantResult<()> {
    if optimizer == TrainingOptimizerKind::Argmin && !cfg!(feature = "optimize") {
        return Err(ResearchError::DatasetBuild {
            detail: "research.training.optimizer=argmin requires the `optimize` feature; \
                     rebuild with --features optimize or set optimizer=coordinate_search"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

/// Run the optional `argmin` refinement when the `optimize` feature is enabled.
#[cfg(feature = "optimize")]
fn refine(
    grid_weights: &[Decimal],
    grid_objective: Decimal,
    train_groups: &[CrossSectionGroup],
    evaluator: &ObjectiveEvaluator,
) -> QuantResult<(Vec<Decimal>, ObjectiveComponentReport)> {
    if evaluator.spec().optimizer != TrainingOptimizerKind::Argmin {
        return Ok((
            grid_weights.to_vec(),
            evaluator.evaluate(grid_weights, train_groups)?,
        ));
    }

    let refined =
        optimize::refine_weights(grid_weights, train_groups, evaluator).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("argmin refinement failed: {error}"),
            }
        })?;
    match refined {
        Some(refined) => {
            let refined_report = evaluator.evaluate(&refined, train_groups)?;
            let refined_objective = refined_report.objective_value();
            if refined_objective > grid_objective {
                return Ok((refined, refined_report));
            }
            Ok((
                grid_weights.to_vec(),
                evaluator.evaluate(grid_weights, train_groups)?,
            ))
        }
        None => Ok((
            grid_weights.to_vec(),
            evaluator.evaluate(grid_weights, train_groups)?,
        )),
    }
}

/// Base build: the coordinate-search solution is the final solution.
#[cfg(not(feature = "optimize"))]
fn refine(
    grid_weights: &[Decimal],
    _grid_objective: Decimal,
    train_groups: &[CrossSectionGroup],
    evaluator: &ObjectiveEvaluator,
) -> QuantResult<(Vec<Decimal>, ObjectiveComponentReport)> {
    Ok((
        grid_weights.to_vec(),
        evaluator.evaluate(grid_weights, train_groups)?,
    ))
}

/// Deterministic steepest-descent coordinate search over the weight simplex.
///
/// Each round enumerates every weight-shifting move from the current best in a
/// fixed order (step descending, then `from`/`to` ascending), scores them all in
/// parallel against the **round-start** best, and applies the single
/// strictly-improving move with the highest objective (earliest in the fixed
/// order on ties). Because every move is scored against the same fixed point and
/// the winner is chosen by a serial reduction over the source-ordered scores, the
/// result is independent of `rayon`'s thread count — a hard, money-critical
/// determinism invariant.
/// Deterministic coordinate search on the weight simplex.
///
/// Returns `(weights, effective_n_trials)` where `effective_n_trials` is the
/// Bailey-style independent-trial count for DSR: each improving round counts
/// as one independent configuration (correlated local moves within a round
/// collapse to a single effective trial). Always at least 1 (the seed).
fn coordinate_search(
    seed: &[Decimal],
    groups: &[CrossSectionGroup],
    evaluator: &ObjectiveEvaluator,
    cancellation: &CancellationProbe,
) -> QuantResult<(Vec<Decimal>, u32)> {
    cancellation.check("coordinate-search seed")?;
    let n = seed.len();
    let mut best = seed.to_vec();
    let mut best_obj = evaluator.evaluate(&best, groups)?.objective_value();
    let mut improving_rounds = 0_u32;
    if n < 2 {
        return Ok((best, 1));
    }

    for _ in 0..MAX_SEARCH_ROUNDS {
        cancellation.check("coordinate-search round")?;
        let moves = enumerate_moves(n, &best);
        if moves.is_empty() {
            break;
        }
        let scored: Vec<QuantResult<Decimal>> =
            par_map_with_index(&moves, |_, &(from, to, step)| {
                let mut trial = best.clone();
                trial[from] -= step;
                trial[to] += step;
                evaluator
                    .evaluate(&trial, groups)
                    .map(|report| report.objective_value())
            });
        let mut objectives = Vec::with_capacity(scored.len());
        for result in scored {
            objectives.push(result?);
        }

        // Pick the single best strictly-improving move; ties resolve to the
        // earliest move in the fixed enumeration order (deterministic).
        let mut chosen: Option<(usize, Decimal)> = None;
        for (idx, obj) in objectives.iter().copied().enumerate() {
            if obj > best_obj && chosen.is_none_or(|(_, c)| obj > c) {
                chosen = Some((idx, obj));
            }
        }
        let Some((idx, obj)) = chosen else {
            break;
        };
        let (from, to, step) = moves[idx];
        best[from] -= step;
        best[to] += step;
        best_obj = obj;
        improving_rounds = improving_rounds.checked_add(1).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "weighted coordinate-search trial count overflow".to_owned(),
            }
        })?;
    }
    // Seed + each accepted improving round = effective independent trials.
    let effective_trials =
        improving_rounds
            .checked_add(1)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "weighted coordinate-search effective trial count overflow".to_owned(),
            })?;
    Ok((best, effective_trials))
}

/// Enumerate every feasible weight-shifting move from `best` in the fixed order
/// `(step desc, from asc, to asc)`. A move is feasible only when `best[from]` has
/// at least `step` mass to donate (so weights stay non-negative).
fn enumerate_moves(n: usize, best: &[Decimal]) -> Vec<(usize, usize, Decimal)> {
    let mut moves = Vec::new();
    for &step_h in &SEARCH_STEPS {
        let step = Decimal::new(step_h, 2);
        for (from, donor) in best.iter().enumerate() {
            if *donor < step {
                continue;
            }
            for to in 0..n {
                if from != to {
                    moves.push((from, to, step));
                }
            }
        }
    }
    moves
}

/// Normalize a non-negative vector onto the unit simplex (sum to 1). A zero
/// vector becomes uniform.
fn normalize_simplex(weights: &[Decimal]) -> Vec<Decimal> {
    let n = weights.len();
    if n == 0 {
        return Vec::new();
    }
    let sum: Decimal = weights.iter().map(|w| (*w).max(Decimal::ZERO)).sum();
    if sum.is_zero() {
        let uniform = Decimal::ONE / Decimal::from(n as u64);
        return vec![uniform; n];
    }
    weights
        .iter()
        .map(|w| (*w).max(Decimal::ZERO) / sum)
        .collect()
}

/// The reduced, time-ordered training dataset for the weighted trainer.
struct TrainingDataset {
    groups: Vec<CrossSectionGroup>,
    sample_count: u64,
    dropped_singleton_groups: u64,
    dropped_singleton_rows: u64,
}

impl TrainingDataset {
    /// Extract signed factor contributions + label per resolved example, sorted
    /// by `(decision_at, market_id, token_id)` for deterministic folding. Examples
    /// without the target label are skipped (never silently zero-filled).
    /// Cross-sections with fewer than two rows are dropped and counted.
    fn build(
        examples: &[TrainingExample],
        label: &LabelSelector,
        factors: &[FactorName],
    ) -> QuantResult<Self> {
        let mut sorted: Vec<&TrainingExample> = examples.iter().collect();
        sorted.sort_by(|a, b| {
            a.decision_at()
                .cmp(&b.decision_at())
                .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
                .then_with(|| a.token_id.as_str().cmp(b.token_id.as_str()))
        });

        let mut groups = Vec::new();
        let mut dropped_singleton_groups = 0_u64;
        let mut dropped_singleton_rows = 0_u64;
        let mut current_decision_at = None;
        let mut current_horizon_end = None;
        let mut current_rows = Vec::new();
        for example in sorted {
            if current_decision_at.is_some_and(|decision_at| decision_at != example.decision_at()) {
                push_group(
                    &mut groups,
                    current_decision_at,
                    current_horizon_end,
                    mem::take(&mut current_rows),
                    &mut dropped_singleton_groups,
                    &mut dropped_singleton_rows,
                )?;
                current_horizon_end = None;
            }
            current_decision_at = Some(example.decision_at());
            let Some(label_row) = example.labels.iter().find(|row| {
                (&row.label_name, row.horizon_secs) == (&label.name, label.horizon_secs)
            }) else {
                continue;
            };
            let label_value = label_row.value;
            current_horizon_end = Some(
                current_horizon_end
                    .map_or(label_row.matured_at, |end| end.max(label_row.matured_at)),
            );
            let signed = factors
                .iter()
                .map(|factor| signed_contribution(example, factor))
                .collect();
            current_rows.push(SampleRow {
                allocation_key: format!(
                    "{}:{}",
                    example.market_id.as_str(),
                    example.token_id.as_str()
                ),
                signed,
                label: label_value,
            });
        }
        if current_decision_at.is_some() {
            push_group(
                &mut groups,
                current_decision_at,
                current_horizon_end,
                current_rows,
                &mut dropped_singleton_groups,
                &mut dropped_singleton_rows,
            )?;
        }
        let sample_count = groups.iter().try_fold(0_u64, |total, group| {
            let row_count =
                u64::try_from(group.rows.len()).map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("training group row-count conversion failed: {error}"),
                })?;
            total.checked_add(row_count).ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: "training sample count overflow".to_owned(),
                })
            })
        })?;
        if groups.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "no decision_at cross-section group with ≥2 rows for label `{}`@{}s \
                     (dropped_singleton_groups={dropped_singleton_groups}, \
                     dropped_singleton_rows={dropped_singleton_rows})",
                    label.name.as_str(),
                    label.horizon_secs
                ),
            }
            .into());
        }
        Ok(Self {
            groups,
            sample_count,
            dropped_singleton_groups,
            dropped_singleton_rows,
        })
    }
}

fn push_group(
    groups: &mut Vec<CrossSectionGroup>,
    decision_at: Option<DateTime<Utc>>,
    label_horizon_end: Option<DateTime<Utc>>,
    rows: Vec<SampleRow>,
    dropped_singleton_groups: &mut u64,
    dropped_singleton_rows: &mut u64,
) -> QuantResult<()> {
    let (Some(decision_at), Some(label_horizon_end)) = (decision_at, label_horizon_end) else {
        return Ok(());
    };
    if rows.len() >= 2 {
        groups.push(CrossSectionGroup::new(decision_at, label_horizon_end, rows));
        return Ok(());
    }
    if rows.is_empty() {
        return Ok(());
    }
    *dropped_singleton_groups =
        dropped_singleton_groups
            .checked_add(1)
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "dropped singleton-group count overflow".to_owned(),
            })?;
    let row_count = u64::try_from(rows.len()).map_err(|error| ResearchError::DatasetBuild {
        detail: format!("singleton row-count conversion failed: {error}"),
    })?;
    *dropped_singleton_rows = dropped_singleton_rows
        .checked_add(row_count)
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "dropped singleton-row count overflow".to_owned(),
        })?;
    Ok(())
}

/// `dir_sign · normalized · confidence` for one factor of one example, or `0`
/// when the factor is absent / unresolved (confidence carries the missingness).
pub(crate) fn signed_contribution(example: &TrainingExample, factor: &FactorName) -> Decimal {
    example
        .factor_values
        .iter()
        .find(|value| value.name == *factor)
        .and_then(|value| {
            let direction = Decimal::from(value.direction.as_i8());
            value
                .normalized_score()
                .map(|score| direction * score.inner() * value.confidence.inner())
        })
        .unwrap_or(Decimal::ZERO)
}

/// A contiguous, time-ordered fold layout for rolling validation.
struct TimeSplit {
    boundaries: Vec<usize>,
}

impl TimeSplit {
    /// Split `len` time-ordered rows into `folds` contiguous blocks.
    fn new(len: usize, folds: usize) -> QuantResult<Self> {
        let folds = folds.max(2);
        let capacity =
            folds
                .checked_add(1)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "weighted fold boundary capacity overflow".to_owned(),
                })?;
        let mut boundaries = Vec::with_capacity(capacity);
        for k in 0..=folds {
            let boundary = len
                .checked_mul(k)
                .map(|product| product / folds)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "weighted fold boundary arithmetic overflow".to_owned(),
                })?;
            boundaries.push(boundary);
        }
        Ok(Self { boundaries })
    }

    /// Every fold block (diagnostic), skipping empties.
    fn blocks(&self) -> Vec<TimeBlock> {
        (1..self.boundaries.len())
            .map(move |k| self.boundaries[k - 1]..self.boundaries[k])
            .filter(|range| range.start < range.end)
            .map(|range| TimeBlock {
                start: range.start,
                end: range.end,
            })
            .collect()
    }

    /// Walk-forward OOS blocks. The first block is never evaluated because no
    /// strictly earlier train partition exists for it.
    fn validation_blocks(&self) -> Vec<TimeBlock> {
        self.blocks().into_iter().skip(1).collect()
    }
}

/// Contiguous group range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeBlock {
    start: usize,
    end: usize,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        slice,
    };

    use chrono::{TimeZone, Utc};
    use quant_pivot_error::{QuantError, research::ResearchError};
    #[cfg(not(feature = "optimize"))]
    use quant_pivot_models::runtime_config::TrainingOptimizerKind;
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{
            common::MarketCategory,
            factor::FactorFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        runtime_config::{FactorCrossSectionConfig, RankLossKind, SmallCrossSectionPolicy},
        types::{
            MarketId, ModelInputContract, Probability, SchemaVersion, TokenId, TrainingExampleId,
            TrainingSampleSource,
            factor::{
                FactorAlphaOrientation, FactorExplanation, FactorOutputSemantics,
                FactorServingPlane,
            },
        },
    };
    use rayon::ThreadPoolBuilder;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CancellationProbe, LabelSelector, ModelTrainer, TimeSplit, TrainModelRequest,
        TrainingDataset, TrainingObjectiveSpec, ValidationSpec, WeightedFactorTrainer,
        coordinate_search, weighted_training_input_hash,
    };
    use crate::{
        factors::{
            FactorName, FactorValue, FrozenReferenceQuantiles, NormalizedFactor,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        features::FeatureVector,
        model::{
            ReturnModelSpec,
            artifact::{HorizonMultipliers, SubstitutionConfidenceRules},
            objective::{CrossSectionGroup, ObjectiveEvaluator, SampleRow},
        },
        test_support::{factor_head, factor_revision},
        training::{LabelName, TrainingExample, TrainingLabel, fixtures},
    };

    fn label_name() -> LabelName {
        LabelName::new("token_payout_ratio")
    }

    fn training_factor_plane() -> FactorServingPlane {
        FactorServingPlane::try_seal(vec![
            factor_revision(
                LIQUIDITY_DEPTH,
                FactorFamily::Liquidity,
                FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::CanonicalYes,
                },
            ),
            factor_revision(
                MOMENTUM_ROC,
                FactorFamily::Momentum,
                FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::CanonicalYes,
                },
            ),
        ])
        .expect("training factor plane")
    }

    fn example(
        idx: i64,
        liq: Decimal,
        mom: Decimal,
        dir: FactorDirection,
        y: Decimal,
    ) -> TrainingExample {
        let fv = FeatureVector {
            market_id: MarketId::new(format!("0x{idx}")),
            token_id: Some(TokenId::new("yes")),
            decision_at: Utc.timestamp_opt(1_700_000_000 + (idx / 2), 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        };
        let plane = training_factor_plane();
        let mk = |name: FactorName, score: Decimal| {
            let revision = plane
                .definitions()
                .iter()
                .find(|revision| revision.factor_name() == &name)
                .expect("training factor revision");
            let raw_value = if dir == FactorDirection::Negative {
                -score
            } else {
                score
            };
            FactorValue {
                definition_id: revision.factor_definition_id(),
                name: revision.factor_name().clone(),
                family: revision.definition().family,
                raw_value: Some(raw_value),
                normalization: NormalizedFactor::cross_section(Probability::new(score)),
                direction: revision
                    .definition()
                    .contribution_direction(raw_value)
                    .expect("training factor direction"),
                confidence: Probability::new(dec!(1)),
                explanation: FactorExplanation {
                    headline: "t".to_owned(),
                    drivers: Vec::new(),
                },
                input_feature_refs: Vec::new(),
            }
        };
        let as_of = Utc.timestamp_opt(1_700_000_000 + (idx / 2), 0).unwrap();
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new(format!("0x{idx}")),
            token_id: TokenId::new("yes"),
            selected_market: fixtures::selected_market(
                &MarketId::new(format!("0x{idx}")),
                &TokenId::new("yes"),
                MarketCategory::Sports,
            ),
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: fv,
            factor_values: vec![mk(LIQUIDITY_DEPTH, liq), mk(MOMENTUM_ROC, mom)],
            labels: vec![TrainingLabel {
                label_name: label_name(),
                horizon_secs: 0,
                value: y,
                is_resolved: true,
                matured_at: as_of,
            }],
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    fn request(examples: Vec<TrainingExample>) -> TrainModelRequest {
        let factor_plane = training_factor_plane();
        TrainModelRequest {
            cancellation: CancellationProbe::default(),
            examples: examples.into(),
            label: LabelSelector {
                name: label_name(),
                horizon_secs: 0,
            },
            seed_head: factor_head(&factor_plane),
            factor_plane,
            objective: TrainingObjectiveSpec {
                lambda_tail: Decimal::ZERO,
                lambda_turnover: Decimal::ZERO,
                lambda_l2: Decimal::ZERO,
                ..TrainingObjectiveSpec::default()
            },
            validation: ValidationSpec {
                folds: 3,
                ..ValidationSpec::default()
            },
            horizon_multipliers: HorizonMultipliers::conservative(),
            substitution_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            input_contract: ModelInputContract::single_required("book.mid"),
            factor_cross_section: FactorCrossSectionConfig::default(),
        }
    }

    /// Momentum tracks the label while liquidity anti-tracks it, so the rank-IC
    /// objective is strictly improved by shifting weight onto momentum.
    fn momentum_dataset() -> Vec<TrainingExample> {
        (0..20)
            .map(|i| {
                let strong = i % 2 == 0;
                let (liq, mom, y) = if strong {
                    (dec!(0.2), dec!(0.9), dec!(1))
                } else {
                    (dec!(0.8), dec!(0.1), dec!(0))
                };
                example(i, liq, mom, FactorDirection::Positive, y)
            })
            .collect()
    }

    #[test]
    fn weighted_trainer_keeps_split() {
        let zero_examples = momentum_dataset();
        let mut split_examples = zero_examples.clone();
        split_examples[1].labels[0].value = dec!(0.5);
        let mut full_examples = zero_examples.clone();
        full_examples[1].labels[0].value = dec!(1);
        let factors = vec![LIQUIDITY_DEPTH, MOMENTUM_ROC];
        let references = FrozenReferenceQuantiles::empty();
        let cross_section = FactorCrossSectionConfig::default();
        let input_hash = |examples: &[TrainingExample]| {
            weighted_training_input_hash(
                examples,
                &LabelSelector {
                    name: label_name(),
                    horizon_secs: 0,
                },
                &factors,
                &references,
                Some(&cross_section),
            )
            .expect("payout-ratio training input hash")
        };

        let zero_hash = input_hash(&zero_examples);
        let split_hash = input_hash(&split_examples);
        let full_hash = input_hash(&full_examples);
        assert_ne!(
            split_hash, zero_hash,
            "split payout must not coerce to zero"
        );
        assert_ne!(split_hash, full_hash, "split payout must not coerce to one");

        request(split_examples)
            .train_weighted()
            .expect("continuous trainer must accept a split payout");
    }

    #[tokio::test]
    async fn weighted_trainer_produces_hash() {
        let trainer = WeightedFactorTrainer::new();
        // Same request and data produce byte-identical payload commitments.
        let req = request(momentum_dataset());
        let a = trainer.train(req.clone()).await.expect("train a");
        let b = trainer.train(req).await.expect("train b");
        assert_eq!(
            a.payload
                .model_payload_hash()
                .expect("first model payload hash"),
            b.payload
                .model_payload_hash()
                .expect("second model payload hash"),
            "training must be deterministic"
        );
        a.payload
            .validate_for_plane(&training_factor_plane())
            .expect("valid trained payload");
        assert!(
            a.training_input_hash
                .as_bytes()
                .iter()
                .any(|byte| *byte != 0)
        );
        assert_eq!(
            a.in_sample_metrics.objective_value,
            b.in_sample_metrics.objective_value
        );
    }

    #[test]
    fn prepared_fold_matches_direct() {
        let mut direct_request = request(momentum_dataset());
        direct_request.validation.folds = 1;
        let objective = direct_request.objective.clone();
        let prepared_request = direct_request.clone();

        let direct = direct_request
            .train_weighted()
            .expect("direct full-window fold fit");
        let prepared = prepared_request
            .prepare_fold()
            .expect("prepare exact fold matrix")
            .train(&objective)
            .expect("fit prepared fold objective");

        assert_eq!(prepared.training_input_hash, direct.training_input_hash);
        assert_eq!(prepared.input_contract_hash, direct.input_contract_hash);
        assert_eq!(prepared.input_transform_hash, direct.input_transform_hash);
        assert_eq!(prepared.in_sample_metrics, direct.in_sample_metrics);
        assert_eq!(prepared.validation_metrics, direct.validation_metrics);
        assert_eq!(
            prepared
                .payload
                .model_payload_hash()
                .expect("prepared payload hash"),
            direct
                .payload
                .model_payload_hash()
                .expect("direct payload hash")
        );
    }

    #[test]
    fn prepared_rejects_inner_cv() {
        match request(momentum_dataset()).prepare_fold() {
            Err(QuantError::Research(ResearchError::ValidationMethodology { .. })) => {}
            Err(error) => panic!("unexpected prepared-fold error: {error}"),
            Ok(_) => panic!("prepared folds must not replace inner validation"),
        }
    }

    #[tokio::test]
    async fn weighted_trainer_before_build() {
        let mut request = request(momentum_dataset());
        request.cancellation = CancellationProbe::new(|| true);

        let error = WeightedFactorTrainer::new()
            .train(request)
            .await
            .expect_err("cancelled training must fail closed");

        assert!(matches!(
            error,
            QuantError::Research(ResearchError::Cancelled { .. })
        ));
    }

    #[test]
    fn future_validation_cannot_transform() {
        let baseline_examples = momentum_dataset();
        let mut shifted_examples = baseline_examples.clone();
        let mut group_times = baseline_examples
            .iter()
            .map(TrainingExample::decision_at)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        group_times.sort();
        let future_start = group_times[6];
        for example in shifted_examples
            .iter_mut()
            .filter(|example| example.decision_at() >= future_start)
        {
            for (index, factor) in example.factor_values.iter_mut().enumerate() {
                factor.raw_value = Some(Decimal::from(1_000 + index));
            }
        }
        let mut baseline_request = request(baseline_examples);
        baseline_request
            .factor_cross_section
            .small_cross_section_policy = SmallCrossSectionPolicy::FrozenReferenceQuantile;
        let mut shifted_request = request(shifted_examples);
        shifted_request
            .factor_cross_section
            .small_cross_section_policy = SmallCrossSectionPolicy::FrozenReferenceQuantile;

        let baseline = baseline_request.train_weighted().expect("baseline folds");
        let shifted = shifted_request.train_weighted().expect("shifted folds");
        assert_eq!(
            baseline.validation_metrics.fold_components[0],
            shifted.validation_metrics.fold_components[0],
            "future validation rows must not fit an earlier fold reference CDF or estimator"
        );
        assert_ne!(
            baseline
                .payload
                .model_payload_hash()
                .expect("baseline payload hash"),
            shifted
                .payload
                .model_payload_hash()
                .expect("shifted payload hash"),
            "the explicit final full-window refit must still bind the changed future rows"
        );
    }

    #[test]
    fn weighted_training_binds_boundary() {
        let examples = momentum_dataset();
        let label = LabelSelector {
            name: label_name(),
            horizon_secs: 0,
        };
        let factors = vec![LIQUIDITY_DEPTH, MOMENTUM_ROC];
        let references = FrozenReferenceQuantiles::empty();
        let cross_section = FactorCrossSectionConfig::default();
        let original = weighted_training_input_hash(
            &examples,
            &label,
            &factors,
            &references,
            Some(&cross_section),
        )
        .expect("original input hash");
        let mut changed = examples;
        let decision_at = changed[0].decision_at();
        changed[0].decision_boundary = DecisionClock::new(1)
            .boundary(decision_at)
            .expect("changed boundary");
        let changed = weighted_training_input_hash(
            &changed,
            &label,
            &factors,
            &references,
            Some(&cross_section),
        )
        .expect("changed input hash");
        assert_ne!(original, changed);
    }

    /// The artifact hash must not depend on `rayon`'s thread count: the parallel
    /// coordinate search + fold reduction is a pure, source-ordered computation,
    /// so a 1-thread pool and a 4-thread pool must produce a byte-identical model.
    #[test]
    fn weighted_trainer_thread_invariant() {
        let req = request(momentum_dataset());
        let single = ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("single-thread pool");
        let many = ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("multi-thread pool");
        let a = single
            .install(|| req.train_weighted())
            .expect("train single");
        let b = many.install(|| req.train_weighted()).expect("train many");
        assert_eq!(
            a.payload
                .model_payload_hash()
                .expect("single-thread payload hash"),
            b.payload
                .model_payload_hash()
                .expect("multi-thread payload hash"),
            "rayon thread count must not change the trained artifact"
        );
    }

    #[tokio::test]
    async fn weighted_trainer_seeds_weights() {
        // Momentum predicts the label; the optimizer should give momentum more
        // weight than the 0.5 seed.
        let factor_trainer = WeightedFactorTrainer::new();
        for rank_loss in [
            RankLossKind::TargetRankIcWeightedRanknet,
            RankLossKind::PairwiseRanknet,
        ] {
            let mut request = request(momentum_dataset());
            request.objective.rank_loss = rank_loss;
            let outcome = factor_trainer.train(request).await.expect("train");
            let momentum = outcome
                .payload
                .factor_head
                .alpha_weights
                .iter()
                .find(|w| w.factor == MOMENTUM_ROC)
                .expect("momentum weight");
            assert!(
                momentum.weight > dec!(0.5),
                "{rank_loss:?} momentum weight {} should exceed the 0.5 seed",
                momentum.weight
            );
            let report = &outcome.in_sample_metrics;
            assert_eq!(
                report.objective_value,
                outcome.in_sample_metrics.objective_value
            );
            assert_eq!(report.objective_value, -report.components.total_loss);
            assert!(report.components.rank_loss > Decimal::ZERO);
            assert!(report.components.pair_count > 0);
            let diagnostics = report.diagnostics.as_ref().expect("diagnostics");
            assert!(diagnostics.group_count > 0);
            assert_eq!(diagnostics.ndcg_k, 20);
            assert!(outcome.validation_metrics.held_out_diagnostics.is_some());
            assert_eq!(
                outcome.validation_metrics.fold_objectives.len(),
                2,
                "three time blocks must produce two independently fitted walk-forward OOS folds"
            );
            assert_eq!(outcome.validation_metrics.fold_components.len(), 2);
        }
    }

    #[test]
    fn time_split_atomic_groups() {
        // `TrainingDataset::build` collapses each decision_at into one group; TimeSplit
        // indexes groups (not rows), so a cross-section cannot be bisected.
        let examples = momentum_dataset();
        let factors = [LIQUIDITY_DEPTH, MOMENTUM_ROC];
        let dataset = TrainingDataset::build(
            &examples,
            &LabelSelector {
                name: label_name(),
                horizon_secs: 0,
            },
            &factors,
        )
        .expect("dataset");
        let distinct_decision_times: BTreeSet<_> =
            examples.iter().map(TrainingExample::decision_at).collect();
        assert_eq!(
            dataset.groups.len(),
            distinct_decision_times.len(),
            "one group per decision_at (momentum_dataset has >=2 rows each)"
        );
        for group in &dataset.groups {
            assert!(group.rows.len() >= 2);
        }
        assert_eq!(dataset.dropped_singleton_groups, 0);
        assert_eq!(dataset.dropped_singleton_rows, 0);
        let split = TimeSplit::new(dataset.groups.len(), 3).expect("time split");
        let covered: usize = split
            .blocks()
            .iter()
            .map(|block| block.end - block.start)
            .sum();
        assert_eq!(covered, dataset.groups.len());
        assert_eq!(split.validation_blocks().len(), 2);
        assert_eq!(
            split
                .validation_blocks()
                .last()
                .expect("held-out block")
                .end,
            dataset.groups.len()
        );
    }

    #[test]
    fn singleton_decision_groups_counted() {
        // Pair even/odd into one decision instant via `idx / 2`. Inject one singleton.
        let mut examples = momentum_dataset();
        examples.push(example(
            100,
            dec!(0.5),
            dec!(0.5),
            FactorDirection::Positive,
            dec!(1),
        ));
        let factors = [LIQUIDITY_DEPTH, MOMENTUM_ROC];
        let dataset = TrainingDataset::build(
            &examples,
            &LabelSelector {
                name: label_name(),
                horizon_secs: 0,
            },
            &factors,
        )
        .expect("dataset");
        assert_eq!(dataset.dropped_singleton_groups, 1);
        assert_eq!(dataset.dropped_singleton_rows, 1);
        assert_eq!(dataset.groups.len(), 10); // momentum_dataset: 20 rows / 2
    }

    #[test]
    fn lambda_tail_changes_negative() {
        // Two-row group: selecting the high-score name yields a large loss.
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let group = CrossSectionGroup::new(
            decision_at,
            decision_at,
            vec![
                SampleRow {
                    allocation_key: "m:a".to_owned(),
                    signed: vec![dec!(1), dec!(0)],
                    label: dec!(-500),
                },
                SampleRow {
                    allocation_key: "m:b".to_owned(),
                    signed: vec![dec!(0), dec!(1)],
                    label: dec!(100),
                },
            ],
        );
        let weights = [dec!(1), dec!(0)]; // selects m:a into Top1
        let zero_tail = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            pseudo_top_n: 1,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&weights, slice::from_ref(&group))
        .expect("zero");
        let heavy_tail = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: dec!(2),
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            pseudo_top_n: 1,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&weights, &[group])
        .expect("heavy");
        assert!(heavy_tail.tail_penalty > Decimal::ZERO);
        assert!(
            heavy_tail.total_loss > zero_tail.total_loss,
            "lambda_tail must increase total_loss when TopN return is negative \
             (zero={}, heavy={})",
            zero_tail.total_loss,
            heavy_tail.total_loss
        );
    }

    #[tokio::test]
    async fn validation_exposes_dropped_count() {
        let outcome = request(momentum_dataset()).train_weighted().expect("train");
        assert_eq!(outcome.validation_metrics.dropped_singleton_groups, 0);
        assert!(outcome.in_sample_metrics.components.rank_loss_group_count > 0);
        assert!(
            outcome
                .in_sample_metrics
                .summary
                .contains("TopN pseudo=rank-equal")
        );
    }

    #[cfg(not(feature = "optimize"))]
    #[test]
    fn argmin_without_optimize_rejects() {
        let mut req = request(momentum_dataset());
        req.objective.optimizer = TrainingOptimizerKind::Argmin;
        let err = req.train_weighted().expect_err("argmin must fail closed");
        let detail = err.to_string();
        assert!(
            detail.contains("optimize"),
            "error should mention optimize feature: {detail}"
        );
    }

    /// Long label horizons that overlap the held-out fold must be purged from
    /// the trainer CV train set (same semantics as CPCV `PurgedSplitter`).
    #[test]
    fn trainer_cv_respects_purge() {
        // 6 decision-time groups (2 markets each). Held-out fold for folds=3 is the
        // last 2 groups. Give early groups a matured_at that overlaps the
        // held-out decision time so purge removes them from train.
        let mut examples = Vec::new();
        for group_idx in 0..6_i64 {
            let as_of = Utc
                .timestamp_opt(1_700_000_000 + group_idx * 100, 0)
                .unwrap();
            // Groups 0..=3 mature after group 4's decision_at → overlap held-out.
            let matured_at = if group_idx <= 3 {
                Utc.timestamp_opt(1_700_000_000 + 450, 0).unwrap()
            } else {
                as_of
            };
            for market in 0..2_i64 {
                let idx = group_idx * 10 + market;
                let mut ex = example(
                    idx,
                    dec!(0.5),
                    dec!(0.5),
                    FactorDirection::Positive,
                    Decimal::from(market),
                );
                ex.decision_boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
                ex.feature_vector.decision_at = as_of;
                ex.labels[0].matured_at = matured_at;
                examples.push(ex);
            }
        }
        let mut req = request(examples);
        req.validation = ValidationSpec {
            folds: 3,
            embargo_pct: Decimal::ZERO,
            min_embargo_secs: 0,
        };
        // With aggressive overlap, purged train may be empty → fail closed.
        let result = req.train_weighted();
        match result {
            Ok(outcome) => {
                // If enough non-overlapping groups remain, training succeeds
                // and still records coord-search effective N ≥ 1.
                assert!(outcome.validation_metrics.coord_search_effective_n >= 1);
            }
            Err(err) => {
                let detail = err.to_string();
                assert!(
                    detail.contains("purged") || detail.contains("insufficient"),
                    "expected purge/insufficient failure, got: {detail}"
                );
            }
        }
    }

    #[test]
    fn coordinate_search_effective_seed() {
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let groups = vec![CrossSectionGroup::new(
            decision_at,
            decision_at,
            vec![
                SampleRow {
                    allocation_key: "m:a".to_owned(),
                    signed: vec![dec!(1), dec!(0)],
                    label: dec!(1),
                },
                SampleRow {
                    allocation_key: "m:b".to_owned(),
                    signed: vec![dec!(0), dec!(1)],
                    label: dec!(0),
                },
            ],
        )];
        let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            ..TrainingObjectiveSpec::default()
        });
        let seed = vec![dec!(0.5), dec!(0.5)];
        let (_, effective_n) =
            coordinate_search(&seed, &groups, &evaluator, &CancellationProbe::default())
                .expect("search");
        assert!(
            effective_n >= 1,
            "seed always counts as one effective trial"
        );
    }
}

/// `optimize`-feature tests: the `argmin` refinement is gradient-free,
/// seeded from the grid solution, and accepted only when it strictly improves the
/// Decimal objective — so it can never weaken the model and its accepted result
/// is deterministic on a fixed platform.
#[cfg(all(test, feature = "optimize"))]
mod optimize_tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::types::model_training::TrainingObjectiveSpec;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{CancellationProbe, coordinate_search, refine};
    use crate::model::{
        objective::{CrossSectionGroup, ObjectiveEvaluator, SampleRow},
        optimize::refine_weights,
    };

    /// A tiny deterministic LCG so the property test needs no `rand` dependency.
    const fn next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state >> 33
    }

    /// Synthesize `count` rows over `n` factors where factor 0 carries the signal
    /// (the label equals factor 0's signed contribution), so the rank-IC optimum
    /// lies near the `e0` simplex vertex.
    fn synth_group(seed: u64, n: usize, count: usize) -> Vec<CrossSectionGroup> {
        let mut state = seed.wrapping_add(1);
        let rows = (0..count)
            .map(|_| {
                let signed: Vec<Decimal> = (0..n)
                    .map(|_| Decimal::from(next(&mut state) % 1000) / dec!(1000) - dec!(0.5))
                    .collect();
                let label = signed[0];
                SampleRow {
                    allocation_key: format!("m{}:t0", next(&mut state)),
                    signed,
                    label,
                }
            })
            .collect();
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        vec![CrossSectionGroup::new(decision_at, decision_at, rows)]
    }

    fn uniform(n: usize) -> Vec<Decimal> {
        let w = Decimal::ONE / Decimal::from(n as u64);
        vec![w; n]
    }

    fn evaluator(l2: Decimal) -> ObjectiveEvaluator {
        ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: l2,
            ..TrainingObjectiveSpec::default()
        })
    }

    /// The same inputs must refine to byte-identical Decimal weights (the accepted
    /// result is deterministic on a fixed platform — the cross-platform `f64`
    /// optimizer is only a candidate generator; adoption is decided in Decimal).
    #[test]
    fn optimize_refine_deterministic_platform() {
        let groups = synth_group(7, 3, 60);
        let seed = vec![dec!(0.2), dec!(0.3), dec!(0.5)];
        let evaluator = evaluator(dec!(0.01));
        let a = refine_weights(&seed, &groups, &evaluator).expect("first refinement");
        let b = refine_weights(&seed, &groups, &evaluator).expect("second refinement");
        assert_eq!(a, b, "refine_weights must be deterministic");
        assert!(a.is_some(), "refinement produced a candidate");
    }

    /// Over many random simplex seeds, the accepted (grid + refine) objective is
    /// never worse than the grid objective, and the result stays on the simplex.
    #[test]
    fn optimize_never_worsens_objective() {
        let l2 = dec!(0.01);
        for trial in 0..100u64 {
            let n = 3;
            let groups = synth_group(trial, n, 50);
            let evaluator = evaluator(l2);
            let (grid, _) = coordinate_search(
                &uniform(n),
                &groups,
                &evaluator,
                &CancellationProbe::default(),
            )
            .expect("grid");
            let grid_obj = evaluator
                .evaluate(&grid, &groups)
                .expect("eval")
                .objective_value();
            let (weights, objective) =
                refine(&grid, grid_obj, &groups, &evaluator).expect("refine");
            assert!(
                objective.objective_value() >= grid_obj,
                "trial {trial}: refined objective {} < grid {grid_obj}",
                objective.objective_value()
            );
            for w in &weights {
                assert!(*w >= dec!(0), "trial {trial}: negative weight {w}");
            }
            let sum: Decimal = weights.iter().sum();
            assert!(
                (sum - Decimal::ONE).abs() <= dec!(0.0001),
                "trial {trial}: weights off simplex (sum {sum})"
            );
        }
    }

    /// On a dataset where the grid already sits at the global vertex optimum,
    /// refinement cannot improve it, so the trainer falls back to the grid weights.
    #[test]
    fn optimize_converges_falls_grid() {
        let l2 = Decimal::ZERO;
        let groups = synth_group(42, 3, 80);
        let evaluator = evaluator(l2);
        let (grid, _) = coordinate_search(
            &uniform(3),
            &groups,
            &evaluator,
            &CancellationProbe::default(),
        )
        .expect("grid");
        let grid_obj = evaluator
            .evaluate(&grid, &groups)
            .expect("eval")
            .objective_value();
        let (_, objective) = refine(&grid, grid_obj, &groups, &evaluator).expect("refine");
        assert!(
            objective.objective_value() >= grid_obj,
            "refine never worsens the grid"
        );
    }
}
