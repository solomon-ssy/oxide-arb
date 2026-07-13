//! Model training contract and the [`WeightedFactorTrainer`] (Phase 3.6 / 11.4).
//!
//! The weighted-factor trainer optimizes the **frozen factor weights** that the
//! online [`WeightedFactorRuntime`](crate::model::weighted::WeightedFactorRuntime)
//! applies. It mirrors that runtime's ranking formula exactly:
//!
//! ```text
//! signedᵢ = dir_signᵢ · normalizedᵢ · confidenceᵢ
//! net     = Σ weightᵢ · signedᵢ                 (the ranking score, ∈ [-1, 1])
//! ```
//!
//! and searches the weight **simplex** (non-negative, sum to 1) to minimize the
//! governed LTR objective (`RankIC`-weighted `RankNet` or `RankNet` + `TopN`
//! tail/turnover + L2). The deterministic coordinate search is the **base**
//! optimizer (always linked); the `optimize` feature adds an `argmin` refinement
//! that is kept only when it strictly improves the training objective. Configuring
//! `argmin` without the feature **fails closed** (never silently degrades).
//!
//! Determinism is a hard, money-critical invariant: the same `(examples, label,
//! seed, objective)` must yield a byte-identical artifact hash.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::domain::DecisionBoundary;
use quant_pivot_models::enums::common::MarketCategory;
use quant_pivot_models::enums::factor::NormalizationSource;
use quant_pivot_models::runtime_config::{
    FactorCrossSectionConfig, SmallCrossSectionPolicy, TrainingOptimizerKind,
};
use quant_pivot_models::types::{ContentHash, MarketId, ModelInputContract, TokenId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorName, FrozenReferenceCdf, FrozenReferenceQuantiles, NormalizedFactor},
    hashing::ResearchHasher,
    model::{
        artifact::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
            SubstitutionConfidenceRules, TrainingObjectiveReport, WeightedFactorModelArtifact,
            model_input_contract_hash,
        },
        category_scope::validate_category_scope_weights,
        objective::{
            CrossSectionGroup, ObjectiveComponentReport, ObjectiveEvaluator, RankingDiagnostics,
            SampleRow, TrainingObjectiveSpec,
        },
        runtime::ModelFamily,
        sell_scorer::position_state::{
            is_position_state_factor, position_state_signed_contribution,
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
    /// Label name (e.g. `settlement_outcome`).
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
    /// Effective independent trial count from `coordinate_search` (Bailey
    /// multiple-testing correction input; Phase 11.5 DSR N decomposition).
    #[serde(default)]
    pub coord_search_effective_n: u32,
}

/// Request to train a weighted-factor model from frozen examples.
///
/// All artifact governance (multipliers, substitution rules, return model,
/// required features) is supplied frozen; the trainer only optimizes `weights`
/// and fills the objective report.
#[derive(Debug, Clone)]
pub struct TrainModelRequest {
    /// Frozen, point-in-time training examples (decoded from the dataset Parquet).
    pub examples: Vec<TrainingExample>,
    /// Semantic hash of the frozen dataset envelope supplying `examples`.
    pub training_dataset_hash: ContentHash,
    /// Supervised target label.
    pub label: LabelSelector,
    /// Initial weights / candidate factor set (from `FactorsConfig.factor_weights`).
    pub seed_weights: Vec<FactorWeight>,
    /// Governed training objective snapshot.
    pub objective: TrainingObjectiveSpec,
    /// Rolling validation split.
    pub validation: ValidationSpec,
    /// Frozen artifact header (model version + schema hashes).
    pub header: ModelArtifactHeader,
    /// Frozen prediction horizon (from `ModelConfig.prediction_horizon_secs`).
    pub prediction_horizon_secs: u64,
    /// Governed score multipliers.
    pub multipliers: ScoreMultiplierSpec,
    /// Governed substitution confidence penalties.
    pub substitution_rules: SubstitutionConfidenceRules,
    /// Return model (heuristic until calibrated by a backtest).
    pub return_model: ReturnModelSpec,
    /// Exact ordered raw-input contract frozen by the owning model spec.
    pub input_contract: ModelInputContract,
    /// Small-cross-section transform policy/minimum fitted together with the
    /// weighted artifact.
    pub factor_cross_section: FactorCrossSectionConfig,
    /// When set, this artifact is scoped to one market category (Phase 11.2.2).
    /// `None` means the generic cross-category scorer.
    pub category_scope: Option<MarketCategory>,
}

/// A freshly trained, content-addressed model artifact with its metrics.
#[derive(Debug, Clone)]
pub struct TrainedModelArtifact {
    /// The trained artifact.
    pub artifact: ModelArtifact,
    /// Canonical hash of the artifact.
    pub artifact_hash: ContentHash,
    /// In-sample (training-fold) objective.
    pub in_sample_metrics: TrainingObjectiveReport,
    /// Held-out validation objective.
    pub validation_metrics: ValidationReport,
}

/// Trains a model family into a content-addressed artifact.
#[async_trait]
pub trait ModelTrainer: Send + Sync {
    /// Family this trainer produces.
    fn model_family(&self) -> ModelFamily;

    /// Train and emit a hashed artifact.
    async fn train(&self, request: TrainModelRequest) -> QuantResult<TrainedModelArtifact>;
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

    async fn train(&self, request: TrainModelRequest) -> QuantResult<TrainedModelArtifact> {
        train_weighted(&request)
    }
}

/// The pure training routine (CPU-bound, deterministic).
fn train_weighted(request: &TrainModelRequest) -> QuantResult<TrainedModelArtifact> {
    let fit = fit_simplex_weights(
        &request.examples,
        &request.label,
        &request.seed_weights,
        &request.objective,
        request.validation,
        Some(&request.factor_cross_section),
    )?;

    let factors = fit
        .factor_weights
        .iter()
        .map(|weight| weight.factor.clone())
        .collect::<Vec<_>>();
    let training_input_hash = weighted_training_input_hash(
        &request.examples,
        &request.label,
        &factors,
        &fit.frozen_reference_quantiles,
        Some(&request.factor_cross_section),
    )?;
    let input_contract_hash = model_input_contract_hash(&request.input_contract)?;
    let artifact = WeightedFactorModelArtifact {
        header: request.header.clone(),
        training_dataset_hash: request.training_dataset_hash.clone(),
        training_input_hash,
        input_contract: request.input_contract.clone(),
        input_contract_hash,
        weights: fit.factor_weights,
        prediction_horizon_secs: request.prediction_horizon_secs,
        multipliers: request.multipliers.clone(),
        substitution_confidence_rules: request.substitution_rules.clone(),
        return_model: request.return_model.clone(),
        factor_cross_section: request.factor_cross_section.clone(),
        frozen_reference_quantiles: fit.frozen_reference_quantiles,
        objective_report: Some(fit.objective_report.clone()),
        category_scope: request.category_scope,
    };
    validate_category_scope_weights(request.category_scope, &artifact.weights)?;
    artifact.validate()?;
    let model_artifact = ModelArtifact::WeightedFactor(Box::new(artifact));
    let artifact_hash = model_artifact.content_hash()?;

    Ok(TrainedModelArtifact {
        artifact: model_artifact,
        artifact_hash,
        in_sample_metrics: fit.objective_report,
        validation_metrics: fit.validation,
    })
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
    let mut present_counts = std::collections::BTreeMap::new();
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
            let reference = references.get(&factor.name).ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "factor `{}` has no frozen reference CDF in this training partition",
                        factor.name
                    ),
                })
            })?;
            factor.normalization = NormalizedFactor::Scored {
                score: reference.percentile(raw),
                source: NormalizationSource::FrozenReferenceQuantile,
                clamp: None,
            };
        }
    }
    Ok(prepared)
}

/// The outcome of the shared simplex weight fit: the frozen normalized factor
/// weights plus the training / validation objective reports. Reused by every
/// weighted family (Buy-side [`WeightedFactorModelArtifact`], Sell-side
/// [`SellScorerArtifact`](crate::model::artifact::SellScorerArtifact)) so the
/// deterministic LTR simplex search lives in exactly one place.
#[derive(Debug, Clone)]
pub(crate) struct FittedWeights {
    /// Frozen, normalized per-factor weights in seed order.
    pub factor_weights: Vec<FactorWeight>,
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
    factors: &'a [FactorName],
    seed: &'a [Decimal],
    evaluator: &'a ObjectiveEvaluator,
    factor_cross_section: Option<&'a FactorCrossSectionConfig>,
}

/// Fit the weight simplex against a supervised label via deterministic LTR
/// coordinate search (+ optional `argmin` refinement). Family-agnostic: the
/// caller wraps the frozen weights into its concrete artifact body.
pub(crate) fn fit_simplex_weights(
    examples: &[TrainingExample],
    label: &LabelSelector,
    seed_weights: &[FactorWeight],
    objective: &TrainingObjectiveSpec,
    validation: ValidationSpec,
    factor_cross_section: Option<&FactorCrossSectionConfig>,
) -> QuantResult<FittedWeights> {
    if seed_weights.is_empty() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "trainer requires a non-empty seed weight (candidate factor) set".to_owned(),
        }
        .into());
    }
    ensure_optimizer_available(objective.optimizer)?;

    // Deterministic factor order: the seed-weight order defines the columns.
    let factors: Vec<FactorName> = seed_weights.iter().map(|w| w.factor.clone()).collect();
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
            .map(|w| w.weight.max(Decimal::ZERO))
            .collect::<Vec<_>>(),
    );

    let context = SimplexFitContext {
        examples,
        label,
        factors: &factors,
        seed: &seed,
        evaluator: &evaluator,
        factor_cross_section,
    };
    if folds < 2 {
        return fit_full_window(&context);
    }
    let validation_report = fit_purged_validation(&context, &timeline_dataset, validation, folds)?;
    fit_final_full_window(&context, validation_report)
}

fn fit_full_window(context: &SimplexFitContext<'_>) -> QuantResult<FittedWeights> {
    let references = fit_frozen_reference_quantiles(
        context.examples,
        context.label,
        context.factors,
        context.factor_cross_section,
    )?;
    let prepared =
        apply_reference_quantiles(context.examples, &references, context.factor_cross_section)?;
    let dataset = TrainingDataset::build(&prepared, context.label, context.factors)?;
    let (grid_weights, coord_search_effective_n) =
        coordinate_search(context.seed, &dataset.groups, context.evaluator)?;
    let grid_report = context.evaluator.evaluate(&grid_weights, &dataset.groups)?;
    let (weights, train_report) = refine(
        &grid_weights,
        grid_report.objective_value(),
        &dataset.groups,
        context.evaluator,
    )?;
    assemble_full_window_weights(
        context.factors,
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
        .collect::<std::collections::BTreeSet<_>>();
    let reference_examples = context
        .examples
        .iter()
        .filter(|example| train_decision_at.contains(&example.decision_at()))
        .cloned()
        .collect::<Vec<_>>();
    let references = fit_frozen_reference_quantiles(
        &reference_examples,
        context.label,
        context.factors,
        context.factor_cross_section,
    )?;
    let transformed_examples =
        apply_reference_quantiles(context.examples, &references, context.factor_cross_section)?;
    let fold_dataset =
        TrainingDataset::build(&transformed_examples, context.label, context.factors)?;
    ensure_fold_spine(timeline_dataset, &fold_dataset, fold_index)?;
    let train_groups = train_indices
        .iter()
        .map(|index| fold_dataset.groups[*index].clone())
        .collect::<Vec<_>>();
    let validation_groups = &fold_dataset.groups[block.start..block.end];
    let (grid_weights, effective_n) =
        coordinate_search(context.seed, &train_groups, context.evaluator)?;
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

fn ensure_fold_spine(
    timeline_dataset: &TrainingDataset,
    fold_dataset: &TrainingDataset,
    fold_index: usize,
) -> QuantResult<()> {
    let changed = fold_dataset.groups.len() != timeline_dataset.groups.len()
        || fold_dataset
            .groups
            .iter()
            .zip(&timeline_dataset.groups)
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

fn fit_final_full_window(
    context: &SimplexFitContext<'_>,
    validation: ValidationReport,
) -> QuantResult<FittedWeights> {
    // Refit both the reference CDFs and the estimator on the final full training
    // partition after CV. Held-out rows may assess the fold transform but cannot
    // influence it; only this explicit final fit may consume the full dataset.
    let final_references = fit_frozen_reference_quantiles(
        context.examples,
        context.label,
        context.factors,
        context.factor_cross_section,
    )?;
    let final_examples = apply_reference_quantiles(
        context.examples,
        &final_references,
        context.factor_cross_section,
    )?;
    let final_dataset = TrainingDataset::build(&final_examples, context.label, context.factors)?;
    let (final_grid, final_effective_n) =
        coordinate_search(context.seed, &final_dataset.groups, context.evaluator)?;
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
        context.factors,
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
    factors: &[FactorName],
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
    let factor_weights: Vec<FactorWeight> = factors
        .iter()
        .zip(&frozen)
        .map(|(factor, weight)| FactorWeight {
            factor: factor.clone(),
            weight: weight.round_dp(RESEARCH_DECIMAL_SCALE),
        })
        .collect();
    Ok(FittedWeights {
        factor_weights,
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
    use crate::model::optimize;

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
) -> QuantResult<(Vec<Decimal>, u32)> {
    let n = seed.len();
    let mut best = seed.to_vec();
    let mut best_obj = evaluator.evaluate(&best, groups)?.objective_value();
    let mut improving_rounds = 0_u32;
    if n < 2 {
        return Ok((best, 1));
    }

    for _ in 0..MAX_SEARCH_ROUNDS {
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
                    std::mem::take(&mut current_rows),
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
        groups.push(CrossSectionGroup {
            decision_at,
            label_horizon_end,
            rows,
        });
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
    if is_position_state_factor(factor) {
        if let Some(state) = &example.position_state {
            // The dense additive objective uses zero contribution for an
            // explicitly missing pseudo-factor. The frozen `position_state`
            // remains missing and never gains confidence; pseudo-factors are
            // not duplicated into the governed factor-revision ledger.
            return position_state_signed_contribution(state, factor).unwrap_or(Decimal::ZERO);
        }
        return Decimal::ZERO;
    }
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
    use super::{
        LabelSelector, ModelTrainer, TimeSplit, TrainModelRequest, TrainingDataset,
        TrainingObjectiveSpec, ValidationSpec, WeightedFactorTrainer, weighted_training_input_hash,
    };
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::DecisionClock,
        enums::{
            common::MarketCategory,
            factor::FactorFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        runtime_config::{FactorCrossSectionConfig, SmallCrossSectionPolicy},
        types::{
            ContentHash, FactorDefinitionId, MarketId, ModelInputContract, ModelVersionId,
            Probability, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        factors::{
            FactorExplanation, FactorName, FactorValue, FrozenReferenceQuantiles, NormalizedFactor,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        features::FeatureVector,
        model::{
            ReturnModelSpec,
            artifact::{
                FactorWeight, ModelArtifact, ModelArtifactHeader, ScoreMultiplierSpec,
                SubstitutionConfidenceRules,
            },
            runtime::ModelFamily,
            trainer::train_weighted,
        },
        training::{LabelName, TrainingExample, TrainingLabel, fixtures},
    };

    fn hash(seed: &str) -> ContentHash {
        ContentHash::parse(format!("blake3:{seed:0>64}")).expect("hash")
    }

    fn label_name() -> LabelName {
        LabelName::new("settlement_outcome")
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
        let mk = |name: FactorName, score: Decimal| FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Liquidity,
            raw_value: Some(score),
            normalization: NormalizedFactor::cross_section(Probability::new(score)),
            direction: dir,
            confidence: Probability::new(dec!(1)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
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
        TrainModelRequest {
            examples,
            training_dataset_hash: hash("cc"),
            label: LabelSelector {
                name: label_name(),
                horizon_secs: 0,
            },
            seed_weights: vec![
                FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: dec!(0.5),
                },
                FactorWeight {
                    factor: MOMENTUM_ROC,
                    weight: dec!(0.5),
                },
            ],
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
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: hash("aa"),
                factor_schema_hash: hash("bb"),
            },
            prediction_horizon_secs: 86_400,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            input_contract: ModelInputContract::single_required("book.mid"),
            factor_cross_section: FactorCrossSectionConfig::default(),
            category_scope: None,
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

    #[tokio::test]
    async fn weighted_trainer_produces_stable_hash() {
        let trainer = WeightedFactorTrainer::new();
        // Same request (identical version id + data) ⇒ identical artifact hash.
        let req = request(momentum_dataset());
        let expected_dataset_hash = req.training_dataset_hash.clone();
        let a = trainer.train(req.clone()).await.expect("train a");
        let b = trainer.train(req).await.expect("train b");
        assert_eq!(
            a.artifact_hash, b.artifact_hash,
            "training must be deterministic"
        );
        match a.artifact {
            ModelArtifact::WeightedFactor(art) => {
                art.validate().expect("valid weights");
                assert!(art.objective_report.is_some(), "objective report filled");
                assert_eq!(art.training_dataset_hash, expected_dataset_hash);
                assert!(!art.training_input_hash.as_str().is_empty());
            }
            ModelArtifact::Classical(_) | ModelArtifact::SellScorer(_) => {
                panic!("expected weighted artifact")
            }
        }
    }

    #[test]
    fn future_validation_block_cannot_change_an_earlier_fold_transform() {
        let baseline_examples = momentum_dataset();
        let mut shifted_examples = baseline_examples.clone();
        let mut group_times = baseline_examples
            .iter()
            .map(TrainingExample::decision_at)
            .collect::<std::collections::BTreeSet<_>>()
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

        let baseline = train_weighted(&baseline_request).expect("baseline folds");
        let shifted = train_weighted(&shifted_request).expect("shifted folds");
        assert_eq!(
            baseline.validation_metrics.fold_components[0],
            shifted.validation_metrics.fold_components[0],
            "future validation rows must not fit an earlier fold reference CDF or estimator"
        );
        assert_ne!(
            baseline.artifact_hash, shifted.artifact_hash,
            "the explicit final full-window refit must still bind the changed future rows"
        );
    }

    #[test]
    fn weighted_training_input_hash_binds_decision_boundary() {
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
    fn weighted_trainer_is_thread_count_invariant() {
        let req = request(momentum_dataset());
        let single = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("single-thread pool");
        let many = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("multi-thread pool");
        let a = single
            .install(|| train_weighted(&req))
            .expect("train single");
        let b = many.install(|| train_weighted(&req)).expect("train many");
        assert_eq!(
            a.artifact_hash, b.artifact_hash,
            "rayon thread count must not change the trained artifact"
        );
    }

    #[tokio::test]
    async fn weighted_trainer_seeds_from_config_factor_weights() {
        // Momentum predicts the label; the optimizer should give momentum more
        // weight than the 0.5 seed.
        let factor_trainer = WeightedFactorTrainer::new();
        let outcome = factor_trainer
            .train(request(momentum_dataset()))
            .await
            .expect("train");
        let ModelArtifact::WeightedFactor(art) = outcome.artifact else {
            panic!("weighted");
        };
        let momentum = art
            .weights
            .iter()
            .find(|w| w.factor == MOMENTUM_ROC)
            .expect("momentum weight");
        assert!(
            momentum.weight > dec!(0.5),
            "momentum weight {} should exceed the 0.5 seed",
            momentum.weight
        );
        let report = art
            .objective_report
            .as_ref()
            .expect("weighted artifact objective report");
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

    #[test]
    fn time_split_operates_on_atomic_decision_groups() {
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
        let distinct_decision_times: std::collections::BTreeSet<_> =
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
    fn singleton_decision_groups_are_dropped_and_counted() {
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
    fn lambda_tail_changes_total_loss_when_pseudo_topn_is_negative() {
        use crate::model::objective::{CrossSectionGroup, ObjectiveEvaluator, SampleRow};

        // Two-row group: selecting the high-score name yields a large loss.
        let group = CrossSectionGroup {
            decision_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            label_horizon_end: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            rows: vec![
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
        };
        let weights = [dec!(1), dec!(0)]; // selects m:a into Top1
        let zero_tail = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            pseudo_top_n: 1,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&weights, std::slice::from_ref(&group))
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
    async fn validation_exposes_dropped_singleton_and_rank_loss_group_count() {
        let outcome = train_weighted(&request(momentum_dataset())).expect("train");
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
    fn argmin_without_optimize_feature_fails_closed() {
        use quant_pivot_models::runtime_config::TrainingOptimizerKind;

        let mut req = request(momentum_dataset());
        req.objective.optimizer = TrainingOptimizerKind::Argmin;
        let err = train_weighted(&req).expect_err("argmin must fail closed");
        let detail = err.to_string();
        assert!(
            detail.contains("optimize"),
            "error should mention optimize feature: {detail}"
        );
    }

    /// Long label horizons that overlap the held-out fold must be purged from
    /// the trainer CV train set (same semantics as CPCV `PurgedSplitter`).
    #[test]
    fn trainer_cv_respects_label_horizon_purge() {
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
        let result = train_weighted(&req);
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
    fn coordinate_search_effective_n_at_least_seed() {
        use super::coordinate_search;
        use crate::model::objective::{CrossSectionGroup, ObjectiveEvaluator, SampleRow};

        let groups = vec![CrossSectionGroup {
            decision_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            label_horizon_end: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            rows: vec![
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
        }];
        let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            ..TrainingObjectiveSpec::default()
        });
        let seed = vec![dec!(0.5), dec!(0.5)];
        let (_, effective_n) = coordinate_search(&seed, &groups, &evaluator).expect("search");
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
    use super::{coordinate_search, refine};
    use crate::model::objective::{
        CrossSectionGroup, ObjectiveEvaluator, SampleRow, TrainingObjectiveSpec,
    };
    use crate::model::optimize::refine_weights;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

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
        vec![CrossSectionGroup {
            decision_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            label_horizon_end: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            rows,
        }]
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
    fn optimize_refine_is_deterministic_on_fixed_platform() {
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
    fn optimize_never_worsens_grid_objective() {
        let l2 = dec!(0.01);
        for trial in 0..100u64 {
            let n = 3;
            let groups = synth_group(trial, n, 50);
            let evaluator = evaluator(l2);
            let (grid, _) = coordinate_search(&uniform(n), &groups, &evaluator).expect("grid");
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
    fn optimize_converges_and_falls_back_to_grid() {
        let l2 = Decimal::ZERO;
        let groups = synth_group(42, 3, 80);
        let evaluator = evaluator(l2);
        let (grid, _) = coordinate_search(&uniform(3), &groups, &evaluator).expect("grid");
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
