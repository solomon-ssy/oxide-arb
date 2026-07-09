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
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::enums::common::MarketCategory;
use quant_pivot_models::runtime_config::TrainingOptimizerKind;
use quant_pivot_models::types::ContentHash;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::FactorName,
    features::FeatureName,
    model::{
        artifact::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
            SubstitutionConfidenceRules, TrainingObjectiveReport, WeightedFactorModelArtifact,
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
};

/// Which forward label a trainer targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelSelector {
    /// Label name (e.g. `settlement_outcome`).
    pub name: LabelName,
    /// Label horizon in seconds (`0` for horizon-independent labels).
    pub horizon_secs: u64,
}

/// Rolling time-ordered validation split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationSpec {
    /// Number of contiguous, time-ordered folds (`>= 2`). The last fold is held
    /// out; earlier folds train, and each rolling split contributes one
    /// validation objective.
    pub folds: u32,
}

impl Default for ValidationSpec {
    fn default() -> Self {
        Self { folds: 3 }
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
    /// Per-block objective values (time-ordered; includes in-sample blocks for
    /// weighted trainers — diagnostic only, not a pure OOS mean).
    pub fold_objectives: Vec<Decimal>,
    /// Per-block component breakdowns (time-ordered).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fold_components: Vec<ObjectiveComponentReport>,
    /// Total resolved samples kept in cross-section groups (`as_of` with ≥2 rows).
    pub sample_count: u64,
    /// Number of `as_of` cross-sections dropped because they had fewer than 2 rows.
    #[serde(default)]
    pub dropped_singleton_groups: u64,
    /// Number of sample rows discarded with those singleton cross-sections.
    #[serde(default)]
    pub dropped_singleton_rows: u64,
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
    /// Features the model requires (selection eligibility).
    pub required_features: Vec<FeatureName>,
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
    )?;

    let artifact = WeightedFactorModelArtifact {
        header: request.header.clone(),
        weights: fit.factor_weights,
        prediction_horizon_secs: request.prediction_horizon_secs,
        multipliers: request.multipliers.clone(),
        substitution_confidence_rules: request.substitution_rules.clone(),
        return_model: request.return_model.clone(),
        required_features: request.required_features.clone(),
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
    let dataset = TrainingDataset::build(examples, label, &factors)?;
    if dataset.groups.len() < validation.folds.max(2) as usize {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "insufficient resolved cross-section groups ({}) for {} validation folds",
                dataset.groups.len(),
                validation.folds
            ),
        }
        .into());
    }

    let folds = validation.folds.max(2) as usize;
    let split = TimeSplit::new(dataset.groups.len(), folds);

    // Seed simplex (normalized, non-negative).
    let seed = normalize_simplex(
        &seed_weights
            .iter()
            .map(|w| w.weight.max(Decimal::ZERO))
            .collect::<Vec<_>>(),
    );

    let evaluator = ObjectiveEvaluator::new(objective.clone());
    let train_groups = &dataset.groups[..split.train_end];

    // Base optimizer: deterministic coordinate search on the simplex.
    let grid_weights = coordinate_search(&seed, train_groups, &evaluator)?;
    let grid_report = evaluator.evaluate(&grid_weights, train_groups)?;
    let grid_objective = grid_report.objective_value();

    // Optional refinement: argmin (kept only if it strictly improves).
    let (weights, train_report) = refine(&grid_weights, grid_objective, train_groups, &evaluator)?;
    assemble_fitted_weights(
        &factors,
        &weights,
        train_report.rounded(),
        &evaluator,
        &dataset,
        &split,
        folds,
    )
}

/// Freeze weights, evaluate fold/held-out reports, and build the fit outcome.
fn assemble_fitted_weights(
    factors: &[FactorName],
    weights: &[Decimal],
    train_components: ObjectiveComponentReport,
    evaluator: &ObjectiveEvaluator,
    dataset: &TrainingDataset,
    split: &TimeSplit,
    folds: usize,
) -> QuantResult<FittedWeights> {
    let train_objective = train_components.objective_value();
    let train_groups = &dataset.groups[..split.train_end];
    let train_diagnostics = evaluator.diagnostics(weights, train_groups);

    // Per-block diagnostics over every block; the held-out last block is the
    // true validation objective (the final weights never saw it). Evaluated
    // serially so a non-finite score fails closed without rayon Result plumbing.
    let blocks = split.blocks();
    let mut fold_components = Vec::with_capacity(blocks.len());
    for block in &blocks {
        fold_components.push(
            evaluator
                .evaluate(weights, &dataset.groups[block.start..block.end])?
                .rounded(),
        );
    }
    let fold_objectives: Vec<Decimal> = fold_components
        .iter()
        .map(ObjectiveComponentReport::objective_value)
        .collect();
    let held_out = split.held_out();
    let held_out_groups = &dataset.groups[held_out.start..held_out.end];
    let held_out_components = evaluator.evaluate(weights, held_out_groups)?.rounded();
    let held_out_objective = held_out_components.objective_value();
    let held_out_diagnostics = evaluator.diagnostics(weights, held_out_groups);

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

    let objective_report = TrainingObjectiveReport {
        objective_value: train_objective.round_dp(RESEARCH_DECIMAL_SCALE),
        spec: evaluator.spec().clone(),
        components: train_components,
        diagnostics: Some(train_diagnostics),
        summary: format!(
            "ltr {:?} train={} held_out={} ndcg@{}={} rank_ic={} over {} folds, {} groups, {} samples, \
             dropped_singleton_groups={}, dropped_singleton_rows={} (TopN pseudo=rank-equal)",
            evaluator.spec().rank_loss,
            train_objective.round_dp(4),
            held_out_objective.round_dp(4),
            held_out_diagnostics.ndcg_k,
            held_out_diagnostics.mean_ndcg_at_k.round_dp(4),
            held_out_diagnostics.mean_rank_ic.round_dp(4),
            folds,
            dataset.groups.len(),
            dataset.sample_count,
            dataset.dropped_singleton_groups,
            dataset.dropped_singleton_rows
        ),
    };

    Ok(FittedWeights {
        factor_weights,
        objective_report,
        validation: ValidationReport {
            held_out_objective: held_out_objective.round_dp(RESEARCH_DECIMAL_SCALE),
            held_out_components: Some(held_out_components),
            held_out_diagnostics: Some(held_out_diagnostics),
            fold_objectives: fold_objectives
                .iter()
                .map(|v| v.round_dp(RESEARCH_DECIMAL_SCALE))
                .collect(),
            fold_components,
            sample_count: dataset.sample_count,
            dropped_singleton_groups: dataset.dropped_singleton_groups,
            dropped_singleton_rows: dataset.dropped_singleton_rows,
        },
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

    match optimize::refine_weights(grid_weights, train_groups, evaluator) {
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
fn coordinate_search(
    seed: &[Decimal],
    groups: &[CrossSectionGroup],
    evaluator: &ObjectiveEvaluator,
) -> QuantResult<Vec<Decimal>> {
    let n = seed.len();
    let mut best = seed.to_vec();
    let mut best_obj = evaluator.evaluate(&best, groups)?.objective_value();
    if n < 2 {
        return Ok(best);
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
    }
    Ok(best)
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
    /// by `(as_of, market_id, token_id)` for deterministic folding. Examples
    /// without the target label are skipped (never silently zero-filled).
    /// Cross-sections with fewer than two rows are dropped and counted.
    fn build(
        examples: &[TrainingExample],
        label: &LabelSelector,
        factors: &[FactorName],
    ) -> QuantResult<Self> {
        let mut sorted: Vec<&TrainingExample> = examples.iter().collect();
        sorted.sort_by(|a, b| {
            a.as_of
                .cmp(&b.as_of)
                .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
                .then_with(|| a.token_id.as_str().cmp(b.token_id.as_str()))
        });

        let mut groups = Vec::new();
        let mut dropped_singleton_groups = 0_u64;
        let mut dropped_singleton_rows = 0_u64;
        let mut current_as_of = None;
        let mut current_rows = Vec::new();
        for example in sorted {
            if current_as_of.is_some_and(|as_of| as_of != example.as_of) {
                push_group(
                    &mut groups,
                    std::mem::take(&mut current_rows),
                    &mut dropped_singleton_groups,
                    &mut dropped_singleton_rows,
                );
            }
            current_as_of = Some(example.as_of);
            let Some(label_value) = example
                .labels
                .iter()
                .find(|row| {
                    (&row.label_name, row.horizon_secs) == (&label.name, label.horizon_secs)
                })
                .map(|row| row.value)
            else {
                continue;
            };
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
        if current_as_of.is_some() {
            push_group(
                &mut groups,
                current_rows,
                &mut dropped_singleton_groups,
                &mut dropped_singleton_rows,
            );
        }
        let sample_count = groups
            .iter()
            .map(|group| group.rows.len() as u64)
            .sum::<u64>();
        if groups.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "no as_of cross-section group with ≥2 rows for label `{}`@{}s \
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
    rows: Vec<SampleRow>,
    dropped_singleton_groups: &mut u64,
    dropped_singleton_rows: &mut u64,
) {
    if rows.len() >= 2 {
        groups.push(CrossSectionGroup { rows });
        return;
    }
    if rows.is_empty() {
        return;
    }
    *dropped_singleton_groups = dropped_singleton_groups.saturating_add(1);
    *dropped_singleton_rows = dropped_singleton_rows.saturating_add(rows.len() as u64);
}

/// `dir_sign · normalized · confidence` for one factor of one example, or `0`
/// when the factor is absent / unresolved (confidence carries the missingness).
pub(crate) fn signed_contribution(example: &TrainingExample, factor: &FactorName) -> Decimal {
    if is_position_state_factor(factor) {
        if let Some(state) = &example.position_state {
            return position_state_signed_contribution(state, factor);
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
    train_end: usize,
    boundaries: Vec<usize>,
}

impl TimeSplit {
    /// Split `len` time-ordered rows into `folds` contiguous blocks. The last
    /// block is the held-out validation fold; everything before it trains.
    fn new(len: usize, folds: usize) -> Self {
        let folds = folds.max(2);
        let mut boundaries = Vec::with_capacity(folds + 1);
        for k in 0..=folds {
            boundaries.push(len * k / folds);
        }
        let train_end = boundaries[folds - 1].max(1).min(len);
        Self {
            train_end,
            boundaries,
        }
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

    /// The held-out last block (true validation range).
    fn held_out(&self) -> TimeBlock {
        let last = self.boundaries.len() - 1;
        TimeBlock {
            start: self.boundaries[last - 1],
            end: self.boundaries[last],
        }
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
        TrainingObjectiveSpec, ValidationSpec, WeightedFactorTrainer,
    };
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            factor::FactorFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        types::{
            ContentHash, FactorDefinitionId, MarketId, ModelVersionId, Probability, SchemaVersion,
            TokenId, TrainingExampleId, TrainingSampleSource,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        factors::{
            FactorExplanation, FactorName, FactorValue, NormalizedFactor,
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
        training::{LabelName, TrainingExample, TrainingLabel},
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
            as_of: Utc.timestamp_opt(1_700_000_000 + (idx / 2), 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        };
        let mk = |name: FactorName, score: Decimal| FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Liquidity,
            raw_value: Some(dec!(1)),
            normalization: NormalizedFactor::cross_section(Probability::new(score)),
            direction: dir,
            confidence: Probability::new(dec!(1)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        };
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new(format!("0x{idx}")),
            token_id: TokenId::new("yes"),
            as_of: Utc.timestamp_opt(1_700_000_000 + (idx / 2), 0).unwrap(),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: fv,
            factor_values: vec![mk(LIQUIDITY_DEPTH, liq), mk(MOMENTUM_ROC, mom)],
            labels: vec![TrainingLabel {
                label_name: label_name(),
                horizon_secs: 0,
                value: y,
                is_resolved: true,
            }],
            source_refs: Vec::new(),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    fn request(examples: Vec<TrainingExample>) -> TrainModelRequest {
        TrainModelRequest {
            examples,
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
            validation: ValidationSpec { folds: 3 },
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
            required_features: Vec::new(),
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
            }
            ModelArtifact::Classical(_) | ModelArtifact::SellScorer(_) => {
                panic!("expected weighted artifact")
            }
        }
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
    }

    #[test]
    fn time_split_operates_on_atomic_as_of_groups() {
        // `TrainingDataset::build` collapses each as_of into one group; TimeSplit
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
        let distinct_as_ofs: std::collections::BTreeSet<_> =
            examples.iter().map(|example| example.as_of).collect();
        assert_eq!(
            dataset.groups.len(),
            distinct_as_ofs.len(),
            "one group per as_of (momentum_dataset has >=2 rows each)"
        );
        for group in &dataset.groups {
            assert!(group.rows.len() >= 2);
        }
        assert_eq!(dataset.dropped_singleton_groups, 0);
        assert_eq!(dataset.dropped_singleton_rows, 0);
        let split = TimeSplit::new(dataset.groups.len(), 3);
        let covered: usize = split
            .blocks()
            .iter()
            .map(|block| block.end - block.start)
            .sum();
        assert_eq!(covered, dataset.groups.len());
        assert_eq!(
            split.held_out().end - split.held_out().start,
            dataset.groups.len() - split.train_end
        );
    }

    #[test]
    fn singleton_as_of_groups_are_dropped_and_counted() {
        // Pair even/odd into shared as_of via `idx / 2`. Inject one lone as_of.
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
        vec![CrossSectionGroup { rows }]
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
        let a = refine_weights(&seed, &groups, &evaluator);
        let b = refine_weights(&seed, &groups, &evaluator);
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
            let grid = coordinate_search(&uniform(n), &groups, &evaluator).expect("grid");
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
        let grid = coordinate_search(&uniform(3), &groups, &evaluator).expect("grid");
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
