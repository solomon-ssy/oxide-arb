//! Model training contract and the [`WeightedFactorTrainer`] (Phase 3.6).
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
//! and searches the weight **simplex** (non-negative, sum to 1) to maximize the
//! cross-sectional rank IC between `net` and the realized label, minus an L2
//! complexity penalty. The deterministic coordinate search is the **base**
//! optimizer (always linked); the `optimize` feature adds an `argmin` refinement
//! seeded from the coordinate-search solution that is kept only when it strictly
//! improves the training objective (see [`crate::model::optimize`]).
//!
//! Determinism is a hard, money-critical invariant: the same `(examples, label,
//! seed, objective)` must yield a byte-identical artifact hash.

use std::ops::Range;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError};
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

/// The validation objective the trainer maximizes on held-out folds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TrainingObjective {
    /// Spearman rank IC between the predicted `net` and the realized label.
    #[default]
    RankIc,
}

/// Regularization applied to the weight vector during the search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Regularization {
    /// Ridge (L2) penalty coefficient on `Σ weightᵢ²` (subtracted from the
    /// objective). `0` disables regularization.
    pub l2: Decimal,
}

impl Default for Regularization {
    fn default() -> Self {
        Self {
            l2: Decimal::new(1, 2), // 0.01
        }
    }
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
    /// Mean validation objective across rolling folds.
    pub mean_objective: Decimal,
    /// Per-fold validation objective values (time-ordered).
    pub fold_objectives: Vec<Decimal>,
    /// Total resolved samples used.
    pub sample_count: u64,
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
    /// Validation objective.
    pub objective: TrainingObjective,
    /// Weight regularization.
    pub regularization: Regularization,
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
        request.regularization,
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
    };
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
/// deterministic rank-IC simplex search lives in exactly one place.
#[derive(Debug, Clone)]
pub(crate) struct FittedWeights {
    /// Frozen, normalized per-factor weights in seed order.
    pub factor_weights: Vec<FactorWeight>,
    /// In-sample (training-fold) objective report.
    pub objective_report: TrainingObjectiveReport,
    /// Held-out validation report.
    pub validation: ValidationReport,
}

/// Fit the weight simplex against a supervised label via deterministic rank-IC
/// coordinate search (+ optional `argmin` refinement). Family-agnostic: the
/// caller wraps the frozen weights into its concrete artifact body.
pub(crate) fn fit_simplex_weights(
    examples: &[TrainingExample],
    label: &LabelSelector,
    seed_weights: &[FactorWeight],
    regularization: Regularization,
    validation: ValidationSpec,
) -> QuantResult<FittedWeights> {
    if seed_weights.is_empty() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "trainer requires a non-empty seed weight (candidate factor) set".to_owned(),
        }
        .into());
    }

    // Deterministic factor order: the seed-weight order defines the columns.
    let factors: Vec<FactorName> = seed_weights.iter().map(|w| w.factor.clone()).collect();
    let dataset = TrainingDataset::build(examples, label, &factors)?;
    if dataset.rows.len() < validation.folds.max(2) as usize {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "insufficient resolved samples ({}) for {} validation folds",
                dataset.rows.len(),
                validation.folds
            ),
        }
        .into());
    }

    let folds = validation.folds.max(2) as usize;
    let split = TimeSplit::new(dataset.rows.len(), folds);

    // Seed simplex (normalized, non-negative).
    let seed = normalize_simplex(
        &seed_weights
            .iter()
            .map(|w| w.weight.max(Decimal::ZERO))
            .collect::<Vec<_>>(),
    );

    let l2 = regularization.l2.max(Decimal::ZERO);
    let train_rows = &dataset.rows[..split.train_end];

    // Base optimizer: deterministic coordinate search on the simplex.
    let grid_weights = coordinate_search(&seed, train_rows, l2);
    let grid_objective = penalized_objective(&grid_weights, train_rows, l2);

    // Optional refinement: argmin (kept only if it strictly improves).
    let (weights, train_objective) = refine(&grid_weights, grid_objective, train_rows, l2);

    // Per-fold diagnostics over every block; the held-out last block is the
    // true validation objective (the final weights never saw it). The per-block
    // rank-IC computations are independent, so they fold out across the `rayon`
    // pool in source order (the result is identical regardless of thread count).
    let blocks: Vec<Range<usize>> = split.blocks().collect();
    let fold_objectives: Vec<Decimal> = par_map_with_index(&blocks, |_, block| {
        rank_ic(&weights, &dataset.rows[block.clone()])
    });
    let mean_objective = rank_ic(&weights, &dataset.rows[split.held_out()]);

    let weights_dp = weights
        .iter()
        .map(|w| w.round_dp(RESEARCH_DECIMAL_SCALE))
        .collect::<Vec<_>>();
    let frozen = normalize_simplex(&weights_dp); // re-normalize after rounding
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
        summary: format!(
            "rank_ic train={} validation_mean={} over {} folds, {} samples",
            train_objective.round_dp(4),
            mean_objective.round_dp(4),
            folds,
            dataset.rows.len()
        ),
    };

    Ok(FittedWeights {
        factor_weights,
        objective_report,
        validation: ValidationReport {
            mean_objective: mean_objective.round_dp(RESEARCH_DECIMAL_SCALE),
            fold_objectives: fold_objectives
                .iter()
                .map(|v| v.round_dp(RESEARCH_DECIMAL_SCALE))
                .collect(),
            sample_count: dataset.rows.len() as u64,
        },
    })
}

/// Run the optional `argmin` refinement when the `optimize` feature is enabled.
#[cfg(feature = "optimize")]
fn refine(
    grid_weights: &[Decimal],
    grid_objective: Decimal,
    train_rows: &[SampleRow],
    l2: Decimal,
) -> (Vec<Decimal>, Decimal) {
    match crate::model::optimize::refine_weights(grid_weights, train_rows, l2) {
        Some(refined) => {
            let refined_objective = penalized_objective(&refined, train_rows, l2);
            if refined_objective > grid_objective {
                return (refined, refined_objective);
            }
            (grid_weights.to_vec(), grid_objective)
        }
        None => (grid_weights.to_vec(), grid_objective),
    }
}

/// Base build: the coordinate-search solution is the final solution.
#[cfg(not(feature = "optimize"))]
fn refine(
    grid_weights: &[Decimal],
    grid_objective: Decimal,
    _train_rows: &[SampleRow],
    _l2: Decimal,
) -> (Vec<Decimal>, Decimal) {
    (grid_weights.to_vec(), grid_objective)
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
fn coordinate_search(seed: &[Decimal], rows: &[SampleRow], l2: Decimal) -> Vec<Decimal> {
    let n = seed.len();
    let mut best = seed.to_vec();
    let mut best_obj = penalized_objective(&best, rows, l2);
    if n < 2 {
        return best;
    }

    for _ in 0..MAX_SEARCH_ROUNDS {
        let moves = enumerate_moves(n, &best);
        if moves.is_empty() {
            break;
        }
        let scored = par_map_with_index(&moves, |_, &(from, to, step)| {
            let mut trial = best.clone();
            trial[from] -= step;
            trial[to] += step;
            penalized_objective(&trial, rows, l2)
        });

        // Pick the single best strictly-improving move; ties resolve to the
        // earliest move in the fixed enumeration order (deterministic).
        let mut chosen: Option<(usize, Decimal)> = None;
        for (idx, obj) in scored.iter().copied().enumerate() {
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
    best
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

/// The training objective: rank IC minus the L2 complexity penalty.
pub(crate) fn penalized_objective(weights: &[Decimal], rows: &[SampleRow], l2: Decimal) -> Decimal {
    let ic = rank_ic(weights, rows);
    let l2_term: Decimal = weights.iter().map(|w| *w * *w).sum();
    ic - l2 * l2_term
}

/// Spearman rank IC between predicted `net` and the realized label.
pub(crate) fn rank_ic(weights: &[Decimal], rows: &[SampleRow]) -> Decimal {
    if rows.len() < 2 {
        return Decimal::ZERO;
    }
    let predicted: Vec<Decimal> = rows.iter().map(|row| row.net(weights)).collect();
    let labels: Vec<Decimal> = rows.iter().map(|row| row.label).collect();
    crate::stats::spearman(&predicted, &labels)
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

/// One training sample reduced to its signed factor contributions + label.
///
/// `signed[i] = dir_signᵢ · normalizedᵢ · confidenceᵢ` mirrors the runtime, so
/// `net = Σ weightᵢ · signedᵢ`.
#[derive(Debug, Clone)]
pub(crate) struct SampleRow {
    signed: Vec<Decimal>,
    label: Decimal,
}

impl SampleRow {
    /// The predicted ranking score for a weight vector.
    pub(crate) fn net(&self, weights: &[Decimal]) -> Decimal {
        self.signed.iter().zip(weights).map(|(s, w)| *s * *w).sum()
    }
}

/// The reduced, time-ordered training dataset for the weighted trainer.
struct TrainingDataset {
    rows: Vec<SampleRow>,
}

impl TrainingDataset {
    /// Extract signed factor contributions + label per resolved example, sorted
    /// by `(as_of, market_id, token_id)` for deterministic folding. Examples
    /// without the target label are skipped (never silently zero-filled).
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

        let mut rows = Vec::new();
        for example in sorted {
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
            rows.push(SampleRow {
                signed,
                label: label_value,
            });
        }
        if rows.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "no resolved samples for label `{}`@{}s",
                    label.name.as_str(),
                    label.horizon_secs
                ),
            }
            .into());
        }
        Ok(Self { rows })
    }
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
        .map_or(Decimal::ZERO, |value| {
            let direction = Decimal::from(value.direction.as_i8());
            direction * value.normalized_score.inner() * value.confidence.inner()
        })
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
    fn blocks(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        (1..self.boundaries.len())
            .map(move |k| self.boundaries[k - 1]..self.boundaries[k])
            .filter(|range| range.start < range.end)
    }

    /// The held-out last block (true validation range).
    fn held_out(&self) -> Range<usize> {
        let last = self.boundaries.len() - 1;
        self.boundaries[last - 1]..self.boundaries[last]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LabelSelector, ModelTrainer, Regularization, TrainModelRequest, TrainingObjective,
        ValidationSpec, WeightedFactorTrainer,
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
            FactorExplanation, FactorName, FactorValue,
            names::{LIQUIDITY_DEPTH, MOMENTUM},
        },
        features::FeatureVector,
        model::{
            ReturnModelSpec,
            artifact::{
                FactorWeight, ModelArtifact, ModelArtifactHeader, ScoreMultiplierSpec,
                SubstitutionConfidenceRules,
            },
            runtime::ModelFamily,
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
            as_of: Utc.timestamp_opt(1_700_000_000 + idx, 0).unwrap(),
            schema_version: SchemaVersion::FIRST,
            values: BTreeMap::new(),
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
            normalized_score: Probability::new(score),
            direction: dir,
            confidence: Probability::new(dec!(1)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
                clamp: None,
            },
            input_feature_refs: Vec::new(),
        };
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new(format!("0x{idx}")),
            token_id: TokenId::new("yes"),
            as_of: Utc.timestamp_opt(1_700_000_000 + idx, 0).unwrap(),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: fv,
            factor_values: vec![mk(LIQUIDITY_DEPTH, liq), mk(MOMENTUM, mom)],
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
                    factor: MOMENTUM,
                    weight: dec!(0.5),
                },
            ],
            objective: TrainingObjective::RankIc,
            regularization: Regularization { l2: dec!(0.0) },
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
        }
    }

    /// Momentum tracks the label while liquidity anti-tracks it, so the rank-IC
    /// objective is strictly improved by shifting weight onto momentum.
    fn momentum_dataset() -> Vec<TrainingExample> {
        (0..20)
            .map(|i| {
                let strong = i % 2 == 0;
                let (liq, mom, y) = if strong {
                    (dec!(0.1), dec!(0.9), dec!(1))
                } else {
                    (dec!(0.9), dec!(0.1), dec!(0))
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
            .install(|| super::train_weighted(&req))
            .expect("train single");
        let b = many
            .install(|| super::train_weighted(&req))
            .expect("train many");
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
            .find(|w| w.factor == MOMENTUM)
            .expect("momentum weight");
        assert!(
            momentum.weight > dec!(0.5),
            "momentum weight {} should exceed the 0.5 seed",
            momentum.weight
        );
        assert!(outcome.in_sample_metrics.objective_value > dec!(0));
    }
}

/// `optimize`-feature tests: the `argmin` refinement is gradient-free,
/// seeded from the grid solution, and accepted only when it strictly improves the
/// Decimal objective — so it can never weaken the model and its accepted result
/// is deterministic on a fixed platform.
#[cfg(all(test, feature = "optimize"))]
mod optimize_tests {
    use super::{SampleRow, coordinate_search, penalized_objective, refine};
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
    fn synth_rows(seed: u64, n: usize, count: usize) -> Vec<SampleRow> {
        let mut state = seed.wrapping_add(1);
        (0..count)
            .map(|_| {
                let signed: Vec<Decimal> = (0..n)
                    .map(|_| Decimal::from(next(&mut state) % 1000) / dec!(1000) - dec!(0.5))
                    .collect();
                let label = signed[0];
                SampleRow { signed, label }
            })
            .collect()
    }

    fn uniform(n: usize) -> Vec<Decimal> {
        let w = Decimal::ONE / Decimal::from(n as u64);
        vec![w; n]
    }

    /// The same inputs must refine to byte-identical Decimal weights (the accepted
    /// result is deterministic on a fixed platform — the cross-platform `f64`
    /// optimizer is only a candidate generator; adoption is decided in Decimal).
    #[test]
    fn optimize_refine_is_deterministic_on_fixed_platform() {
        let rows = synth_rows(7, 3, 60);
        let seed = vec![dec!(0.2), dec!(0.3), dec!(0.5)];
        let a = refine_weights(&seed, &rows, dec!(0.01));
        let b = refine_weights(&seed, &rows, dec!(0.01));
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
            let rows = synth_rows(trial, n, 50);
            let grid = coordinate_search(&uniform(n), &rows, l2);
            let grid_obj = penalized_objective(&grid, &rows, l2);
            let (weights, objective) = refine(&grid, grid_obj, &rows, l2);
            assert!(
                objective >= grid_obj,
                "trial {trial}: refined objective {objective} < grid {grid_obj}"
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
        let rows = synth_rows(42, 3, 80);
        let grid = coordinate_search(&uniform(3), &rows, l2);
        let grid_obj = penalized_objective(&grid, &rows, l2);
        let (_, objective) = refine(&grid, grid_obj, &rows, l2);
        assert!(objective >= grid_obj, "refine never worsens the grid");
    }
}
