//! Shared offline dataset replay: schedule + label truth from Parquet, features
//! and factors recomputed point-in-time (Phase 3.6).
//!
//! Both [`ModelTrainerService`](crate::service::model_training::ModelTrainerService)
//! and [`BacktestService`](crate::service::backtest::BacktestService) consume this
//! module so training and backtest score the **same** factor table the online
//! plane would produce from identical prefetched facts. Parquet is never trusted
//! for features or factors — only for the replay schedule and forward label truth.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::TrainingDatasetInfo,
    types::{MarketId, TokenId, TrainingExampleId, TrainingSampleSource},
};
use quant_pivot_repository::traits::{MarketRepository, QuantFactReadRepository};
use quant_pivot_research::{
    factors::{FactorEligibility, FactorEngine, FactorValue},
    features::{ConfiguredFeatureBuilder, FeatureVector},
    model::sell_scorer::position_state_factor_values,
    training::{LabelName, TrainingExample, TrainingLabel},
};
use rust_decimal::Decimal;

use crate::{
    pipeline::historical_window::{HistoricalWindowLoader, ReplaySample, WindowSpec},
    service::historical_replay::{CrossSectionRequest, ReplayConfig, materialize_cross_section},
};

/// A recomputed market cross-section entry: the PIT feature vector plus its
/// eligible market factor values (position-state pseudo-factors merged per-lot).
type MarketFactorReplay = (FeatureVector, Vec<FactorValue>);

/// The replay schedule + forward label truth extracted from frozen Parquet rows.
pub struct ReplaySchedule {
    /// Distinct `(market, token)` sample set to prefetch facts for.
    pub sample_set: Vec<ReplaySample>,
    /// Samples grouped by `as_of` (ascending) for cross-sectional materialization.
    pub by_as_of: BTreeMap<DateTime<Utc>, Vec<ReplaySample>>,
    /// Forward settlement truth keyed by `(as_of, market)`: `(settled_yes, matured)`.
    pub settlement: HashMap<(DateTime<Utc>, MarketId), (bool, bool)>,
}

impl ReplaySchedule {
    /// Extract the schedule + settlement truth from decoded examples.
    pub fn from_examples(examples: &[TrainingExample]) -> Self {
        let settlement_label = LabelName::new("settlement_outcome");
        let mut sample_set = Vec::new();
        let mut seen: HashSet<(MarketId, TokenId)> = HashSet::new();
        let mut by_as_of: BTreeMap<DateTime<Utc>, Vec<ReplaySample>> = BTreeMap::new();
        let mut settlement = HashMap::new();
        for example in examples {
            let sample = ReplaySample {
                market_id: example.market_id.clone(),
                token_id: example.token_id.clone(),
            };
            if seen.insert((example.market_id.clone(), example.token_id.clone())) {
                sample_set.push(sample.clone());
            }
            by_as_of.entry(example.as_of).or_default().push(sample);
            settlement.insert(
                (example.as_of, example.market_id.clone()),
                settlement_outcome(example, &settlement_label),
            );
        }
        Self {
            sample_set,
            by_as_of,
            settlement,
        }
    }
}

/// Labels + stable example id carried forward from the frozen Parquet row.
struct ParquetExampleMeta {
    example_id: TrainingExampleId,
    labels: Vec<TrainingLabel>,
}

/// Recompute every training example's features and factors point-in-time.
///
/// The `parquet_examples` supply only:
/// - the replay **schedule** (`as_of`, `market_id`, `token_id`);
/// - forward **label truth** (supervised targets and settlement outcomes).
///
/// Features and factors always flow through [`materialize_cross_section`], the
/// same kernel used by the dataset build and the backtest replay.
pub async fn rematerialize_training_examples(
    dataset: &TrainingDatasetInfo,
    parquet_examples: &[TrainingExample],
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    replay: &ReplayConfig,
    max_book_staleness: Duration,
) -> QuantResult<Vec<TrainingExample>> {
    let schedule = ReplaySchedule::from_examples(parquet_examples);
    let label_index = parquet_label_index(parquet_examples);

    let source_delay = Duration::from_secs(u64::try_from(dataset.source_delay_secs).unwrap_or(0));
    let lookback = Duration::from_secs(replay.features.max_lookback_secs());
    let max_horizon_secs = max_horizon(dataset);

    let loader = HistoricalWindowLoader::new(fact_read, market_repo, max_book_staleness);
    let window = loader
        .load(&WindowSpec {
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            samples: schedule.sample_set.clone(),
            lookback,
            source_delay,
            max_horizon_secs,
        })
        .await?;

    let builder = ConfiguredFeatureBuilder::new(&replay.features);
    let engine = FactorEngine::new(&replay.factors, &replay.features, replay.bias_table.clone());

    let mut rematerialized = Vec::new();
    for (as_of, group) in &schedule.by_as_of {
        let Some(cross) = materialize_cross_section(
            &builder,
            &engine,
            replay,
            &CrossSectionRequest {
                pit: &window.pit,
                prefetched: &window.prefetched,
                as_of: *as_of,
                group,
                source_delay,
                lookback,
            },
        )
        .await?
        else {
            continue;
        };

        for (index, vector) in cross.vectors.iter().enumerate() {
            let market = &cross.markets[index];
            let outcome = &cross.outcomes[index];
            let Some(meta) = label_index.get(&(
                cross.as_of,
                market.market_id.clone(),
                market.primary_token_id.clone(),
            )) else {
                continue;
            };
            let factor_values = match &outcome.eligibility {
                FactorEligibility::Eligible => outcome
                    .factors
                    .iter()
                    .map(|scored| scored.value.clone())
                    .collect(),
                FactorEligibility::RejectCandidate { .. }
                | FactorEligibility::NotApplicable { .. } => Vec::new(),
            };
            rematerialized.push(TrainingExample {
                example_id: meta.example_id.clone(),
                market_id: market.market_id.clone(),
                token_id: market.primary_token_id.clone(),
                as_of: cross.as_of,
                sample_source: TrainingSampleSource::HistoricalPit,
                feature_vector: vector.clone(),
                factor_values,
                labels: meta.labels.clone(),
                source_refs: vector.source_refs.clone(),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            });
        }
    }

    if rematerialized.is_empty() {
        return Err(ResearchError::DatasetBuild {
            detail: "PIT rematerialization produced zero training examples".to_owned(),
        }
        .into());
    }
    Ok(rematerialized)
}

/// Recompute the **market** factors for `ExitDecision` samples point-in-time
/// while preserving each lot-timeline row's frozen per-lot truth (labels,
/// position-state, lot context, book fidelity).
///
/// The general [`rematerialize_training_examples`] path is wrong for the Sell
/// scorer: it rewrites every row to `HistoricalPit` and discards `position_state`
/// / `lot_context`, and its `(as_of, market, token)` label index collapses the
/// multiple lots that share a token at one instant. Here market factors still
/// flow through the shared [`materialize_cross_section`] kernel (never trusted
/// from Parquet), but they are keyed by `(as_of, market, token)` and re-attached
/// to each frozen lot row, which carries its own labels + position-state. The
/// lot's position-state pseudo-factors are re-merged so the trainer sees the
/// same feature shape the dataset build produced.
pub async fn rematerialize_exit_decision_examples(
    dataset: &TrainingDatasetInfo,
    parquet_examples: &[TrainingExample],
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    replay: &ReplayConfig,
    max_book_staleness: Duration,
) -> QuantResult<Vec<TrainingExample>> {
    let schedule = ReplaySchedule::from_examples(parquet_examples);
    let source_delay = Duration::from_secs(u64::try_from(dataset.source_delay_secs).unwrap_or(0));
    let lookback = Duration::from_secs(replay.features.max_lookback_secs());
    let max_horizon_secs = max_horizon(dataset);

    let loader = HistoricalWindowLoader::new(fact_read, market_repo, max_book_staleness);
    let window = loader
        .load(&WindowSpec {
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            samples: schedule.sample_set.clone(),
            lookback,
            source_delay,
            max_horizon_secs,
        })
        .await?;

    let builder = ConfiguredFeatureBuilder::new(&replay.features);
    let engine = FactorEngine::new(&replay.factors, &replay.features, replay.bias_table.clone());

    // Recompute the PIT market factors once per (as_of, market, token); many
    // lots may share the same key and reuse the same market cross-section.
    let mut market_factors: HashMap<(DateTime<Utc>, MarketId, TokenId), MarketFactorReplay> =
        HashMap::new();
    for (as_of, group) in &schedule.by_as_of {
        let Some(cross) = materialize_cross_section(
            &builder,
            &engine,
            replay,
            &CrossSectionRequest {
                pit: &window.pit,
                prefetched: &window.prefetched,
                as_of: *as_of,
                group,
                source_delay,
                lookback,
            },
        )
        .await?
        else {
            continue;
        };
        for (index, vector) in cross.vectors.iter().enumerate() {
            let market = &cross.markets[index];
            let outcome = &cross.outcomes[index];
            let factors = match &outcome.eligibility {
                FactorEligibility::Eligible => outcome
                    .factors
                    .iter()
                    .map(|scored| scored.value.clone())
                    .collect(),
                FactorEligibility::RejectCandidate { .. }
                | FactorEligibility::NotApplicable { .. } => Vec::new(),
            };
            market_factors.insert(
                (
                    cross.as_of,
                    market.market_id.clone(),
                    market.primary_token_id.clone(),
                ),
                (vector.clone(), factors),
            );
        }
    }

    let mut rematerialized = Vec::with_capacity(parquet_examples.len());
    for example in parquet_examples {
        let key = (
            example.as_of,
            example.market_id.clone(),
            example.token_id.clone(),
        );
        let Some((vector, factors)) = market_factors.get(&key) else {
            continue;
        };
        let mut factor_values = factors.clone();
        if let Some(state) = &example.position_state {
            factor_values.extend(position_state_factor_values(state));
        }
        rematerialized.push(TrainingExample {
            example_id: example.example_id.clone(),
            market_id: example.market_id.clone(),
            token_id: example.token_id.clone(),
            as_of: example.as_of,
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: vector.clone(),
            factor_values,
            labels: example.labels.clone(),
            source_refs: vector.source_refs.clone(),
            lot_context: example.lot_context.clone(),
            position_state: example.position_state.clone(),
            book_fidelity: example.book_fidelity,
        });
    }

    if rematerialized.is_empty() {
        return Err(ResearchError::DatasetBuild {
            detail: "PIT rematerialization produced zero ExitDecision training examples".to_owned(),
        }
        .into());
    }
    Ok(rematerialized)
}

/// Index forward label truth keyed by the replay sample identity.
fn parquet_label_index(
    examples: &[TrainingExample],
) -> HashMap<(DateTime<Utc>, MarketId, TokenId), ParquetExampleMeta> {
    examples
        .iter()
        .map(|example| {
            (
                (
                    example.as_of,
                    example.market_id.clone(),
                    example.token_id.clone(),
                ),
                ParquetExampleMeta {
                    example_id: example.example_id.clone(),
                    labels: example.labels.clone(),
                },
            )
        })
        .collect()
}

/// Settlement outcome (`settled_yes`, matured) from the example's settlement label.
pub fn settlement_outcome(example: &TrainingExample, label: &LabelName) -> (bool, bool) {
    example
        .labels
        .iter()
        .find(|l| l.label_name == *label)
        .map_or((false, false), |l| (l.value >= Decimal::ONE, l.is_resolved))
}

/// The maximum forward horizon (seconds) the dataset materialized.
pub fn max_horizon(dataset: &TrainingDatasetInfo) -> u64 {
    dataset.horizons_secs.0.iter().copied().max().unwrap_or(0)
}
