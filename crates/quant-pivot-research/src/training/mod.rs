//! Training-data plane: dataset planning, labeling, leakage checks, matrix and
//! parquet materialization.
//!
//! Offline closure (3.5). This module owns the **pure** compute contracts and
//! algorithms; the impure orchestration (ClickHouse/Postgres reads, batch
//! prefetch, persistence) lives in `quant-pivot-core`'s `TrainingDatasetService`.
//! Every label and feature is point-in-time correct: features are bounded by
//! `as_of - source_delay`, labels look strictly forward of `as_of`, and the
//! whole dataset is content-hashed for reproducibility.

mod labeler;
mod leakage;
mod lot_hold_value;
mod matrix;
#[cfg(feature = "dataframe")]
mod parquet;
mod planner;

pub use labeler::{
    HOLD_VS_EXIT_ALPHA_BPS, HoldVsExitProceedsLabeler, LiquidityExitLabeler,
    MaxAdverseExcursionLabeler, MaxFavorableExcursionLabeler, ReturnToHorizonLabeler,
    SettlementOutcomeLabeler, label_names, label_names_for_sources,
};
pub use leakage::{
    LeakageFindings, LeakageViolation, assert_no_future_leakage, scan_future_leakage,
};
pub use lot_hold_value::{
    LotExitEvent, LotTerminalSnapshot, hold_terminal_proceeds, proceeds_before, remaining_shares_at,
};
pub use matrix::{
    FeatureColumnSpec, FeatureMatrixSpec, MatrixScale, TrainingMatrix, build_training_matrix,
    probe_matrix_coverage,
};
#[cfg(feature = "dataframe")]
pub use parquet::DatasetParquetCodec;
pub use planner::{count_samples, plan_lot_timeline_samples, plan_samples};
pub use quant_pivot_models::types::{DatasetCoverage, MatrixCoverageProbe};

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{ExitTrainingLotRow, LotExitEventRow, market::book::BookLevel},
    types::{
        ArtifactUri, Bps, ContentHash, MarketId, ModelSpecId, OrderIntentId, PositionId, Price,
        RuntimeConfigVersionId, SchemaVersion, Shares, TokenId, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, Usd, default_sample_sources,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    execution_sim::BookFidelity,
    factors::FactorValue,
    features::{EvidenceSourceRef, FeatureVector},
    hashing::ResearchHasher,
    model::sell_scorer::PositionStateFeatures,
    naming::stable_name,
};

stable_name! {
    /// Stable, compile-time-known label name (e.g. `"return_to_horizon"`).
    LabelName
}

// ── Plan ─────────────────────────────────────────────────────────────────

/// Request to plan a training dataset over a historical window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPlanRequest {
    /// Model spec the dataset is built for.
    pub model_spec_id: ModelSpecId,
    /// Config version governing selection / feature / factor / label schemas.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Inclusive window start (first sample `as_of`).
    pub window_start: DateTime<Utc>,
    /// Exclusive window end (samples are strictly before this).
    pub window_end: DateTime<Utc>,
    /// Deterministic sampling cadence within the window, in seconds (`>= 1`).
    pub sample_interval_secs: u64,
    /// Forward label horizons, in seconds (one label column per horizon).
    pub horizons_secs: Vec<u64>,
    /// Source visibility delay applied to features, in seconds.
    pub source_delay_secs: u64,
    /// Feature schema version to materialize against.
    pub feature_schema_version: SchemaVersion,
    /// Which sample sources to materialize.
    #[serde(default = "default_sample_sources")]
    pub sample_sources: Vec<TrainingSampleSource>,
    /// Pre-assigned dataset id (build path); minted by the planner when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub training_dataset_id: Option<TrainingDatasetId>,
}

/// A market eligible for sampling, with its lifecycle bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Primary (YES) token id sampled for this market.
    pub token_id: TokenId,
    /// Catalog creation time; samples before this are skipped (market absent).
    pub created_at: DateTime<Utc>,
    /// Scheduled resolution time, when known; samples at/after this are skipped.
    pub end_date: Option<DateTime<Utc>>,
}

/// One deterministic `(market, token, as_of)` sampling instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePlan {
    /// Market sampled.
    pub market_id: MarketId,
    /// Primary token sampled.
    pub token_id: TokenId,
    /// Decision time of the sample.
    pub as_of: DateTime<Utc>,
}

/// One hold-vs-exit decision instant along a closed lot's timeline (Phase 06.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LotSamplePlan {
    /// Entry intent owning the lot.
    pub order_intent_id: OrderIntentId,
    /// Position lot id.
    pub position_id: PositionId,
    /// Market the lot trades.
    pub market_id: MarketId,
    /// Outcome token held.
    pub token_id: TokenId,
    /// Hold-vs-exit decision time.
    pub as_of: DateTime<Utc>,
    /// When the lot opened.
    pub opened_at: DateTime<Utc>,
    /// When the lot closed / settled.
    pub closed_at: DateTime<Utc>,
}

/// Lot-scoped context carried on `ExitDecision` training rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LotTrainingContext {
    pub order_intent_id: OrderIntentId,
    pub position_id: PositionId,
    pub remaining_shares: Shares,
    pub avg_price: Price,
    pub peak_mark: Option<Price>,
    pub opened_at: DateTime<Utc>,
    pub max_hold_secs: u64,
}

/// Book slice used to simulate an exit at the decision instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionBook {
    /// Full L2 bid ladder (best-first).
    L2 { bids: Arc<[BookLevel]> },
    /// Best bid + aggregate depth fallback.
    Microstructure { best_bid: Price, depth: Shares },
}

/// Pre-fetched hold-vs-exit label inputs for one lot decision point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitDecisionLabelContext {
    pub remaining_shares: Shares,
    pub avg_price: Price,
    pub fee_bps: Bps,
    pub terminal: LotTerminalSnapshot,
    pub decision_book: Option<DecisionBook>,
}

/// A resolved plan: which instants the dataset will materialize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPlan {
    /// The originating request.
    pub request: DatasetPlanRequest,
    /// Dataset identity (assigned by the planner).
    pub training_dataset_id: TrainingDatasetId,
    /// Deterministic market-grid sample instants, ordered by `(market_id, as_of)`.
    pub samples: Vec<SamplePlan>,
    /// Lot-timeline hold-vs-exit samples (Phase 06.1).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lot_samples: Vec<LotSamplePlan>,
    /// Closed lots backing [`Self::lot_samples`] (not hashed; planner metadata).
    #[serde(skip)]
    pub exit_training_lots: Vec<ExitTrainingLotRow>,
    /// Labels this dataset materializes (one logical name; horizons fan out).
    pub label_names: Vec<LabelName>,
}

// ── Labels ───────────────────────────────────────────────────────────────

/// A resolved, forward-looking training label value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingLabel {
    /// Logical label name.
    pub label_name: LabelName,
    /// Forward horizon, in seconds, the label looks ahead to (post-`as_of`).
    pub horizon_secs: u64,
    /// Resolved label value (units are label-specific; bps for excursions).
    pub value: Decimal,
    /// Whether the outcome was fully realized (vs. censored / settlement-final).
    pub is_resolved: bool,
}

/// Why a mature label is not yet available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelDelayReason {
    /// The forward horizon has not elapsed within the available data.
    HorizonNotElapsed,
    /// A settlement-keyed label is pending market resolution.
    SettlementPending,
}

/// Why a label can never be produced for this sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MissingLabelReason {
    /// No entry (`as_of`) reference price was available.
    NoEntryPrice,
    /// No forward observations exist in the horizon window.
    NoForwardData,
    /// No forward price could be read at the horizon.
    NoExitPrice,
}

/// The outcome of building one label for one sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelBuildOutput {
    /// The label resolved to a usable value.
    Available(TrainingLabel),
    /// The label is valid but not yet mature; retry after `available_after`.
    NotMature {
        /// When the label is expected to become resolvable.
        available_after: DateTime<Utc>,
        /// Why it is not yet mature.
        reason: LabelDelayReason,
    },
    /// The label can never be produced for this sample.
    Unavailable {
        /// Why the label is unavailable.
        reason: MissingLabelReason,
    },
}

/// One forward microstructure observation (decoded from `book_microstructure_1s`
/// by the orchestrator), carrying the intra-bucket extremes excursion labels
/// need. All prices are `[0, 1]` Polymarket prices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardSample {
    /// Bucket time (strictly `> as_of` within a [`ForwardWindow`]).
    pub at: DateTime<Utc>,
    /// Closing mid price in the bucket.
    pub mid_close: Option<Price>,
    /// Highest best-bid in the bucket (favorable exit ceiling for a long).
    pub best_bid_high: Option<Price>,
    /// Lowest best-bid in the bucket (adverse exit floor for a long).
    pub best_bid_low: Option<Price>,
    /// Top-1 visible depth in the bucket, USD.
    pub top1_depth_usd: Option<Usd>,
}

/// Authoritative settlement for a market, resolved point-in-time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketResolution {
    /// The winning outcome token (label-agnostic settlement key).
    pub winning_token_id: TokenId,
    /// Economic settlement time.
    pub resolved_at: DateTime<Utc>,
    /// When the resolution was observed (ingested).
    pub observed_at: DateTime<Utc>,
}

/// A pre-fetched, strictly-forward window of observations for one sample.
///
/// Carries the market's settlement (if resolved). The orchestrator guarantees
/// every [`ForwardSample::at`] is `> anchor`, so labelers read only post-decision
/// state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardWindow {
    /// The sample decision time the window is anchored at.
    pub anchor: DateTime<Utc>,
    /// Latest fact time available in the source (the maturity bound): a horizon
    /// extending past this is `NotMature`, never silently truncated.
    pub data_available_until: DateTime<Utc>,
    /// Forward observations, ascending by time, all strictly `> anchor`.
    pub samples: Vec<ForwardSample>,
    /// Settlement strictly after [`Self::anchor`] when the market has resolved
    /// (independent of microstructure [`Self::data_available_until`]).
    pub resolution: Option<MarketResolution>,
}

/// Inputs to building one training label, all pre-fetched (no DB in the loop).
pub struct LabelBuildInput<'a> {
    /// Market the label is for.
    pub market_id: &'a MarketId,
    /// Outcome token the label is for.
    pub token_id: &'a TokenId,
    /// The market's YES token (settlement keys on this).
    pub yes_token_id: &'a TokenId,
    /// Decision time the label is anchored at.
    pub as_of: DateTime<Utc>,
    /// Entry reference mid price at `as_of` (from the PIT book), if quoted.
    pub entry_mid: Option<Price>,
    /// Forward horizon, in seconds, the label looks ahead to.
    pub horizon_secs: u64,
    /// Minimum USD depth required for a "liquidity exit possible" label.
    pub min_exit_depth_usd: Usd,
    /// Pre-fetched forward observations + settlement for this sample.
    pub forward: &'a ForwardWindow,
    /// Hold-vs-exit lot context (`ExitDecision` rows only).
    pub exit_decision: Option<&'a ExitDecisionLabelContext>,
}

// ── Example / artifact ─────────────────────────────────────────────────────

/// One materialized training example (row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingExample {
    /// Surrogate row id (excluded from the dataset content hash).
    pub example_id: TrainingExampleId,
    /// Market the example describes.
    pub market_id: MarketId,
    /// Outcome token the example describes.
    pub token_id: TokenId,
    /// Decision time the example was computed as of.
    pub as_of: DateTime<Utc>,
    /// Source pipeline that produced this row.
    pub sample_source: TrainingSampleSource,
    /// The point-in-time feature vector.
    pub feature_vector: FeatureVector,
    /// The factor values derived from the feature vector.
    pub factor_values: Vec<FactorValue>,
    /// Resolved forward labels (one per horizon × labeler that matured).
    pub labels: Vec<TrainingLabel>,
    /// Provenance of the inputs, for audit and replay.
    pub source_refs: Vec<EvidenceSourceRef>,
    /// Lot-scoped replay context (`ExitDecision` rows only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lot_context: Option<LotTrainingContext>,
    /// Position-state pseudo-factors aligned with runtime scoring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_state: Option<PositionStateFeatures>,
    /// Exit-fill simulation fidelity (`ExitDecision` rows only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_fidelity: Option<BookFidelity>,
}

/// A frozen, content-addressed training dataset artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingDatasetArtifact {
    /// Dataset id.
    pub training_dataset_id: TrainingDatasetId,
    /// Model spec the dataset was built for.
    pub model_spec_id: ModelSpecId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
    /// Materialized examples, ordered by `(market_id, token_id, as_of)`.
    pub examples: Vec<TrainingExample>,
    /// Feature-schema hash the dataset was built against.
    pub feature_schema_hash: ContentHash,
    /// Factor-schema hash the dataset was built against.
    pub factor_schema_hash: ContentHash,
    /// Label-schema hash the dataset was built against.
    pub label_schema_hash: ContentHash,
    /// Content hash over schema hashes + canonical example content.
    pub dataset_hash: ContentHash,
    /// Location of the materialized parquet bytes.
    pub parquet_uri: ArtifactUri,
    /// Coverage accounting.
    pub coverage: DatasetCoverage,
}

/// Canonical, surrogate-free projection used to compute `dataset_hash`.
///
/// Excludes the dataset id, parquet URI, and per-row surrogate ids so the hash
/// is a pure function of the schema bindings + the materialized content. Any
/// change to a feature value, factor value, or label flips the digest; a rename
/// of a surrogate id does not.
#[derive(Serialize)]
struct DatasetHashInput<'a> {
    model_spec_id: &'a ModelSpecId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    feature_schema_hash: &'a ContentHash,
    factor_schema_hash: &'a ContentHash,
    label_schema_hash: &'a ContentHash,
    examples: Vec<CanonicalExample<'a>>,
}

/// Surrogate-free view of one example for hashing.
#[derive(Serialize)]
struct CanonicalExample<'a> {
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    as_of: DateTime<Utc>,
    sample_source: TrainingSampleSource,
    order_intent_id: Option<&'a OrderIntentId>,
    feature_vector: &'a FeatureVector,
    factor_values: &'a [FactorValue],
    position_state: Option<&'a PositionStateFeatures>,
    book_fidelity: Option<BookFidelity>,
    labels: &'a [TrainingLabel],
}

impl TrainingDatasetArtifact {
    /// Compute the content hash over schema bindings + canonical example content.
    ///
    /// Examples are sorted by `(market_id, token_id, as_of)` so build order never
    /// perturbs the digest.
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn compute_dataset_hash(
        model_spec_id: &ModelSpecId,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        feature_schema_hash: &ContentHash,
        factor_schema_hash: &ContentHash,
        label_schema_hash: &ContentHash,
        examples: &[TrainingExample],
    ) -> QuantResult<ContentHash> {
        let mut ordered: Vec<&TrainingExample> = examples.iter().collect();
        ordered.sort_by(|a, b| {
            (
                a.market_id.as_str(),
                a.token_id.as_str(),
                a.lot_context
                    .as_ref()
                    .map(|ctx| ctx.order_intent_id.to_string())
                    .unwrap_or_default(),
                a.as_of,
            )
                .cmp(&(
                    b.market_id.as_str(),
                    b.token_id.as_str(),
                    b.lot_context
                        .as_ref()
                        .map(|ctx| ctx.order_intent_id.to_string())
                        .unwrap_or_default(),
                    b.as_of,
                ))
        });
        let canonical = ordered
            .into_iter()
            .map(|e| CanonicalExample {
                market_id: &e.market_id,
                token_id: &e.token_id,
                as_of: e.as_of,
                sample_source: e.sample_source,
                order_intent_id: e.lot_context.as_ref().map(|ctx| &ctx.order_intent_id),
                feature_vector: &e.feature_vector,
                factor_values: &e.factor_values,
                position_state: e.position_state.as_ref(),
                book_fidelity: e.book_fidelity,
                labels: &e.labels,
            })
            .collect();
        ResearchHasher::canonical(&DatasetHashInput {
            model_spec_id,
            window_start,
            window_end,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            examples: canonical,
        })
    }
}

// ── Traits ─────────────────────────────────────────────────────────────────

/// Plans a training dataset (which markets / instants to materialize).
#[async_trait]
pub trait TrainingDatasetPlanner: Send + Sync {
    /// Resolve a plan from a request.
    async fn plan(&self, request: DatasetPlanRequest) -> QuantResult<DatasetPlan>;
}

/// Materializes a planned dataset into a frozen, hashed artifact.
#[async_trait]
pub trait TrainingDatasetBuilder: Send + Sync {
    /// Build the dataset artifact from a resolved plan.
    async fn build(&self, plan: DatasetPlan) -> QuantResult<TrainingDatasetArtifact>;
}

/// Builds a single forward-looking training label, point-in-time correct.
///
/// Pure: all forward data is pre-fetched into [`LabelBuildInput::forward`], so a
/// labeler never touches a database and is trivially testable.
pub trait Labeler: Send + Sync {
    /// The label this labeler produces.
    fn label_name(&self) -> LabelName;

    /// Whether the label is computed once per horizon (`true`, the default) or
    /// once per sample irrespective of horizon (`false`, e.g. settlement).
    fn is_horizon_dependent(&self) -> bool {
        true
    }

    /// Resolve the label for one sample.
    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput;
}

impl From<&LotExitEventRow> for LotExitEvent {
    fn from(row: &LotExitEventRow) -> Self {
        Self {
            at: row.at,
            shares: row.shares,
            net_proceeds: row.net_proceeds,
        }
    }
}

impl From<&ExitTrainingLotRow> for LotTerminalSnapshot {
    fn from(lot: &ExitTrainingLotRow) -> Self {
        Self {
            entry_shares: lot.entry_shares,
            opened_at: lot.opened_at,
            closed_at: lot.closed_at,
            total_net_proceeds: lot.total_net_proceeds,
            exit_events: lot.exit_events.iter().map(LotExitEvent::from).collect(),
        }
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::{LabelName, TrainingExample, TrainingLabel};
    use crate::features::FeatureVector;
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource},
    };
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;

    /// A minimal example with one label of `label_value`, for hash / codec tests.
    pub fn example(market: &str, label_value: i64) -> TrainingExample {
        let as_of: DateTime<Utc> = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new(market),
            token_id: TokenId::new(format!("{market}-yes")),
            as_of,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id: MarketId::new(market),
                token_id: Some(TokenId::new(format!("{market}-yes"))),
                as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                substitutions: Vec::new(),
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                source_refs: Vec::new(),
            },
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: LabelName::from_static("return_to_horizon"),
                horizon_secs: 60,
                value: Decimal::from(label_value),
                is_resolved: true,
            }],
            source_refs: Vec::new(),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TrainingDatasetArtifact, TrainingExample, fixtures::example};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        hashing::CanonicalDigest,
        types::{ContentHash, ModelSpecId},
    };

    fn hashes() -> (ContentHash, ContentHash, ContentHash) {
        (
            CanonicalDigest::content_hash_json("feature").expect("h"),
            CanonicalDigest::content_hash_json("factor").expect("h"),
            CanonicalDigest::content_hash_json("label").expect("h"),
        )
    }

    fn dataset_hash(model_spec_id: &ModelSpecId, examples: &[TrainingExample]) -> ContentHash {
        let (feature, factor, label) = hashes();
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        TrainingDatasetArtifact::compute_dataset_hash(
            model_spec_id,
            start,
            start,
            &feature,
            &factor,
            &label,
            examples,
        )
        .expect("dataset hash")
    }

    #[test]
    fn dataset_hash_ignores_surrogate_ids_and_order() {
        let spec = ModelSpecId::from_v7();
        // Same content, different surrogate example ids + reversed order.
        let a = vec![example("aaa", 100), example("bbb", 200)];
        let b = vec![example("bbb", 200), example("aaa", 100)];
        assert_eq!(dataset_hash(&spec, &a), dataset_hash(&spec, &b));
    }

    #[test]
    fn dataset_hash_changes_on_any_input_change() {
        let spec = ModelSpecId::from_v7();
        let base = vec![example("aaa", 100)];
        let changed = vec![example("aaa", 101)];
        assert_ne!(dataset_hash(&spec, &base), dataset_hash(&spec, &changed));
    }
}
