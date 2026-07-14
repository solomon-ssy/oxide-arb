//! Training-data plane: dataset planning, labeling, leakage checks, matrix and
//! parquet materialization.
//!
//! Offline closure (3.5). This module owns the **pure** compute contracts and
//! algorithms; the impure orchestration (ClickHouse/Postgres reads, batch
//! prefetch, persistence) lives in `quant-pivot-core`'s `TrainingDatasetService`.
//! Every label and feature is point-in-time correct: features are bounded by
//! the source cutoffs frozen in each `DecisionBoundary`, labels look strictly
//! forward from `decision_at`, and the whole dataset is content-hashed for
//! reproducibility.

mod labeler;
mod leakage;
mod lot_hold_value;
mod matrix;
#[cfg(feature = "dataframe")]
mod parquet;
mod planner;

pub use labeler::{
    HOLD_VS_EXIT_ALPHA_BPS, HoldVsExitProceedsLabeler, LiquidityExitLabeler,
    MAX_ADVERSE_EXCURSION_BPS, MAX_FAVORABLE_EXCURSION_BPS, MaxAdverseExcursionLabeler,
    MaxFavorableExcursionLabeler, POLICY_ENTRY_FILL_RATIO, POLICY_EXIT_FILL_RATIO,
    POLICY_NET_POSITIVE, POLICY_NET_RETURN_BPS, RETURN_TO_HORIZON, ReturnToHorizonLabeler,
    SETTLEMENT_OUTCOME, SettlementOutcomeLabeler, label_names, label_names_for_sources,
};
pub use leakage::{
    LeakageFindings, LeakageViolation, assert_no_future_leakage, scan_future_leakage,
};
pub use lot_hold_value::{
    LotExitEvent, LotTerminalSnapshot, hold_terminal_proceeds, proceeds_before, remaining_shares_at,
};
pub use matrix::{
    FeatureColumnSpec, FeatureMatrixSpec, ModelInputCell, TrainingMatrix, build_training_matrix,
    matrix_spec_from_contract, matrix_spec_from_schema, model_input_cell, probe_matrix_coverage,
    training_input_hash,
};
#[cfg(feature = "dataframe")]
pub use parquet::{DatasetParquetCodec, DecodedDatasetParquet};
pub use planner::{count_samples, plan_lot_timeline_samples, plan_samples};
pub use quant_pivot_models::types::{DatasetCoverage, MatrixCoverageProbe};

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionSource, ExitTrainingLotRow, LotExitEventRow,
        market::{book::BookLevel, fee::MarketFeeSchedule},
    },
    enums::{feature::EvidenceSourceKind, quant::DatasetPurpose},
    types::{
        ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetManifest, MarketId,
        ModelSpecId, OrderIntentId, PositionId, Price, RuntimeConfigVersionId, SchemaVersion,
        Shares, TokenId, TradePolicyArtifactId, TradePolicyArtifactPayload, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, Usd, default_sample_sources,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    execution_semantics::BookFidelity,
    factors::FactorValue,
    features::{DecisionCaptureEvidence, EvidenceSourceRef, FeatureVector},
    hashing::ResearchHasher,
    model::sell_scorer::PositionStateFeatures,
    naming::stable_name,
    selection::SelectedMarket,
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
    /// Knowledge lag used once to derive feature source cutoffs, in seconds.
    pub knowledge_lag_secs: u64,
    /// Feature schema version to materialize against.
    pub feature_schema_version: SchemaVersion,
    /// Which sample sources to materialize.
    #[serde(default = "default_sample_sources")]
    pub sample_sources: Vec<TrainingSampleSource>,
    /// Pre-assigned dataset id (build path); minted by the planner when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub training_dataset_id: Option<TrainingDatasetId>,
    /// Whether samples are for model training or independent calibration (Phase 11.3).
    #[serde(default)]
    pub purpose: DatasetPurpose,
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

/// One deterministic `(market, token, decision_at)` sampling instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePlan {
    /// Market sampled.
    pub market_id: MarketId,
    /// Primary token sampled.
    pub token_id: TokenId,
    /// Decision time of the sample.
    pub decision_at: DateTime<Utc>,
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
    pub decision_at: DateTime<Utc>,
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
}

/// Pre-fetched hold-vs-exit label inputs for one lot decision point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitDecisionLabelContext {
    pub remaining_shares: Shares,
    pub avg_price: Price,
    /// Exact market schedule visible at the decision boundary.
    pub fee_schedule: Option<MarketFeeSchedule>,
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
    /// Published trade-policy binding used to derive executable barrier labels.
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    /// Content hash verified when the plan was resolved.
    pub trade_policy_hash: Option<ContentHash>,
    /// Immutable policy payload used by pure label construction.
    #[serde(skip)]
    pub trade_policy: Option<TradePolicyArtifactPayload>,
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
    /// The instant this label's forward-looking window closes and its value
    /// becomes knowable (Phase 11.5 `label_horizon_end`): `as_of + horizon_secs`
    /// for horizon-dependent labels, the settlement `resolved_at` for
    /// `settlement_outcome`, or the owning lot's `closed_at` for
    /// `hold_vs_exit_alpha_bps`. This is the conservative upper bound
    /// `PurgedSplitter` purges against — a training row whose `matured_at`
    /// overlaps a test window's span leaks the test outcome into training.
    pub matured_at: DateTime<Utc>,
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
    /// No published, quality-gated policy cohort matched this sample.
    NoTradePolicyCohort,
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
    /// The label contract itself is invalid and the dataset build must fail.
    Invalid {
        /// Precise invariant violation; never converted into an unavailable
        /// label count because that would hide a malformed training request.
        detail: String,
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
    /// Closing best bid used for an executable vertical-barrier return.
    pub best_bid_close: Option<Price>,
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
    /// Frozen decision time the label is anchored at.
    pub decision_at: DateTime<Utc>,
    /// Executable entry price at `decision_at` (ask-side fill basis), if quoted.
    pub entry_price: Option<Price>,
    /// Forward horizon, in seconds, the label looks ahead to.
    pub horizon_secs: u64,
    /// Minimum USD depth required for a "liquidity exit possible" label.
    pub min_exit_depth_usd: Usd,
    /// Pre-fetched forward observations + settlement for this sample.
    pub forward: &'a ForwardWindow,
    /// Hold-vs-exit lot context (`ExitDecision` rows only).
    pub exit_decision: Option<&'a ExitDecisionLabelContext>,
}

/// Atomic output of one policy state-machine simulation for an observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySimulationOutcome {
    pub net_return_bps: Decimal,
    pub entry_fill_ratio: Decimal,
    pub exit_fill_ratio: Decimal,
    pub terminal_at: DateTime<Utc>,
}

/// All policy-dependent labels produced from one simulation, or one atomic
/// failure applying to the complete set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicySimulationLabels {
    Available(Box<[TrainingLabel; 4]>),
    NotMature { available_after: DateTime<Utc> },
    Invalid { detail: String },
}

/// Atomic projection of one verified policy state-machine outcome.
///
/// The simulator runs exactly once per observation. This function validates
/// that one outcome and returns the four policy-dependent labels together, so
/// callers cannot mix values produced by different execution paths.
pub fn policy_simulation_labels(
    outcome: &PolicySimulationOutcome,
    decision_at: DateTime<Utc>,
    horizon_secs: u64,
    data_available_until: DateTime<Utc>,
) -> PolicySimulationLabels {
    let Ok(horizon_seconds_i64) = i64::try_from(horizon_secs) else {
        return PolicySimulationLabels::Invalid {
            detail: "policy simulation horizon does not fit chrono seconds".to_owned(),
        };
    };
    let Some(horizon_end) =
        decision_at.checked_add_signed(chrono::Duration::seconds(horizon_seconds_i64))
    else {
        return PolicySimulationLabels::Invalid {
            detail: "policy simulation horizon overflows its decision time".to_owned(),
        };
    };
    if outcome.terminal_at < decision_at || outcome.terminal_at > horizon_end {
        return PolicySimulationLabels::Invalid {
            detail: "policy simulation terminal time is outside its decision/horizon interval"
                .to_owned(),
        };
    }
    if outcome.terminal_at > data_available_until {
        return PolicySimulationLabels::NotMature {
            available_after: outcome.terminal_at,
        };
    }
    if !(Decimal::ZERO..=Decimal::ONE).contains(&outcome.entry_fill_ratio)
        || !(Decimal::ZERO..=Decimal::ONE).contains(&outcome.exit_fill_ratio)
    {
        return PolicySimulationLabels::Invalid {
            detail: "policy simulation fill ratios must both be within [0, 1]".to_owned(),
        };
    }
    let positive = if outcome.net_return_bps > Decimal::ZERO {
        Decimal::ONE
    } else {
        Decimal::ZERO
    };
    PolicySimulationLabels::Available(Box::new([
        TrainingLabel {
            label_name: POLICY_NET_RETURN_BPS,
            horizon_secs,
            value: outcome.net_return_bps,
            is_resolved: true,
            matured_at: outcome.terminal_at,
        },
        TrainingLabel {
            label_name: POLICY_NET_POSITIVE,
            horizon_secs,
            value: positive,
            is_resolved: true,
            matured_at: outcome.terminal_at,
        },
        TrainingLabel {
            label_name: POLICY_ENTRY_FILL_RATIO,
            horizon_secs,
            value: outcome.entry_fill_ratio,
            is_resolved: true,
            matured_at: outcome.terminal_at,
        },
        TrainingLabel {
            label_name: POLICY_EXIT_FILL_RATIO,
            horizon_secs,
            value: outcome.exit_fill_ratio,
            is_resolved: true,
            matured_at: outcome.terminal_at,
        },
    ]))
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
    /// Exact selection member used to build the feature/model context.
    pub selected_market: SelectedMarket,
    /// Complete immutable decision and source-visibility boundary.
    pub decision_boundary: DecisionBoundary,
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
    /// Exact catalog/book/business capture used to materialize this row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_capture: Option<DecisionCaptureEvidence>,
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

impl TrainingExample {
    /// Decision instant anchoring features, factors, labels, and CV ordering.
    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        self.decision_boundary.decision_at()
    }
}

/// A frozen, content-addressed training dataset artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingDatasetArtifact {
    /// Breaking Parquet/manifest wire format.
    pub format_version: u32,
    /// Dataset id.
    pub training_dataset_id: TrainingDatasetId,
    /// Model spec the dataset was built for.
    pub model_spec_id: ModelSpecId,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
    /// Materialized examples, ordered by `(market_id, token_id, decision_at)`.
    pub examples: Vec<TrainingExample>,
    /// Feature-schema hash the dataset was built against.
    pub feature_schema_hash: ContentHash,
    /// Factor-schema hash the dataset was built against.
    pub factor_schema_hash: ContentHash,
    /// Label-schema hash the dataset was built against.
    pub label_schema_hash: ContentHash,
    /// Content hash over schema hashes + canonical example content.
    pub dataset_hash: ContentHash,
    /// Frozen manifest embedded in the Parquet artifact.
    pub manifest: DatasetManifest,
    /// Exact BLAKE3 hash of the persisted Parquet bytes.
    pub artifact_bytes_hash: ContentHash,
    /// Location of the materialized parquet bytes.
    pub parquet_uri: ArtifactUri,
    /// Coverage accounting.
    pub coverage: DatasetCoverage,
}

/// Canonical content hash persisted alongside the Parquet byte hash.
pub fn dataset_manifest_hash(manifest: &DatasetManifest) -> QuantResult<ContentHash> {
    ResearchHasher::canonical(manifest)
}

/// Deterministically fingerprint the provenance assigned to every row.
pub fn dataset_source_fingerprint(examples: &[TrainingExample]) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct RowSources<'a> {
        market_id: &'a MarketId,
        token_id: &'a TokenId,
        boundary: &'a DecisionBoundary,
        refs: Vec<String>,
        decision_capture_hash: Option<ContentHash>,
    }

    let mut rows = Vec::with_capacity(examples.len());
    for example in examples {
        let mut refs = example
            .source_refs
            .iter()
            .chain(example.selected_market.source_refs.iter())
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("dataset source reference serialization failed: {error}"),
            })?;
        refs.sort();
        rows.push(RowSources {
            market_id: &example.market_id,
            token_id: &example.token_id,
            boundary: &example.decision_boundary,
            refs,
            decision_capture_hash: example
                .decision_capture
                .as_ref()
                .map(ResearchHasher::canonical)
                .transpose()?,
        });
    }
    rows.sort_by(|left, right| {
        (
            left.market_id.as_str(),
            left.token_id.as_str(),
            left.boundary.decision_at(),
        )
            .cmp(&(
                right.market_id.as_str(),
                right.token_id.as_str(),
                right.boundary.decision_at(),
            ))
    });
    ResearchHasher::canonical(&rows)
}

/// Verify the embedded manifest against the decoded immutable rows.
pub fn verify_dataset_manifest(
    manifest: &DatasetManifest,
    examples: &[TrainingExample],
) -> QuantResult<()> {
    if manifest.format_version != DATASET_ARTIFACT_FORMAT_VERSION {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "unsupported dataset manifest format {}, expected {}",
                manifest.format_version, DATASET_ARTIFACT_FORMAT_VERSION
            ),
        }
        .into());
    }
    let sample_count =
        u64::try_from(examples.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("dataset row count conversion failed: {error}"),
        })?;
    if manifest.sample_count != sample_count {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "dataset manifest row count mismatch: manifest {}, decoded {sample_count}",
                manifest.sample_count
            ),
        }
        .into());
    }
    for example in examples {
        validate_example_boundary(example, manifest)?;
        validate_example_capture(example)?;
    }
    let semantic_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: &manifest.model_spec_id,
            window_start: manifest.window_start,
            window_end: manifest.window_end,
            purpose: manifest.purpose,
            feature_schema_hash: &manifest.feature_schema_hash,
            factor_schema_hash: &manifest.factor_schema_hash,
            label_schema_hash: &manifest.label_schema_hash,
        },
        examples,
    )?;
    if semantic_hash != manifest.semantic_dataset_hash {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "dataset manifest semantic hash mismatch: manifest {}, decoded {semantic_hash}",
                manifest.semantic_dataset_hash
            ),
        }
        .into());
    }
    let source_fingerprint = dataset_source_fingerprint(examples)?;
    if source_fingerprint != manifest.source_fingerprint {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "dataset source fingerprint mismatch: manifest {}, decoded {source_fingerprint}",
                manifest.source_fingerprint
            ),
        }
        .into());
    }
    Ok(())
}

fn validate_example_boundary(
    example: &TrainingExample,
    manifest: &DatasetManifest,
) -> QuantResult<()> {
    let boundary = &example.decision_boundary;
    boundary.validate()?;
    let decision_at = boundary.decision_at();
    if example.feature_vector.decision_at != decision_at {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "example {} feature decision time {} does not match boundary {decision_at}",
                example.example_id, example.feature_vector.decision_at
            ),
        }
        .into());
    }
    if example.feature_vector.market_id != example.market_id
        || example.feature_vector.token_id.as_ref() != Some(&example.token_id)
        || example.selected_market.market_id != example.market_id
        || example.selected_market.primary_token_id != example.token_id
    {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "example {} feature-vector market/token binding does not match the row",
                example.example_id
            ),
        }
        .into());
    }
    if !matches!(example.sample_source, TrainingSampleSource::LiveAttribution)
        && boundary.knowledge_lag_secs() != manifest.knowledge_lag_secs
    {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "example {} knowledge lag {} does not match manifest {}",
                example.example_id,
                boundary.knowledge_lag_secs(),
                manifest.knowledge_lag_secs
            ),
        }
        .into());
    }
    for evidence in example
        .source_refs
        .iter()
        .chain(example.selected_market.source_refs.iter())
        .chain(
            example
                .feature_vector
                .iter_cells()
                .filter_map(|(_, cell)| cell.evidence.as_ref()),
        )
    {
        let cutoff = decision_source_for_evidence(evidence.source_kind)
            .map_or(decision_at, |source| boundary.cutoff_for(source));
        if evidence.effective_at > cutoff {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "example {} evidence `{}` effective at {} exceeds {:?} cutoff {cutoff}",
                    example.example_id,
                    evidence.reference,
                    evidence.effective_at,
                    evidence.source_kind
                ),
            }
            .into());
        }
        if evidence
            .available_at
            .is_some_and(|available_at| available_at > decision_at)
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "example {} evidence `{}` available at {:?} exceeds decision {decision_at}",
                    example.example_id, evidence.reference, evidence.available_at
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn validate_example_capture(example: &TrainingExample) -> QuantResult<()> {
    let capture = example
        .decision_capture
        .as_ref()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!("example {} has no v2 decision capture", example.example_id),
        })?;
    let snapshot = &capture.snapshot;
    let boundary = &example.decision_boundary;
    let catalog_cutoff = boundary.cutoff_for(DecisionSource::Catalog);
    let book_cutoff = boundary.cutoff_for(DecisionSource::Book);
    if snapshot.boundary != *boundary
        || snapshot.market_id != example.market_id
        || snapshot.token_id != example.token_id
        || snapshot.event_id != example.selected_market.event_id
        || snapshot.book_snapshot_ref.token_id != example.token_id
        || capture.data_quality != example.feature_vector.data_quality
        || snapshot.catalog.market_effective_at > catalog_cutoff
        || snapshot.catalog.event_effective_at > catalog_cutoff
        || snapshot.catalog.market_available_at > boundary.decision_at()
        || snapshot.catalog.event_available_at > boundary.decision_at()
        || snapshot.book_effective_at > book_cutoff
        || snapshot.book_available_at > boundary.decision_at()
    {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "example {} decision capture violates its boundary/identity/data-quality binding",
                example.example_id
            ),
        }
        .into());
    }
    Ok(())
}

const fn decision_source_for_evidence(source: EvidenceSourceKind) -> Option<DecisionSource> {
    match source {
        EvidenceSourceKind::Book => Some(DecisionSource::Book),
        EvidenceSourceKind::GammaMetadata => Some(DecisionSource::Catalog),
        EvidenceSourceKind::ClickHouseFact => Some(DecisionSource::Microstructure),
        EvidenceSourceKind::TradeTape => Some(DecisionSource::TradeTape),
        EvidenceSourceKind::DomainExternal => Some(DecisionSource::DomainCrypto),
        EvidenceSourceKind::Linkage => Some(DecisionSource::Linkage),
        EvidenceSourceKind::Derived => None,
    }
}

/// Canonical, surrogate-free projection used to compute `dataset_hash`.
///
/// Excludes the dataset id, parquet URI, and per-row surrogate ids so the hash
/// is a pure function of the schema bindings + the materialized content. Any
/// change to a feature value, factor value, or label flips the digest; a rename
/// of a surrogate id does not.
#[derive(Serialize)]
struct DatasetHashInput<'a> {
    format_version: u32,
    model_spec_id: &'a ModelSpecId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    /// Included so a `Training` dataset never collides with a `Calibration`
    /// dataset over identical content — `uq_quant_training_dataset_hash` is a
    /// single global unique index across every purpose, and the purge/embargo
    /// invariant (Phase 11.3 §0) depends on the two ledgers never being
    /// silently conflated by a shared hash.
    purpose: DatasetPurpose,
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
    selected_market: &'a SelectedMarket,
    decision_boundary: &'a DecisionBoundary,
    sample_source: TrainingSampleSource,
    order_intent_id: Option<&'a OrderIntentId>,
    feature_vector: &'a FeatureVector,
    factor_values: &'a [FactorValue],
    position_state: Option<&'a PositionStateFeatures>,
    book_fidelity: Option<BookFidelity>,
    labels: &'a [TrainingLabel],
    decision_capture: Option<&'a DecisionCaptureEvidence>,
}

/// Governed bindings included in the semantic dataset hash.
///
/// This type deliberately excludes storage identity (`training_dataset_id`,
/// Parquet URI, byte hash) and row surrogate ids. Callers must provide every
/// model/schema/window binding explicitly before canonical example content can
/// be hashed.
#[derive(Clone, Copy)]
pub struct DatasetHashContract<'a> {
    /// Owning immutable model specification.
    pub model_spec_id: &'a ModelSpecId,
    /// Inclusive dataset decision-window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive dataset decision-window end.
    pub window_end: DateTime<Utc>,
    /// Governed dataset purpose (training or calibration).
    pub purpose: DatasetPurpose,
    /// Frozen governed feature-schema hash.
    pub feature_schema_hash: &'a ContentHash,
    /// Frozen factor-definition/schema hash.
    pub factor_schema_hash: &'a ContentHash,
    /// Frozen label-schema hash.
    pub label_schema_hash: &'a ContentHash,
}

impl TrainingDatasetArtifact {
    /// Compute the content hash over schema bindings + canonical example content.
    ///
    /// Examples are sorted by `(market_id, token_id, decision_at)` so build order never
    /// perturbs the digest.
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn compute_dataset_hash(
        contract: DatasetHashContract<'_>,
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
                a.decision_at(),
            )
                .cmp(&(
                    b.market_id.as_str(),
                    b.token_id.as_str(),
                    b.lot_context
                        .as_ref()
                        .map(|ctx| ctx.order_intent_id.to_string())
                        .unwrap_or_default(),
                    b.decision_at(),
                ))
        });
        let canonical = ordered
            .into_iter()
            .map(|e| CanonicalExample {
                market_id: &e.market_id,
                token_id: &e.token_id,
                selected_market: &e.selected_market,
                decision_boundary: &e.decision_boundary,
                sample_source: e.sample_source,
                order_intent_id: e.lot_context.as_ref().map(|ctx| &ctx.order_intent_id),
                feature_vector: &e.feature_vector,
                factor_values: &e.factor_values,
                position_state: e.position_state.as_ref(),
                book_fidelity: e.book_fidelity,
                labels: &e.labels,
                decision_capture: e.decision_capture.as_ref(),
            })
            .collect();
        ResearchHasher::canonical(&DatasetHashInput {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            model_spec_id: contract.model_spec_id,
            window_start: contract.window_start,
            window_end: contract.window_end,
            purpose: contract.purpose,
            feature_schema_hash: contract.feature_schema_hash,
            factor_schema_hash: contract.factor_schema_hash,
            label_schema_hash: contract.label_schema_hash,
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
    use crate::{
        features::{
            CatalogDecisionRef, DecisionCaptureEvidence, DecisionSnapshotEvidence, FeatureVector,
        },
        selection::SelectedMarket,
    };
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{DecisionClock, DecisionSource},
        enums::{
            common::{MarketCategory, TickSize::Hundredth},
            market::MarketStatus,
            quant::DataQualityStatus,
        },
        types::{
            BookSnapshotRef, BookSnapshotSource, Bps, CatalogSyncBatchId, ContentHash,
            EventCatalogVersionId, EventId, MarketCatalogVersionId, MarketContext, MarketId, Price,
            Probability, RecommendationIdentity, SchemaVersion, TokenId, TrainingExampleId,
            TrainingSampleSource, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    pub fn selected_market(
        market_id: &MarketId,
        token_id: &TokenId,
        category: MarketCategory,
    ) -> SelectedMarket {
        SelectedMarket {
            market_id: market_id.clone(),
            event_id: EventId::new(format!("event:{}", market_id.as_str())),
            category,
            primary_token_id: token_id.clone(),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        }
    }

    /// A minimal example with one label of `label_value`, for hash / codec tests.
    pub fn example(market: &str, label_value: i64) -> TrainingExample {
        let as_of: DateTime<Utc> = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let market_id = MarketId::new(market);
        let token_id = TokenId::new(format!("{market}-yes"));
        let mut example = TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            selected_market: selected_market(&market_id, &token_id, MarketCategory::Sports),
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                decision_at: as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::new(),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: LabelName::from_static("return_to_horizon"),
                horizon_secs: 60,
                value: Decimal::from(label_value),
                is_resolved: true,
                matured_at: as_of + Duration::seconds(60),
            }],
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        };
        bind_capture_to_boundary(&mut example);
        example
    }

    /// Rebuild the fixture's complete v2 capture after a test changes its
    /// decision boundary. Every evidence clock is derived from the already
    /// frozen boundary; no lag is subtracted a second time.
    pub fn bind_capture_to_boundary(example: &mut TrainingExample) {
        let boundary = example.decision_boundary.clone();
        let decision_at = boundary.decision_at();
        let catalog_effective_at = boundary.cutoff_for(DecisionSource::Catalog);
        let book_effective_at = boundary.cutoff_for(DecisionSource::Book);
        let hash = |seed: char| {
            ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64)))
                .expect("fixture content hash")
        };
        let book_age_ms = u64::try_from((decision_at - book_effective_at).num_milliseconds())
            .expect("fixture book cutoff is not after decision time");
        let token_id = example.token_id.clone();
        example.decision_capture = Some(DecisionCaptureEvidence {
            snapshot: DecisionSnapshotEvidence {
                boundary,
                market_id: example.market_id.clone(),
                event_id: example.selected_market.event_id.clone(),
                token_id: token_id.clone(),
                catalog: CatalogDecisionRef {
                    catalog_sync_batch_id: CatalogSyncBatchId::new(Uuid::from_u128(1)),
                    market_catalog_version_id: MarketCatalogVersionId::new(Uuid::from_u128(2)),
                    event_catalog_version_id: EventCatalogVersionId::new(Uuid::from_u128(3)),
                    market_content_hash: hash('1'),
                    event_content_hash: hash('2'),
                    membership_hash: hash('3'),
                    market_effective_at: catalog_effective_at,
                    market_available_at: decision_at,
                    event_effective_at: catalog_effective_at,
                    event_available_at: decision_at,
                    market_timestamp_quality: "source".to_owned(),
                    event_timestamp_quality: "source".to_owned(),
                },
                book_snapshot_ref: BookSnapshotRef {
                    token_id,
                    source: BookSnapshotSource::CanonicalL2 {
                        stream_session_id: Uuid::from_u128(4),
                        token_sequence: 1,
                        source_event_hash: hash('5'),
                        event_time_ms: book_effective_at.timestamp_millis(),
                        ingestion_time_ms: decision_at.timestamp_millis(),
                    },
                    content_hash: hash('4'),
                },
                book_effective_at,
                book_available_at: decision_at,
            },
            identity: RecommendationIdentity {
                category: MarketCategory::Sports,
                question: "Fixture market?".to_owned(),
                outcome_name: "Yes".to_owned(),
            },
            market_context: MarketContext {
                best_bid: Some(Price::new(dec!(0.49))),
                best_ask: Some(Price::new(dec!(0.51))),
                mid_price: Some(Price::new(dec!(0.50))),
                spread_bps: Some(Bps::new(dec!(400))),
                depth_usd: Usd::new(dec!(100)),
                volume_24h_usd: Some(Usd::new(dec!(1_000))),
                book_age_ms,
                time_to_resolution_secs: None,
                market_status: MarketStatus::Active,
                neg_risk: false,
                tick_size: Hundredth,
                fee_rate: None,
            },
            data_quality: example.feature_vector.data_quality,
            liquidity_score: Probability::ONE,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DatasetHashContract, PolicySimulationLabels, PolicySimulationOutcome,
        TrainingDatasetArtifact, TrainingExample, dataset_source_fingerprint,
        fixtures::{bind_capture_to_boundary, example},
        policy_simulation_labels,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{DecisionClock, DecisionSource},
        enums::quant::DatasetPurpose,
        hashing::CanonicalDigest,
        types::{ContentHash, ModelSpecId},
    };
    use rust_decimal::Decimal;

    fn hashes() -> (ContentHash, ContentHash, ContentHash) {
        (
            CanonicalDigest::content_hash_json("feature").expect("h"),
            CanonicalDigest::content_hash_json("factor").expect("h"),
            CanonicalDigest::content_hash_json("label").expect("h"),
        )
    }

    #[test]
    fn policy_simulation_projects_four_labels_atomically() {
        let terminal_at = Utc.timestamp_opt(1_000_060, 0).single().expect("ts");
        let outcome = PolicySimulationOutcome {
            net_return_bps: Decimal::from(250),
            entry_fill_ratio: Decimal::ONE,
            exit_fill_ratio: Decimal::new(8, 1),
            terminal_at,
        };
        let decision_at = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let output = policy_simulation_labels(&outcome, decision_at, 3_600, terminal_at);
        let PolicySimulationLabels::Available(labels) = output else {
            panic!("valid atomic policy outcome must produce all labels");
        };
        assert_eq!(labels.len(), 4);
        assert!(labels.iter().all(|label| label.matured_at == terminal_at));
        assert_eq!(labels[0].value, Decimal::from(250));
        assert_eq!(labels[1].value, Decimal::ONE);
        assert_eq!(labels[2].value, Decimal::ONE);
        assert_eq!(labels[3].value, Decimal::new(8, 1));
    }

    fn dataset_hash(model_spec_id: &ModelSpecId, examples: &[TrainingExample]) -> ContentHash {
        dataset_hash_with_purpose(model_spec_id, examples, DatasetPurpose::Training)
    }

    fn dataset_hash_with_purpose(
        model_spec_id: &ModelSpecId,
        examples: &[TrainingExample],
        purpose: DatasetPurpose,
    ) -> ContentHash {
        let (feature, factor, label) = hashes();
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id,
                window_start: start,
                window_end: start,
                purpose,
                feature_schema_hash: &feature,
                factor_schema_hash: &factor,
                label_schema_hash: &label,
            },
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

    #[test]
    fn dataset_hash_and_source_fingerprint_bind_per_source_cutoffs() {
        let spec = ModelSpecId::from_v7();
        let base = vec![example("aaa", 100)];
        let mut changed = base.clone();
        let decision_at = changed[0].decision_at();
        changed[0].decision_boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary")
            .with_source_cutoff(DecisionSource::Book, 30)
            .expect("book cutoff");
        bind_capture_to_boundary(&mut changed[0]);

        assert_ne!(dataset_hash(&spec, &base), dataset_hash(&spec, &changed));
        assert_ne!(
            dataset_source_fingerprint(&base).expect("base fingerprint"),
            dataset_source_fingerprint(&changed).expect("changed fingerprint")
        );
    }

    #[test]
    fn dataset_hash_differs_by_purpose_for_identical_content() {
        // A `Training` and a `Calibration` dataset with byte-identical
        // examples/schemas must never collide on `uq_quant_training_dataset_hash`
        // (Phase 11.3 P2 — the two ledgers must never be silently conflated).
        let spec = ModelSpecId::from_v7();
        let examples = vec![example("aaa", 100)];
        let training = dataset_hash_with_purpose(&spec, &examples, DatasetPurpose::Training);
        let calibration = dataset_hash_with_purpose(&spec, &examples, DatasetPurpose::Calibration);
        assert_ne!(training, calibration);
    }
}
