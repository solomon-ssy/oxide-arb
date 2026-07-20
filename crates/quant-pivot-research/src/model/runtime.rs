//! Model runtime contract: [`QuantModelRuntime`], [`ModelRuntimeFactory`], and the
//! runtime I/O types.
//!
//! [`ModelFamily`] / [`ClassicalKind`] are re-exported from `quant_pivot_models`
//! (flat Postgres `qp_model_family` labels).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ModelVersionInfo,
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, ModelWeightSource},
    },
    runtime_config::FactorCrossSectionConfig,
    types::{ContentHash, MarketId, ModelRunId, ModelVersionId, Price, TokenId, Usd},
};
use serde::{Deserialize, Serialize};

pub use quant_pivot_models::enums::model::{ClassicalKind, ModelFamily, ModelFamilyParseError};

use crate::{
    factors::{FactorValue, FrozenReferenceQuantiles},
    features::{FeatureCell, FeatureName, NullReason},
    model::{overlay::WeightOverlay, sell_scorer::SellScorerRuntime, signal::SignalCandidate},
};

/// Per-market context the scorer needs beyond the factor vector.
///
/// Carries executable prices (for the entry reference + side selection), liquidity
/// and data-quality (for the governed multipliers), the resolution horizon, and
/// the audited feature substitutions (for the governed confidence penalty).
///
/// Projected by the core `ModelRunner` from the selection snapshot and the
/// market's feature vector — never re-derived inside the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInferenceContext {
    /// The NO / secondary outcome token, when binary (targeted by `BuyNo`).
    pub secondary_token_id: Option<TokenId>,
    /// Executable reference price of the YES outcome token.
    pub yes_price: Price,
    /// Executable reference price of the NO outcome token, when binary.
    pub no_price: Option<Price>,
    /// Reported visible liquidity, when known (liquidity multiplier input).
    pub liquidity_usd: Option<Usd>,
    /// Aggregate data-quality classification (data-quality multiplier input).
    pub data_quality: DataQualityStatus,
    /// Seconds until market resolution, when known (horizon multiplier input).
    pub time_to_resolution_secs: Option<u64>,
    /// Reasons of substituted feature cells (confidence-penalty input).
    pub substitution_reasons: Vec<NullReason>,
}

/// One market's factor row for factor-table inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorInferenceRow {
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id (the YES / primary token).
    pub token_id: TokenId,
    /// Computed factor values for the market.
    pub factors: Vec<FactorValue>,
    /// Per-market scoring context (prices, liquidity, quality, horizon).
    pub context: MarketInferenceContext,
}

/// A batch of per-market factor rows (weighted-factor scorer input).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorInferenceTable {
    /// The owning model run; stamped onto every emitted candidate.
    pub model_run_id: ModelRunId,
    /// Frozen decision time for the inference batch.
    pub decision_at: DateTime<Utc>,
    /// Per-market factor rows.
    pub rows: Vec<FactorInferenceRow>,
}

/// One market's dense feature row for matrix inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceMatrixRow {
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id.
    pub token_id: TokenId,
    /// Feature cells, column-aligned with [`InferenceMatrix::feature_names`].
    pub features: Vec<FeatureCell>,
    /// Per-market scoring context (prices, liquidity, quality, horizon) — the
    /// classical runtime prices and sides its candidates from this, exactly like
    /// the weighted factor-table path.
    pub context: MarketInferenceContext,
}

/// A dense feature matrix (classical-ML input).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceMatrix {
    /// Owning model run; stamped onto every emitted candidate.
    pub model_run_id: ModelRunId,
    /// Frozen decision time for the inference batch.
    pub decision_at: DateTime<Utc>,
    /// Column order for every row.
    pub feature_names: Vec<FeatureName>,
    /// Per-market feature rows.
    pub rows: Vec<InferenceMatrixRow>,
}

/// Input to a model runtime's batch inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelRuntimeInput {
    /// Per-market factor table (weighted-factor scorer).
    FactorTable(FactorInferenceTable),
    /// Dense feature matrix (classical ML).
    FeatureMatrix(InferenceMatrix),
    // Onnx(..) / DomainText(..) reserved — Phase 06 / 08.
}

impl ModelRuntimeInput {
    /// Frozen decision time for this inference batch.
    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        match self {
            Self::FactorTable(table) => table.decision_at,
            Self::FeatureMatrix(matrix) => matrix.decision_at,
        }
    }

    /// The per-market scoring context for every row, regardless of input shape —
    /// so the backtester can attribute realized outcomes to the data-quality /
    /// liquidity / horizon / substitution stratum each market was scored under.
    #[must_use]
    pub fn market_contexts(&self) -> Vec<(&MarketId, &MarketInferenceContext)> {
        match self {
            Self::FactorTable(table) => table
                .rows
                .iter()
                .map(|row| (&row.market_id, &row.context))
                .collect(),
            Self::FeatureMatrix(matrix) => matrix
                .rows
                .iter()
                .map(|row| (&row.market_id, &row.context))
                .collect(),
        }
    }
}

/// Throughput / latency metrics for one inference batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRuntimeMetrics {
    /// Number of markets scored.
    pub markets_scored: u32,
    /// Number of candidates emitted.
    pub candidates_emitted: u32,
    /// Wall-clock inference duration, in milliseconds.
    pub inference_duration_ms: u64,
}

/// Semantic state of one raw value consumed by a serving model.
///
/// Classical inputs retain the source [`FeatureCell`] state. Weighted inputs
/// retain the factor engine's normalization outcome, so an absent factor score
/// is never encoded as a numeric zero in serving evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelInputAuditState {
    Observed,
    Substituted,
    Missing,
    NotApplicable,
    Scored,
    MissingInput,
    Indeterminate,
}

impl ModelInputAuditState {
    /// Stable `ClickHouse` wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Substituted => "substituted",
            Self::Missing => "missing",
            Self::NotApplicable => "not_applicable",
            Self::Scored => "scored",
            Self::MissingInput => "missing_input",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// Exact evidence emitted by the same inference transform invocation that fed
/// the estimator.
///
/// `encoded_value_bits` is the IEEE-754 payload consumed by a classical
/// estimator (or the normalized weighted-factor score). It is absent for a
/// factor that was not scored; no sentinel numeric value is permitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInputAuditRow {
    pub model_version_id: ModelVersionId,
    pub model_family: ModelFamily,
    pub market_id: MarketId,
    pub raw_input_name: String,
    pub raw_state: ModelInputAuditState,
    pub raw_value: Option<String>,
    pub encoded_column: String,
    pub encoded_value_bits: Option<u64>,
    pub input_contract_hash: ContentHash,
    pub transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
}

/// Output of a model runtime's batch inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRuntimeOutput {
    /// Emitted candidates (pre-portfolio).
    pub candidates: Vec<SignalCandidate>,
    /// Runtime metrics for the batch.
    pub runtime_metrics: ModelRuntimeMetrics,
    /// Exact model inputs produced during this inference call.
    pub input_audit: Vec<ModelInputAuditRow>,
}

/// Unified inference entry point. Business layers depend only on this trait.
#[async_trait]
pub trait QuantModelRuntime: Send + Sync {
    /// The published model version this runtime serves.
    fn model_version_id(&self) -> ModelVersionId;

    /// The model family.
    fn model_family(&self) -> ModelFamily;

    /// The feature-schema hash this runtime was built against; a mismatch with
    /// the active schema must abort inference.
    fn feature_schema_hash(&self) -> ContentHash;

    /// Features this model requires; surfaced to the 03.1 selector so a market
    /// missing any of them is filtered before it reaches inference. Empty means
    /// the model imposes no extra selection requirement.
    fn required_features(&self) -> Vec<FeatureName>;

    /// Ordered raw inputs needed to assemble this runtime's inference payload.
    ///
    /// Defaults to required features. Classical runtimes override this to add
    /// optional inputs without making them pre-selection rejection gates.
    fn input_features(&self) -> Vec<FeatureName> {
        self.required_features()
    }

    /// The single market category this runtime's frozen artifact declares
    /// itself scoped to, or `None` for a generic cross-category scorer
    /// (11.2.2 category routing). Enforced by the core `ModelRunner` and
    /// `CategoryPointerGuard` against `model.category_model_pointers`: a
    /// pointer's target must declare exactly the pointer's own category, or
    /// `None`. Defaults to `None`; only the weighted-factor artifact carries
    /// a scope today.
    fn category_scope(&self) -> Option<MarketCategory> {
        None
    }

    /// Whether this runtime is scoring on its frozen artifact weights or on a
    /// runtime-config weight overlay. Defaults to [`ModelWeightSource::Artifact`];
    /// only the weighted-factor runtime overrides it. Surfaced into the run
    /// metrics for governance audit (3.7).
    fn weight_source(&self) -> ModelWeightSource {
        ModelWeightSource::Artifact
    }

    /// Weighted-only normalization policy frozen in the artifact.
    fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
        None
    }

    /// Weighted-only train-fitted reference distributions. Classical runtimes
    /// return `None` because they never enter the factor plane.
    fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
        None
    }

    /// Score a batch, producing candidates.
    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput>;
}

/// Loads a [`QuantModelRuntime`] from a model version. The only place that reads
/// artifact bytes and knows a concrete artifact type.
#[async_trait]
pub trait ModelRuntimeFactory: Send + Sync {
    /// Load and validate the runtime for a model version (hash + schema checks),
    /// optionally applying a non-persisted [`WeightOverlay`] for a non-published
    /// candidate / shadow version (3.7). `overlay` is honoured only by the
    /// weighted-factor family; other families ignore it.
    async fn load(
        &self,
        model_version: &ModelVersionInfo,
        overlay: Option<WeightOverlay>,
    ) -> QuantResult<Box<dyn QuantModelRuntime>>;

    /// Load and validate a Sell-side hold-vs-exit scorer for a model version
    /// (Phase 06.1). Same fail-closed hash + schema-binding checks as [`Self::load`],
    /// but returns the exit-scorer runtime family; rejects a non-Sell artifact.
    async fn load_sell_scorer(
        &self,
        model_version: &ModelVersionInfo,
    ) -> QuantResult<Box<dyn SellScorerRuntime>>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{ClassicalKind, ModelFamily};

    #[test]
    fn model_family_string_roundtrip() {
        for family in [
            ModelFamily::WeightedFactor,
            ModelFamily::from_classical(ClassicalKind::RandomForest),
            ModelFamily::from_classical(ClassicalKind::ElasticNet),
        ] {
            let text = family.to_string();
            let parsed = ModelFamily::from_str(&text).expect("round-trip");
            assert_eq!(parsed, family);
        }
    }

    #[test]
    fn unknown_family_is_rejected() {
        assert!(ModelFamily::from_str("mystery").is_err());
        assert!(ModelFamily::from_str("classical:transformer").is_err());
    }
}
