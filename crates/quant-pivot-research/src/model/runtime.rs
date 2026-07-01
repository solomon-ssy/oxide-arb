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
    enums::quant::DataQualityStatus,
    types::{ContentHash, MarketId, ModelRunId, ModelVersionId, Price, TokenId, Usd},
};
use serde::{Deserialize, Serialize};

pub use quant_pivot_models::enums::model::{ClassicalKind, ModelFamily, ModelFamilyParseError};

use crate::{
    factors::{FactorName, FactorValue},
    features::{FeatureName, FeatureValue, SubstitutionAudit},
    model::{
        overlay::{WeightOverlay, WeightSource},
        sell_scorer::SellScorerRuntime,
        signal::SignalCandidate,
    },
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
    /// Audited feature substitutions (confidence-penalty input).
    pub substitutions: Vec<SubstitutionAudit>,
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
    /// Decision time the batch was assembled as of.
    pub as_of: DateTime<Utc>,
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
    /// Feature values, column-aligned with [`InferenceMatrix::feature_names`].
    pub features: Vec<FeatureValue>,
    /// Per-market scoring context (prices, liquidity, quality, horizon) — the
    /// classical runtime prices and sides its candidates from this, exactly like
    /// the weighted factor-table path.
    pub context: MarketInferenceContext,
}

/// A dense feature matrix (classical-ML input).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceMatrix {
    /// Decision time the matrix was assembled as of.
    pub as_of: DateTime<Utc>,
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
    /// Decision time the batch was assembled as of.
    #[must_use]
    pub const fn as_of(&self) -> DateTime<Utc> {
        match self {
            Self::FactorTable(table) => table.as_of,
            Self::FeatureMatrix(matrix) => matrix.as_of,
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

/// A non-fatal warning surfaced by a model runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRuntimeWarning {
    /// A schema hash mismatch was tolerated in a degraded mode.
    SchemaHashMismatch {
        /// Hash the runtime expected.
        expected: ContentHash,
        /// Hash actually presented.
        actual: ContentHash,
    },
    /// A required factor was missing for a market.
    MissingFactor {
        /// Affected market.
        market_id: MarketId,
        /// Missing factor.
        factor: FactorName,
    },
    /// Any other degradation, described inline.
    Degraded(String),
}

/// Output of a model runtime's batch inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRuntimeOutput {
    /// Emitted candidates (pre-portfolio).
    pub candidates: Vec<SignalCandidate>,
    /// Runtime metrics for the batch.
    pub runtime_metrics: ModelRuntimeMetrics,
    /// Non-fatal warnings.
    pub warnings: Vec<ModelRuntimeWarning>,
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

    /// Whether this runtime is scoring on its frozen artifact weights or on a
    /// runtime-config weight overlay. Defaults to [`WeightSource::Artifact`];
    /// only the weighted-factor runtime overrides it. Surfaced into the run
    /// metrics for governance audit (3.7).
    fn weight_source(&self) -> WeightSource {
        WeightSource::Artifact
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
