//! Model runtime contract: [`QuantModelRuntime`], [`ModelRuntimeFactory`], the
//! runtime I/O types, and the strongly-typed [`ModelFamily`].
//!
//! Business code (runner, Phase 04 report builder) depends only on
//! `dyn QuantModelRuntime`; the factory is the single place that reads artifact
//! bytes and knows a concrete model type. 3.4 implements the weighted-factor
//! runtime; classical/ONNX runtimes follow in 3.6 / Phase 06.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ModelVersionInfo,
    enums::quant::DataQualityStatus,
    types::{ContentHash, MarketId, ModelRunId, ModelVersionId, Price, TokenId, Usd},
};
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorName, FactorValue},
    features::{FeatureName, FeatureValue, SubstitutionAudit},
    model::{
        overlay::{WeightOverlay, WeightSource},
        signal::SignalCandidate,
    },
};

/// Concrete classical-ML model kind (smartcore-backed in 3.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassicalKind {
    /// Random forest regressor.
    RandomForest,
    /// Extremely randomized trees regressor.
    ExtraTrees,
    /// Logistic regression classifier (yes-probability output).
    LogisticRegression,
    /// Ridge (L2) linear regression.
    Ridge,
    /// Lasso (L1) linear regression.
    Lasso,
    /// Elastic-net linear regression.
    ElasticNet,
}

impl ClassicalKind {
    /// Stable wire string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RandomForest => "random_forest",
            Self::ExtraTrees => "extra_trees",
            Self::LogisticRegression => "logistic_regression",
            Self::Ridge => "ridge",
            Self::Lasso => "lasso",
            Self::ElasticNet => "elastic_net",
        }
    }
}

impl Display for ClassicalKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ClassicalKind {
    type Err = ParseModelFamilyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "random_forest" => Ok(Self::RandomForest),
            "extra_trees" => Ok(Self::ExtraTrees),
            "logistic_regression" => Ok(Self::LogisticRegression),
            "ridge" => Ok(Self::Ridge),
            "lasso" => Ok(Self::Lasso),
            "elastic_net" => Ok(Self::ElasticNet),
            other => Err(ParseModelFamilyError {
                value: other.to_owned(),
            }),
        }
    }
}

/// The strongly-typed model family, round-tripped to the `quant_model_spec`
/// `model_family` text column via [`Display`] / [`FromStr`] (DB column type
/// unchanged).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFamily {
    /// First-class, fully-explainable weighted factor scorer (3.4).
    WeightedFactor,
    /// Classical ML model of a given kind (3.6 shadow candidates).
    Classical(ClassicalKind),
}

impl Display for ModelFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::WeightedFactor => f.write_str("weighted_factor"),
            Self::Classical(kind) => write!(f, "classical:{kind}"),
        }
    }
}

impl FromStr for ModelFamily {
    type Err = ParseModelFamilyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "weighted_factor" {
            return Ok(Self::WeightedFactor);
        }
        if let Some(kind) = s.strip_prefix("classical:") {
            return Ok(Self::Classical(kind.parse()?));
        }
        Err(ParseModelFamilyError {
            value: s.to_owned(),
        })
    }
}

/// Error parsing a [`ModelFamily`] / [`ClassicalKind`] from its wire string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseModelFamilyError {
    /// The unrecognized value.
    pub value: String,
}

impl Display for ParseModelFamilyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "unrecognized model family: {:?}", self.value)
    }
}

impl std::error::Error for ParseModelFamilyError {}

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
}

#[cfg(test)]
mod tests {
    use super::{ClassicalKind, ModelFamily};

    #[test]
    fn model_family_string_roundtrip() {
        for family in [
            ModelFamily::WeightedFactor,
            ModelFamily::Classical(ClassicalKind::RandomForest),
            ModelFamily::Classical(ClassicalKind::ElasticNet),
        ] {
            let text = family.to_string();
            let parsed: ModelFamily = text.parse().expect("round-trip");
            assert_eq!(parsed, family);
        }
    }

    #[test]
    fn unknown_family_is_rejected() {
        assert!("mystery".parse::<ModelFamily>().is_err());
        assert!("classical:transformer".parse::<ModelFamily>().is_err());
    }
}
