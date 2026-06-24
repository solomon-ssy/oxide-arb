//! Factor compute-domain value types: [`FactorName`], [`FactorValue`],
//! [`FactorDefinitionSpec`], normalization, and explanation.
//!
//! A factor score is **not** a recommendation score — it is a normalized,
//! explainable model input. The full registry and the nine generic factors land
//! in 3.3; 3.0 fixes the value + spec contract.

use quant_pivot_models::{
    enums::quant::FactorDirection,
    types::{FactorDefinitionId, Probability},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{features::FeatureName, naming::stable_name, vertical::DomainFamily};

stable_name! {
    /// Stable, compile-time-known factor name (e.g. `"liquidity_depth"`).
    FactorName
}

/// Factor family grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorFamily {
    /// Order-book depth / available liquidity.
    Liquidity,
    /// Order-flow microstructure.
    Microstructure,
    /// Trend / momentum.
    Momentum,
    /// Mean reversion.
    MeanReversion,
    /// Realized / implied volatility regime.
    Volatility,
    /// Market activity (quote/trade rate).
    Activity,
    /// Resolution timing / ambiguity.
    Resolution,
    /// Data-quality-derived factors.
    DataQuality,
    /// Vertical/domain-specific factors.
    Domain(DomainFamily),
}

/// How a factor's raw value is normalized into a `[0, 1]` score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "method")]
pub enum NormalizationSpec {
    /// Z-score normalization, clamped at `±clamp_sigma` standard deviations.
    ZScore {
        /// Sigma clamp bound.
        clamp_sigma: Decimal,
    },
    /// Linear min/max scaling between `lo` and `hi`.
    MinMax {
        /// Lower bound mapped to 0.
        lo: Decimal,
        /// Upper bound mapped to 1.
        hi: Decimal,
    },
    /// Cross-sectional rank in `[0, 1]` (requires the batch interface).
    Rank,
    /// Logistic squashing with steepness `k` and midpoint `x0`.
    Logistic {
        /// Logistic steepness.
        k: Decimal,
        /// Logistic midpoint.
        x0: Decimal,
    },
}

/// Output classification of a factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorOutputKind {
    /// A normalized `[0, 1]` score.
    NormalizedScore,
    /// A directional score (sign carries meaning).
    Directional,
}

/// A governance gate a factor definition must clear (refined in 3.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorQualityGate {
    /// Human-readable gate name.
    pub name: String,
    /// Minimum confidence the factor must report to count.
    pub min_confidence: Probability,
}

/// Governed factor definition: the stable contract for a factor's inputs,
/// output, normalization, and ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorDefinitionSpec {
    /// Stable factor name.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Features this factor consumes (schema dependency).
    pub input_features: Vec<FeatureName>,
    /// Output classification.
    pub output_kind: FactorOutputKind,
    /// Default contribution direction.
    pub default_direction: FactorDirection,
    /// Normalization applied to the raw value.
    pub normalization: NormalizationSpec,
    /// Owning team / person.
    pub owner: String,
    /// Quality gates governing publication.
    pub quality_gates: Vec<FactorQualityGate>,
}

/// A single explanation driver: a feature and its signed contribution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorDriver {
    /// The feature driving the factor.
    pub feature_name: FeatureName,
    /// Signed contribution of that feature to the factor.
    pub contribution: Decimal,
}

/// Human- and machine-readable explanation of a factor value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExplanation {
    /// One-line human summary.
    pub headline: String,
    /// Ranked drivers (feature → contribution).
    pub drivers: Vec<FactorDriver>,
}

/// The computed output of a factor for one feature vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorValue {
    /// Governing definition id.
    pub definition_id: FactorDefinitionId,
    /// Factor name.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Raw (pre-normalization) value, when defined.
    pub raw_value: Option<Decimal>,
    /// Normalized score clamped into `[0, 1]`.
    pub normalized_score: Probability,
    /// Contribution direction.
    pub direction: FactorDirection,
    /// Confidence in the factor value.
    pub confidence: Probability,
    /// Explanation of the value.
    pub explanation: FactorExplanation,
    /// Features that fed this factor.
    pub input_feature_refs: Vec<FeatureName>,
}

/// An ordered set of governed factor definitions whose change perturbs the
/// `factor_schema_hash` bound to models and datasets.
///
/// Hash via [`crate::hashing::ResearchHasher::factor_schema`] so definition
/// insertion order does not affect the digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorSet {
    /// The factor definitions in this set.
    pub definitions: Vec<FactorDefinitionSpec>,
}
