//! Factor compute-domain value types: [`FactorName`], [`FactorValue`],
//! [`FactorDefinitionSpec`], normalization, explanation, and the engine's
//! per-market [`MarketFactorOutcome`].
//!
//! A factor score is **not** a recommendation score — it is a normalized,
//! explainable model input. The pipeline splits responsibilities cleanly:
//! a [`FactorComputer`](crate::factors::FactorComputer) produces a per-market
//! [`RawFactor`] (no normalization, no cross-section), and the
//! [`FactorEngine`](crate::factors::FactorEngine) applies the (possibly
//! cross-sectional) [`NormalizationSpec`] to yield the final [`FactorValue`].

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{factor::FactorFamily, quant::FactorDirection},
    types::{FactorDefinitionId, MarketId, Probability},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{features::FeatureName, naming::stable_name};

stable_name! {
    /// Stable, compile-time-known factor name (e.g. `"liquidity_depth"`).
    FactorName
}

/// How a factor's raw value is normalized into a `[0, 1]` score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "method")]
pub enum NormalizationSpec {
    /// Z-score normalization, clamped at `±clamp_sigma` standard deviations.
    ///
    /// **Cross-sectional**: requires the whole same-`as_of` selection (the batch
    /// engine), since the mean / standard deviation are computed across markets.
    ZScore {
        /// Sigma clamp bound.
        clamp_sigma: Decimal,
    },
    /// Linear min/max scaling between `lo` and `hi` (per-market).
    MinMax {
        /// Lower bound mapped to 0.
        lo: Decimal,
        /// Upper bound mapped to 1.
        hi: Decimal,
    },
    /// Cross-sectional rank in `[0, 1]` (requires the batch interface).
    Rank,
    /// Logistic squashing with steepness `k` and midpoint `x0` (per-market).
    Logistic {
        /// Logistic steepness.
        k: Decimal,
        /// Logistic midpoint.
        x0: Decimal,
    },
}

impl NormalizationSpec {
    /// Whether this normalization needs the full same-`as_of` cross-section, and
    /// so can only be evaluated through the batch engine.
    #[must_use]
    pub const fn is_cross_sectional(&self) -> bool {
        matches!(self, Self::ZScore { .. } | Self::Rank)
    }
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

/// A governance gate a factor definition must clear.
///
/// A non-empty `quality_gates` list marks a factor as **required**: when a
/// required factor is missing or below the configured confidence floor and the
/// runtime `missing_factor_policy` is `RejectCandidate`, the whole market is
/// rejected (see [`FactorEligibility`]).
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
    /// Quality gates governing publication; a non-empty list marks the factor
    /// **required** for `RejectCandidate` market eligibility.
    pub quality_gates: Vec<FactorQualityGate>,
}

impl FactorDefinitionSpec {
    /// Whether this factor is **required** for market eligibility (it declares at
    /// least one quality gate).
    #[must_use]
    pub fn is_required(&self) -> bool {
        !self.quality_gates.is_empty()
    }
}

/// A single explanation driver: a feature and its signed contribution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorDriver {
    /// The feature driving the factor.
    pub feature_name: FeatureName,
    /// Signed contribution of that feature to the factor.
    pub contribution: Decimal,
}

/// An audited normalization clamp: the out-of-domain raw value and the bound it
/// was clamped to. Clamping is **never silent** — every clamp is recorded here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationClampAudit {
    /// The normalization method whose domain was exceeded.
    pub method: String,
    /// The raw value (or intermediate, e.g. z-score) that fell out of domain.
    pub raw: Decimal,
    /// The bound the value was clamped to before mapping into `[0, 1]`.
    pub clamped_to: Decimal,
}

/// Human- and machine-readable explanation of a factor value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExplanation {
    /// One-line human summary.
    pub headline: String,
    /// Ranked drivers (feature → signed contribution).
    pub drivers: Vec<FactorDriver>,
    /// Recorded normalization clamp, when the raw value was out of domain.
    pub clamp: Option<NormalizationClampAudit>,
}

/// The computed output of a factor for one feature vector.
///
/// `normalized_score` is always the `[0, 1]` normalized **magnitude**; the
/// `direction` carries the sign of its effect on a candidate score. A factor
/// whose inputs were missing carries `raw_value = None`, `confidence = 0`, and a
/// neutral placeholder `normalized_score` that never contributes downstream.
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

/// A per-market raw factor output, prior to (possibly cross-sectional)
/// normalization.
///
/// Produced by [`FactorComputer::compute_raw`](crate::factors::FactorComputer);
/// pure, side-effect-free, and deterministic. `raw_value = None` means the
/// factor's inputs were unavailable for this market (never silently zero).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawFactor {
    /// Governing definition id.
    pub definition_id: FactorDefinitionId,
    /// Factor name.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Raw value, or `None` when the inputs were unavailable.
    pub raw_value: Option<Decimal>,
    /// Contribution direction.
    pub direction: FactorDirection,
    /// Confidence in the raw value (0 when inputs are missing).
    pub confidence: Probability,
    /// One-line human summary carried into the final explanation.
    pub headline: String,
    /// Ranked, signed drivers carried into the final explanation.
    pub drivers: Vec<FactorDriver>,
    /// Features that fed this factor.
    pub input_feature_refs: Vec<FeatureName>,
}

/// A factor value paired with its **transient** scoring eligibility.
///
/// `contributes` / `below_confidence_floor` are scoring decisions derived from
/// the runtime `min_factor_confidence` floor and `missing_factor_policy`. They
/// are **not persisted** (the floor can hot-update and weighting is a model-layer
/// concern, 3.4): the immutable [`FactorValue`] is the persisted fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoredFactor {
    /// The persisted factor fact.
    pub value: FactorValue,
    /// Whether this factor contributes to downstream scoring (present and at or
    /// above the confidence floor).
    pub contributes: bool,
    /// Whether the factor's confidence fell below the configured floor.
    pub below_confidence_floor: bool,
}

/// Whether a market's factor bundle is eligible to proceed to model inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum FactorEligibility {
    /// The market proceeds; its factors feed the model runtime.
    Eligible,
    /// A required factor was missing / below floor under `RejectCandidate`; the
    /// market is excluded from the candidate set (it produces no factor rows).
    RejectCandidate {
        /// Why the market was rejected.
        reason: String,
    },
}

impl FactorEligibility {
    /// Whether this market proceeds downstream.
    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible)
    }
}

/// The engine's complete per-market factor outcome: every enabled factor with
/// its transient scoring eligibility, plus the market-level eligibility verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFactorOutcome {
    /// The market this outcome describes.
    pub market_id: MarketId,
    /// Decision time the factors were computed as of.
    pub as_of: DateTime<Utc>,
    /// Market-level eligibility verdict.
    pub eligibility: FactorEligibility,
    /// Every enabled factor for this market, with transient scoring flags.
    pub factors: Vec<ScoredFactor>,
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
