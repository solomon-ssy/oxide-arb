//! Factor compute-domain value types: [`FactorName`], [`FactorValue`],
//! [`FactorDefinitionSpec`], explanation, and the engine's per-market
//! [`MarketFactorOutcome`].
//!
//! A factor score is **not** a recommendation score — it is a normalized,
//! explainable model input. The pipeline splits responsibilities cleanly:
//! a [`FactorComputer`](crate::factors::FactorComputer) produces a per-market
//! [`RawFactor`] (no normalization, no cross-section), and the
//! [`FactorEngine`](crate::factors::FactorEngine) applies the (possibly
//! cross-sectional) normalization to yield the final [`FactorValue`].
//!
//! The normalization **method** ([`FactorNormalization`]) is the factor's
//! semantic contract (bound into `factor_schema_hash`); the distributional
//! *parameters* are resolved from runtime config — never hardcoded here.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{
        factor::{
            FactorFamily, FactorIndeterminateReason, FactorNormalization, NormalizationSource,
        },
        quant::FactorDirection,
    },
    types::{FactorDefinitionId, MarketId, Probability},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{factors::normalize::NormalizedFactor, features::FeatureName, naming::stable_name};

stable_name! {
    /// Stable, compile-time-known factor name (e.g. `"liquidity_depth"`).
    FactorName
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
/// output, normalization method, and ownership.
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
    /// Normalization **method** applied to the raw value (distributional
    /// parameters are resolved from runtime config, never inline constants).
    pub normalization: FactorNormalization,
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
    pub const fn is_required(&self) -> bool {
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

/// Human- and machine-readable explanation of a factor value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExplanation {
    /// One-line human summary.
    pub headline: String,
    /// Ranked drivers (feature → signed contribution).
    pub drivers: Vec<FactorDriver>,
}

/// The computed output of a factor for one feature vector.
///
/// The `normalization` outcome is explicit: a `Scored` value carries the
/// `[0, 1]` normalized magnitude and its provenance; a factor whose inputs were
/// missing is `MissingInput` (`confidence = 0`); a factor whose cross-section
/// was too small or degenerate is `Indeterminate` (with a recorded reason) —
/// **never a silent neutral 0.5**. The `direction` carries the sign of the
/// factor's effect on a candidate score.
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
    /// Normalization outcome (scored / missing input / indeterminate).
    pub normalization: NormalizedFactor,
    /// Contribution direction.
    pub direction: FactorDirection,
    /// Confidence in the factor value.
    pub confidence: Probability,
    /// Explanation of the value.
    pub explanation: FactorExplanation,
    /// Features that fed this factor.
    pub input_feature_refs: Vec<FeatureName>,
}

impl FactorValue {
    /// The normalized `[0, 1]` score when the factor was scored, else `None`.
    #[must_use]
    pub const fn normalized_score(&self) -> Option<Probability> {
        match &self.normalization {
            NormalizedFactor::Scored { score, .. } => Some(*score),
            NormalizedFactor::MissingInput | NormalizedFactor::Indeterminate { .. } => None,
        }
    }

    /// Whether the factor carries a usable normalized score.
    #[must_use]
    pub const fn is_scored(&self) -> bool {
        matches!(self.normalization, NormalizedFactor::Scored { .. })
    }

    /// How the score was derived, when the factor was scored.
    #[must_use]
    pub const fn normalization_source(&self) -> Option<NormalizationSource> {
        match &self.normalization {
            NormalizedFactor::Scored { source, .. } => Some(*source),
            NormalizedFactor::MissingInput | NormalizedFactor::Indeterminate { .. } => None,
        }
    }

    /// The indeterminate reason, when the factor could not be normalized.
    #[must_use]
    pub const fn indeterminate_reason(&self) -> Option<FactorIndeterminateReason> {
        match &self.normalization {
            NormalizedFactor::Indeterminate { reason } => Some(*reason),
            NormalizedFactor::Scored { .. } | NormalizedFactor::MissingInput => None,
        }
    }
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
    /// Whether this factor contributes to downstream scoring (scored and at or
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
