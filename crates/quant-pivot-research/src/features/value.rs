//! Feature compute-domain value types: [`FeatureName`], [`FeatureValue`],
//! [`FeatureVector`], and provenance / null-reason scaffolding.
//!
//! These are the strongly-typed, point-in-time feature carriers. Persistence
//! projects them to `quant_feature_vector` (`payload` = canonical JSON of
//! [`FeatureVector::values`]); the compute path never reads the opaque payload
//! back — this type is the single source of truth.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::quant::DataQualityStatus,
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::naming::stable_name;

stable_name! {
    /// Stable, compile-time-known feature name (e.g. `"spread_bps"`).
    FeatureName
}

/// Why a feature value is absent. Missing values are **never** silently zero —
/// they carry one of these reasons and flow through the null policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullReason {
    /// The required source was unavailable at `as_of`.
    SourceUnavailable,
    /// The freshest available datum is staler than policy allows.
    StaleBeyondPolicy,
    /// A computed value fell outside the feature's valid range.
    OutOfValidRange,
    /// A domain (vertical) data source had no value for this market.
    DomainDataMissing,
    /// Insufficient history to compute a windowed feature.
    InsufficientHistory,
}

/// The data source a feature value was derived from, for audit / replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSourceKind {
    /// Live or historical CLOB order book.
    Book,
    /// Gamma market metadata.
    GammaMetadata,
    /// A persisted `ClickHouse` fact.
    ClickHouseFact,
    /// Derived/computed from other in-memory inputs.
    Derived,
}

/// A provenance reference tying a feature value back to its evidence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EvidenceSourceRef {
    /// The kind of source.
    pub source_kind: EvidenceSourceKind,
    /// A source-local reference (fact id, query fingerprint, snapshot key).
    pub reference: String,
    /// When the underlying datum was observed.
    pub observed_at: DateTime<Utc>,
}

/// A strongly-typed feature value. No silent `f64`, no untyped JSON on the
/// compute path; missing values are explicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FeatureValue {
    /// A dimensionless decimal.
    Decimal(Decimal),
    /// A probability / confidence in `[0, 1]`.
    Probability(Probability),
    /// A basis-point quantity.
    Bps(Decimal),
    /// A USD-denominated amount.
    Usd(Usd),
    /// A non-negative count.
    Count(u64),
    /// A boolean flag.
    Bool(bool),
    /// An explicitly-missing value with its reason.
    Missing(NullReason),
}

impl FeatureValue {
    /// Whether this value is [`FeatureValue::Missing`].
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }
}

/// An in-memory, point-in-time feature vector for one market.
///
/// Keyed by stable feature name and canonical-hashable: `values` is a
/// `BTreeMap`, so the digest is independent of insertion order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureVector {
    /// The market this vector describes.
    pub market_id: MarketId,
    /// The specific outcome token, when the vector is token-scoped.
    pub token_id: Option<TokenId>,
    /// Decision time the vector was computed as of.
    pub as_of: DateTime<Utc>,
    /// Schema version that produced this vector.
    pub schema_version: SchemaVersion,
    /// Feature values keyed by stable name (sorted → canonical).
    pub values: BTreeMap<FeatureName, FeatureValue>,
    /// Aggregate data-quality classification for the vector.
    pub data_quality: DataQualityStatus,
    /// Worst-case staleness of the inputs, in milliseconds.
    pub staleness_ms: u64,
    /// Provenance of the values, for audit and replay.
    pub source_refs: Vec<EvidenceSourceRef>,
}
