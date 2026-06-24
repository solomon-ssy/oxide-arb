//! Feature compute-domain value types: [`FeatureName`], [`FeatureValue`],
//! [`FeatureVector`], and provenance / null-reason scaffolding.
//!
//! These are the strongly-typed, point-in-time feature carriers. Persistence
//! projects them to `quant_feature_vector` (`payload` = canonical JSON of
//! [`FeatureVector::values`]); the compute path never reads the opaque payload
//! back — this type is the single source of truth.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::DataQualityStatus},
    runtime_config::{FeatureNameRef, FeaturesConfig},
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::naming::stable_name;

stable_name! {
    /// Stable, compile-time-known feature name (e.g. `"spread_bps"`).
    FeatureName
}

impl FeatureName {
    /// Bar-window return feature: `ts.return_{window_secs}s`.
    #[must_use]
    pub fn ts_return(window_secs: u64) -> Self {
        Self::new(format!("ts.return_{window_secs}s"))
    }

    /// Bar-window spread-trend feature: `ts.spread_trend_{window_secs}s`.
    #[must_use]
    pub fn ts_spread_trend(window_secs: u64) -> Self {
        Self::new(format!("ts.spread_trend_{window_secs}s"))
    }

    /// Bar-window depth-trend feature: `ts.depth_trend_{window_secs}s`.
    #[must_use]
    pub fn ts_depth_trend(window_secs: u64) -> Self {
        Self::new(format!("ts.depth_trend_{window_secs}s"))
    }

    /// Momentum feature over a configured window: `ts.momentum_{window_secs}s`.
    #[must_use]
    pub fn ts_momentum(window_secs: u64) -> Self {
        Self::new(format!("ts.momentum_{window_secs}s"))
    }

    /// Realized-volatility feature: `ts.realized_vol_{window_secs}s`.
    #[must_use]
    pub fn ts_realized_vol(window_secs: u64) -> Self {
        Self::new(format!("ts.realized_vol_{window_secs}s"))
    }

    /// Top-of-book depth at a configured level: `book.depth_top{level}_usd`.
    #[must_use]
    pub fn book_depth_top(level: u32) -> Self {
        Self::new(format!("book.depth_top{level}_usd"))
    }
}

impl From<&FeatureNameRef> for FeatureName {
    fn from(reference: &FeatureNameRef) -> Self {
        Self::new(&reference.name)
    }
}

/// Model-required features merged with config [`FeaturesConfig::required_features`].
///
/// Single merge point for the builder null-policy gate and the feature-plane
/// rejection partition — both must use the same set.
#[must_use]
pub fn merged_required_features(
    model_required: &[FeatureName],
    config: &FeaturesConfig,
) -> HashSet<FeatureName> {
    let mut required: HashSet<FeatureName> = model_required.iter().cloned().collect();
    for reference in &config.required_features {
        required.insert(FeatureName::from(reference));
    }
    required
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
///
/// This is the single taxonomy of "where a feature value came from". It is the
/// strongly-typed origin both feature builders attach to evidence and the
/// `quant_feature_event` fact records — never a name-prefix guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSourceKind {
    /// Live or historical CLOB order book.
    Book,
    /// Gamma market metadata.
    GammaMetadata,
    /// A persisted `ClickHouse` fact (microstructure / tick window).
    ClickHouseFact,
    /// An external vertical (domain) data source.
    DomainExternal,
    /// Derived/computed from other in-memory inputs.
    Derived,
}

impl EvidenceSourceKind {
    /// The stable wire label persisted to the `quant_feature_event.source_kind`
    /// column.
    ///
    /// This is an **append-only contract**, deliberately decoupled from the Rust
    /// identifier and serde representation so renaming a variant can never
    /// silently rewrite persisted analytics. Never change an existing label.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::GammaMetadata => "gamma_metadata",
            Self::ClickHouseFact => "clickhouse_fact",
            Self::DomainExternal => "domain_external",
            Self::Derived => "derived",
        }
    }
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

/// The dimensional kind of a present [`FeatureValue`].
///
/// Carries a stable `i8` code that is persisted to the `quant_feature_event`
/// `ClickHouse` fact (`value_kind` column). The codes are an append-only
/// contract: never renumber an existing variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureValueKind {
    /// A dimensionless decimal.
    Decimal,
    /// A probability / confidence in `[0, 1]`.
    Probability,
    /// A basis-point quantity.
    Bps,
    /// A USD-denominated amount.
    Usd,
    /// A non-negative count.
    Count,
    /// A boolean flag.
    Bool,
    /// A categorical market class (faithful enum; encoding is a downstream
    /// normalization concern — never consumed as an ordinal number).
    Category,
}

impl FeatureValueKind {
    /// The stable `ClickHouse` `value_kind` code (append-only contract).
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Decimal => 0,
            Self::Probability => 1,
            Self::Bps => 2,
            Self::Usd => 3,
            Self::Count => 4,
            Self::Bool => 5,
            Self::Category => 6,
        }
    }

    /// Decode a persisted `value_kind` code, rejecting unknown values.
    #[must_use]
    pub const fn from_i8(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Decimal),
            1 => Some(Self::Probability),
            2 => Some(Self::Bps),
            3 => Some(Self::Usd),
            4 => Some(Self::Count),
            5 => Some(Self::Bool),
            6 => Some(Self::Category),
            _ => None,
        }
    }
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
    /// A categorical market class. Stored faithfully as the enum; the
    /// long-format fact projects its stable `table_index` code. Downstream
    /// normalization (one-hot / target encoding) owns any numeric encoding —
    /// the raw code must never be fed to a model as an ordinal feature.
    Category(MarketCategory),
    /// An explicitly-missing value with its reason.
    Missing(NullReason),
}

impl FeatureValue {
    /// Whether this value is [`FeatureValue::Missing`].
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }

    /// The dimensional kind of a present value, or `None` when missing.
    #[must_use]
    pub const fn kind(&self) -> Option<FeatureValueKind> {
        match self {
            Self::Decimal(_) => Some(FeatureValueKind::Decimal),
            Self::Probability(_) => Some(FeatureValueKind::Probability),
            Self::Bps(_) => Some(FeatureValueKind::Bps),
            Self::Usd(_) => Some(FeatureValueKind::Usd),
            Self::Count(_) => Some(FeatureValueKind::Count),
            Self::Bool(_) => Some(FeatureValueKind::Bool),
            Self::Category(_) => Some(FeatureValueKind::Category),
            Self::Missing(_) => None,
        }
    }

    /// The missing reason, when this value is [`FeatureValue::Missing`].
    #[must_use]
    pub const fn null_reason(&self) -> Option<NullReason> {
        match self {
            Self::Missing(reason) => Some(*reason),
            _ => None,
        }
    }

    /// The numeric projection used by the long-format `quant_feature_event` fact.
    ///
    /// Missing values return `None` (they are never written as a fact row);
    /// `Bool` maps to `0`/`1`, `Count` to its integer decimal, and the typed
    /// numerics to their underlying decimal.
    #[must_use]
    pub fn to_fact_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Decimal(value) | Self::Bps(value) => Some(*value),
            Self::Probability(value) => Some(value.inner()),
            Self::Usd(value) => Some(value.inner()),
            Self::Count(value) => Some(Decimal::from(*value)),
            Self::Bool(flag) => Some(if *flag { Decimal::ONE } else { Decimal::ZERO }),
            Self::Category(category) => Some(Decimal::from(
                u64::try_from(category.table_index()).unwrap_or_default(),
            )),
            Self::Missing(_) => None,
        }
    }
}

/// An audited neutral-value substitution applied by the null-policy engine.
///
/// Every non-trivial substitution is recorded so persistence and audit can
/// reconstruct exactly which values were imputed and why — silent imputation is
/// forbidden. A governed confidence penalty for imputed values is a model-layer
/// concern and is introduced in 3.4 (see `03.4` doc), not carried here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstitutionAudit {
    /// The substituted feature.
    pub feature: FeatureName,
    /// Why the original value was absent.
    pub reason: NullReason,
    /// The neutral value that was substituted.
    pub substituted: FeatureValue,
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
    /// Audited neutral-value substitutions applied during the build.
    pub substitutions: Vec<SubstitutionAudit>,
    /// Aggregate data-quality classification for the vector.
    pub data_quality: DataQualityStatus,
    /// Worst-case staleness of the inputs, in milliseconds.
    pub staleness_ms: u64,
    /// Provenance of the values, for audit and replay.
    pub source_refs: Vec<EvidenceSourceRef>,
}
