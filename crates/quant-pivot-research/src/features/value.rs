//! Feature compute-domain value types: [`FeatureName`], [`FeatureValue`],
//! [`FeatureVector`], and provenance / null-reason scaffolding.
//!
//! These are the strongly-typed, point-in-time feature carriers. Persistence
//! projects them to `quant_feature_vector` (`payload` = canonical JSON of
//! [`FeatureVector::values`]); the compute path never reads the opaque payload
//! back — this type is the single source of truth.

use std::collections::{BTreeMap, HashSet};

use crate::naming::stable_name;
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::DataQualityStatus},
    runtime_config::{FeatureNameRef, FeaturesConfig},
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use quant_pivot_models::enums::feature::{EvidenceSourceKind, FeatureValueKind};

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
