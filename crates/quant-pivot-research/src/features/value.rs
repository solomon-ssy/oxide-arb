//! Feature compute-domain value types: [`FeatureName`], [`FeatureValue`],
//! [`FeatureVector`], and provenance / null-reason scaffolding.
//!
//! These are the strongly-typed, point-in-time feature carriers. Persistence
//! projects them to `quant_feature_vector` (`payload` = canonical JSON of
//! [`FeatureVector::generic`] and the optional [`FeatureVector::domain`]
//! slice); the compute path never reads the opaque payload back — this type
//! is the single source of truth.

use std::collections::{BTreeMap, HashSet};

use crate::naming::stable_name;
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{common::MarketCategory, domain::DomainFamily, quant::DataQualityStatus},
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

    /// Lag-skipped rate-of-change momentum: `ts.momentum_roc_{window_secs}s`.
    #[must_use]
    pub fn ts_momentum_roc(window_secs: u64) -> Self {
        Self::new(format!("ts.momentum_roc_{window_secs}s"))
    }

    /// EMA-slope momentum over a configured window: `ts.ema_slope_{window_secs}s`.
    #[must_use]
    pub fn ts_ema_slope(window_secs: u64) -> Self {
        Self::new(format!("ts.ema_slope_{window_secs}s"))
    }

    /// Volatility-adjusted return: `ts.vol_adjusted_return_{window_secs}s`.
    #[must_use]
    pub fn ts_vol_adjusted_return(window_secs: u64) -> Self {
        Self::new(format!("ts.vol_adjusted_return_{window_secs}s"))
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
    /// Insufficient history to compute a windowed feature.
    InsufficientHistory,
    /// The feature does not apply to this market's structure (e.g. a neg-risk
    /// full-leg aggregate on a binary market). Structurally absent — never a
    /// data gap, never a fabricated zero.
    NotApplicable,
    /// A neg-risk sibling leg's order book was absent at `as_of`, so a full-leg
    /// structural feature could not be computed (fail-closed, never zero).
    LegBookMissing,
    /// No trade-tape window was available at `as_of`.
    TradeTapeUnavailable,
    /// The trade-tape window exists but does not meet the configured sample or
    /// notional floor.
    InsufficientTradeTape,
    /// The trade-tape source does not provide enough maker/taker role coverage
    /// for role-specific features.
    InsufficientRoleCoverage,
    /// No PIT domain observation was available for the linked instrument at
    /// `as_of` (external source gap — never a fabricated zero).
    DomainSourceUnavailable,
    /// The market has no `Resolved` linkage at `as_of`, so domain features
    /// cannot bind to an external instrument (fail-closed).
    LinkageUnresolved,
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

/// The category-mapped external-vertical slice of a [`FeatureVector`].
///
/// Present **only** when the market's category maps to a vertical with a
/// resolved linkage; individual values inside the slice may still be
/// [`FeatureValue::Missing`] (present-but-missing is a data gap; an absent
/// slice is a structural non-applicability — the two are never conflated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainFeatureSlice {
    /// The vertical this slice belongs to.
    pub family: DomainFamily,
    /// Domain feature-schema version that produced this slice.
    pub schema_version: SchemaVersion,
    /// Domain feature values keyed by stable name (sorted → canonical).
    pub values: BTreeMap<FeatureName, FeatureValue>,
}

/// An in-memory, point-in-time feature vector for one market: a fixed-width
/// **generic** slice (platform-computable, always present) plus an optional,
/// category-scoped **domain** slice.
///
/// There is no missing-value pollution across the layer boundary: a market
/// whose category maps to no vertical (or whose vertical is unresolved /
/// unavailable) carries `domain = None`, which is *structurally distinct* from
/// a present-but-missing domain feature. Both maps are `BTreeMap`s, so the
/// canonical digest is independent of insertion order, and the digest
/// distinguishes the domain family and both schema versions by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureVector {
    /// The market this vector describes.
    pub market_id: MarketId,
    /// The specific outcome token, when the vector is token-scoped.
    pub token_id: Option<TokenId>,
    /// Decision time the vector was computed as of.
    pub as_of: DateTime<Utc>,
    /// Generic feature-schema version that produced the generic slice.
    pub generic_schema_version: SchemaVersion,
    /// Generic + structural plane (platform-computable, always present).
    pub generic: BTreeMap<FeatureName, FeatureValue>,
    /// Category-mapped external vertical slice; `None` when the category maps
    /// to no vertical or the vertical is unresolved/unavailable (fail-closed,
    /// never a fabricated zero row).
    pub domain: Option<DomainFeatureSlice>,
    /// Audited neutral-value substitutions applied during the build.
    pub substitutions: Vec<SubstitutionAudit>,
    /// Aggregate data-quality classification for the vector.
    pub data_quality: DataQualityStatus,
    /// Worst-case staleness of the inputs, in milliseconds.
    pub staleness_ms: u64,
    /// Provenance of the values, for audit and replay.
    pub source_refs: Vec<EvidenceSourceRef>,
}

impl FeatureVector {
    /// Look up a feature value across the generic slice, then the domain slice.
    ///
    /// Names are namespace-disjoint (`domain.<family>.*` vs everything else),
    /// so the two-layer lookup can never shadow.
    #[must_use]
    pub fn value(&self, name: &FeatureName) -> Option<&FeatureValue> {
        self.generic.get(name).or_else(|| {
            self.domain
                .as_ref()
                .and_then(|slice| slice.values.get(name))
        })
    }

    /// Iterate `(name, value)` pairs across both slices (generic first).
    pub fn iter_values(&self) -> impl Iterator<Item = (&FeatureName, &FeatureValue)> {
        self.generic
            .iter()
            .chain(self.domain.iter().flat_map(|slice| slice.values.iter()))
    }

    /// Total value count across both slices.
    #[must_use]
    pub fn value_count(&self) -> usize {
        self.generic.len() + self.domain.as_ref().map_or(0, |slice| slice.values.len())
    }
}
