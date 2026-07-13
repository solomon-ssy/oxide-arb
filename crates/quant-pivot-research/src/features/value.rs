//! Feature compute-domain value types: [`FeatureName`], [`FeatureValue`],
//! [`FeatureVector`], and provenance / null-reason scaffolding.
//!
//! These are the strongly-typed, point-in-time feature carriers. Persistence
//! projects them to `quant_feature_vector` (`payload` = canonical JSON of
//! [`FeatureVector::generic`] and the optional [`FeatureVector::domain`]
//! slice); the compute path never reads the opaque payload back — this type
//! is the single source of truth.

use std::collections::BTreeMap;

use crate::naming::stable_name;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{common::MarketCategory, domain::DomainFamily, quant::DataQualityStatus},
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
    /// Source-effective time of the underlying datum.
    pub effective_at: DateTime<Utc>,
    /// Time at which the datum became visible to this system. `None` is an
    /// explicit unknown and must never be replaced with an adjacent clock.
    pub available_at: Option<DateTime<Utc>>,
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
}

impl FeatureValue {
    /// The dimensional kind of this present value.
    #[must_use]
    pub const fn kind(&self) -> FeatureValueKind {
        match self {
            Self::Decimal(_) => FeatureValueKind::Decimal,
            Self::Probability(_) => FeatureValueKind::Probability,
            Self::Bps(_) => FeatureValueKind::Bps,
            Self::Usd(_) => FeatureValueKind::Usd,
            Self::Count(_) => FeatureValueKind::Count,
            Self::Bool(_) => FeatureValueKind::Bool,
            Self::Category(_) => FeatureValueKind::Category,
        }
    }

    /// The numeric projection used by the long-format `quant_feature_event` fact.
    ///
    /// Called only for observed/substituted cells. Stateful feature facts still
    /// write missing/not-applicable cells with an absent `raw_value`; `Bool`
    /// maps to `0`/`1`, `Count` to its integer decimal, and typed numerics to
    /// their underlying decimal.
    pub fn to_fact_decimal(&self) -> QuantResult<Decimal> {
        Ok(match self {
            Self::Decimal(value) | Self::Bps(value) => *value,
            Self::Probability(value) => value.inner(),
            Self::Usd(value) => value.inner(),
            Self::Count(value) => Decimal::from(*value),
            Self::Bool(flag) => {
                if *flag {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                }
            }
            Self::Category(category) => {
                let index = u64::try_from(category.table_index()).map_err(|_| {
                    ResearchError::Determinism {
                        detail: format!("category index does not fit u64: {category}"),
                    }
                })?;
                Decimal::from(index)
            }
        })
    }
}

/// Semantic state of one feature cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureCellState {
    /// A value computed from genuine source evidence.
    Observed,
    /// A value supplied by an explicit governed substitution policy.
    Substituted,
    /// The feature applies, but no usable value was available.
    Missing,
    /// The feature does not apply to this market or model row.
    NotApplicable,
}

/// Per-cell source freshness. Unknown is distinct from a fresh age of zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum FeatureStaleness {
    /// Source age is known and non-negative.
    Known { age_ms: u64 },
    /// No source timestamp exists for this cell.
    Unknown,
}

/// A complete feature value, state, reason, provenance and freshness record.
///
/// Constructors enforce the state/value invariant: observed and substituted
/// cells carry a value; missing and not-applicable cells never do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureCell {
    pub state: FeatureCellState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<FeatureValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<NullReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<EvidenceSourceRef>,
    pub staleness: FeatureStaleness,
}

impl FeatureCell {
    #[must_use]
    pub const fn observed(
        value: FeatureValue,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Observed,
            value: Some(value),
            reason: None,
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn substituted(
        value: FeatureValue,
        reason: NullReason,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Substituted,
            value: Some(value),
            reason: Some(reason),
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn missing(
        reason: NullReason,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    ) -> Self {
        Self {
            state: FeatureCellState::Missing,
            value: None,
            reason: Some(reason),
            evidence,
            staleness,
        }
    }

    #[must_use]
    pub const fn not_applicable(reason: NullReason) -> Self {
        Self {
            state: FeatureCellState::NotApplicable,
            value: None,
            reason: Some(reason),
            evidence: None,
            staleness: FeatureStaleness::Unknown,
        }
    }

    #[must_use]
    pub const fn value(&self) -> Option<&FeatureValue> {
        self.value.as_ref()
    }
}

/// The category-mapped external-vertical slice of a [`FeatureVector`].
///
/// Present whenever the market's category maps to an enabled vertical. An
/// unresolved linkage or unavailable source is represented by explicit
/// [`FeatureCellState::Missing`] cells; an absent slice therefore means only
/// structural non-applicability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainFeatureSlice {
    /// The vertical this slice belongs to.
    pub family: DomainFamily,
    /// Domain feature-schema version that produced this slice.
    pub schema_version: SchemaVersion,
    /// Domain feature values keyed by stable name (sorted → canonical).
    pub values: BTreeMap<FeatureName, FeatureCell>,
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
    /// Decision time at which this vector was computed.
    pub decision_at: DateTime<Utc>,
    /// Generic feature-schema version that produced the generic slice.
    pub generic_schema_version: SchemaVersion,
    /// Generic + structural plane (platform-computable, always present).
    pub generic: BTreeMap<FeatureName, FeatureCell>,
    /// Category-mapped external vertical slice; `None` only when the category
    /// maps to no enabled vertical. Resolution/source gaps remain explicit
    /// missing cells (fail-closed, never a fabricated zero row).
    pub domain: Option<DomainFeatureSlice>,
    /// Aggregate data-quality classification for the vector.
    pub data_quality: DataQualityStatus,
}

impl FeatureVector {
    /// Look up a feature value across the generic slice, then the domain slice.
    ///
    /// Names are namespace-disjoint (`domain.<family>.*` vs everything else),
    /// so the two-layer lookup can never shadow.
    #[must_use]
    pub fn cell(&self, name: &FeatureName) -> Option<&FeatureCell> {
        self.generic.get(name).or_else(|| {
            self.domain
                .as_ref()
                .and_then(|slice| slice.values.get(name))
        })
    }

    /// Look up a present value. Missing and not-applicable cells return `None`.
    #[must_use]
    pub fn value(&self, name: &FeatureName) -> Option<&FeatureValue> {
        self.cell(name).and_then(FeatureCell::value)
    }

    /// Iterate `(name, value)` pairs across both slices (generic first).
    pub fn iter_cells(&self) -> impl Iterator<Item = (&FeatureName, &FeatureCell)> {
        self.generic
            .iter()
            .chain(self.domain.iter().flat_map(|slice| slice.values.iter()))
    }

    /// Iterate present values only.
    pub fn iter_values(&self) -> impl Iterator<Item = (&FeatureName, &FeatureValue)> {
        self.iter_cells()
            .filter_map(|(name, cell)| cell.value().map(|value| (name, value)))
    }

    /// Total value count across both slices.
    #[must_use]
    pub fn value_count(&self) -> usize {
        self.generic.len() + self.domain.as_ref().map_or(0, |slice| slice.values.len())
    }

    /// Worst known source age across cells. `None` means every cell has unknown
    /// freshness; it must never be interpreted as zero.
    #[must_use]
    pub fn max_known_staleness_ms(&self) -> Option<u64> {
        self.iter_cells()
            .filter_map(|(_, cell)| match cell.staleness {
                FeatureStaleness::Known { age_ms } => Some(age_ms),
                FeatureStaleness::Unknown => None,
            })
            .max()
    }

    /// Audited substituted cells, preserving stable feature order.
    pub fn substituted_cells(&self) -> impl Iterator<Item = (&FeatureName, &FeatureCell)> {
        self.iter_cells()
            .filter(|(_, cell)| cell.state == FeatureCellState::Substituted)
    }

    /// Distinct evidence references projected from cells in stable feature order.
    #[must_use]
    pub fn evidence_refs(&self) -> Vec<EvidenceSourceRef> {
        let mut refs = Vec::new();
        for evidence in self
            .iter_cells()
            .filter_map(|(_, cell)| cell.evidence.as_ref())
        {
            if !refs.contains(evidence) {
                refs.push(evidence.clone());
            }
        }
        refs
    }

    /// Missing reasons for all substituted cells in stable feature order.
    #[must_use]
    pub fn substitution_reasons(&self) -> Vec<NullReason> {
        self.substituted_cells()
            .filter_map(|(_, cell)| cell.reason)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{FeatureCell, FeatureCellState, FeatureStaleness, FeatureValue, NullReason};
    use rust_decimal_macros::dec;

    #[test]
    fn feature_cell_states_do_not_fabricate_values() {
        let observed = FeatureCell::observed(
            FeatureValue::Decimal(dec!(1.25)),
            None,
            FeatureStaleness::Unknown,
        );
        let substituted = FeatureCell::substituted(
            FeatureValue::Decimal(dec!(0.5)),
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        );
        let missing = FeatureCell::missing(
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        );
        let not_applicable = FeatureCell::not_applicable(NullReason::NotApplicable);

        assert_eq!(observed.state, FeatureCellState::Observed);
        assert!(observed.value().is_some());
        assert_eq!(substituted.state, FeatureCellState::Substituted);
        assert!(substituted.value().is_some());
        assert_eq!(missing.state, FeatureCellState::Missing);
        assert!(missing.value().is_none());
        assert_eq!(not_applicable.state, FeatureCellState::NotApplicable);
        assert!(not_applicable.value().is_none());
    }
}
