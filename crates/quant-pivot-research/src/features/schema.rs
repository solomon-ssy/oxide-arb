//! Governed feature schema: [`FeatureSpec`] and the versioned
//! [`FeatureSchema`] registry built from runtime config.
//!
//! The schema is the single declaration of every feature the plane can produce:
//! its dimensional kind, unit, valid range, null policy, source requirement,
//! point-in-time rule, and staleness policy. One static catalog drives both the
//! online and offline executors; config selects the enabled families and the
//! windowed expansions (bar / momentum / volatility windows, depth levels), so
//! the same definition serves live and historical builds.

use std::{collections::HashMap, ops::RangeInclusive};

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::{FeatureFamily, FeaturesConfig},
    types::SchemaVersion,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::features::{
    FeatureName,
    names::{
        book::{
            AGE_MS, BEST_ASK, BEST_BID, CROSSED, DEPTH_IMBALANCE, EMPTY, MID, SECONDARY_BEST_ASK,
            SLOPE, SPREAD_BPS, VISIBLE_LIQUIDITY_USD,
        },
        domain_crypto::{
            BASIS_VS_RESOLUTION_SOURCE, DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION,
            UNDERLYING_MOMENTUM, UNDERLYING_REALIZED_VOL,
        },
        domain_weather::{
            ENSEMBLE_BIN_PROBABILITY, ENSEMBLE_SPREAD, NOAA_RESOLUTION_BASIS_RISK,
            OBSERVED_EXTREME_HEADROOM,
        },
        market::{CATEGORY, EVENT_AGE_SECS, IS_ACTIVE, NEG_RISK, TIME_TO_RESOLUTION_SECS},
        micro::{
            ADVERSE_SELECTION_PROXY, BOOK_CHURN, QUEUE_DEPLETION, QUOTE_UPDATE_RATE,
            STALE_QUOTE_FREQUENCY, SUDDEN_LIQUIDITY_WITHDRAWAL,
        },
        structural::{
            BOOK_CHURN_INTENSITY, MAKER_GINI, NEGRISK_CONVERT_EDGE, NEGRISK_LEG_ASK_SUM,
            NEGRISK_LEG_BID_SUM, NEGRISK_LEG_COUNT, PARTICIPANT_COUNT, PARTICIPANT_COVERAGE_RATIO,
            PARTICIPANT_CR1_SHARE, PARTICIPANT_GINI, PARTICIPANT_HHI, PRICE_EXTREMITY, SHOCK_RATIO,
            SHORT_RETURN, TAKER_GINI, TRADE_TAPE_COUNT, TRADE_TAPE_NOTIONAL_USD,
        },
        ts::{MACD_NORM, PRICE_REVERSAL},
    },
    value::{EvidenceSourceKind, FeatureValueKind},
};

/// The dimensional unit a feature is expressed in (documentation + UI only;
/// arithmetic is governed by [`FeatureValueKind`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureUnit {
    /// Dimensionless.
    None,
    /// A ratio / fraction (e.g. return, imbalance).
    Ratio,
    /// Basis points.
    Bps,
    /// US dollars.
    Usd,
    /// Condition-token shares.
    Shares,
    /// A probability in `[0, 1]` (prediction-market prices live here).
    Probability,
    /// Whole seconds.
    Seconds,
    /// Whole milliseconds.
    Milliseconds,
    /// A plain count.
    Count,
    /// A per-second rate.
    PerSecond,
}

/// The evidence a feature requires to be computable.
///
/// Drives the [`crate::features::availability::FeatureAvailabilityOracle`] (which
/// translates a model's required-feature set into per-market eligibility) and the
/// null-policy engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub enum SourceRequirement {
    /// A published L2 order book for the feature's primary or secondary
    /// outcome token.
    PublishedL2Book,
    /// Gamma market / event metadata.
    GammaMetadata,
    /// A window of `book_microstructure_*` facts.
    MicrostructureWindow,
    /// Same-`as_of` order books of the market's neg-risk sibling YES legs
    /// (structural full-leg aggregates). No external data source.
    NegRiskSiblingLegs,
    /// A window of persisted full-market trade-tape participant facts.
    TradeTapeWindow,
    /// One resolved revision from the append-only market-linkage ledger.
    ResolvedLinkage,
    /// A resolved market linkage plus a PIT window of external domain
    /// observations (`quant_domain_observation`) for the linked instrument.
    DomainObservationWindow,
}

impl SourceRequirement {
    /// The evidence origin this requirement resolves to.
    ///
    /// This is the schema-authoritative mapping consumed by the long-format fact
    /// writer, so the persisted `source_kind` is derived from the governed spec
    /// rather than reverse-engineered from the feature name.
    #[must_use]
    pub const fn evidence_kind(self) -> EvidenceSourceKind {
        match self {
            Self::PublishedL2Book | Self::NegRiskSiblingLegs => EvidenceSourceKind::Book,
            Self::GammaMetadata => EvidenceSourceKind::GammaMetadata,
            Self::MicrostructureWindow => EvidenceSourceKind::ClickHouseFact,
            Self::TradeTapeWindow => EvidenceSourceKind::TradeTape,
            Self::ResolvedLinkage => EvidenceSourceKind::Linkage,
            Self::DomainObservationWindow => EvidenceSourceKind::DomainExternal,
        }
    }
}

/// The point-in-time visibility rule a feature's inputs must satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PitRule {
    /// The book version must be effective at or before its source cutoff and
    /// available no later than the decision time.
    BookVersionAtOrBeforeSourceCutoff,
    /// The fact must be effective at or before its already-derived source
    /// cutoff. The feature plane must never subtract knowledge lag again.
    FactAtOrBeforeSourceCutoff,
    /// The catalog change must be effective at or before its source cutoff and
    /// available no later than the decision time.
    MetadataVersionAtOrBeforeSourceCutoff,
    /// The linkage revision must be effective at or before its source cutoff
    /// and inserted no later than the decision time.
    LinkageVersionAtOrBeforeSourceCutoff,
}

/// The staleness bound applied to a feature's freshest input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StalenessRule {
    /// No staleness bound (the feature is intrinsically point-in-time).
    None,
    /// Bounded by `data_quality.max_book_age_ms`.
    MaxBookAge,
    /// Bounded by `data_quality.max_feature_bucket_age_secs`.
    MaxFeatureBucketAge,
    /// Bounded by `data_quality.max_trade_tape_age_secs`.
    MaxTradeTapeAge,
    /// Bounded by `data_quality.max_domain_observation_age_secs`.
    MaxDomainObservationAge,
}

/// How a feature behaves when its value is absent. The four-state policy is the
/// only sanctioned way to handle missing data — silent zero is forbidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "policy", content = "value")]
pub enum NullPolicy {
    /// Missing ⇒ the market does not enter the candidate set.
    RejectMarket,
    /// Missing ⇒ substitute the given audited neutral value.
    NeutralValue(Decimal),
    /// Missing ⇒ keep missing but degrade data quality / confidence.
    Penalize,
    /// Missing ⇒ keep missing without degrading; the value is legitimately
    /// optional for this market (e.g. a neg-risk aggregate on a binary market).
    Optional,
}

/// A fully-specified feature definition (the eight-element schema record).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSpec {
    /// Stable feature name.
    pub name: FeatureName,
    /// Revision of the executable computation semantics for this feature.
    pub compute_revision: u32,
    /// Feature family (governs config gating + builder ownership).
    pub family: FeatureFamily,
    /// Dimensional kind of the produced value.
    pub value_kind: FeatureValueKind,
    /// Human/UI unit annotation.
    pub unit: FeatureUnit,
    /// Inclusive valid range; values outside it are rejected as out-of-range.
    pub valid_range: Option<RangeInclusive<Decimal>>,
    /// Null policy applied when the value is absent.
    pub null_policy: NullPolicy,
    /// Evidence the feature requires.
    pub source_requirement: SourceRequirement,
    /// Point-in-time visibility rule.
    pub point_in_time_rule: PitRule,
    /// Staleness bound on the freshest input.
    pub staleness_policy: StalenessRule,
}

/// Fluent builder for a [`FeatureSpec`] with safe defaults.
///
/// Defaults: unit [`FeatureUnit::None`], no valid range, [`NullPolicy::Penalize`],
/// Source / PIT / staleness are required up front because they are
/// the load-bearing correctness metadata.
#[must_use]
struct FeatureSpecBuilder {
    spec: FeatureSpec,
}

impl FeatureSpecBuilder {
    const fn new(
        name: FeatureName,
        family: FeatureFamily,
        value_kind: FeatureValueKind,
        source_requirement: SourceRequirement,
        point_in_time_rule: PitRule,
        staleness_policy: StalenessRule,
    ) -> Self {
        Self {
            spec: FeatureSpec {
                name,
                compute_revision: 1,
                family,
                value_kind,
                unit: FeatureUnit::None,
                valid_range: None,
                null_policy: NullPolicy::Penalize,
                source_requirement,
                point_in_time_rule,
                staleness_policy,
            },
        }
    }

    /// Set the unit annotation.
    const fn unit(mut self, unit: FeatureUnit) -> Self {
        self.spec.unit = unit;
        self
    }

    /// Set the inclusive valid range.
    const fn range(mut self, lo: Decimal, hi: Decimal) -> Self {
        self.spec.valid_range = Some(lo..=hi);
        self
    }

    /// Set the null policy.
    const fn null_policy(mut self, policy: NullPolicy) -> Self {
        self.spec.null_policy = policy;
        self
    }

    /// Finish building.
    fn build(self) -> FeatureSpec {
        self.spec
    }
}

/// A versioned, hashable feature schema: the registry of governed specs.
///
/// `feature_schema_hash` (via [`crate::hashing::ResearchHasher::feature_schema`])
/// is order-independent over the specs and folds in [`Self::version`], so a
/// version bump or any spec change perturbs the digest.
///
/// The registry is immutable after construction and carries a `name → index`
/// map so every lookup ([`Self::by_name`] / [`Self::contains`]) is `O(1)` on the
/// per-value hot paths (long-format projection, availability oracle). The index
/// is derived from `specs` and never serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeatureSchema {
    /// Monotonic schema version (`>= 1`).
    version: SchemaVersion,
    /// Governed feature specs (registry; order-independent for hashing).
    specs: Vec<FeatureSpec>,
    /// `name → index into specs`, derived from `specs` (not serialized).
    #[serde(skip)]
    by_name: HashMap<FeatureName, usize>,
}

impl FeatureSchema {
    /// Assemble a schema from a version and its specs, building the name index.
    pub fn new(version: SchemaVersion, specs: Vec<FeatureSpec>) -> QuantResult<Self> {
        let mut by_name = HashMap::with_capacity(specs.len());
        for (idx, spec) in specs.iter().enumerate() {
            if by_name.insert(spec.name.clone(), idx).is_some() {
                return Err(ResearchError::Determinism {
                    detail: format!("duplicate feature name `{}`", spec.name),
                }
                .into());
            }
        }
        Ok(Self {
            version,
            specs,
            by_name,
        })
    }

    /// Build the active schema from frozen feature config.
    ///
    /// Enables only the configured families and expands windowed families using
    /// the configured bar / momentum / volatility windows and depth levels. The
    /// schema version is taken from `config.feature_schema_version`.
    pub fn build(config: &FeaturesConfig) -> QuantResult<Self> {
        let mut specs = Vec::new();
        for family in &config.enabled_feature_families {
            match family {
                FeatureFamily::MarketMetadata => market_metadata_specs(&mut specs),
                FeatureFamily::PriceBook => price_book_specs(config, &mut specs),
                FeatureFamily::TimeSeries => time_series_specs(config, &mut specs),
                FeatureFamily::Microstructure => microstructure_specs(&mut specs),
                FeatureFamily::Structural => structural_specs(&mut specs),
                FeatureFamily::Domain => domain_specs(&mut specs),
            }
        }
        Self::new(config.feature_schema_version, specs)
    }

    /// The schema version.
    #[must_use]
    pub const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// The governed specs (registry order).
    #[must_use]
    pub fn specs(&self) -> &[FeatureSpec] {
        &self.specs
    }

    /// Look up a spec by name (`O(1)`).
    #[must_use]
    pub fn by_name(&self, name: &FeatureName) -> Option<&FeatureSpec> {
        self.by_name.get(name).map(|&idx| &self.specs[idx])
    }

    /// All governed feature names (registry order).
    #[must_use]
    pub fn names(&self) -> Vec<FeatureName> {
        self.specs.iter().map(|spec| spec.name.clone()).collect()
    }

    /// Whether the schema declares a feature with this name (`O(1)`).
    #[must_use]
    pub fn contains(&self, name: &FeatureName) -> bool {
        self.by_name.contains_key(name)
    }

    /// Iterate specs in a given family.
    pub fn by_family(&self, family: FeatureFamily) -> impl Iterator<Item = &FeatureSpec> {
        self.specs.iter().filter(move |spec| spec.family == family)
    }

    /// Whether any governed spec needs a point-in-time resolved order book.
    ///
    /// Drives the builder's PIT gating: when no spec requires a book, the build
    /// loop skips the book resolution entirely.
    #[must_use]
    pub fn needs_book(&self) -> bool {
        self.specs
            .iter()
            .any(|spec| matches!(spec.source_requirement, SourceRequirement::PublishedL2Book))
    }

    /// Whether any governed spec needs point-in-time Gamma market metadata.
    #[must_use]
    pub fn needs_market_metadata(&self) -> bool {
        self.specs
            .iter()
            .any(|spec| matches!(spec.source_requirement, SourceRequirement::GammaMetadata))
    }

    /// Whether any governed spec needs a pre-fetched microstructure window.
    ///
    /// Drives the orchestrator's window prefetch: a book/metadata-only schema
    /// skips the `ClickHouse` window read entirely.
    #[must_use]
    pub fn needs_window(&self) -> bool {
        self.specs.iter().any(|spec| {
            matches!(
                spec.source_requirement,
                SourceRequirement::MicrostructureWindow
            )
        })
    }

    /// Whether any governed spec needs same-`as_of` neg-risk sibling-leg books.
    ///
    /// Drives the feature pipeline's sibling-leg prefetch: a schema without any
    /// structural neg-risk aggregate skips the sibling resolution entirely.
    #[must_use]
    pub fn needs_sibling_legs(&self) -> bool {
        self.specs.iter().any(|spec| {
            matches!(
                spec.source_requirement,
                SourceRequirement::NegRiskSiblingLegs
            )
        })
    }

    /// Whether any governed spec needs a pre-fetched trade-tape window.
    #[must_use]
    pub fn needs_trade_tape(&self) -> bool {
        self.specs
            .iter()
            .any(|spec| matches!(spec.source_requirement, SourceRequirement::TradeTapeWindow))
    }

    /// Whether any governed spec needs a resolved linkage + domain window.
    ///
    /// Drives the pipeline's linkage / domain-observation prefetch: a schema
    /// without the domain family never touches the linkage store.
    #[must_use]
    pub fn needs_domain(&self) -> bool {
        self.specs.iter().any(|spec| {
            matches!(
                spec.source_requirement,
                SourceRequirement::DomainObservationWindow | SourceRequirement::ResolvedLinkage
            )
        })
    }

    /// The governed specs of the domain family (the domain-slice schema).
    pub fn domain_specs(&self) -> impl Iterator<Item = &FeatureSpec> {
        self.by_family(FeatureFamily::Domain)
    }
}

// ── Static catalog ─────────────────────────────────────────────────────────

const fn spec(
    name: FeatureName,
    family: FeatureFamily,
    value_kind: FeatureValueKind,
    source: SourceRequirement,
    pit: PitRule,
    staleness: StalenessRule,
) -> FeatureSpecBuilder {
    FeatureSpecBuilder::new(name, family, value_kind, source, pit, staleness)
}

fn market_metadata_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            CATEGORY,
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Category,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .null_policy(NullPolicy::RejectMarket)
        .build(),
    );
    out.push(
        spec(
            TIME_TO_RESOLUTION_SECS,
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Count,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .unit(FeatureUnit::Seconds)
        .null_policy(NullPolicy::RejectMarket)
        .build(),
    );
    out.push(
        spec(
            EVENT_AGE_SECS,
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Count,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .unit(FeatureUnit::Seconds)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            NEG_RISK,
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Bool,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .null_policy(NullPolicy::NeutralValue(Decimal::ZERO))
        .build(),
    );
    out.push(
        spec(
            IS_ACTIVE,
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Bool,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .null_policy(NullPolicy::RejectMarket)
        .build(),
    );
}

/// Shared (family, source, pit, staleness) for every price/book spec.
const fn book_spec(name: FeatureName, kind: FeatureValueKind) -> FeatureSpecBuilder {
    spec(
        name,
        FeatureFamily::PriceBook,
        kind,
        SourceRequirement::PublishedL2Book,
        PitRule::BookVersionAtOrBeforeSourceCutoff,
        StalenessRule::MaxBookAge,
    )
}

/// A `[0, 1]` price feature whose intrinsic null policy rejects the market.
fn price_spec(name: FeatureName) -> FeatureSpec {
    book_spec(name, FeatureValueKind::Probability)
        .unit(FeatureUnit::Probability)
        .range(Decimal::ZERO, Decimal::ONE)
        .null_policy(NullPolicy::RejectMarket)
        .build()
}

fn price_book_specs(config: &FeaturesConfig, out: &mut Vec<FeatureSpec>) {
    out.push(price_spec(BEST_BID));
    out.push(price_spec(BEST_ASK));
    out.push(
        book_spec(SECONDARY_BEST_ASK, FeatureValueKind::Probability)
            .unit(FeatureUnit::Probability)
            .range(Decimal::ZERO, Decimal::ONE)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(price_spec(MID));
    out.push(
        book_spec(SPREAD_BPS, FeatureValueKind::Bps)
            .unit(FeatureUnit::Bps)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        book_spec(DEPTH_IMBALANCE, FeatureValueKind::Decimal)
            .unit(FeatureUnit::Ratio)
            .range(Decimal::NEGATIVE_ONE, Decimal::ONE)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(SLOPE, FeatureValueKind::Decimal)
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(VISIBLE_LIQUIDITY_USD, FeatureValueKind::Usd)
            .unit(FeatureUnit::Usd)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(AGE_MS, FeatureValueKind::Count)
            .unit(FeatureUnit::Milliseconds)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        book_spec(CROSSED, FeatureValueKind::Bool)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        book_spec(EMPTY, FeatureValueKind::Bool)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    for level in &config.depth_levels {
        out.push(
            book_spec(FeatureName::book_depth_top(*level), FeatureValueKind::Usd)
                .unit(FeatureUnit::Usd)
                .null_policy(NullPolicy::Penalize)
                .build(),
        );
    }
}

fn time_series_specs(config: &FeaturesConfig, out: &mut Vec<FeatureSpec>) {
    for window in &config.bar_windows_secs {
        out.push(
            spec(
                FeatureName::ts_return(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
        out.push(
            spec(
                FeatureName::ts_spread_trend(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
        out.push(
            spec(
                FeatureName::ts_depth_trend(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    momentum_specs(config, out);
    for window in &config.volatility_windows_secs {
        out.push(
            spec(
                FeatureName::ts_realized_vol(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .range(Decimal::ZERO, Decimal::from(1_000_000))
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
        out.push(
            spec(
                FeatureName::ts_vol_adjusted_return(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    out.push(
        spec(
            PRICE_REVERSAL,
            FeatureFamily::TimeSeries,
            FeatureValueKind::Decimal,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
}

/// Momentum-family time-series specs: lag-skipped ROC, EMA slope, and the
/// vol-normalized MACD (distinct estimators, never a return clone).
fn momentum_specs(config: &FeaturesConfig, out: &mut Vec<FeatureSpec>) {
    for window in &config.momentum.roc_windows_secs {
        out.push(
            spec(
                FeatureName::ts_momentum_roc(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    for window in &config.momentum.slope_windows_secs {
        out.push(
            spec(
                FeatureName::ts_ema_slope(*window),
                FeatureFamily::TimeSeries,
                FeatureValueKind::Decimal,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    out.push(
        spec(
            MACD_NORM,
            FeatureFamily::TimeSeries,
            FeatureValueKind::Decimal,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
}

fn microstructure_specs(out: &mut Vec<FeatureSpec>) {
    for (name, kind, unit) in [
        (
            QUOTE_UPDATE_RATE,
            FeatureValueKind::Decimal,
            FeatureUnit::PerSecond,
        ),
        (BOOK_CHURN, FeatureValueKind::Decimal, FeatureUnit::Ratio),
        (
            QUEUE_DEPLETION,
            FeatureValueKind::Decimal,
            FeatureUnit::Ratio,
        ),
        (
            SUDDEN_LIQUIDITY_WITHDRAWAL,
            FeatureValueKind::Decimal,
            FeatureUnit::Ratio,
        ),
        (
            ADVERSE_SELECTION_PROXY,
            FeatureValueKind::Decimal,
            FeatureUnit::Ratio,
        ),
    ] {
        out.push(
            spec(
                name,
                FeatureFamily::Microstructure,
                kind,
                SourceRequirement::MicrostructureWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxFeatureBucketAge,
            )
            .unit(unit)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    out.push(
        spec(
            STALE_QUOTE_FREQUENCY,
            FeatureFamily::Microstructure,
            FeatureValueKind::Probability,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Probability)
        .range(Decimal::ZERO, Decimal::ONE)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
}

/// Structural (prediction-market-aware) feature specs.
///
/// Platform-computable from existing facts (book, market metadata, microstructure
/// window, and same-`as_of` neg-risk sibling-leg books) — no external source.
/// Neg-risk aggregates are `Optional`: on a binary market they resolve to
/// `NullReason::NotApplicable`, and on a missing leg to `NullReason::LegBookMissing`
/// — never a fabricated zero.
fn structural_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            SHORT_RETURN,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            SHOCK_RATIO,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .range(Decimal::ZERO, Decimal::from(1_000_000))
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            PRICE_EXTREMITY,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::PublishedL2Book,
            PitRule::BookVersionAtOrBeforeSourceCutoff,
            StalenessRule::MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        // Signed `mid − 0.5` ∈ [−0.5, 0.5].
        .range(Decimal::new(-5, 1), Decimal::new(5, 1))
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            BOOK_CHURN_INTENSITY,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::MicrostructureWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    trade_tape_structural_specs(out);
    structural_neg_risk_specs(out);
}

fn trade_tape_structural_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            TRADE_TAPE_COUNT,
            FeatureFamily::Structural,
            FeatureValueKind::Count,
            SourceRequirement::TradeTapeWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            PARTICIPANT_COUNT,
            FeatureFamily::Structural,
            FeatureValueKind::Count,
            SourceRequirement::TradeTapeWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    out.push(
        spec(
            TRADE_TAPE_NOTIONAL_USD,
            FeatureFamily::Structural,
            FeatureValueKind::Usd,
            SourceRequirement::TradeTapeWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Usd)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
    for name in [
        PARTICIPANT_COVERAGE_RATIO,
        PARTICIPANT_GINI,
        PARTICIPANT_HHI,
        PARTICIPANT_CR1_SHARE,
        MAKER_GINI,
        TAKER_GINI,
    ] {
        out.push(
            spec(
                name,
                FeatureFamily::Structural,
                FeatureValueKind::Decimal,
                SourceRequirement::TradeTapeWindow,
                PitRule::FactAtOrBeforeSourceCutoff,
                StalenessRule::MaxTradeTapeAge,
            )
            .unit(FeatureUnit::Ratio)
            .range(Decimal::ZERO, Decimal::ONE)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
}

fn structural_neg_risk_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            NEGRISK_LEG_ASK_SUM,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::NegRiskSiblingLegs,
            PitRule::BookVersionAtOrBeforeSourceCutoff,
            StalenessRule::MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            NEGRISK_LEG_BID_SUM,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::NegRiskSiblingLegs,
            PitRule::BookVersionAtOrBeforeSourceCutoff,
            StalenessRule::MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            NEGRISK_LEG_COUNT,
            FeatureFamily::Structural,
            FeatureValueKind::Count,
            SourceRequirement::NegRiskSiblingLegs,
            PitRule::BookVersionAtOrBeforeSourceCutoff,
            StalenessRule::MaxBookAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            NEGRISK_CONVERT_EDGE,
            FeatureFamily::Structural,
            FeatureValueKind::Decimal,
            SourceRequirement::NegRiskSiblingLegs,
            PitRule::BookVersionAtOrBeforeSourceCutoff,
            StalenessRule::MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
}

/// Crypto external-vertical (domain-slice) feature specs.
///
/// Every spec requires a resolved PIT linkage. Observation-derived specs also
/// require a PIT window of external domain facts; subject-derived specs cite
/// the linkage revision itself. All are `NullPolicy::Optional`: a market whose category
/// maps to no vertical simply carries no domain slice, and a linked market with
/// a source gap keeps an explicit present-but-missing value — never a
/// fabricated zero, never market rejection.
fn domain_specs(out: &mut Vec<FeatureSpec>) {
    crypto_domain_specs(out);
    weather_domain_specs(out);
}

fn crypto_domain_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            DISTANCE_TO_STRIKE,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            UNDERLYING_MOMENTUM,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            UNDERLYING_REALIZED_VOL,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .range(Decimal::ZERO, Decimal::from(1_000_000))
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        // Intrinsically point-in-time: derived from the frozen subject's
        // observation instant, not from any observation feed.
        spec(
            TIME_TO_OBSERVATION,
            FeatureFamily::Domain,
            FeatureValueKind::Count,
            SourceRequirement::ResolvedLinkage,
            PitRule::LinkageVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .unit(FeatureUnit::Seconds)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            BASIS_VS_RESOLUTION_SOURCE,
            FeatureFamily::Domain,
            FeatureValueKind::Bps,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Bps)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
}

fn weather_domain_specs(out: &mut Vec<FeatureSpec>) {
    out.push(
        spec(
            ENSEMBLE_BIN_PROBABILITY,
            FeatureFamily::Domain,
            FeatureValueKind::Probability,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Probability)
        .range(Decimal::ZERO, Decimal::ONE)
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            ENSEMBLE_SPREAD,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .range(Decimal::ZERO, Decimal::from(100))
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            OBSERVED_EXTREME_HEADROOM,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .null_policy(NullPolicy::Optional)
        .build(),
    );
    out.push(
        spec(
            NOAA_RESOLUTION_BASIS_RISK,
            FeatureFamily::Domain,
            FeatureValueKind::Decimal,
            SourceRequirement::DomainObservationWindow,
            PitRule::FactAtOrBeforeSourceCutoff,
            StalenessRule::MaxDomainObservationAge,
        )
        .range(Decimal::ZERO, Decimal::from(100))
        .null_policy(NullPolicy::Optional)
        .build(),
    );
}

#[cfg(test)]
mod contract_tests {
    use quant_pivot_models::{
        runtime_config::{FeatureFamily, FeaturesConfig},
        types::SchemaVersion,
    };

    use super::{
        FeatureSchema, FeatureSpecBuilder, FeatureUnit, NullPolicy, PitRule, SourceRequirement,
        StalenessRule,
    };
    use crate::features::{
        FeatureName, FeatureSpec, FeatureValueKind, names::book::SECONDARY_BEST_ASK,
    };

    fn sample_spec() -> FeatureSpec {
        FeatureSpecBuilder::new(
            FeatureName::from_static("test.duplicate"),
            FeatureFamily::MarketMetadata,
            FeatureValueKind::Decimal,
            SourceRequirement::GammaMetadata,
            PitRule::MetadataVersionAtOrBeforeSourceCutoff,
            StalenessRule::None,
        )
        .build()
    }

    #[test]
    fn duplicate_feature_names_fail_construction() {
        let error = FeatureSchema::new(SchemaVersion::new(6), vec![sample_spec(), sample_spec()])
            .expect_err("duplicate name must fail");
        assert!(error.to_string().contains("duplicate feature name"));
    }

    #[test]
    fn compute_revision_participates_in_equality() {
        let first = sample_spec();
        let mut second = first.clone();
        second.compute_revision += 1;
        assert_ne!(first, second);
    }

    #[test]
    fn secondary_executable_ask_contract_is_explicit_and_non_substituting() {
        let schema = FeatureSchema::build(&FeaturesConfig::default()).expect("schema");
        let spec = schema
            .by_name(&SECONDARY_BEST_ASK)
            .expect("secondary ask feature");

        assert_eq!(spec.compute_revision, 1);
        assert_eq!(spec.value_kind, FeatureValueKind::Probability);
        assert_eq!(spec.unit, FeatureUnit::Probability);
        assert_eq!(spec.null_policy, NullPolicy::Penalize);
        assert_eq!(spec.source_requirement, SourceRequirement::PublishedL2Book);
        assert_eq!(
            spec.point_in_time_rule,
            PitRule::BookVersionAtOrBeforeSourceCutoff
        );
        assert_eq!(spec.staleness_policy, StalenessRule::MaxBookAge);
    }
}
