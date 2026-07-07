//! Governed feature schema: the eight-element [`FeatureSpec`] and the versioned
//! [`FeatureSchema`] registry built from runtime config.
//!
//! The schema is the single declaration of every feature the plane can produce:
//! its dimensional kind, unit, valid range, null policy, source requirement,
//! point-in-time rule, and staleness policy. One static catalog drives both the
//! online and offline executors; config selects the enabled families and the
//! windowed expansions (bar / momentum / volatility windows, depth levels), so
//! the same definition serves live and historical builds.

use std::{collections::HashMap, ops::RangeInclusive};

use linkme::distributed_slice;
use quant_pivot_error::config_validation::{ConfigValidationError, ConfigValidationReport};
use quant_pivot_models::{
    runtime_config::{
        FeatureFamily, FeaturesConfig,
        validation::{FEATURES_CONFIG_VALIDATORS, FeaturesConfigValidator},
    },
    types::SchemaVersion,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::features::{
    FeatureName,
    names::{
        book, domain_crypto as domain_crypto_names, market as market_names, micro,
        structural as structural_names, ts,
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
    /// A published L2 order book for the market's token.
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
            Self::DomainObservationWindow => EvidenceSourceKind::DomainExternal,
        }
    }
}

/// The point-in-time visibility rule a feature's inputs must satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PitRule {
    /// The book version must be published at or before `as_of`.
    BookVersionAtOrBeforeAsOf,
    /// The fact must be published at or before `as_of - source_delay`.
    FactBeforeAsOfMinusDelay,
    /// Gamma metadata visible as of `as_of`.
    MetadataAtAsOf,
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
    /// When true, a missing value rejects the market regardless of null policy.
    pub critical: bool,
}

/// Fluent builder for a [`FeatureSpec`] with safe defaults.
///
/// Defaults: unit [`FeatureUnit::None`], no valid range, [`NullPolicy::Penalize`],
/// non-critical. Source / PIT / staleness are required up front because they are
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
                family,
                value_kind,
                unit: FeatureUnit::None,
                valid_range: None,
                null_policy: NullPolicy::Penalize,
                source_requirement,
                point_in_time_rule,
                staleness_policy,
                critical: false,
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

    /// Mark the feature critical (missing ⇒ reject market).
    const fn critical(mut self) -> Self {
        self.spec.critical = true;
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
    #[must_use]
    pub fn new(version: SchemaVersion, specs: Vec<FeatureSpec>) -> Self {
        let by_name = specs
            .iter()
            .enumerate()
            .map(|(idx, spec)| (spec.name.clone(), idx))
            .collect();
        Self {
            version,
            specs,
            by_name,
        }
    }

    /// Build the active schema from frozen feature config.
    ///
    /// Enables only the configured families and expands windowed families using
    /// the configured bar / momentum / volatility windows and depth levels. The
    /// schema version is taken from `config.feature_schema_version`.
    #[must_use]
    pub fn build(config: &FeaturesConfig) -> Self {
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
                SourceRequirement::DomainObservationWindow
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
    use FeatureFamily::MarketMetadata as F;
    use FeatureValueKind::{Bool, Category, Count};
    use PitRule::MetadataAtAsOf as Pit;
    use SourceRequirement::GammaMetadata as Src;
    use StalenessRule::None as Fresh;

    out.push(
        spec(market_names::CATEGORY, F, Category, Src, Pit, Fresh)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        spec(
            market_names::TIME_TO_RESOLUTION_SECS,
            F,
            Count,
            Src,
            Pit,
            Fresh,
        )
        .unit(FeatureUnit::Seconds)
        .null_policy(NullPolicy::RejectMarket)
        .build(),
    );
    out.push(
        spec(market_names::EVENT_AGE_SECS, F, Count, Src, Pit, Fresh)
            .unit(FeatureUnit::Seconds)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        spec(market_names::OUTCOME_COUNT, F, Count, Src, Pit, Fresh)
            .unit(FeatureUnit::Count)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        spec(market_names::NEG_RISK, F, Bool, Src, Pit, Fresh)
            .null_policy(NullPolicy::NeutralValue(Decimal::ZERO))
            .build(),
    );
    out.push(
        spec(market_names::IS_ACTIVE, F, Bool, Src, Pit, Fresh)
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
        PitRule::BookVersionAtOrBeforeAsOf,
        StalenessRule::MaxBookAge,
    )
}

/// A critical `[0, 1]` price feature.
fn price_spec(name: FeatureName) -> FeatureSpec {
    book_spec(name, FeatureValueKind::Probability)
        .unit(FeatureUnit::Probability)
        .range(Decimal::ZERO, Decimal::ONE)
        .null_policy(NullPolicy::RejectMarket)
        .critical()
        .build()
}

fn price_book_specs(config: &FeaturesConfig, out: &mut Vec<FeatureSpec>) {
    out.push(price_spec(book::BEST_BID));
    out.push(price_spec(book::BEST_ASK));
    out.push(price_spec(book::MID));
    out.push(
        book_spec(book::SPREAD_BPS, FeatureValueKind::Bps)
            .unit(FeatureUnit::Bps)
            .null_policy(NullPolicy::RejectMarket)
            .critical()
            .build(),
    );
    out.push(
        book_spec(book::DEPTH_IMBALANCE, FeatureValueKind::Decimal)
            .unit(FeatureUnit::Ratio)
            .range(Decimal::NEGATIVE_ONE, Decimal::ONE)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(book::SLOPE, FeatureValueKind::Decimal)
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(book::VISIBLE_LIQUIDITY_USD, FeatureValueKind::Usd)
            .unit(FeatureUnit::Usd)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
    out.push(
        book_spec(book::AGE_MS, FeatureValueKind::Count)
            .unit(FeatureUnit::Milliseconds)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        book_spec(book::CROSSED, FeatureValueKind::Bool)
            .null_policy(NullPolicy::RejectMarket)
            .build(),
    );
    out.push(
        book_spec(book::EMPTY, FeatureValueKind::Bool)
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
    use FeatureFamily::TimeSeries as F;
    use FeatureValueKind::Decimal as Dec;
    use PitRule::FactBeforeAsOfMinusDelay as Pit;
    use SourceRequirement::MicrostructureWindow as Src;
    use StalenessRule::MaxFeatureBucketAge as Stale;

    for window in &config.bar_windows_secs {
        out.push(
            spec(FeatureName::ts_return(*window), F, Dec, Src, Pit, Stale)
                .unit(FeatureUnit::Ratio)
                .null_policy(NullPolicy::Penalize)
                .build(),
        );
        out.push(
            spec(
                FeatureName::ts_spread_trend(*window),
                F,
                Dec,
                Src,
                Pit,
                Stale,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
        out.push(
            spec(
                FeatureName::ts_depth_trend(*window),
                F,
                Dec,
                Src,
                Pit,
                Stale,
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
                F,
                Dec,
                Src,
                Pit,
                Stale,
            )
            .unit(FeatureUnit::Ratio)
            .range(Decimal::ZERO, Decimal::from(1_000_000))
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
        out.push(
            spec(
                FeatureName::ts_vol_adjusted_return(*window),
                F,
                Dec,
                Src,
                Pit,
                Stale,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    out.push(
        spec(ts::PRICE_REVERSAL, F, Dec, Src, Pit, Stale)
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
}

/// Momentum-family time-series specs: lag-skipped ROC, EMA slope, and the
/// vol-normalized MACD (distinct estimators, never a return clone).
fn momentum_specs(config: &FeaturesConfig, out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::TimeSeries as F;
    use FeatureValueKind::Decimal as Dec;
    use PitRule::FactBeforeAsOfMinusDelay as Pit;
    use SourceRequirement::MicrostructureWindow as Src;
    use StalenessRule::MaxFeatureBucketAge as Stale;

    for window in &config.momentum.roc_windows_secs {
        out.push(
            spec(
                FeatureName::ts_momentum_roc(*window),
                F,
                Dec,
                Src,
                Pit,
                Stale,
            )
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
        );
    }
    for window in &config.momentum.slope_windows_secs {
        out.push(
            spec(FeatureName::ts_ema_slope(*window), F, Dec, Src, Pit, Stale)
                .unit(FeatureUnit::Ratio)
                .null_policy(NullPolicy::Penalize)
                .build(),
        );
    }
    out.push(
        spec(ts::MACD_NORM, F, Dec, Src, Pit, Stale)
            .unit(FeatureUnit::Ratio)
            .null_policy(NullPolicy::Penalize)
            .build(),
    );
}

fn microstructure_specs(out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::Microstructure as F;
    use FeatureValueKind::{Decimal as Dec, Probability};
    use PitRule::FactBeforeAsOfMinusDelay as Pit;
    use SourceRequirement::MicrostructureWindow as Src;
    use StalenessRule::MaxFeatureBucketAge as Stale;

    for (name, kind, unit) in [
        (micro::QUOTE_UPDATE_RATE, Dec, FeatureUnit::PerSecond),
        (micro::BOOK_CHURN, Dec, FeatureUnit::Ratio),
        (micro::QUEUE_DEPLETION, Dec, FeatureUnit::Ratio),
        (micro::SUDDEN_LIQUIDITY_WITHDRAWAL, Dec, FeatureUnit::Ratio),
        (micro::ADVERSE_SELECTION_PROXY, Dec, FeatureUnit::Ratio),
    ] {
        out.push(
            spec(name, F, kind, Src, Pit, Stale)
                .unit(unit)
                .null_policy(NullPolicy::Penalize)
                .build(),
        );
    }
    out.push(
        spec(
            micro::STALE_QUOTE_FREQUENCY,
            F,
            Probability,
            Src,
            Pit,
            Stale,
        )
        .unit(FeatureUnit::Probability)
        .range(Decimal::ZERO, Decimal::ONE)
        .null_policy(NullPolicy::Penalize)
        .build(),
    );
}

/// Structural (prediction-market-aware) feature specs (Phase 11.2.1).
///
/// Platform-computable from existing facts (book, market metadata, microstructure
/// window, and same-`as_of` neg-risk sibling-leg books) — no external source.
/// Neg-risk aggregates are `Optional`: on a binary market they resolve to
/// `NullReason::NotApplicable`, and on a missing leg to `NullReason::LegBookMissing`
/// — never a fabricated zero.
fn structural_specs(out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::Structural as F;
    use FeatureValueKind::Decimal as Dec;
    use NullPolicy::Penalize;
    use PitRule::{BookVersionAtOrBeforeAsOf as BookPit, FactBeforeAsOfMinusDelay as WindowPit};
    use SourceRequirement::{MicrostructureWindow, PublishedL2Book};
    use StalenessRule::{MaxBookAge, MaxFeatureBucketAge};

    out.push(
        spec(
            structural_names::SHORT_RETURN,
            F,
            Dec,
            MicrostructureWindow,
            WindowPit,
            MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Penalize)
        .build(),
    );
    out.push(
        spec(
            structural_names::SHOCK_RATIO,
            F,
            Dec,
            MicrostructureWindow,
            WindowPit,
            MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .range(Decimal::ZERO, Decimal::from(1_000_000))
        .null_policy(Penalize)
        .build(),
    );
    out.push(
        spec(
            structural_names::PRICE_EXTREMITY,
            F,
            Dec,
            PublishedL2Book,
            BookPit,
            MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        // Signed `mid − 0.5` ∈ [−0.5, 0.5].
        .range(Decimal::new(-5, 1), Decimal::new(5, 1))
        .null_policy(Penalize)
        .build(),
    );
    out.push(
        spec(
            structural_names::BOOK_CHURN_INTENSITY,
            F,
            Dec,
            MicrostructureWindow,
            WindowPit,
            MaxFeatureBucketAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Penalize)
        .build(),
    );
    trade_tape_structural_specs(out);
    structural_neg_risk_specs(out);
}

fn trade_tape_structural_specs(out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::Structural as F;
    use FeatureValueKind::{Count, Decimal as Dec, Usd};
    use NullPolicy::Penalize;
    use PitRule::FactBeforeAsOfMinusDelay as WindowPit;
    use SourceRequirement::TradeTapeWindow;
    use StalenessRule::MaxTradeTapeAge;

    out.push(
        spec(
            structural_names::TRADE_TAPE_COUNT,
            F,
            Count,
            TradeTapeWindow,
            WindowPit,
            MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(Penalize)
        .build(),
    );
    out.push(
        spec(
            structural_names::PARTICIPANT_COUNT,
            F,
            Count,
            TradeTapeWindow,
            WindowPit,
            MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(Penalize)
        .build(),
    );
    out.push(
        spec(
            structural_names::TRADE_TAPE_NOTIONAL_USD,
            F,
            Usd,
            TradeTapeWindow,
            WindowPit,
            MaxTradeTapeAge,
        )
        .unit(FeatureUnit::Usd)
        .null_policy(Penalize)
        .build(),
    );
    for name in [
        structural_names::PARTICIPANT_COVERAGE_RATIO,
        structural_names::PARTICIPANT_GINI,
        structural_names::PARTICIPANT_HHI,
        structural_names::PARTICIPANT_CR1_SHARE,
        structural_names::MAKER_GINI,
        structural_names::TAKER_GINI,
    ] {
        out.push(
            spec(name, F, Dec, TradeTapeWindow, WindowPit, MaxTradeTapeAge)
                .unit(FeatureUnit::Ratio)
                .range(Decimal::ZERO, Decimal::ONE)
                .null_policy(Penalize)
                .build(),
        );
    }
}

fn structural_neg_risk_specs(out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::Structural as F;
    use FeatureValueKind::{Count, Decimal as Dec};
    use NullPolicy::Optional;
    use PitRule::BookVersionAtOrBeforeAsOf as BookPit;
    use SourceRequirement::NegRiskSiblingLegs;
    use StalenessRule::MaxBookAge;

    out.push(
        spec(
            structural_names::NEGRISK_LEG_ASK_SUM,
            F,
            Dec,
            NegRiskSiblingLegs,
            BookPit,
            MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            structural_names::NEGRISK_LEG_BID_SUM,
            F,
            Dec,
            NegRiskSiblingLegs,
            BookPit,
            MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            structural_names::NEGRISK_LEG_COUNT,
            F,
            Count,
            NegRiskSiblingLegs,
            BookPit,
            MaxBookAge,
        )
        .unit(FeatureUnit::Count)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            structural_names::NEGRISK_CONVERT_EDGE,
            F,
            Dec,
            NegRiskSiblingLegs,
            BookPit,
            MaxBookAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Optional)
        .build(),
    );
}

/// Crypto external-vertical (domain-slice) feature specs (Phase 11.2.2).
///
/// Every spec requires a resolved market linkage plus a PIT window of external
/// domain observations. All are `Optional`: a market whose category maps to no
/// vertical simply carries no domain slice (the specs then structurally do not
/// apply), and a linked market with a source gap keeps an explicit
/// present-but-missing value — never a fabricated zero, never market rejection.
fn domain_specs(out: &mut Vec<FeatureSpec>) {
    use FeatureFamily::Domain as F;
    use FeatureValueKind::{Bps, Count, Decimal as Dec};
    use NullPolicy::Optional;
    use PitRule::FactBeforeAsOfMinusDelay as Pit;
    use SourceRequirement::DomainObservationWindow as Src;
    use StalenessRule::{MaxDomainObservationAge, None as Fresh};

    out.push(
        spec(
            domain_crypto_names::DISTANCE_TO_STRIKE,
            F,
            Dec,
            Src,
            Pit,
            MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            domain_crypto_names::UNDERLYING_MOMENTUM,
            F,
            Dec,
            Src,
            Pit,
            MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            domain_crypto_names::UNDERLYING_REALIZED_VOL,
            F,
            Dec,
            Src,
            Pit,
            MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Ratio)
        .range(Decimal::ZERO, Decimal::from(1_000_000))
        .null_policy(Optional)
        .build(),
    );
    out.push(
        // Intrinsically point-in-time: derived from the frozen subject's
        // observation instant, not from any observation feed.
        spec(
            domain_crypto_names::TIME_TO_OBSERVATION,
            F,
            Count,
            Src,
            Pit,
            Fresh,
        )
        .unit(FeatureUnit::Seconds)
        .null_policy(Optional)
        .build(),
    );
    out.push(
        spec(
            domain_crypto_names::BASIS_VS_RESOLUTION_SOURCE,
            F,
            Bps,
            Src,
            Pit,
            MaxDomainObservationAge,
        )
        .unit(FeatureUnit::Bps)
        .null_policy(Optional)
        .build(),
    );
}

/// Validate `features.required_features` against the active [`FeatureSchema`].
///
/// Registered into models' [`FEATURES_CONFIG_VALIDATORS`] at link time so
/// [`validate_runtime_config`](quant_pivot_models::runtime_config::validate_runtime_config)
/// performs schema membership checks in the same pass as structural invariants.
pub fn validate_required_features(features: &FeaturesConfig, report: &mut ConfigValidationReport) {
    let schema = FeatureSchema::build(features);
    for feature_ref in &features.required_features {
        let label = feature_ref.name.trim();
        if label.is_empty() {
            continue;
        }
        let name = FeatureName::from(feature_ref);
        if !schema.contains(&name) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.required_features",
                detail: format!("unknown feature `{label}` for the active schema"),
            });
        }
    }
}

fn validate_required_features_hook(features: &FeaturesConfig, report: &mut ConfigValidationReport) {
    validate_required_features(features, report);
}

#[allow(unsafe_code)]
#[distributed_slice(FEATURES_CONFIG_VALIDATORS)]
static REGISTER_REQUIRED_FEATURES: FeaturesConfigValidator = validate_required_features_hook;

#[cfg(test)]
mod validation_tests {
    use super::validate_required_features;
    use quant_pivot_error::config_validation::ConfigValidationReport;
    use quant_pivot_models::runtime_config::{
        FeatureNameRef, RuntimeConfig, validate_runtime_config,
    };

    #[test]
    fn default_runtime_config_passes_full_validation() {
        let report = validate_runtime_config(&RuntimeConfig::default());
        assert!(!report.has_errors());
    }

    #[test]
    fn unknown_required_feature_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![FeatureNameRef::new("book.not_a_feature")];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn known_required_feature_is_accepted() {
        let mut config = RuntimeConfig::default();
        config
            .features
            .required_features
            .push(FeatureNameRef::new("book.spread_bps"));
        let report = validate_runtime_config(&config);
        assert!(!report.has_errors());
    }

    #[test]
    fn validate_required_features_can_be_called_directly() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![FeatureNameRef::new("book.not_a_feature")];
        let mut report = ConfigValidationReport::default();
        validate_required_features(&config.features, &mut report);
        assert!(report.has_errors());
    }
}
