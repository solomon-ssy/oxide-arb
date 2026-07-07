//! The feature build pipeline: compose group builders, resolve PIT inputs, apply
//! the null policy, and assemble a canonical [`FeatureVector`].
//!
//! [`ConfiguredFeatureBuilder`] is the one [`FeatureBuilder`] implementation. The
//! async [`resolve_inputs`](ConfiguredFeatureBuilder::resolve_inputs) step is the
//! only place that touches the PIT source; [`compute_vector`] is a pure function
//! of the resolved inputs, so it is deterministic, unit-testable in isolation,
//! and safe to run in parallel via [`build_batch`]. The same definition produces
//! an identical vector online and offline.

use crate::{
    features::{
        FeatureBuildInput, FeatureBuilder, FeatureVector, PitView,
        decision_capture::{
            ResolvedMarketBundle, empty_book, market_decision_capture_from_resolved,
            stub_market_context,
        },
        domain::{
            CryptoDomainFeatureBuilder, DomainComputeCtx, DomainFeatureBuilder, DomainSliceInputs,
        },
        generic::{
            book::PriceBookFeatureBuilder, market::MarketMetadataFeatureBuilder,
            microstructure::MicrostructureFeatureBuilder, structural::StructuralFeatureBuilder,
            timeseries::TimeSeriesFeatureBuilder,
        },
        null_policy::{NullDecision, NullPolicyEngine},
        resolved::{
            MarketWindowSnapshot, ResolvedBook, ResolvedMarketContext, TradeTapeWindowSnapshot,
        },
        schema::{FeatureSchema, FeatureSpec, StalenessRule},
        value::{
            DomainFeatureSlice, EvidenceSourceRef, FeatureName, FeatureValue, NullReason,
            SubstitutionAudit, merged_required_features,
        },
    },
    selection::SelectedMarket,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{NegRiskLeg, market::registry::NegRiskLegSet},
    enums::{common::MarketCategory, domain::DomainFamily, quant::DataQualityStatus},
    runtime_config::{DataQualityConfig, DomainConfig, FeatureFamily, FeaturesConfig},
    types::{BookSnapshotRef, MarketId, SchemaVersion, TokenId, Usd},
};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashSet};

/// A single raw feature produced by a group builder before null-policy resolution.
pub struct RawFeature {
    /// Feature name (must match a schema spec to be retained).
    pub name: FeatureName,
    /// The computed value, or the reason it is absent.
    pub value: Result<FeatureValue, NullReason>,
    /// Provenance of the value, when one was produced.
    pub evidence: Option<EvidenceSourceRef>,
}

impl RawFeature {
    /// A present value with evidence.
    #[must_use]
    pub const fn present(
        name: FeatureName,
        value: FeatureValue,
        evidence: EvidenceSourceRef,
    ) -> Self {
        Self {
            name,
            value: Ok(value),
            evidence: Some(evidence),
        }
    }

    /// A missing value with a reason.
    #[must_use]
    pub const fn missing(name: FeatureName, reason: NullReason) -> Self {
        Self {
            name,
            value: Err(reason),
            evidence: None,
        }
    }
}

/// One neg-risk sibling leg resolved point-in-time at the SAME `as_of` as the
/// primary market (Phase 11.2.1). Online and offline both resolve through
/// [`PitView::resolve_book`], so the structural full-leg aggregates are
/// byte-identical across backends.
pub struct ResolvedLeg {
    /// The sibling market that owns this YES leg.
    pub market_id: MarketId,
    /// The YES outcome token of the sibling leg.
    pub token_id: TokenId,
    /// The sibling YES-leg book resolved at `as_of`.
    pub book: ResolvedBook,
}

/// Everything a group builder needs to compute its features, all borrowed.
pub struct FeatureComputeCtx<'a> {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// The selected market's category (drives domain routing).
    pub category: MarketCategory,
    /// Primary-token book resolved at `as_of`, when available.
    pub book: Option<&'a ResolvedBook>,
    /// Market metadata resolved at `as_of`, when available.
    pub market: Option<&'a ResolvedMarketContext>,
    /// Pre-fetched windowed microstructure history for the primary token.
    pub window: &'a MarketWindowSnapshot,
    /// Pre-fetched trade-tape participant history for the primary token.
    pub trade_tape: &'a TradeTapeWindowSnapshot,
    /// Same-`as_of` neg-risk sibling YES legs whose book resolved (empty for
    /// binary markets or when the schema declares no structural neg-risk aggregate).
    pub sibling_legs: &'a [ResolvedLeg],
    /// The number of event YES legs the resolver was asked to fetch — the
    /// expected leg count. `sibling_legs.len() < sibling_leg_total` means at least
    /// one leg's book was absent at `as_of` (fail-closed → `LegBookMissing`).
    pub sibling_leg_total: usize,
    /// Frozen feature config.
    pub config: &'a FeaturesConfig,
    /// Frozen data-quality config.
    pub data_quality: &'a DataQualityConfig,
    /// Book replay handle frozen at resolve (when a book was captured).
    pub book_snapshot_ref: Option<&'a BookSnapshotRef>,
}

/// A pure feature-group computation (no I/O, no clock, no mutable state).
pub trait FeatureGroupBuilder: Send + Sync {
    /// The family this group owns.
    fn family(&self) -> FeatureFamily;

    /// Compute the group's raw features for one market.
    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature>;
}

/// Source-agnostic, PIT-resolved inputs for one market's feature build.
///
/// Produced by the async [`ConfiguredFeatureBuilder::resolve_inputs`] (the only
/// I/O step) and consumed by the pure [`ConfiguredFeatureBuilder::compute_vector`].
pub struct ResolvedInputs<'a> {
    /// The selected market (frozen selection snapshot).
    pub market: &'a SelectedMarket,
    /// Decision time the inputs were resolved as of.
    pub as_of: DateTime<Utc>,
    /// Primary-token book resolved at `as_of`, when available.
    pub book: Option<ResolvedBook>,
    /// Market metadata resolved at `as_of`, when available.
    pub market_ctx: Option<ResolvedMarketContext>,
    /// Pre-fetched, PIT-bounded microstructure window for the primary token.
    pub window: &'a MarketWindowSnapshot,
    /// Pre-fetched, PIT-bounded trade-tape window for the primary token.
    pub trade_tape: &'a TradeTapeWindowSnapshot,
    /// Pre-fetched domain-slice inputs (present only for category-mapped
    /// markets with a resolved linkage).
    pub domain: Option<&'a DomainSliceInputs>,
    /// Same-`as_of` neg-risk sibling YES legs whose book resolved (empty unless
    /// the market is neg-risk and the schema declares a structural aggregate).
    pub sibling_legs: Vec<ResolvedLeg>,
    /// Expected event YES-leg count (resolved < expected ⇒ a leg was missing).
    pub sibling_leg_total: usize,
}

/// Pre-fetched source windows for one `(market, as_of)` feature build.
#[derive(Clone, Copy)]
pub struct FeatureSourceWindows<'a> {
    pub microstructure: &'a MarketWindowSnapshot,
    pub trade_tape: &'a TradeTapeWindowSnapshot,
    /// Domain-slice inputs; `None` for markets outside every vertical.
    pub domain: Option<&'a DomainSliceInputs>,
}

/// The configured, composable feature builder.
pub struct ConfiguredFeatureBuilder {
    schema: FeatureSchema,
    groups: Vec<Box<dyn FeatureGroupBuilder>>,
    domain_builders: Vec<Box<dyn DomainFeatureBuilder>>,
    /// Frozen domain-plane parameters (windows, cross-check policy).
    domain_config: DomainConfig,
    source_needs: FeatureSourceNeeds,
}

/// Precomputed PIT/source gating for the active feature schema.
#[derive(Clone, Copy)]
struct FeatureSourceNeeds {
    bits: u8,
}

const NEED_BOOK: u8 = 1 << 0;
const NEED_MARKET: u8 = 1 << 1;
const NEED_SIBLING_LEGS: u8 = 1 << 2;
const NEED_TRADE_TAPE: u8 = 1 << 3;
const NEED_DOMAIN: u8 = 1 << 4;

impl FeatureSourceNeeds {
    fn from_schema(schema: &FeatureSchema) -> Self {
        let mut bits = 0_u8;
        if schema.needs_book() {
            bits |= NEED_BOOK;
        }
        if schema.needs_market_metadata() {
            bits |= NEED_MARKET;
        }
        if schema.needs_sibling_legs() {
            bits |= NEED_SIBLING_LEGS;
        }
        if schema.needs_trade_tape() {
            bits |= NEED_TRADE_TAPE;
        }
        if schema.needs_domain() {
            bits |= NEED_DOMAIN;
        }
        Self { bits }
    }

    const fn book(self) -> bool {
        self.bits & NEED_BOOK != 0
    }

    const fn market(self) -> bool {
        self.bits & NEED_MARKET != 0
    }

    const fn sibling_legs(self) -> bool {
        self.bits & NEED_SIBLING_LEGS != 0
    }

    const fn trade_tape(self) -> bool {
        self.bits & NEED_TRADE_TAPE != 0
    }

    const fn domain(self) -> bool {
        self.bits & NEED_DOMAIN != 0
    }
}

impl ConfiguredFeatureBuilder {
    /// Build the standard pipeline for the given feature config.
    ///
    /// The schema and the active group set are both derived from
    /// `enabled_feature_families`, so only configured families are computed. The
    /// PIT gating flags are resolved once here from the schema's source
    /// requirements, so the build loop never issues a book/metadata lookup no
    /// enabled feature consumes.
    #[must_use]
    pub fn new(config: &FeaturesConfig, domain_config: &DomainConfig) -> Self {
        let schema = FeatureSchema::build(config);
        let mut groups: Vec<Box<dyn FeatureGroupBuilder>> = Vec::new();
        let mut domain_builders: Vec<Box<dyn DomainFeatureBuilder>> = Vec::new();
        for family in &config.enabled_feature_families {
            match family {
                FeatureFamily::MarketMetadata => {
                    groups.push(Box::new(MarketMetadataFeatureBuilder));
                }
                FeatureFamily::PriceBook => groups.push(Box::new(PriceBookFeatureBuilder)),
                FeatureFamily::TimeSeries => groups.push(Box::new(TimeSeriesFeatureBuilder)),
                FeatureFamily::Microstructure => {
                    groups.push(Box::new(MicrostructureFeatureBuilder));
                }
                FeatureFamily::Structural => groups.push(Box::new(StructuralFeatureBuilder)),
                FeatureFamily::Domain => {
                    if domain_config.family_enabled(DomainFamily::Crypto) {
                        domain_builders.push(Box::new(CryptoDomainFeatureBuilder));
                    }
                }
            }
        }
        let source_needs = FeatureSourceNeeds::from_schema(&schema);
        Self {
            schema,
            groups,
            domain_builders,
            domain_config: domain_config.clone(),
            source_needs,
        }
    }

    /// The governed schema this builder produces.
    #[must_use]
    pub const fn schema(&self) -> &FeatureSchema {
        &self.schema
    }

    /// Whether the active schema declares any trade-tape feature.
    #[must_use]
    pub const fn needs_trade_tape(&self) -> bool {
        self.source_needs.trade_tape()
    }

    /// Whether the active schema declares any domain-slice feature.
    #[must_use]
    pub const fn needs_domain(&self) -> bool {
        self.source_needs.domain()
    }

    /// Resolve the PIT inputs for one market (the only async / I/O step).
    ///
    /// Decision capture always resolves book + market context for evidence refs.
    /// Feature compute inputs (`ResolvedInputs::book` / `market_ctx`) remain gated
    /// on the schema's source requirements.
    ///
    /// # Errors
    ///
    /// Propagates the PIT view's resolution errors (historical engine queries).
    pub async fn resolve_inputs<'a>(
        &self,
        market: &'a SelectedMarket,
        as_of: DateTime<Utc>,
        pit: PitView<'a>,
        windows: FeatureSourceWindows<'a>,
        sibling: &NegRiskLegSet,
        liquidity_cap_usd: Usd,
    ) -> QuantResult<ResolvedMarketBundle<'a>> {
        let capture_book = pit
            .resolve_book(&market.primary_token_id, as_of)
            .await?
            .unwrap_or_else(|| empty_book(market.primary_token_id.clone(), as_of));
        let capture_market = pit
            .resolve_market(&market.market_id, as_of)
            .await?
            .unwrap_or_else(|| stub_market_context(market.market_id.clone(), as_of));
        let registry = pit.resolve_registry(&market.market_id, as_of)?;
        let capture = market_decision_capture_from_resolved(
            as_of,
            market,
            capture_book,
            capture_market,
            registry.as_deref(),
            liquidity_cap_usd,
        )?;
        let book = if self.source_needs.book() {
            Some(capture.book.clone())
        } else {
            None
        };
        let market_ctx = if self.source_needs.market() {
            Some(capture.market.clone())
        } else {
            None
        };
        let sibling_leg_total = if self.source_needs.sibling_legs() {
            sibling.expected_legs
        } else {
            0
        };
        let sibling_legs = self.resolve_sibling_legs(pit, as_of, &sibling.legs).await?;
        Ok(ResolvedMarketBundle {
            inputs: ResolvedInputs {
                market,
                as_of,
                book,
                market_ctx,
                window: windows.microstructure,
                trade_tape: windows.trade_tape,
                domain: if self.source_needs.domain() {
                    windows.domain
                } else {
                    None
                },
                sibling_legs,
                sibling_leg_total,
            },
            capture,
        })
    }

    /// Resolve every neg-risk sibling YES leg's book at the SAME `as_of`.
    ///
    /// A leg whose book is absent at `as_of` is **dropped** from the resolved set
    /// (never a fabricated empty book); the structural builder then reports the
    /// missing coverage as [`NullReason::LegBookMissing`], and the factor plane
    /// as `Indeterminate { LegBookMissing }` — never a silent zero.
    async fn resolve_sibling_legs(
        &self,
        pit: PitView<'_>,
        as_of: DateTime<Utc>,
        legs: &[NegRiskLeg],
    ) -> QuantResult<Vec<ResolvedLeg>> {
        if !self.source_needs.sibling_legs() || legs.is_empty() {
            return Ok(Vec::new());
        }
        let mut resolved = Vec::with_capacity(legs.len());
        for leg in legs {
            if let Some(book) = pit.resolve_book(&leg.yes_token_id, as_of).await? {
                resolved.push(ResolvedLeg {
                    market_id: leg.market_id.clone(),
                    token_id: leg.yes_token_id.clone(),
                    book,
                });
            }
        }
        Ok(resolved)
    }

    /// Compute the canonical [`FeatureVector`] from resolved inputs (pure).
    #[must_use]
    pub fn compute_vector(
        &self,
        bundle: &ResolvedMarketBundle<'_>,
        required_features: &[FeatureName],
        config: &FeaturesConfig,
        data_quality: &DataQualityConfig,
    ) -> FeatureVector {
        let resolved = &bundle.inputs;
        let ctx = FeatureComputeCtx {
            as_of: resolved.as_of,
            category: resolved.market.category,
            book: resolved.book.as_ref(),
            market: resolved.market_ctx.as_ref(),
            window: resolved.window,
            trade_tape: resolved.trade_tape,
            sibling_legs: &resolved.sibling_legs,
            sibling_leg_total: resolved.sibling_leg_total,
            config,
            data_quality,
            book_snapshot_ref: Some(&bundle.capture.book_snapshot_ref),
        };

        // Collect every generic group's raw features into a name-keyed map.
        let mut raw: BTreeMap<FeatureName, RawFeature> = BTreeMap::new();
        for group in &self.groups {
            for feature in group.compute(&ctx) {
                raw.insert(feature.name.clone(), feature);
            }
        }

        let required = merged_required_features(required_features, config);
        let mut assembly = self.assemble(&raw, &required, data_quality, resolved.as_of);
        let domain = self.compute_domain_slice(resolved, &required, data_quality, &mut assembly);

        let book_age_ms = book_age_ms(resolved.as_of, resolved.book.as_ref());
        let feature_bucket_age_ms = feature_bucket_age_ms(resolved.as_of, resolved.window);
        let trade_tape_age_ms = trade_tape_age_ms(resolved.as_of, resolved.trade_tape);
        let domain_age_ms = domain_age_ms(resolved.as_of, resolved.domain);
        let data_quality_status = classify(
            assembly.rejected,
            assembly.degraded,
            FreshnessAges {
                book: book_age_ms,
                feature_bucket: feature_bucket_age_ms,
                trade_tape: trade_tape_age_ms,
                domain: domain_age_ms,
            },
            data_quality,
        );

        FeatureVector {
            market_id: resolved.market.market_id.clone(),
            token_id: Some(resolved.market.primary_token_id.clone()),
            as_of: resolved.as_of,
            generic_schema_version: self.schema.version(),
            generic: assembly.values,
            domain,
            substitutions: assembly.substitutions,
            data_quality: data_quality_status,
            staleness_ms: book_age_ms
                .max(feature_bucket_age_ms)
                .max(trade_tape_age_ms)
                .max(domain_age_ms),
            source_refs: assembly.evidence,
        }
    }

    /// Build the optional domain slice for a category-mapped, linkage-resolved
    /// market (pure; inputs were prefetched by the pipeline).
    ///
    /// Returns `None` when the vector structurally carries no domain slice:
    /// no domain inputs (category maps to no vertical, family disabled, or
    /// linkage unresolved) or no registered builder for the family. When the
    /// slice applies, its values flow through the same governed null-policy /
    /// valid-range / staleness machinery as the generic slice, and its
    /// substitutions and evidence merge into the vector-level audit trail.
    fn compute_domain_slice(
        &self,
        resolved: &ResolvedInputs<'_>,
        required: &HashSet<FeatureName>,
        data_quality: &DataQualityConfig,
        assembly: &mut Assembly,
    ) -> Option<DomainFeatureSlice> {
        let inputs = resolved.domain?;
        let builder = self
            .domain_builders
            .iter()
            .find(|builder| builder.family() == inputs.family)?;
        let ctx = DomainComputeCtx {
            as_of: resolved.as_of,
            binding: &inputs.binding,
            primary: &inputs.primary,
            oracle: inputs.oracle.as_ref(),
            crypto: &self.domain_config.crypto,
        };
        let mut raw: BTreeMap<FeatureName, RawFeature> = BTreeMap::new();
        for feature in builder.compute(&ctx) {
            raw.insert(feature.name.clone(), feature);
        }

        let mut values = BTreeMap::new();
        for spec in self.schema.domain_specs() {
            let is_required = required.contains(&spec.name);
            let raw_feature = raw.get(&spec.name);
            match resolve_value(raw_feature, spec, resolved.as_of, data_quality) {
                Resolved::Present {
                    value,
                    evidence: ev,
                } => {
                    if let Some(ev) = ev {
                        push_unique(&mut assembly.evidence, ev);
                    }
                    values.insert(spec.name.clone(), value);
                }
                Resolved::Absent(reason) => {
                    match NullPolicyEngine::decide(spec, reason, data_quality, is_required) {
                        NullDecision::Reject(reason) => {
                            assembly.rejected = true;
                            values.insert(spec.name.clone(), FeatureValue::Missing(reason));
                        }
                        NullDecision::Substitute { value } => {
                            assembly.substitutions.push(SubstitutionAudit {
                                feature: spec.name.clone(),
                                reason,
                                substituted: value.clone(),
                            });
                            values.insert(spec.name.clone(), value);
                        }
                        NullDecision::KeepMissing { reason, degrade } => {
                            assembly.degraded |= degrade;
                            values.insert(spec.name.clone(), FeatureValue::Missing(reason));
                        }
                    }
                }
            }
        }

        Some(DomainFeatureSlice {
            family: inputs.family,
            schema_version: self.schema.version(),
            values,
        })
    }

    /// Compute vectors for a batch of resolved inputs, in parallel, preserving
    /// input order.
    ///
    /// `compute_vector` is pure and deterministic (statistical values are
    /// quantized), so parallel evaluation is order-independent and reproducible.
    #[must_use]
    pub fn build_batch(
        &self,
        bundles: &[ResolvedMarketBundle<'_>],
        required_features: &[FeatureName],
        config: &FeaturesConfig,
        data_quality: &DataQualityConfig,
    ) -> Vec<FeatureVector> {
        bundles
            .par_iter()
            .map(|bundle| self.compute_vector(bundle, required_features, config, data_quality))
            .collect()
    }
}

/// The intermediate accumulation while assembling a vector.
struct Assembly {
    values: BTreeMap<FeatureName, FeatureValue>,
    substitutions: Vec<SubstitutionAudit>,
    evidence: Vec<EvidenceSourceRef>,
    rejected: bool,
    degraded: bool,
}

#[async_trait]
impl FeatureBuilder for ConfiguredFeatureBuilder {
    fn schema_version(&self) -> SchemaVersion {
        self.schema.version()
    }

    async fn build(&self, input: FeatureBuildInput<'_>) -> QuantResult<FeatureVector> {
        let bundle = self
            .resolve_inputs(
                input.market,
                input.as_of,
                input.pit,
                FeatureSourceWindows {
                    microstructure: input.window,
                    trade_tape: input.trade_tape,
                    domain: input.domain,
                },
                input.sibling,
                Usd::ZERO,
            )
            .await?;
        Ok(self.compute_vector(
            &bundle,
            input.required_features,
            input.config,
            input.data_quality,
        ))
    }
}

impl ConfiguredFeatureBuilder {
    /// Resolve each schema spec against the raw features under the null policy.
    fn assemble(
        &self,
        raw: &BTreeMap<FeatureName, RawFeature>,
        required: &HashSet<FeatureName>,
        data_quality: &DataQualityConfig,
        as_of: DateTime<Utc>,
    ) -> Assembly {
        let mut values = BTreeMap::new();
        let mut substitutions = Vec::new();
        let mut evidence = Vec::new();
        let mut rejected = false;
        let mut degraded = false;

        for spec in self.schema.specs() {
            // Domain-family specs belong to the optional domain slice and are
            // assembled by `compute_domain_slice` — never into the generic map.
            if spec.family == FeatureFamily::Domain {
                continue;
            }
            let is_required = required.contains(&spec.name);
            let raw_feature = raw.get(&spec.name);
            let resolved = resolve_value(raw_feature, spec, as_of, data_quality);

            match resolved {
                Resolved::Present {
                    value,
                    evidence: ev,
                } => {
                    if let Some(ev) = ev {
                        push_unique(&mut evidence, ev);
                    }
                    values.insert(spec.name.clone(), value);
                }
                Resolved::Absent(reason) => {
                    match NullPolicyEngine::decide(spec, reason, data_quality, is_required) {
                        NullDecision::Reject(reason) => {
                            rejected = true;
                            values.insert(spec.name.clone(), FeatureValue::Missing(reason));
                        }
                        NullDecision::Substitute { value } => {
                            substitutions.push(SubstitutionAudit {
                                feature: spec.name.clone(),
                                reason,
                                substituted: value.clone(),
                            });
                            values.insert(spec.name.clone(), value);
                        }
                        NullDecision::KeepMissing { reason, degrade } => {
                            degraded |= degrade;
                            values.insert(spec.name.clone(), FeatureValue::Missing(reason));
                        }
                    }
                }
            }
        }

        Assembly {
            values,
            substitutions,
            evidence,
            rejected,
            degraded,
        }
    }
}

/// The outcome of validating a raw feature against its spec.
enum Resolved {
    Present {
        value: FeatureValue,
        evidence: Option<EvidenceSourceRef>,
    },
    Absent(NullReason),
}

/// Validate a raw feature against its spec's valid range and staleness policy.
fn resolve_value(
    raw: Option<&RawFeature>,
    spec: &FeatureSpec,
    as_of: DateTime<Utc>,
    data_quality: &DataQualityConfig,
) -> Resolved {
    let Some(raw) = raw else {
        return Resolved::Absent(NullReason::SourceUnavailable);
    };
    match &raw.value {
        Err(reason) | Ok(FeatureValue::Missing(reason)) => Resolved::Absent(*reason),
        Ok(value) => {
            if !in_range(spec, value) {
                return Resolved::Absent(NullReason::OutOfValidRange);
            }
            // Per-feature staleness: a present value whose freshest input is
            // older than the spec's bound becomes stale and flows through the
            // null policy (it is never silently used as if fresh).
            if staleness_breach(spec, raw.evidence.as_ref(), as_of, data_quality) {
                return Resolved::Absent(NullReason::StaleBeyondPolicy);
            }
            Resolved::Present {
                value: value.clone(),
                evidence: raw.evidence.clone(),
            }
        }
    }
}

/// Whether a present value satisfies the spec's valid range (no clamping).
fn in_range(spec: &FeatureSpec, value: &FeatureValue) -> bool {
    match (&spec.valid_range, value.to_fact_decimal()) {
        (Some(range), Some(decimal)) => range.contains(&decimal),
        _ => true,
    }
}

/// Whether a present value's freshest input violates the spec's staleness bound.
///
/// `MaxBookAge` is bounded by `data_quality.max_book_age_ms`;
/// `MaxFeatureBucketAge` by `data_quality.max_feature_bucket_age_secs`. A bound
/// of zero (or [`StalenessRule::None`]) means unbounded. A feature with no
/// evidence carries no measurable age and is never marked stale here (its
/// absence is handled elsewhere).
fn staleness_breach(
    spec: &FeatureSpec,
    evidence: Option<&EvidenceSourceRef>,
    as_of: DateTime<Utc>,
    data_quality: &DataQualityConfig,
) -> bool {
    let bound_ms = match spec.staleness_policy {
        StalenessRule::None => return false,
        StalenessRule::MaxBookAge => data_quality.max_book_age_ms,
        StalenessRule::MaxFeatureBucketAge => data_quality
            .max_feature_bucket_age_secs
            .saturating_mul(1_000),
        StalenessRule::MaxTradeTapeAge => {
            data_quality.max_trade_tape_age_secs.saturating_mul(1_000)
        }
        StalenessRule::MaxDomainObservationAge => data_quality
            .max_domain_observation_age_secs
            .saturating_mul(1_000),
    };
    if bound_ms == 0 {
        return false;
    }
    let Some(evidence) = evidence else {
        return false;
    };
    let age_ms = age_ms(as_of, evidence.observed_at);
    age_ms > bound_ms
}

/// Non-negative age in milliseconds of `observed_at` relative to `as_of`.
fn age_ms(as_of: DateTime<Utc>, observed_at: DateTime<Utc>) -> u64 {
    u64::try_from((as_of - observed_at).num_milliseconds()).unwrap_or(0)
}

/// Append an evidence ref unless an identical one is already present.
fn push_unique(evidence: &mut Vec<EvidenceSourceRef>, candidate: EvidenceSourceRef) {
    if !evidence.contains(&candidate) {
        evidence.push(candidate);
    }
}

/// Book age in milliseconds at `as_of` (zero when no book was resolved).
fn book_age_ms(as_of: DateTime<Utc>, book: Option<&ResolvedBook>) -> u64 {
    book.map_or(0, |book| age_ms(as_of, book.observed_at))
}

/// Freshest materialized feature-bucket age in milliseconds at `as_of`: age of
/// the freshest window bucket.
///
/// Point-in-time correct: a window with no buckets contributes no age (its
/// absence is handled by the null policy, not here).
fn feature_bucket_age_ms(as_of: DateTime<Utc>, window: &MarketWindowSnapshot) -> u64 {
    window
        .freshest_bucket_time()
        .map_or(0, |bucket_time| age_ms(as_of, bucket_time))
}

/// Freshest trade-tape print age in milliseconds at `as_of`.
fn trade_tape_age_ms(as_of: DateTime<Utc>, trade_tape: &TradeTapeWindowSnapshot) -> u64 {
    trade_tape
        .freshest_trade_time()
        .map_or(0, |trade_time| age_ms(as_of, trade_time))
}

/// Freshest domain-observation age in milliseconds at `as_of` (zero for
/// markets that carry no domain slice — absence is structural, not staleness).
fn domain_age_ms(as_of: DateTime<Utc>, domain: Option<&DomainSliceInputs>) -> u64 {
    domain
        .and_then(|inputs| inputs.primary.freshest_time())
        .map_or(0, |observed_at| age_ms(as_of, observed_at))
}

/// Per-source freshness ages feeding the aggregate classification.
#[derive(Copy, Clone)]
struct FreshnessAges {
    book: u64,
    feature_bucket: u64,
    trade_tape: u64,
    domain: u64,
}

/// Classify the vector's aggregate data quality across freshness dimensions:
/// book age, feature-bucket age, trade-tape age, and domain-observation age.
const fn classify(
    rejected: bool,
    degraded: bool,
    ages: FreshnessAges,
    data_quality: &DataQualityConfig,
) -> DataQualityStatus {
    if rejected {
        return DataQualityStatus::Insufficient;
    }
    let book_bound = data_quality.max_book_age_ms;
    let bucket_bound = data_quality
        .max_feature_bucket_age_secs
        .saturating_mul(1_000);
    let tape_bound = data_quality.max_trade_tape_age_secs.saturating_mul(1_000);
    let domain_bound = data_quality
        .max_domain_observation_age_secs
        .saturating_mul(1_000);
    if exceeds(ages.book, book_bound)
        || exceeds(ages.feature_bucket, bucket_bound)
        || exceeds(ages.trade_tape, tape_bound)
        || exceeds(ages.domain, domain_bound)
    {
        return DataQualityStatus::Stale;
    }
    if degraded {
        return DataQualityStatus::Degraded;
    }
    if within_half(ages.book, book_bound)
        && within_half(ages.feature_bucket, bucket_bound)
        && within_half(ages.trade_tape, tape_bound)
        && within_half(ages.domain, domain_bound)
    {
        DataQualityStatus::Fresh
    } else {
        DataQualityStatus::Acceptable
    }
}

/// Whether `age` exceeds a non-zero `bound` (a zero bound is unbounded).
const fn exceeds(age: u64, bound: u64) -> bool {
    bound > 0 && age > bound
}

/// Whether `age` is within half of a non-zero `bound` (a zero bound is satisfied).
const fn within_half(age: u64, bound: u64) -> bool {
    bound == 0 || age <= bound / 2
}
