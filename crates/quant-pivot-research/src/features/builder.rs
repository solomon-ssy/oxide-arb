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
        CatalogDecisionRef, FeatureBuildInput, FeatureBuilder, FeatureVector,
        MarketDecisionCaptureInput,
        decision_capture::{
            ResolvedMarketBundle, book_snapshot_ref_from_resolved,
            market_decision_capture_from_resolved,
        },
        domain::{
            CryptoDomainFeatureBuilder, DomainComputeCtx, DomainFeatureBuilder, DomainSliceInputs,
            WeatherDomainFeatureBuilder,
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
            DomainFeatureSlice, EvidenceSourceRef, FeatureCell, FeatureName, FeatureStaleness,
            FeatureValue, NullReason,
        },
    },
    pit::{PointInTimeSnapshotSource, ResolvedMarketSnapshot},
    selection::SelectedMarket,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{DecisionBoundary, DecisionSource, NegRiskLeg},
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

    /// A missing value whose source snapshot exists and remains auditable.
    ///
    /// This distinguishes an unavailable source from an available-but-unquoted
    /// book side. The latter retains its exact snapshot identity and staleness
    /// even though it carries no numeric value.
    #[must_use]
    pub const fn missing_with_evidence(
        name: FeatureName,
        reason: NullReason,
        evidence: EvidenceSourceRef,
    ) -> Self {
        Self {
            name,
            value: Err(reason),
            evidence: Some(evidence),
        }
    }
}

/// One neg-risk sibling leg resolved point-in-time at the SAME `as_of` as the
/// primary market (Phase 11.2.1). Online and offline both resolve through
/// [`PointInTimeSnapshotSource::book_at_boundary`], so the structural full-leg aggregates are
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
    pub decision_at: DateTime<Utc>,
    /// The selected market's category (drives domain routing).
    pub category: MarketCategory,
    /// Primary-token book resolved at `as_of`, when available.
    pub book: Option<&'a ResolvedBook>,
    /// Secondary/NO-token book resolved under the same boundary, when quoted.
    pub secondary_book: Option<&'a ResolvedBook>,
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
    /// Exact replay handle for the secondary/NO-token book.
    pub secondary_book_snapshot_ref: Option<&'a BookSnapshotRef>,
}

/// A pure feature-group computation (no I/O, no clock, no mutable state).
pub trait FeatureGroupBuilder: Send + Sync {
    /// The family this group owns.
    fn family(&self) -> FeatureFamily;

    /// Compute the group's raw features for one market.
    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>>;
}

/// Source-agnostic, PIT-resolved inputs for one market's feature build.
///
/// Produced by the async [`ConfiguredFeatureBuilder::resolve_inputs`] (the only
/// I/O step) and consumed by the pure [`ConfiguredFeatureBuilder::compute_vector`].
pub struct ResolvedInputs<'a> {
    /// The selected market (frozen selection snapshot).
    pub market: &'a SelectedMarket,
    /// Decision time the inputs were resolved as of.
    pub decision_at: DateTime<Utc>,
    /// Primary-token book resolved at `as_of`, when available.
    pub book: Option<ResolvedBook>,
    /// Secondary/NO-token book resolved at the same source cutoff, when quoted.
    pub secondary_book: Option<ResolvedBook>,
    /// Content-addressed replay handle for `secondary_book`.
    pub secondary_book_snapshot_ref: Option<BookSnapshotRef>,
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

async fn resolve_secondary_book(
    pit: &dyn PointInTimeSnapshotSource,
    market: &SelectedMarket,
    boundary: &DecisionBoundary,
) -> QuantResult<(Option<ResolvedBook>, Option<BookSnapshotRef>)> {
    let book = match market.secondary_token_id.as_ref() {
        Some(token_id) => pit
            .book_at_boundary(token_id, boundary)
            .await?
            .map(ResolvedBook::try_from)
            .transpose()?,
        None => None,
    };
    let snapshot_ref = book
        .as_ref()
        .map(book_snapshot_ref_from_resolved)
        .transpose()?;
    Ok((book, snapshot_ref))
}

async fn resolve_selected_catalog_snapshot(
    pit: &dyn PointInTimeSnapshotSource,
    market: &SelectedMarket,
    boundary: &DecisionBoundary,
) -> QuantResult<ResolvedMarketSnapshot> {
    let catalog_cutoff = boundary.cutoff_for(DecisionSource::Catalog);
    let snapshot = pit
        .market_snapshot_at(&market.market_id, boundary)
        .await?
        .ok_or_else(|| ResearchError::PitResolution {
            detail: format!(
                "no market catalog change for {} at source cutoff {catalog_cutoff}",
                market.market_id
            ),
        })?;
    if snapshot.boundary != *boundary {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "market {} snapshot boundary differs from the decision boundary",
                market.market_id
            ),
        }
        .into());
    }
    if snapshot.event.event_id != market.event_id {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "market {} snapshot event {} does not match selection event {}",
                market.market_id, snapshot.event.event_id, market.event_id
            ),
        }
        .into());
    }
    Ok(snapshot)
}

impl ConfiguredFeatureBuilder {
    /// Build the standard pipeline for the given feature config.
    ///
    /// The schema and the active group set are both derived from
    /// `enabled_feature_families`, so only configured families are computed. The
    /// PIT gating flags are resolved once here from the schema's source
    /// requirements, so the build loop never issues a book/metadata lookup no
    /// enabled feature consumes.
    pub fn new(config: &FeaturesConfig, domain_config: &DomainConfig) -> QuantResult<Self> {
        let schema = FeatureSchema::build(config)?;
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
                    if domain_config.family_enabled(DomainFamily::Weather) {
                        domain_builders.push(Box::new(WeatherDomainFeatureBuilder));
                    }
                }
            }
        }
        let source_needs = FeatureSourceNeeds::from_schema(&schema);
        Ok(Self {
            schema,
            groups,
            domain_builders,
            domain_config: domain_config.clone(),
            source_needs,
        })
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
    /// Propagates durable snapshot resolution errors.
    pub async fn resolve_inputs<'a>(
        &self,
        market: &'a SelectedMarket,
        boundary: &DecisionBoundary,
        pit: &'a dyn PointInTimeSnapshotSource,
        windows: FeatureSourceWindows<'a>,
        liquidity_cap_usd: Usd,
    ) -> QuantResult<ResolvedMarketBundle<'a>> {
        let book_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let capture_book = pit
            .book_at_boundary(&market.primary_token_id, boundary)
            .await?
            .map(ResolvedBook::try_from)
            .transpose()?
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "no order book for token {} at source cutoff {book_cutoff}",
                    market.primary_token_id
                ),
            })?;
        let (secondary_book, secondary_book_snapshot_ref) =
            resolve_secondary_book(pit, market, boundary).await?;
        let snapshot = resolve_selected_catalog_snapshot(pit, market, boundary).await?;
        let catalog = CatalogDecisionRef::from(&snapshot);
        let capture_market = ResolvedMarketContext::from(snapshot.context);
        let registry = snapshot.market;
        let snapshot_sibling = snapshot.neg_risk_leg_set;
        let capture = market_decision_capture_from_resolved(MarketDecisionCaptureInput {
            boundary,
            selected: market,
            book: capture_book,
            market: capture_market,
            registry: Some(registry.as_ref()),
            catalog,
            domain: windows.domain,
            liquidity_cap_usd,
        })?;
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
            snapshot_sibling.expected_legs
        } else {
            0
        };
        let sibling_legs = self
            .resolve_sibling_legs(pit, boundary, &snapshot_sibling.legs)
            .await?;
        Ok(ResolvedMarketBundle {
            inputs: ResolvedInputs {
                market,
                decision_at: boundary.decision_at(),
                book,
                secondary_book,
                secondary_book_snapshot_ref,
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
        pit: &dyn PointInTimeSnapshotSource,
        boundary: &DecisionBoundary,
        legs: &[NegRiskLeg],
    ) -> QuantResult<Vec<ResolvedLeg>> {
        if !self.source_needs.sibling_legs() || legs.is_empty() {
            return Ok(Vec::new());
        }
        let mut resolved = Vec::with_capacity(legs.len());
        for leg in legs {
            if let Some(book) = pit
                .book_at_boundary(&leg.yes_token_id, boundary)
                .await?
                .map(ResolvedBook::try_from)
                .transpose()?
            {
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
    pub fn compute_vector(
        &self,
        bundle: &ResolvedMarketBundle<'_>,
        required_features: &[FeatureName],
        config: &FeaturesConfig,
        data_quality: &DataQualityConfig,
    ) -> QuantResult<FeatureVector> {
        let resolved = &bundle.inputs;
        let ctx = FeatureComputeCtx {
            decision_at: resolved.decision_at,
            category: resolved.market.category,
            book: resolved.book.as_ref(),
            secondary_book: resolved.secondary_book.as_ref(),
            market: resolved.market_ctx.as_ref(),
            window: resolved.window,
            trade_tape: resolved.trade_tape,
            sibling_legs: &resolved.sibling_legs,
            sibling_leg_total: resolved.sibling_leg_total,
            config,
            data_quality,
            book_snapshot_ref: Some(&bundle.capture.book_snapshot_ref),
            secondary_book_snapshot_ref: resolved.secondary_book_snapshot_ref.as_ref(),
        };

        // Collect every generic group's raw features into a name-keyed map.
        let mut raw: BTreeMap<FeatureName, RawFeature> = BTreeMap::new();
        for group in &self.groups {
            for feature in group.compute(&ctx)? {
                let name = feature.name.clone();
                if self.schema.by_name(&name).is_none() {
                    return Err(ResearchError::SchemaHashMismatch {
                        detail: format!(
                            "feature builder emitted `{name}` outside the active schema"
                        ),
                    }
                    .into());
                }
                if raw.insert(name.clone(), feature).is_some() {
                    return Err(ResearchError::Determinism {
                        detail: format!("feature builders emitted duplicate raw feature `{name}`"),
                    }
                    .into());
                }
            }
        }

        let required: HashSet<FeatureName> = required_features.iter().cloned().collect();
        let mut assembly = self.assemble(&raw, &required, data_quality, resolved.decision_at)?;
        let domain = self.compute_domain_slice(resolved, &required, data_quality, &mut assembly)?;

        let book_age_ms = book_age_ms(resolved.decision_at, resolved.book.as_ref())?;
        let feature_bucket_age_ms = feature_bucket_age_ms(resolved.decision_at, resolved.window)?;
        let trade_tape_age_ms = trade_tape_age_ms(resolved.decision_at, resolved.trade_tape)?;
        let domain_age_ms = domain_age_ms(resolved.decision_at, resolved.domain)?;
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
        )?;

        Ok(FeatureVector {
            market_id: resolved.market.market_id.clone(),
            token_id: Some(resolved.market.primary_token_id.clone()),
            decision_at: resolved.decision_at,
            generic_schema_version: self.schema.version(),
            generic: assembly.cells,
            domain,
            data_quality: data_quality_status,
        })
    }

    /// Build the domain slice for a category-mapped market (pure; inputs were
    /// prefetched by the pipeline).
    ///
    /// Returns `None` only when the category maps to no enabled vertical. A
    /// mapped market without a resolved linkage receives an explicit slice of
    /// `Missing(LinkageUnresolved)` cells, preserving `Missing` vs
    /// `NotApplicable` for the shared model-input transform and serving facts.
    fn compute_domain_slice(
        &self,
        resolved: &ResolvedInputs<'_>,
        required: &HashSet<FeatureName>,
        data_quality: &DataQualityConfig,
        assembly: &mut Assembly,
    ) -> QuantResult<Option<DomainFeatureSlice>> {
        let Some(family) = DomainFamily::for_category(resolved.market.category)
            .filter(|family| self.domain_config.family_enabled(*family))
        else {
            return Ok(None);
        };
        if self.schema.domain_specs().next().is_none() {
            return Ok(None);
        }
        let Some(inputs) = resolved.domain else {
            return Ok(Some(self.missing_domain_slice(
                family,
                required,
                data_quality,
                assembly,
                NullReason::LinkageUnresolved,
            )));
        };
        if inputs.family != family {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "domain slice family {:?} does not match category {:?} family {family:?}",
                    inputs.family, resolved.market.category
                ),
            }
            .into());
        }
        let Some(builder) = self
            .domain_builders
            .iter()
            .find(|builder| builder.family() == inputs.family)
        else {
            return Err(ResearchError::Determinism {
                detail: format!("no domain feature builder registered for family {family:?}"),
            }
            .into());
        };
        let ctx = DomainComputeCtx {
            decision_at: resolved.decision_at,
            binding: &inputs.binding,
            linkage_evidence: &inputs.linkage_evidence,
            data: inputs.data.as_ref(),
            domain: &self.domain_config,
        };
        let mut raw: BTreeMap<FeatureName, RawFeature> = BTreeMap::new();
        for feature in builder.compute(&ctx) {
            raw.insert(feature.name.clone(), feature);
        }

        let mut values = BTreeMap::new();
        for spec in self.schema.domain_specs() {
            let is_required = required.contains(&spec.name);
            let raw_feature = raw.get(&spec.name);
            match resolve_value(raw_feature, spec, resolved.decision_at, data_quality)? {
                Resolved::Present {
                    value,
                    evidence,
                    staleness,
                } => {
                    values.insert(
                        spec.name.clone(),
                        FeatureCell::observed(value, evidence, staleness),
                    );
                }
                Resolved::Absent {
                    reason,
                    evidence,
                    staleness,
                } => match NullPolicyEngine::decide(spec, reason, data_quality, is_required) {
                    NullDecision::Reject(reason) => {
                        assembly.rejected = true;
                        values.insert(spec.name.clone(), missing_cell(reason, evidence, staleness));
                    }
                    NullDecision::Substitute { value } => {
                        values.insert(
                            spec.name.clone(),
                            FeatureCell::substituted(value, reason, evidence, staleness),
                        );
                    }
                    NullDecision::KeepMissing { reason, degrade } => {
                        assembly.degraded |= degrade;
                        values.insert(spec.name.clone(), missing_cell(reason, evidence, staleness));
                    }
                },
            }
        }

        Ok(Some(DomainFeatureSlice {
            family: inputs.family,
            schema_version: self.schema.version(),
            values,
        }))
    }

    fn missing_domain_slice(
        &self,
        family: DomainFamily,
        required: &HashSet<FeatureName>,
        data_quality: &DataQualityConfig,
        assembly: &mut Assembly,
        reason: NullReason,
    ) -> DomainFeatureSlice {
        let mut values = BTreeMap::new();
        for spec in self.schema.domain_specs() {
            let is_required = required.contains(&spec.name);
            match NullPolicyEngine::decide(spec, reason, data_quality, is_required) {
                NullDecision::Reject(reason) => {
                    assembly.rejected = true;
                    values.insert(
                        spec.name.clone(),
                        missing_cell(reason, None, FeatureStaleness::Unknown),
                    );
                }
                NullDecision::Substitute { value } => {
                    values.insert(
                        spec.name.clone(),
                        FeatureCell::substituted(value, reason, None, FeatureStaleness::Unknown),
                    );
                }
                NullDecision::KeepMissing { reason, degrade } => {
                    assembly.degraded |= degrade;
                    values.insert(
                        spec.name.clone(),
                        missing_cell(reason, None, FeatureStaleness::Unknown),
                    );
                }
            }
        }
        DomainFeatureSlice {
            family,
            schema_version: self.schema.version(),
            values,
        }
    }

    /// Compute vectors for a batch of resolved inputs, in parallel, preserving
    /// input order.
    ///
    /// `compute_vector` is pure and deterministic (statistical values are
    /// quantized), so parallel evaluation is order-independent and reproducible.
    pub fn build_batch(
        &self,
        bundles: &[ResolvedMarketBundle<'_>],
        required_features: &[FeatureName],
        config: &FeaturesConfig,
        data_quality: &DataQualityConfig,
    ) -> QuantResult<Vec<FeatureVector>> {
        bundles
            .par_iter()
            .map(|bundle| self.compute_vector(bundle, required_features, config, data_quality))
            .collect()
    }
}

/// The intermediate accumulation while assembling a vector.
struct Assembly {
    cells: BTreeMap<FeatureName, FeatureCell>,
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
                input.boundary,
                input.pit,
                FeatureSourceWindows {
                    microstructure: input.window,
                    trade_tape: input.trade_tape,
                    domain: input.domain,
                },
                Usd::ZERO,
            )
            .await?;
        self.compute_vector(
            &bundle,
            input.required_features,
            input.config,
            input.data_quality,
        )
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
    ) -> QuantResult<Assembly> {
        let mut cells = BTreeMap::new();
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
            let resolved = resolve_value(raw_feature, spec, as_of, data_quality)?;

            match resolved {
                Resolved::Present {
                    value,
                    evidence,
                    staleness,
                } => {
                    cells.insert(
                        spec.name.clone(),
                        FeatureCell::observed(value, evidence, staleness),
                    );
                }
                Resolved::Absent {
                    reason,
                    evidence,
                    staleness,
                } => match NullPolicyEngine::decide(spec, reason, data_quality, is_required) {
                    NullDecision::Reject(reason) => {
                        rejected = true;
                        cells.insert(spec.name.clone(), missing_cell(reason, evidence, staleness));
                    }
                    NullDecision::Substitute { value } => {
                        cells.insert(
                            spec.name.clone(),
                            FeatureCell::substituted(value, reason, evidence, staleness),
                        );
                    }
                    NullDecision::KeepMissing { reason, degrade } => {
                        degraded |= degrade;
                        cells.insert(spec.name.clone(), missing_cell(reason, evidence, staleness));
                    }
                },
            }
        }

        Ok(Assembly {
            cells,
            rejected,
            degraded,
        })
    }
}

/// The outcome of validating a raw feature against its spec.
enum Resolved {
    Present {
        value: FeatureValue,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    },
    Absent {
        reason: NullReason,
        evidence: Option<EvidenceSourceRef>,
        staleness: FeatureStaleness,
    },
}

/// Validate a raw feature against its spec's valid range and staleness policy.
fn resolve_value(
    raw: Option<&RawFeature>,
    spec: &FeatureSpec,
    as_of: DateTime<Utc>,
    data_quality: &DataQualityConfig,
) -> QuantResult<Resolved> {
    let Some(raw) = raw else {
        return Ok(Resolved::Absent {
            reason: NullReason::SourceUnavailable,
            evidence: None,
            staleness: FeatureStaleness::Unknown,
        });
    };
    let staleness = feature_staleness(as_of, raw.evidence.as_ref())?;
    Ok(match &raw.value {
        Err(reason) => Resolved::Absent {
            reason: *reason,
            evidence: raw.evidence.clone(),
            staleness,
        },
        Ok(value) => {
            if !in_range(spec, value)? {
                return Ok(Resolved::Absent {
                    reason: NullReason::OutOfValidRange,
                    evidence: raw.evidence.clone(),
                    staleness,
                });
            }
            // Per-feature staleness: a present value whose freshest input is
            // older than the spec's bound becomes stale and flows through the
            // null policy (it is never silently used as if fresh).
            if staleness_breach(spec, staleness, data_quality)? {
                return Ok(Resolved::Absent {
                    reason: NullReason::StaleBeyondPolicy,
                    evidence: raw.evidence.clone(),
                    staleness,
                });
            }
            Resolved::Present {
                value: value.clone(),
                evidence: raw.evidence.clone(),
                staleness,
            }
        }
    })
}

fn missing_cell(
    reason: NullReason,
    evidence: Option<EvidenceSourceRef>,
    staleness: FeatureStaleness,
) -> FeatureCell {
    if reason == NullReason::NotApplicable {
        FeatureCell::not_applicable(reason)
    } else {
        FeatureCell::missing(reason, evidence, staleness)
    }
}

fn feature_staleness(
    as_of: DateTime<Utc>,
    evidence: Option<&EvidenceSourceRef>,
) -> QuantResult<FeatureStaleness> {
    let Some(evidence) = evidence else {
        return Ok(FeatureStaleness::Unknown);
    };
    let delta = as_of.signed_duration_since(evidence.effective_at);
    let age_ms =
        u64::try_from(delta.num_milliseconds()).map_err(|_| ResearchError::PitResolution {
            detail: format!(
                "feature evidence time {} is after decision time {as_of}",
                evidence.effective_at
            ),
        })?;
    Ok(FeatureStaleness::Known { age_ms })
}

/// Whether a present value satisfies the spec's valid range (no clamping).
fn in_range(spec: &FeatureSpec, value: &FeatureValue) -> QuantResult<bool> {
    Ok(match &spec.valid_range {
        Some(range) => range.contains(&value.to_fact_decimal()?),
        None => true,
    })
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
    staleness: FeatureStaleness,
    data_quality: &DataQualityConfig,
) -> QuantResult<bool> {
    let Some(bound_ms) = staleness_bound_ms(spec.staleness_policy, data_quality)? else {
        return Ok(false);
    };
    if bound_ms == 0 {
        return Ok(false);
    }
    Ok(matches!(
        staleness,
        FeatureStaleness::Known { age_ms } if age_ms > bound_ms
    ))
}

fn staleness_bound_ms(
    policy: StalenessRule,
    data_quality: &DataQualityConfig,
) -> QuantResult<Option<u64>> {
    let seconds = match policy {
        StalenessRule::None => return Ok(None),
        StalenessRule::MaxBookAge => return Ok(Some(data_quality.max_book_age_ms)),
        StalenessRule::MaxFeatureBucketAge => data_quality.max_feature_bucket_age_secs,
        StalenessRule::MaxTradeTapeAge => data_quality.max_trade_tape_age_secs,
        StalenessRule::MaxDomainObservationAge => data_quality.max_domain_observation_age_secs,
    };
    seconds.checked_mul(1_000).map(Some).ok_or_else(|| {
        ResearchError::SchemaHashMismatch {
            detail: format!("staleness bound {seconds}s overflows milliseconds"),
        }
        .into()
    })
}

/// Book age in milliseconds at `as_of`; unknown is never encoded as zero.
fn book_age_ms(as_of: DateTime<Utc>, book: Option<&ResolvedBook>) -> QuantResult<Option<u64>> {
    book.map(|book| known_age_ms(as_of, book.effective_at))
        .transpose()
}

/// Freshest materialized feature-bucket age in milliseconds at `as_of`: age of
/// the freshest window bucket.
///
/// Point-in-time correct: a window with no buckets contributes no age (its
/// absence is handled by the null policy, not here).
fn feature_bucket_age_ms(
    as_of: DateTime<Utc>,
    window: &MarketWindowSnapshot,
) -> QuantResult<Option<u64>> {
    window
        .freshest_bucket_time()
        .map(|bucket_time| known_age_ms(as_of, bucket_time))
        .transpose()
}

/// Freshest trade-tape print age in milliseconds at `as_of`.
fn trade_tape_age_ms(
    as_of: DateTime<Utc>,
    trade_tape: &TradeTapeWindowSnapshot,
) -> QuantResult<Option<u64>> {
    trade_tape
        .freshest_trade_time()
        .map(|trade_time| known_age_ms(as_of, trade_time))
        .transpose()
}

/// Freshest domain-observation age in milliseconds at `as_of` (zero for
/// markets that carry no domain slice — absence is structural, not staleness).
fn domain_age_ms(
    as_of: DateTime<Utc>,
    domain: Option<&DomainSliceInputs>,
) -> QuantResult<Option<u64>> {
    domain
        .and_then(|inputs| inputs.data.freshest_time())
        .map(|observed_at| known_age_ms(as_of, observed_at))
        .transpose()
}

fn known_age_ms(as_of: DateTime<Utc>, observed_at: DateTime<Utc>) -> QuantResult<u64> {
    u64::try_from(as_of.signed_duration_since(observed_at).num_milliseconds()).map_err(|_| {
        ResearchError::PitResolution {
            detail: format!(
                "freshness observation time {observed_at} is after decision time {as_of}"
            ),
        }
        .into()
    })
}

/// Per-source freshness ages feeding the aggregate classification.
#[derive(Copy, Clone)]
struct FreshnessAges {
    book: Option<u64>,
    feature_bucket: Option<u64>,
    trade_tape: Option<u64>,
    domain: Option<u64>,
}

/// Classify the vector's aggregate data quality across freshness dimensions:
/// book age, feature-bucket age, trade-tape age, and domain-observation age.
fn classify(
    rejected: bool,
    degraded: bool,
    ages: FreshnessAges,
    data_quality: &DataQualityConfig,
) -> QuantResult<DataQualityStatus> {
    if rejected {
        return Ok(DataQualityStatus::Insufficient);
    }
    let book_bound = data_quality.max_book_age_ms;
    let bucket_bound = staleness_bound_ms(StalenessRule::MaxFeatureBucketAge, data_quality)?
        .ok_or_else(|| ResearchError::SchemaHashMismatch {
            detail: "feature-bucket staleness policy has no bound".to_owned(),
        })?;
    let tape_bound =
        staleness_bound_ms(StalenessRule::MaxTradeTapeAge, data_quality)?.ok_or_else(|| {
            ResearchError::SchemaHashMismatch {
                detail: "trade-tape staleness policy has no bound".to_owned(),
            }
        })?;
    let domain_bound = staleness_bound_ms(StalenessRule::MaxDomainObservationAge, data_quality)?
        .ok_or_else(|| ResearchError::SchemaHashMismatch {
            detail: "domain-observation staleness policy has no bound".to_owned(),
        })?;
    if exceeds(ages.book, book_bound)
        || exceeds(ages.feature_bucket, bucket_bound)
        || exceeds(ages.trade_tape, tape_bound)
        || exceeds(ages.domain, domain_bound)
    {
        return Ok(DataQualityStatus::Stale);
    }
    if degraded {
        return Ok(DataQualityStatus::Degraded);
    }
    if within_half(ages.book, book_bound)
        && within_half(ages.feature_bucket, bucket_bound)
        && within_half(ages.trade_tape, tape_bound)
        && within_half(ages.domain, domain_bound)
    {
        Ok(DataQualityStatus::Fresh)
    } else {
        Ok(DataQualityStatus::Acceptable)
    }
}

/// Whether `age` exceeds a non-zero `bound` (a zero bound is unbounded).
const fn exceeds(age: Option<u64>, bound: u64) -> bool {
    matches!(age, Some(age) if bound > 0 && age > bound)
}

/// Whether `age` is within half of a non-zero `bound` (a zero bound is satisfied).
const fn within_half(age: Option<u64>, bound: u64) -> bool {
    matches!(age, Some(age) if bound == 0 || age <= bound / 2)
}
