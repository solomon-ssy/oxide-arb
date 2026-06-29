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
        book::PriceBookFeatureBuilder,
        decision_capture::{
            ResolvedMarketBundle, empty_book, market_decision_capture_from_resolved,
            stub_market_context,
        },
        domain::DomainFeatureSkeleton,
        market::MarketMetadataFeatureBuilder,
        microstructure::MicrostructureFeatureBuilder,
        null_policy::{NullDecision, NullPolicyEngine},
        resolved::{MarketWindowSnapshot, ResolvedBook, ResolvedMarketContext},
        schema::{FeatureSchema, FeatureSpec, StalenessRule},
        timeseries::TimeSeriesFeatureBuilder,
        value::{
            EvidenceSourceRef, FeatureName, FeatureValue, NullReason, SubstitutionAudit,
            merged_required_features,
        },
    },
    selection::SelectedMarket,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{BookSnapshotRef, Usd};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::DataQualityStatus},
    runtime_config::{DataQualityConfig, FeatureFamily, FeaturesConfig},
    types::SchemaVersion,
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
}

/// The configured, composable feature builder.
pub struct ConfiguredFeatureBuilder {
    schema: FeatureSchema,
    groups: Vec<Box<dyn FeatureGroupBuilder>>,
    /// Precomputed PIT gating: whether any spec needs a resolved order book.
    needs_book: bool,
    /// Precomputed PIT gating: whether any spec needs Gamma market metadata.
    needs_market: bool,
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
    pub fn new(config: &FeaturesConfig) -> Self {
        let schema = FeatureSchema::build(config);
        let mut groups: Vec<Box<dyn FeatureGroupBuilder>> = Vec::new();
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
                FeatureFamily::Domain => groups.push(Box::new(DomainFeatureSkeleton)),
            }
        }
        let needs_book = schema.needs_book();
        let needs_market = schema.needs_market_metadata();
        Self {
            schema,
            groups,
            needs_book,
            needs_market,
        }
    }

    /// The governed schema this builder produces.
    #[must_use]
    pub const fn schema(&self) -> &FeatureSchema {
        &self.schema
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
        window: &'a MarketWindowSnapshot,
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
        let book = if self.needs_book {
            Some(capture.book.clone())
        } else {
            None
        };
        let market_ctx = if self.needs_market {
            Some(capture.market.clone())
        } else {
            None
        };
        Ok(ResolvedMarketBundle {
            inputs: ResolvedInputs {
                market,
                as_of,
                book,
                market_ctx,
                window,
            },
            capture,
        })
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
            config,
            data_quality,
            book_snapshot_ref: Some(&bundle.capture.book_snapshot_ref),
        };

        // Collect every group's raw features into a name-keyed map.
        let mut raw: BTreeMap<FeatureName, RawFeature> = BTreeMap::new();
        for group in &self.groups {
            for feature in group.compute(&ctx) {
                raw.insert(feature.name.clone(), feature);
            }
        }

        let required = merged_required_features(required_features, config);
        let assembly = self.assemble(&raw, &required, data_quality, resolved.as_of);

        let book_age_ms = book_age_ms(resolved.as_of, resolved.book.as_ref());
        let fact_lag_ms = fact_lag_ms(resolved.as_of, resolved.window);
        let data_quality_status = classify(
            assembly.rejected,
            assembly.degraded,
            book_age_ms,
            fact_lag_ms,
            data_quality,
        );

        FeatureVector {
            market_id: resolved.market.market_id.clone(),
            token_id: Some(resolved.market.primary_token_id.clone()),
            as_of: resolved.as_of,
            schema_version: self.schema.version(),
            values: assembly.values,
            substitutions: assembly.substitutions,
            data_quality: data_quality_status,
            staleness_ms: book_age_ms.max(fact_lag_ms),
            source_refs: assembly.evidence,
        }
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
                input.window,
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
/// `MaxBookAge` is bounded by `data_quality.max_book_age_ms`; `MaxFactLag` by
/// `data_quality.max_fact_lag_secs`. A bound of zero (or [`StalenessRule::None`])
/// means unbounded. A feature with no evidence carries no measurable age and is
/// never marked stale here (its absence is handled elsewhere).
fn staleness_breach(
    spec: &FeatureSpec,
    evidence: Option<&EvidenceSourceRef>,
    as_of: DateTime<Utc>,
    data_quality: &DataQualityConfig,
) -> bool {
    let bound_ms = match spec.staleness_policy {
        StalenessRule::None => return false,
        StalenessRule::MaxBookAge => data_quality.max_book_age_ms,
        StalenessRule::MaxFactLag => data_quality.max_fact_lag_secs.saturating_mul(1_000),
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

/// Worst fact lag in milliseconds at `as_of`: age of the freshest window bucket.
///
/// Point-in-time correct: a window with no buckets contributes no lag (its
/// absence is handled by the null policy, not here).
fn fact_lag_ms(as_of: DateTime<Utc>, window: &MarketWindowSnapshot) -> u64 {
    window
        .freshest_bucket_time()
        .map_or(0, |bucket_time| age_ms(as_of, bucket_time))
}

/// Classify the vector's aggregate data quality across both freshness
/// dimensions: book age (bounded by `max_book_age_ms`) and fact lag (bounded by
/// `max_fact_lag_secs`). The two are judged independently — fact lag is never
/// measured against the book-age bound.
const fn classify(
    rejected: bool,
    degraded: bool,
    book_age_ms: u64,
    fact_lag_ms: u64,
    data_quality: &DataQualityConfig,
) -> DataQualityStatus {
    if rejected {
        return DataQualityStatus::Insufficient;
    }
    let book_bound = data_quality.max_book_age_ms;
    let fact_bound = data_quality.max_fact_lag_secs.saturating_mul(1_000);
    if exceeds(book_age_ms, book_bound) || exceeds(fact_lag_ms, fact_bound) {
        return DataQualityStatus::Stale;
    }
    if degraded {
        return DataQualityStatus::Degraded;
    }
    if within_half(book_age_ms, book_bound) && within_half(fact_lag_ms, fact_bound) {
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
