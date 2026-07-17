//! Frozen market → external-subject linkage records (Phase 11.2.2).
//!
//! A [`MarketLinkage`] is the content-addressed, bitemporal binding between a
//! Polymarket market and the external subject it is about (today: a crypto
//! underlying-price question). Records are produced **offline** by the layered
//! deterministic resolver, validated by the grounding gate, and appended to the
//! `quant_market_linkage` ledger. The online / PIT hot path only ever reads
//! frozen records — zero parsing, zero external calls, 100% deterministic.
//!
//! # Bitemporal axes
//!
//! - **Effective axis** — [`MarketLinkage::effective_at`]: when the resolved
//!   outcome became true in source time. It must not exceed the linkage source
//!   cutoff for a decision.
//! - **Availability axis** — [`MarketLinkage::available_at`]: when this ledger
//!   revision became knowable to the system. It must not exceed the decision
//!   time, which prevents a late-arriving backdated correction from leaking
//!   into historical replay.
//!
//! [`MarketLinkage::metadata_hash`] and [`MarketLinkage::resolver_version`]
//! bind the exact metadata snapshot and deterministic ruleset that produced a
//! revision; they are provenance dimensions, not substitutes for either time
//! axis.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, governance::GovernanceError, hashing::CanonicalDigestError};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_market_linkage,
    enums::domain::{DomainFamily, KlineInterval, LinkageSourceRole, LinkageStatus, ResolverTier},
    hashing::CanonicalDigest,
    types::{
        BinanceSymbol, ChainlinkFeedKey, ContentHash, CryptoAsset, CryptoQuote,
        DomainInstrumentKey, DomainSourceId, IcaoStation, MarketId, MarketLinkageId, Probability,
        ResolverVersion, TemperatureBand, TemperatureUnit, Usd,
    },
};

/// The frozen Gamma metadata snapshot one linkage derivation reads.
///
/// This is the resolver's **entire** input surface: every grounding span must
/// anchor into one of these fields, and [`Self::metadata_hash`] is the
/// bitemporal knowledge axis persisted on the derived record. Any metadata
/// revision (question edit, description edit, series change, end-date move)
/// changes the hash and triggers re-resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkageSourceMetadata {
    /// The market being resolved.
    pub market_id: MarketId,
    /// Market slug (Tier-0 deterministic anchor).
    pub slug: String,
    /// Market question / title.
    pub question: String,
    /// Market rules text (resolution-source sentence lives here).
    pub description: Option<String>,
    /// Owning event's recurring-series slug, when present.
    pub series_slug: Option<String>,
    /// Scheduled resolution time, when published.
    pub end_date: Option<DateTime<Utc>>,
}

impl LinkageSourceMetadata {
    /// Canonical hash of this metadata snapshot (the knowledge axis).
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn metadata_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(self)
    }
}

/// How a crypto market's question compares the underlying price to its strike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PriceComparator {
    /// Resolves YES when the underlying settles at or above the strike.
    Above,
    /// Resolves YES when the underlying settles at or below the strike.
    Below,
    /// Resolves YES when the underlying settles inside `[strike, hi]`.
    Between {
        /// Inclusive upper bound of the band (the strike is the lower bound).
        hi: Usd,
    },
    /// Up-or-down market: resolves against the reference observation at
    /// [`CryptoSubject::reference_at`] (no absolute strike exists).
    UpVsReference,
}

/// The settlement oracle a crypto market resolves against.
///
/// Feature-source alignment: Binance klines is always the *feature* source;
/// when the oracle is Chainlink the basis between the two is modeled explicitly
/// (`domain.crypto.basis_vs_resolution_source`). Label truth is **always** the
/// persisted `market_resolution_event` — never any oracle quote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolutionOracle {
    /// Chainlink Data Streams feed (short-period up/down markets; the rules
    /// text carries a literal `data.chain.link/streams/{feed}` anchor).
    ChainlinkDataStreams {
        /// Normalized feed key, e.g. `BTC-USD`.
        feed: ChainlinkFeedKey,
    },
    /// Binance candle close (daily / threshold markets whose rules cite the
    /// Binance 1-minute candle).
    BinanceKline {
        /// Venue symbol, e.g. `BTCUSDT`.
        symbol: BinanceSymbol,
        /// Candle interval cited by the rules text.
        interval: KlineInterval,
    },
}

/// The extracted subject of a crypto market: which underlying, compared how,
/// against what, observed when, settled by which oracle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptoSubject {
    /// Base asset ticker (e.g. `BTC`).
    pub asset: CryptoAsset,
    /// Quote currency (e.g. `USD`).
    pub quote: CryptoQuote,
    /// How the settlement price is compared.
    pub comparator: PriceComparator,
    /// Absolute strike in quote currency; `None` for up/down (relative) markets.
    pub strike: Option<Usd>,
    /// Reference observation instant for up/down markets (the window open).
    pub reference_at: Option<DateTime<Utc>>,
    /// Settlement observation instant (UTC; ET slugs are normalized upstream).
    pub observation_at: DateTime<Utc>,
    /// The oracle the market's rules settle against.
    pub resolution_oracle: ResolutionOracle,
}

/// Frozen airport daily-high subject supported by the Weather vertical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherSubject {
    /// ICAO station whose observations proxy the settlement location.
    pub station: IcaoStation,
    /// IANA timezone used to derive the local calendar day.
    pub timezone: String,
    /// Local calendar date in `timezone`.
    pub local_date: chrono::NaiveDate,
    /// Recommended token's inclusive temperature band.
    pub outcome_band: TemperatureBand,
    /// Unit displayed by the market contract.
    pub market_unit: TemperatureUnit,
    /// Frozen settlement-rule URL. It is evidence only and is never scraped.
    pub settlement_rule_url: String,
    /// Canonical hash of the station coordinates, elevation and source station ids.
    pub station_profile_hash: ContentHash,
    /// Canonical hash of the whole-degree, midpoint-away-from-zero proxy methodology.
    pub proxy_methodology_hash: ContentHash,
}

/// One exact source/instrument binding frozen into a linkage revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSourceBinding {
    pub role: LinkageSourceRole,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    /// First instant this binding was knowable; never backdated by migration.
    pub available_at: DateTime<Utc>,
    /// Source-specific immutable configuration/profile digest.
    pub binding_hash: ContentHash,
}

/// A market's extracted external subject, one variant per domain family.
///
/// Additive: sports / politics / weather / geopolitics verticals extend this
/// enum without touching the crypto path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum MarketSubject {
    /// Crypto underlying-price subject.
    Crypto(CryptoSubject),
    /// Airport local-day maximum-temperature subject.
    Weather(WeatherSubject),
}

impl MarketSubject {
    /// The domain family this subject belongs to.
    #[must_use]
    pub const fn family(&self) -> DomainFamily {
        match self {
            Self::Crypto(_) => DomainFamily::Crypto,
            Self::Weather(_) => DomainFamily::Weather,
        }
    }
}

/// Which metadata field a grounding span was located in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroundingField {
    /// The market slug.
    Slug,
    /// The market question / title.
    Question,
    /// The market description (rules text).
    Description,
    /// The owning event's series slug.
    SeriesSlug,
}

/// Whether a grounding span is independently-extracted literal evidence or a
/// value that is a deterministic function of a fully-matched template.
///
/// The distinction matters because "every field is grounded" cannot mean the
/// same check for every field: `asset` / `strike` / `resolution_oracle` are
/// each extracted from an independent literal occurrence in the source text
/// and must anchor to their own minimal span, while `comparator` /
/// `reference_at` / `observation_at` on a matched up/down template are not
/// separately "written" anywhere in the slug — their value is entailed by the
/// template match as a whole. Collapsing both into one "any span accepted"
/// rule (a bare byte-equality check against an arbitrary span) is exactly the
/// pseudo-grounding hole the anti-hallucination gate exists to close: this
/// type makes the validator enforce the stronger rule (an independently
/// literal span) precisely where it applies, never substituting a
/// template-wide span for genuinely-extracted evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroundingKind {
    /// A minimal literal excerpt of the source text for a value extracted
    /// independently of any surrounding template (asset alias, dollar
    /// amount, oracle citation).
    LiteralSpan,
    /// The span covers the entire recognized template match because the
    /// field's value is a deterministic function of that template as a
    /// whole (e.g. the comparator / window instants of a matched up/down
    /// slug) — never used for a field whose value could vary independently
    /// of the template match.
    TemplateEntailed,
    /// A literal excerpt an operator cited as justification for a manual
    /// override (11.2.2 remediation R4). Distinct from [`Self::LiteralSpan`]
    /// because the citation was never produced by a deterministic extractor
    /// pattern — it is still required to be a byte-exact substring of the
    /// cited source field (the anti-hallucination bar is never relaxed), but
    /// the *provenance* (human judgment vs. automated pattern match) is
    /// audited separately.
    ManualEvidence,
}

/// One operator-submitted literal-text justification for a manual override.
///
/// (11.2.2 remediation R4). The operator names which subject field the text
/// evidences and which metadata field the text was copied from; the
/// resolver-side validator (never the wire layer) verifies the text is a
/// real, byte-exact substring of that field before it becomes a
/// [`GroundingSpan`] — an override can cite real text, never fabricate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualEvidenceInput {
    /// Dotted subject field path this evidence justifies (e.g. `asset`).
    pub subject_field: String,
    /// Which metadata field the operator copied the text from.
    pub source: GroundingField,
    /// The literal text the operator cites — verified as a byte-exact
    /// substring of `source`'s content, never trusted as-is.
    pub text: String,
}

/// One extracted subject field tied to the literal source-text span it came from.
///
/// The anti-hallucination contract: a candidate whose field cannot be anchored to
/// a literal span is rejected, regardless of which tier produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroundingSpan {
    /// Dotted subject field path (e.g. `asset`, `strike`, `resolution_oracle`).
    pub subject_field: String,
    /// Which metadata field the span was found in.
    pub source: GroundingField,
    /// Byte offset of the span start within the source field.
    pub start: usize,
    /// Byte offset of the span end (exclusive).
    pub end: usize,
    /// The literal matched text (denormalized for audit display).
    pub text: String,
    /// Whether this is independently-literal evidence or template-entailed.
    pub kind: GroundingKind,
}

/// The full field → source-span mapping for one accepted subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroundingProof {
    /// One span per grounded subject field.
    pub spans: Vec<GroundingSpan>,
}

/// The audited human justification for an operator override.
///
/// Present **only** on a `resolver_tier = Override` binding — every
/// automated-tier resolution carries `None`. This is the override's real
/// audit trail (never discarded, unlike the pre-remediation stub that dropped
/// the operator's stated reason and never recorded who acted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverrideContext {
    /// Operator-supplied justification (validated non-empty on the wire).
    pub reason: String,
    /// The authenticated actor who performed the override.
    pub actor: String,
}

/// A validated subject binding: the subject, the feature-source instrument it
/// joins to, and the grounding proof that anchored every extracted field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    /// The extracted, validated subject.
    pub subject: MarketSubject,
    /// Exact multi-source bindings, with one row per role/source/instrument.
    pub source_bindings: Vec<ResolvedSourceBinding>,
    /// Field → literal-span grounding proof (empty for an override — an
    /// operator decision is not text-extracted evidence; see
    /// [`Self::override_context`] for its real audit trail).
    pub grounding: GroundingProof,
    /// The audited justification, present iff this binding came from an
    /// operator override.
    pub override_context: Option<OverrideContext>,
}

/// The resolver's outcome for one `(market, metadata, ruleset)` triple.
///
/// Structurally enforces the fail-closed invariant: a binding exists **iff**
/// the record is resolved — there is no state where a subject floats without
/// grounding, or an unresolved record carries a half-built subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LinkageOutcome {
    /// A validated binding exists.
    Resolved(Box<ResolvedBinding>),
    /// No tier produced a validated subject; the domain plane fails closed.
    Unresolved {
        /// Why resolution failed (operator-facing, drives the review queue).
        reason: String,
    },
}

/// A frozen, content-addressed, bitemporal market → external-subject linkage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketLinkage {
    /// Ledger row id.
    pub linkage_id: MarketLinkageId,
    /// The linked market.
    pub market_id: MarketId,
    /// The vertical this record belongs to.
    pub domain_family: DomainFamily,
    /// Resolver outcome (binding iff resolved).
    pub outcome: LinkageOutcome,
    /// Resolver confidence in `[0, 1]` (deterministic tiers emit `1`).
    pub confidence: Probability,
    /// Which tier produced this record.
    pub resolver_tier: ResolverTier,
    /// The frozen ruleset version that produced this record.
    pub resolver_version: ResolverVersion,
    /// Canonical hash of the Gamma metadata snapshot this record was derived
    /// from (immutable provenance, distinct from the two time axes).
    pub metadata_hash: ContentHash,
    /// Content address over the full outcome (idempotent-write key).
    pub content_hash: ContentHash,
    /// Source-effective instant of this resolver outcome. A PIT reader may use
    /// the row only when this is at or before the frozen linkage cutoff.
    pub effective_at: DateTime<Utc>,
    /// System-availability instant assigned by the append-only ledger. A PIT
    /// reader may use the row only when this is at or before `decision_at`.
    /// It is also the second stable ordering key when two revisions share an
    /// effective instant. This field exists only on a database-rehydrated
    /// ledger record; pre-append derivations use [`MarketLinkageDerivation`]
    /// and cannot supply it.
    pub available_at: DateTime<Utc>,
}

/// Canonical projection hashed into [`MarketLinkage::content_hash`].
///
/// Excludes the surrogate id and effective/availability clocks so re-running the same resolver
/// over the same metadata is a no-op (idempotent append).
#[derive(Serialize)]
struct LinkageHashInput<'a> {
    market_id: &'a MarketId,
    domain_family: DomainFamily,
    outcome: &'a LinkageOutcome,
    resolver_tier: ResolverTier,
    resolver_version: ResolverVersion,
    metadata_hash: &'a ContentHash,
}

impl MarketLinkage {
    /// Compute the canonical content hash for a linkage outcome.
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn compute_content_hash(
        market_id: &MarketId,
        domain_family: DomainFamily,
        outcome: &LinkageOutcome,
        resolver_tier: ResolverTier,
        resolver_version: ResolverVersion,
        metadata_hash: &ContentHash,
    ) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(&LinkageHashInput {
            market_id,
            domain_family,
            outcome,
            resolver_tier,
            resolver_version,
            metadata_hash,
        })
    }

    /// Lifecycle status derived from the outcome and tier (never stored apart
    /// from its inputs — an overridden record is a resolved record whose tier
    /// is [`ResolverTier::Override`]).
    #[must_use]
    pub const fn status(&self) -> LinkageStatus {
        match (&self.outcome, self.resolver_tier) {
            (LinkageOutcome::Unresolved { .. }, _) => LinkageStatus::Unresolved,
            (LinkageOutcome::Resolved(_), ResolverTier::Override) => LinkageStatus::Overridden,
            (LinkageOutcome::Resolved(_), _) => LinkageStatus::Resolved,
        }
    }

    /// The validated binding, when this record is resolved.
    #[must_use]
    pub const fn binding(&self) -> Option<&ResolvedBinding> {
        match &self.outcome {
            LinkageOutcome::Resolved(binding) => Some(binding),
            LinkageOutcome::Unresolved { .. } => None,
        }
    }
}

// ── Persistence DTOs (`quant_market_linkage`) ───────────────────────────────

/// Ledger row projection of one frozen linkage record.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_market_linkage::Entity")]
pub struct MarketLinkageInfo {
    pub linkage_id: MarketLinkageId,
    pub market_id: MarketId,
    pub domain_family: DomainFamily,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    pub outcome: serde_json::Value,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    /// First-class projection of `outcome.override_context.reason` (11.2.2
    /// remediation R4) — `None` unless `resolver_tier = override`.
    pub override_reason: Option<String>,
    /// First-class projection of `outcome.override_context.actor`.
    pub override_actor: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    MarketLinkageInfo,
    quant_market_linkage::Model,
    {
        linkage_id,
        market_id,
        domain_family,
        status,
        resolver_tier,
        resolver_version,
        confidence,
        outcome,
        metadata_hash,
        content_hash,
        derived_at,
        override_reason,
        override_actor,
        created_at,
    }
);

/// Insert payload for `quant_market_linkage`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_market_linkage::ActiveModel")]
pub struct NewMarketLinkage {
    pub linkage_id: MarketLinkageId,
    pub market_id: MarketId,
    pub domain_family: DomainFamily,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    pub outcome: serde_json::Value,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    pub override_reason: Option<String>,
    pub override_actor: Option<String>,
}

/// Complete resolver output before the database assigns system availability.
///
/// Deliberately has no `available_at`/`created_at`: those clocks are facts of
/// durable append, not values a resolver may guess. The domain family is also
/// derived from the outcome, so it cannot disagree with a resolved subject.
#[derive(Debug, Clone)]
pub struct MarketLinkageDerivation {
    pub market_id: MarketId,
    pub outcome: LinkageOutcome,
    pub confidence: Probability,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub metadata_hash: ContentHash,
    pub effective_at: DateTime<Utc>,
}

impl NewMarketLinkage {
    /// Build the append payload directly from one resolver derivation.
    ///
    /// This is intentionally distinct from [`MarketLinkage`]: an append
    /// payload has a source-effective clock but cannot know the database-owned
    /// availability clock yet. Only a row reloaded as [`MarketLinkageInfo`] can
    /// become a PIT-readable [`MarketLinkage`], so callers cannot populate
    /// `available_at` with a placeholder.
    ///
    /// Status, content address, denormalized instrument, and override audit
    /// columns are all projected here from the same outcome.
    ///
    /// # Errors
    ///
    /// Returns typed hashing or governance serialization failures; no partial
    /// payload is produced.
    pub fn from_derivation(derivation: MarketLinkageDerivation) -> QuantResult<Self> {
        let MarketLinkageDerivation {
            market_id,
            outcome,
            confidence,
            resolver_tier,
            resolver_version,
            metadata_hash,
            effective_at,
        } = derivation;
        let domain_family = match &outcome {
            LinkageOutcome::Resolved(binding) => binding.subject.family(),
            // The linkage resolver currently owns only the crypto vertical;
            // unresolved outcomes have no subject from which to derive it.
            LinkageOutcome::Unresolved { .. } => DomainFamily::Crypto,
        };
        let status = match (&outcome, resolver_tier) {
            (LinkageOutcome::Unresolved { .. }, _) => LinkageStatus::Unresolved,
            (LinkageOutcome::Resolved(_), ResolverTier::Override) => LinkageStatus::Overridden,
            (LinkageOutcome::Resolved(_), _) => LinkageStatus::Resolved,
        };
        let binding = match &outcome {
            LinkageOutcome::Resolved(binding) => Some(binding),
            LinkageOutcome::Unresolved { .. } => None,
        };
        let content_hash = MarketLinkage::compute_content_hash(
            &market_id,
            domain_family,
            &outcome,
            resolver_tier,
            resolver_version,
            &metadata_hash,
        )?;
        let override_context = binding.and_then(|binding| binding.override_context.as_ref());
        let override_reason = override_context.map(|context| context.reason.clone());
        let override_actor = override_context.map(|context| context.actor.clone());
        let outcome = serde_json::to_value(outcome).map_err(|error| {
            GovernanceError::LinkagePayloadSerialization {
                detail: error.to_string(),
            }
        })?;

        Ok(Self {
            linkage_id: MarketLinkageId::from_v7(),
            market_id,
            domain_family,
            status,
            resolver_tier,
            resolver_version,
            confidence,
            outcome,
            metadata_hash,
            content_hash,
            derived_at: effective_at,
            override_reason,
            override_actor,
        })
    }
}

impl MarketLinkageInfo {
    /// Source-effective clock used by PIT visibility checks.
    #[must_use]
    pub const fn effective_at(&self) -> DateTime<Utc> {
        self.derived_at
    }

    /// System-availability clock used by PIT visibility checks.
    #[must_use]
    pub const fn available_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Decode the ledger row back into the domain record.
    ///
    /// # Errors
    ///
    /// Propagates outcome-payload deserialization failures.
    pub fn into_domain(self) -> Result<MarketLinkage, serde_json::Error> {
        Ok(MarketLinkage {
            linkage_id: self.linkage_id,
            market_id: self.market_id,
            domain_family: self.domain_family,
            outcome: serde_json::from_value(self.outcome)?,
            confidence: self.confidence,
            resolver_tier: self.resolver_tier,
            resolver_version: self.resolver_version,
            metadata_hash: self.metadata_hash,
            content_hash: self.content_hash,
            effective_at: self.derived_at,
            available_at: self.created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CryptoSubject, GroundingProof, GroundingSpan, LinkageOutcome, MarketLinkage,
        MarketLinkageDerivation, MarketSubject, NewMarketLinkage, PriceComparator,
        ResolutionOracle, ResolvedBinding, ResolvedSourceBinding,
    };
    use crate::{
        domain::{GroundingField, GroundingKind},
        enums::domain::{
            DomainFamily, KlineInterval, LinkageSourceRole, LinkageStatus, ResolverTier,
        },
        types::{
            BinanceSymbol, ContentHash, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            DomainSourceId, MarketId, MarketLinkageId, Probability, ResolverVersion,
        },
    };
    use chrono::Utc;

    fn sample_binding() -> ResolvedBinding {
        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(Utc::now()),
                observation_at: Utc::now(),
                resolution_oracle: ResolutionOracle::BinanceKline {
                    symbol: symbol.clone(),
                    interval: KlineInterval::OneMinute,
                },
            }),
            source_bindings: vec![ResolvedSourceBinding {
                role: LinkageSourceRole::Feature,
                source_id: DomainSourceId::binance(),
                instrument_key: DomainInstrumentKey::binance_kline(
                    &symbol,
                    KlineInterval::OneMinute,
                ),
                available_at: Utc::now(),
                binding_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64)))
                    .expect("binding hash"),
            }],
            grounding: GroundingProof {
                spans: vec![GroundingSpan {
                    subject_field: "asset".to_owned(),
                    source: GroundingField::Slug,
                    start: 0,
                    end: 3,
                    text: "btc".to_owned(),
                    kind: GroundingKind::LiteralSpan,
                }],
            },
            override_context: None,
        }
    }

    fn sample_linkage(outcome: LinkageOutcome, tier: ResolverTier) -> MarketLinkage {
        let market_id = MarketId::new("0xmarket");
        let metadata_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        let content_hash = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &outcome,
            tier,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("content hash");
        MarketLinkage {
            linkage_id: MarketLinkageId::from_v7(),
            market_id,
            domain_family: DomainFamily::Crypto,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: tier,
            resolver_version: ResolverVersion::FIRST,
            metadata_hash,
            content_hash,
            effective_at: Utc::now(),
            available_at: Utc::now(),
        }
    }

    #[test]
    fn status_derives_from_outcome_and_tier() {
        let resolved = sample_linkage(
            LinkageOutcome::Resolved(Box::new(sample_binding())),
            ResolverTier::Tier0Slug,
        );
        assert_eq!(resolved.status(), LinkageStatus::Resolved);
        assert!(resolved.binding().is_some());

        let overridden = sample_linkage(
            LinkageOutcome::Resolved(Box::new(sample_binding())),
            ResolverTier::Override,
        );
        assert_eq!(overridden.status(), LinkageStatus::Overridden);

        let unresolved = sample_linkage(
            LinkageOutcome::Unresolved {
                reason: "no template matched".to_owned(),
            },
            ResolverTier::Tier1Template,
        );
        assert_eq!(unresolved.status(), LinkageStatus::Unresolved);
        assert!(unresolved.binding().is_none());
    }

    #[test]
    fn append_derivation_cannot_fabricate_system_availability() {
        let effective_at = Utc::now();
        let pending = NewMarketLinkage::from_derivation(MarketLinkageDerivation {
            market_id: MarketId::new("0xpending"),
            outcome: LinkageOutcome::Resolved(Box::new(sample_binding())),
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Tier0Slug,
            resolver_version: ResolverVersion::FIRST,
            metadata_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            effective_at,
        })
        .expect("derivation");

        assert_eq!(pending.derived_at, effective_at);
        let payload = serde_json::to_value(pending).expect("serialize append payload");
        assert!(payload.get("available_at").is_none());
        assert!(
            payload.get("created_at").is_none(),
            "database-owned availability must not exist before append"
        );
    }

    #[test]
    fn content_hash_is_idempotent_and_outcome_sensitive() {
        let market_id = MarketId::new("0xmarket");
        let metadata_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        let resolved = LinkageOutcome::Resolved(Box::new(sample_binding()));
        let a = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &resolved,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("hash");
        let b = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &resolved,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("hash");
        assert_eq!(a, b, "same inputs must be idempotent");

        let unresolved = LinkageOutcome::Unresolved {
            reason: "miss".to_owned(),
        };
        let c = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &unresolved,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("hash");
        assert_ne!(a, c, "outcome must perturb the digest");
    }
}
