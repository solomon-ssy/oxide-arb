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
//! - **Knowledge axis** — [`MarketLinkage::metadata_hash`]: the canonical hash
//!   of the Gamma metadata snapshot the record was derived from. A metadata
//!   revision triggers re-resolution; point-in-time reads never see a linkage
//!   derived from future metadata.
//! - **Ruleset axis** — [`MarketLinkage::resolver_version`]: the frozen
//!   deterministic ruleset (asset aliases, symbol/feed bindings, templates)
//!   that produced the record, so growing the ruleset never rewrites history.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    enums::domain::{DomainFamily, KlineInterval, LinkageStatus, ResolverTier},
    hashing::CanonicalDigest,
    types::{
        BinanceSymbol, ChainlinkFeedKey, ContentHash, CryptoAsset, CryptoQuote,
        DomainInstrumentKey, MarketId, MarketLinkageId, Probability, ResolverVersion, Usd,
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
    /// An oracle the resolver recognized as present but could not classify.
    /// Basis cross-checking fails closed for this variant.
    Other {
        /// The literal rules-text fragment describing the oracle.
        descriptor: String,
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

/// A market's extracted external subject, one variant per domain family.
///
/// Additive: sports / politics / weather / geopolitics verticals extend this
/// enum without touching the crypto path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum MarketSubject {
    /// Crypto underlying-price subject.
    Crypto(CryptoSubject),
}

impl MarketSubject {
    /// The domain family this subject belongs to.
    #[must_use]
    pub const fn family(&self) -> DomainFamily {
        match self {
            Self::Crypto(_) => DomainFamily::Crypto,
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
}

/// The full field → source-span mapping for one accepted subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroundingProof {
    /// One span per grounded subject field.
    pub spans: Vec<GroundingSpan>,
}

/// A validated subject binding: the subject, the feature-source instrument it
/// joins to, and the grounding proof that anchored every extracted field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBinding {
    /// The extracted, validated subject.
    pub subject: MarketSubject,
    /// Canonical feature-source instrument key (e.g. `BINANCE:BTCUSDT:1m`).
    pub instrument_key: DomainInstrumentKey,
    /// Field → literal-span grounding proof.
    pub grounding: GroundingProof,
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
    Resolved(ResolvedBinding),
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
    /// from (the bitemporal knowledge axis).
    pub metadata_hash: ContentHash,
    /// Content address over the full outcome (idempotent-write key).
    pub content_hash: ContentHash,
    /// When the resolver derived this record (PIT visibility instant).
    pub derived_at: DateTime<Utc>,
}

/// Canonical projection hashed into [`MarketLinkage::content_hash`].
///
/// Excludes the surrogate id and `derived_at` so re-running the same resolver
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
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
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
    pub instrument_key: Option<DomainInstrumentKey>,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    MarketLinkageInfo,
    crate::entities::quant_market_linkage::Model,
    {
        linkage_id,
        market_id,
        domain_family,
        status,
        resolver_tier,
        resolver_version,
        confidence,
        outcome,
        instrument_key,
        metadata_hash,
        content_hash,
        derived_at,
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
    pub instrument_key: Option<DomainInstrumentKey>,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
}

impl MarketLinkage {
    /// Project this record into a `quant_market_linkage` insert payload.
    ///
    /// The derived status and denormalized instrument key are computed here so
    /// no writer can persist a row that disagrees with its own outcome.
    ///
    /// # Errors
    ///
    /// Propagates outcome-payload serialization failures.
    pub fn to_new(&self) -> Result<NewMarketLinkage, serde_json::Error> {
        Ok(NewMarketLinkage {
            linkage_id: self.linkage_id.clone(),
            market_id: self.market_id.clone(),
            domain_family: self.domain_family,
            status: self.status(),
            resolver_tier: self.resolver_tier,
            resolver_version: self.resolver_version,
            confidence: self.confidence,
            outcome: serde_json::to_value(&self.outcome)?,
            instrument_key: self.binding().map(|binding| binding.instrument_key.clone()),
            metadata_hash: self.metadata_hash.clone(),
            content_hash: self.content_hash.clone(),
            derived_at: self.derived_at,
        })
    }
}

impl MarketLinkageInfo {
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
            derived_at: self.derived_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CryptoSubject, GroundingProof, GroundingSpan, LinkageOutcome, MarketLinkage, MarketSubject,
        PriceComparator, ResolutionOracle, ResolvedBinding,
    };
    use crate::{
        enums::domain::{DomainFamily, KlineInterval, LinkageStatus, ResolverTier},
        types::{
            BinanceSymbol, ContentHash, CryptoAsset, CryptoQuote, DomainInstrumentKey, MarketId,
            MarketLinkageId, Probability, ResolverVersion,
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
            instrument_key: DomainInstrumentKey::binance_kline(&symbol, KlineInterval::OneMinute),
            grounding: GroundingProof {
                spans: vec![GroundingSpan {
                    subject_field: "asset".to_owned(),
                    source: super::GroundingField::Slug,
                    start: 0,
                    end: 3,
                    text: "btc".to_owned(),
                }],
            },
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
            derived_at: Utc::now(),
        }
    }

    #[test]
    fn status_derives_from_outcome_and_tier() {
        let resolved = sample_linkage(
            LinkageOutcome::Resolved(sample_binding()),
            ResolverTier::Tier0Slug,
        );
        assert_eq!(resolved.status(), LinkageStatus::Resolved);
        assert!(resolved.binding().is_some());

        let overridden = sample_linkage(
            LinkageOutcome::Resolved(sample_binding()),
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
    fn content_hash_is_idempotent_and_outcome_sensitive() {
        let market_id = MarketId::new("0xmarket");
        let metadata_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        let resolved = LinkageOutcome::Resolved(sample_binding());
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
