//! Layered market-linkage resolution (Phase 11.2.2 §3.6).
//!
//! Deterministic-first: Tier 0 (series-slug direct read) covers the
//! traded-volume bulk with zero parsing ambiguity; Tier 1 (template parser)
//! covers the human-readable ET slugs and threshold questions; the Tier 2 LLM
//! fallback exists behind the same [`SubjectExtractor`] trait but ships in a
//! later phase (design frozen in `phase-11/11.2.3`). Every tier's candidate
//! passes the **single** [`SubjectValidator`] grounding gate before it can
//! become a frozen ledger record — precision ≫ recall, one bad link poisons
//! every downstream join.
//!
//! This module is the pure half: extraction + validation over frozen
//! [`LinkageSourceMetadata`]. The impure orchestration (metadata loading,
//! ledger writes, re-resolution triggers) lives in `quant-pivot-core`'s
//! `LinkageResolverService`; persistence is `quant-pivot-repository`'s
//! `MarketLinkageRepository`.

pub mod extractor;
pub mod manual_evidence;
pub mod oracle;
pub mod ruleset;
pub mod tier0_slug;
pub mod tier1_template;
pub mod weather_daily_high;

pub use extractor::{
    DefaultSubjectValidator, ExtractedCandidate, SubjectExtractor, SubjectValidator,
    ValidationOutcome, validate_structural_consistency,
};
pub use manual_evidence::validate_manual_override;
pub use ruleset::{AssetRule, DOMAIN_RESOLVER_VERSION, find_alias, rule_for_alias, rules};
pub use tier0_slug::Tier0SlugExtractor;
pub use tier1_template::CryptoSubjectParser;
pub use weather_daily_high::{WeatherDailyHighExtractor, WeatherStationCatalog};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        LinkageOutcome, LinkageSourceMetadata, MarketSubject, ResolutionOracle, ResolvedBinding,
        ResolvedSourceBinding,
    },
    enums::domain::{LinkageSourceRole, ResolverTier},
    hashing::CanonicalDigest,
    types::{DomainInstrumentKey, DomainSourceId, Probability, ResolverVersion},
};

/// One resolver pass's verdict for a market, ready to freeze into the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionResult {
    /// The outcome to append (binding iff resolved).
    pub outcome: LinkageOutcome,
    /// The tier that decided (the accepting tier, or the last tier tried).
    pub resolver_tier: ResolverTier,
    /// The frozen ruleset version that governed this pass.
    pub resolver_version: ResolverVersion,
    /// Extractor confidence (zero for unresolved).
    pub confidence: Probability,
}

/// The deterministic layered resolver: ordered tiers behind one grounding gate.
pub struct LayeredResolver {
    tiers: Vec<Box<dyn SubjectExtractor>>,
    validator: Box<dyn SubjectValidator>,
    resolver_version: ResolverVersion,
}

impl LayeredResolver {
    /// The 11.2.2 production resolver: Tier 0 → Tier 1, default grounding gate.
    #[must_use]
    pub fn deterministic(weather_stations: WeatherStationCatalog) -> Self {
        Self {
            tiers: vec![
                Box::new(Tier0SlugExtractor),
                Box::new(CryptoSubjectParser),
                Box::new(WeatherDailyHighExtractor::new(weather_stations)),
            ],
            validator: Box::new(DefaultSubjectValidator),
            resolver_version: DOMAIN_RESOLVER_VERSION,
        }
    }

    /// The frozen ruleset version this resolver stamps on every record.
    #[must_use]
    pub const fn resolver_version(&self) -> ResolverVersion {
        self.resolver_version
    }

    /// Resolve one market's frozen metadata into a ledger-ready verdict.
    ///
    /// Tiers run in order; the first tier that produces a candidate decides
    /// (its candidate goes through the gate — a rejected candidate is a
    /// grounding failure, not a fall-through, so it fails closed rather than
    /// letting a lower-precision tier overrule the gate).
    ///
    /// # Errors
    ///
    /// Propagates irrecoverable extractor failures.
    pub fn resolve(
        &self,
        metadata: &LinkageSourceMetadata,
        available_at: DateTime<Utc>,
    ) -> QuantResult<ResolutionResult> {
        let mut last_tier = ResolverTier::Tier0Slug;
        for tier in &self.tiers {
            last_tier = tier.tier();
            let Some(candidate) = tier.extract(metadata)? else {
                continue;
            };
            return Ok(match self.validator.validate(&candidate, metadata) {
                ValidationOutcome::Accepted => ResolutionResult {
                    outcome: LinkageOutcome::Resolved(Box::new(ResolvedBinding {
                        source_bindings: source_bindings_for_subject(
                            &candidate.subject,
                            available_at,
                        )?,
                        subject: candidate.subject,
                        grounding: candidate.grounding,
                        override_context: None,
                    })),
                    resolver_tier: tier.tier(),
                    resolver_version: self.resolver_version,
                    confidence: candidate.confidence,
                },
                ValidationOutcome::Rejected { reason } => ResolutionResult {
                    outcome: LinkageOutcome::Unresolved {
                        reason: format!("{} candidate rejected: {reason}", tier.tier()),
                    },
                    resolver_tier: tier.tier(),
                    resolver_version: self.resolver_version,
                    confidence: Probability::ZERO,
                },
            });
        }
        Ok(ResolutionResult {
            outcome: LinkageOutcome::Unresolved {
                reason: "no deterministic tier recognized the market".to_owned(),
            },
            resolver_tier: last_tier,
            resolver_version: self.resolver_version,
            confidence: Probability::ZERO,
        })
    }
}

/// Derive the complete, canonical role/source/instrument set for a subject.
///
/// # Errors
///
/// Returns a configuration error when the subject has no frozen source rule,
/// or propagates canonical hashing failures.
pub fn source_bindings_for_subject(
    subject: &MarketSubject,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<ResolvedSourceBinding>> {
    let (specs, binding_context) = match subject {
        MarketSubject::Crypto(subject) => {
            let ticker = subject.asset.as_str().to_lowercase();
            let rule = rule_for_alias(&ticker).ok_or_else(|| {
                QuantError::config(format!(
                    "asset `{}` has no frozen source rule",
                    subject.asset
                ))
            })?;
            let mut specs = vec![(
                LinkageSourceRole::Feature,
                DomainSourceId::binance(),
                rule.instrument_key(),
            )];
            match &subject.resolution_oracle {
                ResolutionOracle::ChainlinkDataStreams { .. } => {
                    let instrument = rule.chainlink_instrument();
                    specs.push((
                        LinkageSourceRole::LiveEvent,
                        DomainSourceId::chainlink_data_streams(),
                        instrument.clone(),
                    ));
                    specs.push((
                        LinkageSourceRole::Resolution,
                        DomainSourceId::chainlink_data_streams(),
                        instrument,
                    ));
                }
                ResolutionOracle::BinanceKline { .. } => {
                    specs.push((
                        LinkageSourceRole::LiveEvent,
                        DomainSourceId::binance_agg_trade(),
                        rule.binance_event_instrument(),
                    ));
                    specs.push((
                        LinkageSourceRole::Resolution,
                        DomainSourceId::binance(),
                        rule.instrument_key(),
                    ));
                }
            }
            (specs, subject.asset.to_string())
        }
        MarketSubject::Weather(subject) => (
            vec![
                (
                    LinkageSourceRole::LiveEvent,
                    DomainSourceId::aviation_weather(),
                    DomainInstrumentKey::aviation_weather(&subject.station),
                ),
                (
                    LinkageSourceRole::HistoricalCalibration,
                    DomainSourceId::ghcnh(),
                    DomainInstrumentKey::ghcnh(&subject.station),
                ),
                (
                    LinkageSourceRole::Forecast,
                    DomainSourceId::gefs(),
                    DomainInstrumentKey::gefs(&subject.station),
                ),
            ],
            subject.station.to_string(),
        ),
    };
    specs
        .into_iter()
        .map(|(role, source_id, instrument_key)| {
            let binding_hash = CanonicalDigest::content_hash_json(&(
                "domain_source_binding_v2",
                &binding_context,
                role,
                &source_id,
                &instrument_key,
                subject,
            ))?;
            Ok(ResolvedSourceBinding {
                role,
                source_id,
                instrument_key,
                available_at,
                binding_hash,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DefaultSubjectValidator, LayeredResolver, SubjectExtractor, SubjectValidator,
        Tier0SlugExtractor, ValidationOutcome, WeatherStationCatalog,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{GroundingField, LinkageOutcome, LinkageSourceMetadata},
        enums::domain::ResolverTier,
        types::MarketId,
    };

    /// The literal Chainlink Data Streams rules-text anchor every observed
    /// short-cycle up/down market carries.
    const CHAINLINK_STREAM_RULES: &str = "The resolution source for this market is the \
        Chainlink BTC/USD data stream, available at https://data.chain.link/streams/btc-usd.";

    fn metadata(slug: &str, question: &str, description: Option<&str>) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: slug.to_owned(),
            question: question.to_owned(),
            description: description.map(str::to_owned),
            series_slug: None,
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap()),
        }
    }

    #[test]
    fn tier0_wins_before_tier1_and_unrecognized_fails_closed() {
        let resolver = LayeredResolver::deterministic(WeatherStationCatalog::default());

        let tier0 = resolver
            .resolve(
                &metadata(
                    "btc-updown-5m-1780319100",
                    "Bitcoin Up or Down",
                    Some(CHAINLINK_STREAM_RULES),
                ),
                Utc::now(),
            )
            .expect("resolve");
        assert_eq!(tier0.resolver_tier, ResolverTier::Tier0Slug);
        assert!(matches!(tier0.outcome, LinkageOutcome::Resolved(_)));

        let unresolved = resolver
            .resolve(
                &metadata("who-wins-the-super-bowl", "Who wins the Super Bowl?", None),
                Utc::now(),
            )
            .expect("resolve");
        assert!(matches!(
            unresolved.outcome,
            LinkageOutcome::Unresolved { .. }
        ));
        assert!(unresolved.confidence.inner().is_zero());
    }

    #[test]
    fn grounding_rejects_field_absent_from_source() {
        // Anti-hallucination: a candidate whose spans do not literally appear
        // in the source metadata must be rejected by the single gate.
        // Build a valid candidate, then corrupt one span's text.
        let source = metadata(
            "btc-updown-5m-1780319100",
            "Bitcoin Up or Down",
            Some(CHAINLINK_STREAM_RULES),
        );
        let mut candidate = Tier0SlugExtractor
            .extract(&source)
            .expect("extract")
            .expect("candidate");
        candidate.grounding.spans[0].text = "hallucinated".to_owned();
        assert!(matches!(
            DefaultSubjectValidator.validate(&candidate, &source),
            ValidationOutcome::Rejected { .. }
        ));

        // And a span pointing at an absent source field is rejected too.
        let mut candidate = Tier0SlugExtractor
            .extract(&source)
            .expect("extract")
            .expect("candidate");
        candidate.grounding.spans[0].source = GroundingField::Description;
        assert!(matches!(
            DefaultSubjectValidator.validate(&candidate, &source),
            ValidationOutcome::Rejected { .. }
        ));
    }
}
