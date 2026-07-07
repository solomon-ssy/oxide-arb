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
pub mod ruleset;
pub mod tier0_slug;
pub mod tier1_template;

pub use extractor::{
    DefaultSubjectValidator, ExtractedCandidate, SubjectExtractor, SubjectValidator,
    ValidationOutcome,
};
pub use ruleset::{AssetRule, CRYPTO_RESOLVER_VERSION, find_alias, rule_for_alias, rules};
pub use tier0_slug::Tier0SlugExtractor;
pub use tier1_template::CryptoSubjectParser;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{LinkageOutcome, LinkageSourceMetadata, ResolvedBinding},
    enums::domain::ResolverTier,
    types::{Probability, ResolverVersion},
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
    pub fn deterministic() -> Self {
        Self {
            tiers: vec![Box::new(Tier0SlugExtractor), Box::new(CryptoSubjectParser)],
            validator: Box::new(DefaultSubjectValidator),
            resolver_version: CRYPTO_RESOLVER_VERSION,
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
    pub fn resolve(&self, metadata: &LinkageSourceMetadata) -> QuantResult<ResolutionResult> {
        let mut last_tier = ResolverTier::Tier0Slug;
        for tier in &self.tiers {
            last_tier = tier.tier();
            let Some(candidate) = tier.extract(metadata)? else {
                continue;
            };
            return Ok(match self.validator.validate(&candidate, metadata) {
                ValidationOutcome::Accepted => ResolutionResult {
                    outcome: LinkageOutcome::Resolved(ResolvedBinding {
                        subject: candidate.subject,
                        instrument_key: candidate.instrument_key,
                        grounding: candidate.grounding,
                    }),
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

#[cfg(test)]
mod tests {
    use super::LayeredResolver;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{GroundingField, LinkageOutcome, LinkageSourceMetadata},
        enums::domain::ResolverTier,
        types::MarketId,
    };

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
        let resolver = LayeredResolver::deterministic();

        let tier0 = resolver
            .resolve(&metadata(
                "btc-updown-5m-1780319100",
                "Bitcoin Up or Down",
                None,
            ))
            .expect("resolve");
        assert_eq!(tier0.resolver_tier, ResolverTier::Tier0Slug);
        assert!(matches!(tier0.outcome, LinkageOutcome::Resolved(_)));

        let unresolved = resolver
            .resolve(&metadata(
                "who-wins-the-super-bowl",
                "Who wins the Super Bowl?",
                None,
            ))
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
        use super::{
            DefaultSubjectValidator, SubjectExtractor, SubjectValidator, Tier0SlugExtractor,
            ValidationOutcome,
        };
        // Build a valid candidate, then corrupt one span's text.
        let source = metadata("btc-updown-5m-1780319100", "Bitcoin Up or Down", None);
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
