//! Layered linkage resolver contracts + the deterministic grounding gate.
//!
//! [`SubjectExtractor`] is one tier of the layered resolver. The deterministic
//! tiers (Tier 0 slug, Tier 1 template) are pure research functions shipped by
//! 11.2.2; the Tier 2 LLM extractor lives behind the same trait and runs
//! **offline only** — never on the online/PIT hot path (non-deterministic,
//! non-replayable). Its design is frozen in `phase-11/11.2.3`.
//!
//! [`SubjectValidator`] is the **single gate**: every candidate subject, from
//! ANY tier, must pass structural validation plus grounding (each extracted
//! field anchored to a literal span of the source metadata) before it can
//! become a frozen linkage. This is why the deterministic validator remains
//! irreplaceable even once the Tier 2 LLM lands — an ungroundable field is a
//! hallucination by definition and is rejected regardless of who produced it.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{GroundingField, GroundingProof, LinkageSourceMetadata, MarketSubject},
    enums::domain::ResolverTier,
    types::{DomainInstrumentKey, Probability},
};

use crate::linkage::ruleset::rule_for_alias;

/// A candidate subject produced by one resolver tier, pending validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedCandidate {
    /// The extracted subject.
    pub subject: MarketSubject,
    /// The feature-source instrument the subject joins to.
    pub instrument_key: DomainInstrumentKey,
    /// Extractor confidence in `[0, 1]` (deterministic tiers emit `1`).
    pub confidence: Probability,
    /// Proposed field → literal-span anchors (verified by the validator).
    pub grounding: GroundingProof,
}

/// One tier of the layered linkage resolver.
pub trait SubjectExtractor: Send + Sync {
    /// Which tier this extractor implements.
    fn tier(&self) -> ResolverTier;

    /// Extract a candidate subject from frozen market metadata.
    ///
    /// `Ok(None)` means "this tier does not recognize the market" — the
    /// resolver falls through to the next tier. Never a fabricated candidate.
    ///
    /// # Errors
    ///
    /// Returns an error only on an irrecoverable extraction failure.
    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>>;
}

/// Whether a candidate cleared the deterministic grounding gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationOutcome {
    /// Every field anchored and structurally consistent.
    Accepted,
    /// The candidate is rejected; the market fails closed to `Unresolved`.
    Rejected {
        /// Operator-facing reason (drives the review queue).
        reason: String,
    },
}

/// The single validation gate for every tier's candidates.
pub trait SubjectValidator: Send + Sync {
    /// Validate a candidate against the source metadata it was extracted from.
    fn validate(
        &self,
        candidate: &ExtractedCandidate,
        metadata: &LinkageSourceMetadata,
    ) -> ValidationOutcome;
}

/// The deterministic default gate: literal-span grounding + structural
/// consistency against the frozen ruleset.
pub struct DefaultSubjectValidator;

impl SubjectValidator for DefaultSubjectValidator {
    fn validate(
        &self,
        candidate: &ExtractedCandidate,
        metadata: &LinkageSourceMetadata,
    ) -> ValidationOutcome {
        // 1. Every proposed span must be a literal slice of its source field.
        for span in &candidate.grounding.spans {
            let Some(source_text) = source_field_text(metadata, span.source) else {
                return ValidationOutcome::Rejected {
                    reason: format!(
                        "grounding span for `{}` references absent source field {:?}",
                        span.subject_field, span.source
                    ),
                };
            };
            let Some(actual) = source_text.get(span.start..span.end) else {
                return ValidationOutcome::Rejected {
                    reason: format!(
                        "grounding span for `{}` is out of bounds ({}..{} over {} bytes)",
                        span.subject_field,
                        span.start,
                        span.end,
                        source_text.len()
                    ),
                };
            };
            if actual != span.text {
                return ValidationOutcome::Rejected {
                    reason: format!(
                        "grounding span for `{}` does not match the source text \
                         (expected `{}`, found `{actual}`)",
                        span.subject_field, span.text
                    ),
                };
            }
        }

        // 2. Load-bearing fields must be grounded: the asset always, the
        //    strike whenever one was extracted, and the settlement oracle.
        let MarketSubject::Crypto(subject) = &candidate.subject;
        if !has_span(candidate, "asset") {
            return ValidationOutcome::Rejected {
                reason: "candidate carries no grounding span for `asset`".to_owned(),
            };
        }
        if subject.strike.is_some() && !has_span(candidate, "strike") {
            return ValidationOutcome::Rejected {
                reason: "candidate extracted a strike without grounding it".to_owned(),
            };
        }
        if !has_span(candidate, "resolution_oracle") {
            return ValidationOutcome::Rejected {
                reason: "candidate carries no grounding span for `resolution_oracle`".to_owned(),
            };
        }

        // 3. Ruleset consistency: the instrument binding must be exactly the
        //    frozen table's binding for the extracted asset — a drifted key
        //    would silently join the wrong price series.
        let ticker = subject.asset.as_str().to_lowercase();
        let Some(rule) = rule_for_alias(&ticker) else {
            return ValidationOutcome::Rejected {
                reason: format!(
                    "asset `{}` is not in the frozen resolver ruleset",
                    subject.asset
                ),
            };
        };
        if candidate.instrument_key != rule.instrument_key() {
            return ValidationOutcome::Rejected {
                reason: format!(
                    "instrument key `{}` disagrees with the ruleset binding `{}`",
                    candidate.instrument_key,
                    rule.instrument_key()
                ),
            };
        }

        ValidationOutcome::Accepted
    }
}

/// Whether the candidate grounds a given subject field.
fn has_span(candidate: &ExtractedCandidate, subject_field: &str) -> bool {
    candidate
        .grounding
        .spans
        .iter()
        .any(|span| span.subject_field == subject_field)
}

/// The text of one metadata field, when present.
fn source_field_text(metadata: &LinkageSourceMetadata, field: GroundingField) -> Option<&str> {
    match field {
        GroundingField::Slug => Some(&metadata.slug),
        GroundingField::Question => Some(&metadata.question),
        GroundingField::Description => metadata.description.as_deref(),
        GroundingField::SeriesSlug => metadata.series_slug.as_deref(),
    }
}
