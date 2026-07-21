//! Layered linkage resolver contracts + the deterministic grounding gate.
//!
//! [`SubjectExtractor`] is one tier of the layered resolver. The deterministic
//! tiers (Tier 0 slug, Tier 1 template) are pure research functions. A future
//! Tier 2 LLM extractor must live behind the same trait and run
//! **offline only** — never on the online/PIT hot path (non-deterministic,
//! non-replayable).
//!
//! [`SubjectValidator`] is the **single gate**: every candidate subject, from
//! ANY tier, must pass structural validation plus grounding (each extracted
//! field anchored to a literal span of the source metadata) before it can
//! become a frozen linkage. This is why the deterministic validator remains
//! irreplaceable even once the Tier 2 LLM lands — an ungroundable field is a
//! hallucination by definition and is rejected regardless of who produced it.

use chrono_tz::Tz;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::{
        CryptoSubject, GroundingField, GroundingKind, GroundingProof, LinkageSourceMetadata,
        LinkageValidationFailure, MarketSubject, ResolutionOracle,
    },
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
        /// Closed diagnostic consumed by persistence and the review queue.
        reason: LinkageValidationFailure,
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
                    reason: LinkageValidationFailure::GroundingSourceAbsent {
                        subject_field: span.subject_field.clone(),
                        source: span.source,
                    },
                };
            };
            let Some(actual) = source_text.get(span.start..span.end) else {
                return ValidationOutcome::Rejected {
                    reason: LinkageValidationFailure::GroundingSpanOutOfBounds {
                        subject_field: span.subject_field.clone(),
                        start: span.start,
                        end: span.end,
                        source_length: source_text.len(),
                    },
                };
            };
            if actual != span.text {
                return ValidationOutcome::Rejected {
                    reason: LinkageValidationFailure::GroundingTextMismatch {
                        subject_field: span.subject_field.clone(),
                    },
                };
            }
        }

        // 2. Every load-bearing subject field must be grounded, and the
        // fields extracted independently of a template (`asset` / `strike`
        // / `resolution_oracle`) must carry a genuinely literal span — a
        // `TemplateEntailed` span is only acceptable for a value that is a
        // deterministic function of a fully-matched template
        // (`comparator` / `reference_at` / `observation_at`), never for
        // independently-extracted evidence. This is the fix for the
        // audited pseudo-grounding hole: a whole-slug span used to satisfy
        // "has any span" for every field regardless of kind.
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            return validate_weather_candidate(candidate);
        };
        if !has_span_of_kind(candidate, "asset", GroundingKind::LiteralSpan) {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingLiteralGrounding {
                    subject_field: "asset".to_owned(),
                },
            };
        }
        if subject.strike.is_some()
            && !has_span_of_kind(candidate, "strike", GroundingKind::LiteralSpan)
        {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingLiteralGrounding {
                    subject_field: "strike".to_owned(),
                },
            };
        }
        if !has_span_of_kind(candidate, "resolution_oracle", GroundingKind::LiteralSpan) {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingLiteralGrounding {
                    subject_field: "resolution_oracle".to_owned(),
                },
            };
        }
        if !has_span(candidate, "comparator") {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingGrounding {
                    subject_field: "comparator".to_owned(),
                },
            };
        }
        if !has_span(candidate, "observation_at") {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingGrounding {
                    subject_field: "observation_at".to_owned(),
                },
            };
        }
        if subject.reference_at.is_some() && !has_span(candidate, "reference_at") {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingGrounding {
                    subject_field: "reference_at".to_owned(),
                },
            };
        }

        match validate_structural_consistency(subject, &candidate.instrument_key) {
            Ok(()) => ValidationOutcome::Accepted,
            Err(reason) => ValidationOutcome::Rejected { reason },
        }
    }
}

fn validate_weather_candidate(candidate: &ExtractedCandidate) -> ValidationOutcome {
    let MarketSubject::Weather(subject) = &candidate.subject else {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::UnsupportedSubject,
        };
    };
    for field in [
        "decision_group.station",
        "decision_group.settlement_rule_url",
        "outcome_band",
        "decision_group.market_unit",
        "decision_group.local_date",
        "decision_group.temperature_statistic",
        "decision_group.finalization_policy",
    ] {
        if !has_span_of_kind(candidate, field, GroundingKind::LiteralSpan) {
            return ValidationOutcome::Rejected {
                reason: LinkageValidationFailure::MissingLiteralGrounding {
                    subject_field: field.to_owned(),
                },
            };
        }
    }
    if !subject.has_valid_decision_group_id() {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::InvalidWeatherDecisionGroupId,
        };
    }
    if !subject.outcome_band.is_valid() {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::InvalidWeatherOutcomeBand,
        };
    }
    if subject
        .outcome_band
        .lower_inclusive
        .is_some_and(|value| !value.fract().is_zero())
        || subject
            .outcome_band
            .upper_inclusive
            .is_some_and(|value| !value.fract().is_zero())
    {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::FractionalWeatherOutcomeBand,
        };
    }
    if subject.decision_group.timezone.parse::<Tz>().is_err() {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::InvalidWeatherTimezone {
                timezone: subject.decision_group.timezone.clone(),
            },
        };
    }
    let expected = DomainInstrumentKey::aviation_weather(&subject.decision_group.station);
    if candidate.instrument_key != expected {
        return ValidationOutcome::Rejected {
            reason: LinkageValidationFailure::WeatherInstrumentMismatch {
                expected,
                actual: candidate.instrument_key.clone(),
            },
        };
    }
    ValidationOutcome::Accepted
}

/// Ruleset-consistency checks shared by every candidate regardless of how it was produced.
///
/// An automated tier's candidate runs this as steps 3–4 of
/// [`DefaultSubjectValidator::validate`]; an operator override (which has no
/// text-extraction evidence to ground and therefore skips the grounding-span
/// checks above) still MUST pass these same structural checks, so a
/// fat-fingered override can never bind a market to the wrong instrument or
/// an internally-inconsistent oracle/asset pair.
///
/// # Errors
///
/// Returns the rejection reason as `Err` (never panics on a malformed subject).
pub fn validate_structural_consistency(
    subject: &CryptoSubject,
    instrument_key: &DomainInstrumentKey,
) -> Result<(), LinkageValidationFailure> {
    // Ruleset consistency: the instrument binding must be exactly the frozen
    // table's binding for the extracted asset — a drifted key would silently
    // join the wrong price series.
    let ticker = subject.asset.as_str().to_lowercase();
    let Some(rule) = rule_for_alias(&ticker) else {
        return Err(LinkageValidationFailure::AssetNotInRuleset {
            asset: subject.asset.clone(),
        });
    };
    if *instrument_key != rule.instrument_key() {
        return Err(LinkageValidationFailure::InstrumentRulesetMismatch {
            expected: rule.instrument_key(),
            actual: instrument_key.clone(),
        });
    }

    // Oracle ↔ asset consistency: a Chainlink feed must name exactly the
    // ruleset's feed for the resolved asset — a mismatched feed (e.g. a
    // copy-pasted rules template citing the wrong pair, or an operator
    // override typo) would silently bind the basis cross-check to the wrong
    // series.
    if let ResolutionOracle::ChainlinkDataStreams { feed } = &subject.resolution_oracle
        && *feed != rule.feed()
    {
        return Err(LinkageValidationFailure::ChainlinkFeedRulesetMismatch {
            asset: subject.asset.clone(),
            expected: rule.feed(),
            actual: feed.clone(),
        });
    }

    // Apply the same consistency check to the Binance-settled path:
    // automated extraction binds product+symbol from the frozen venue rule so this
    // can never fire there, but an operator override supplies the oracle
    // independently — a mismatched symbol would silently join the basis
    // cross-check (or the settlement price itself) to the wrong venue series.
    if let ResolutionOracle::BinanceKline { market, symbol, .. } = &subject.resolution_oracle {
        let expected = rule.symbol();
        if *market != rule.binance_market || *symbol != expected {
            return Err(LinkageValidationFailure::BinanceOracleRulesetMismatch {
                asset: subject.asset.clone(),
                expected_market: rule.binance_market,
                actual_market: *market,
                expected_symbol: expected,
                actual_symbol: symbol.clone(),
            });
        }
    }

    Ok(())
}

/// Whether the candidate grounds a given subject field (any kind).
fn has_span(candidate: &ExtractedCandidate, subject_field: &str) -> bool {
    candidate
        .grounding
        .spans
        .iter()
        .any(|span| span.subject_field == subject_field)
}

/// Whether the candidate grounds a given subject field with a span of exactly `kind`.
fn has_span_of_kind(
    candidate: &ExtractedCandidate,
    subject_field: &str,
    kind: GroundingKind,
) -> bool {
    candidate
        .grounding
        .spans
        .iter()
        .any(|span| span.subject_field == subject_field && span.kind == kind)
}

/// The text of one metadata field, when present.
///
/// `pub(crate)`: also used by [`crate::linkage::manual_evidence`] to anchor
/// operator-submitted override evidence against the same source fields.
pub(crate) fn source_field_text(
    metadata: &LinkageSourceMetadata,
    field: GroundingField,
) -> Option<&str> {
    match field {
        GroundingField::Slug => Some(&metadata.slug),
        GroundingField::Question => Some(&metadata.question),
        GroundingField::Description => metadata.description.as_deref(),
        GroundingField::SeriesSlug => metadata.series_slug.as_deref(),
    }
}
