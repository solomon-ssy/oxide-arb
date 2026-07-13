//! Operator-override grounding gate (11.2.2 remediation R4).
//!
//! An operator override is a human decision, not text-extracted evidence —
//! no [`crate::linkage::SubjectExtractor`] tier produced it. But the
//! anti-hallucination bar this module enforces is the same one every
//! automated tier clears: a claim about what the source metadata says is
//! only accepted when it is a byte-exact substring of that metadata field.
//! Fields whose value is never independently "written" anywhere in the
//! source text on *any* tier (`comparator` / `observation_at` /
//! `reference_at` — see [`crate::linkage::DefaultSubjectValidator`]'s
//! `TemplateEntailed` treatment of the same fields) remain the operator's
//! judgment call and are not required to cite a span here either.

use quant_pivot_models::{
    domain::{
        CryptoSubject, GroundingKind, GroundingProof, GroundingSpan, LinkageSourceMetadata,
        ManualEvidenceInput,
    },
    types::DomainInstrumentKey,
};

use crate::linkage::extractor::{source_field_text, validate_structural_consistency};

/// Subject fields an operator override must ground with a literal citation —
/// exactly the fields [`crate::linkage::DefaultSubjectValidator`] requires a
/// [`GroundingKind::LiteralSpan`] for on automated candidates. `strike` is
/// conditionally required (only when the subject carries one).
const ALWAYS_REQUIRED_FIELDS: [&str; 2] = ["asset", "resolution_oracle"];

/// Validate + ground a manual override.
///
/// Runs the same [`validate_structural_consistency`] every automated
/// candidate must clear, then verifies the operator cited real, byte-exact
/// text for every load-bearing identity field. Returns the constructed
/// [`GroundingProof`] on success — never an empty one, unlike the
/// pre-remediation stub.
///
/// # Errors
///
/// Returns a human-readable rejection reason (never panics) when structural
/// consistency fails, a required field has no cited evidence, the cited
/// source field is absent, or the cited text does not literally occur in it.
pub fn validate_manual_override(
    subject: &CryptoSubject,
    instrument_key: &DomainInstrumentKey,
    metadata: &LinkageSourceMetadata,
    evidence: &[ManualEvidenceInput],
) -> Result<GroundingProof, String> {
    validate_structural_consistency(subject, instrument_key)?;

    let mut required_fields = ALWAYS_REQUIRED_FIELDS.to_vec();
    if subject.strike.is_some() {
        required_fields.push("strike");
    }

    let mut spans = Vec::with_capacity(required_fields.len());
    for field in required_fields {
        let input = evidence
            .iter()
            .find(|item| item.subject_field == field)
            .ok_or_else(|| format!("override carries no cited evidence for `{field}`"))?;
        spans.push(ground_one(field, input, metadata)?);
    }
    Ok(GroundingProof { spans })
}

/// Anchor one operator-cited claim against the real source text.
fn ground_one(
    subject_field: &str,
    input: &ManualEvidenceInput,
    metadata: &LinkageSourceMetadata,
) -> Result<GroundingSpan, String> {
    if input.text.is_empty() {
        return Err(format!(
            "cited evidence for `{subject_field}` is empty — an override must cite real text"
        ));
    }
    let Some(source_text) = source_field_text(metadata, input.source) else {
        return Err(format!(
            "override cites `{subject_field}` evidence from an absent source field {:?}",
            input.source
        ));
    };
    let Some(start) = source_text.find(input.text.as_str()) else {
        return Err(format!(
            "cited evidence for `{subject_field}` (`{}`) does not literally occur in the \
             cited source field — an override can cite real text, never fabricate it",
            input.text
        ));
    };
    Ok(GroundingSpan {
        subject_field: subject_field.to_owned(),
        source: input.source,
        start,
        end: start + input.text.len(),
        text: input.text.clone(),
        kind: GroundingKind::ManualEvidence,
    })
}

#[cfg(test)]
mod tests {
    use crate::linkage;

    use super::validate_manual_override;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, GroundingField, GroundingKind, LinkageSourceMetadata,
            ManualEvidenceInput, PriceComparator, ResolutionOracle,
        },
        types::{ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey, MarketId, Usd},
    };

    fn metadata() -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: "btc-updown-5m-1".to_owned(),
            question: "Bitcoin Up or Down".to_owned(),
            description: Some(
                "The resolution source for this market is the Chainlink BTC/USD data stream."
                    .to_owned(),
            ),
            series_slug: None,
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap()),
        }
    }

    fn subject() -> CryptoSubject {
        CryptoSubject {
            asset: CryptoAsset::parse("BTC").expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::UpVsReference,
            strike: None,
            reference_at: Some(Utc.with_ymd_and_hms(2026, 7, 8, 11, 55, 0).unwrap()),
            observation_at: Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap(),
            resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            },
        }
    }

    fn instrument_key() -> DomainInstrumentKey {
        linkage::rule_for_alias("btc")
            .expect("btc rule")
            .instrument_key()
    }

    #[test]
    fn real_citations_ground_the_override() {
        let evidence = vec![
            ManualEvidenceInput {
                subject_field: "asset".to_owned(),
                source: GroundingField::Slug,
                text: "btc".to_owned(),
            },
            ManualEvidenceInput {
                subject_field: "resolution_oracle".to_owned(),
                source: GroundingField::Description,
                text: "Chainlink BTC/USD data stream".to_owned(),
            },
        ];
        let proof = validate_manual_override(&subject(), &instrument_key(), &metadata(), &evidence)
            .expect("accepted");
        assert_eq!(proof.spans.len(), 2);
        assert!(
            proof
                .spans
                .iter()
                .all(|span| span.kind == GroundingKind::ManualEvidence)
        );
    }

    #[test]
    fn fabricated_citation_is_rejected() {
        let evidence = vec![
            ManualEvidenceInput {
                subject_field: "asset".to_owned(),
                source: GroundingField::Slug,
                text: "btc".to_owned(),
            },
            ManualEvidenceInput {
                subject_field: "resolution_oracle".to_owned(),
                source: GroundingField::Description,
                text: "a phrase that never appears in the description".to_owned(),
            },
        ];
        assert!(
            validate_manual_override(&subject(), &instrument_key(), &metadata(), &evidence)
                .is_err(),
            "an override can cite real text, never fabricate it"
        );
    }

    #[test]
    fn missing_citation_for_a_required_field_is_rejected() {
        let evidence = vec![ManualEvidenceInput {
            subject_field: "asset".to_owned(),
            source: GroundingField::Slug,
            text: "btc".to_owned(),
        }];
        assert!(
            validate_manual_override(&subject(), &instrument_key(), &metadata(), &evidence)
                .is_err(),
            "resolution_oracle has no citation"
        );
    }

    #[test]
    fn strike_is_required_only_when_present() {
        let mut with_strike = subject();
        with_strike.comparator = PriceComparator::Above;
        with_strike.strike = Some(Usd::new(rust_decimal::Decimal::from(100_000)));
        let evidence = vec![
            ManualEvidenceInput {
                subject_field: "asset".to_owned(),
                source: GroundingField::Slug,
                text: "btc".to_owned(),
            },
            ManualEvidenceInput {
                subject_field: "resolution_oracle".to_owned(),
                source: GroundingField::Description,
                text: "Chainlink BTC/USD data stream".to_owned(),
            },
        ];
        assert!(
            validate_manual_override(&with_strike, &instrument_key(), &metadata(), &evidence)
                .is_err(),
            "strike is present but uncited"
        );
    }
}
