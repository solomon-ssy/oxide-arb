//! Tier 0: deterministic series-slug direct read (zero parsing ambiguity).
//!
//! Polymarket's short-period crypto up/down markets carry clock-templated
//! slugs: `{asset}-updown-{5m|15m|4h}-{epoch}` where `epoch` is the window
//! **open** aligned to the interval (`epoch % duration == 0`) and the window is
//! `[epoch, epoch + duration)`. These series settle against the asset's
//! Chainlink Data Streams feed (the rules text carries the literal
//! `data.chain.link/streams/{feed}` reference; the slug template itself is the
//! deterministic evidence for the whole subject).
//!
//! This tier covers the traded-volume bulk of the crypto vertical with **zero**
//! free-text parsing; anything that does not match the template exactly falls
//! through to Tier 1.

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        CryptoSubject, GroundingField, GroundingProof, GroundingSpan, LinkageSourceMetadata,
        MarketSubject, PriceComparator, ResolutionOracle,
    },
    enums::domain::ResolverTier,
    types::Probability,
};

use crate::linkage::{
    extractor::{ExtractedCandidate, SubjectExtractor},
    ruleset::rule_for_alias,
};

/// Tier 0 extractor over the deterministic `{asset}-updown-{tf}-{epoch}` slug.
pub struct Tier0SlugExtractor;

impl SubjectExtractor for Tier0SlugExtractor {
    fn tier(&self) -> ResolverTier {
        ResolverTier::Tier0Slug
    }

    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>> {
        Ok(parse_updown_slug(metadata))
    }
}

/// Interval label → window duration (only the boards Polymarket actually runs
/// with epoch-templated slugs; 1h/1d use human-readable slugs → Tier 1).
fn interval_secs(label: &str) -> Option<i64> {
    match label {
        "5m" => Some(300),
        "15m" => Some(900),
        "4h" => Some(14_400),
        _ => None,
    }
}

/// Parse the deterministic up/down slug into a fully-grounded candidate.
fn parse_updown_slug(metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
    let slug = metadata.slug.as_str();
    let mut parts = slug.split('-');
    let alias = parts.next()?;
    if parts.next()? != "updown" {
        return None;
    }
    let interval_label = parts.next()?;
    let epoch_text = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let rule = rule_for_alias(alias)?;
    let duration_secs = interval_secs(interval_label)?;
    let epoch: i64 = epoch_text.parse().ok()?;
    // The slug encodes the window OPEN, aligned to the interval boundary; a
    // misaligned epoch is not this template (fail through, never guess).
    if epoch <= 0 || epoch % duration_secs != 0 {
        return None;
    }
    let window_open = Utc.timestamp_opt(epoch, 0).single()?;
    let window_close = window_open + Duration::seconds(duration_secs);

    let subject = CryptoSubject {
        asset: rule.asset(),
        quote: rule.quote(),
        comparator: PriceComparator::UpVsReference,
        strike: None,
        reference_at: Some(window_open),
        observation_at: window_close,
        resolution_oracle: ResolutionOracle::ChainlinkDataStreams { feed: rule.feed() },
    };

    // Grounding: the asset anchors to its literal alias; every derived field
    // (comparator, window instants, oracle) is entailed by the full template
    // match, so it anchors to the whole slug span — the template IS the
    // deterministic evidence for those fields.
    let full_slug_span = |subject_field: &str| GroundingSpan {
        subject_field: subject_field.to_owned(),
        source: GroundingField::Slug,
        start: 0,
        end: slug.len(),
        text: slug.to_owned(),
    };
    let grounding = GroundingProof {
        spans: vec![
            GroundingSpan {
                subject_field: "asset".to_owned(),
                source: GroundingField::Slug,
                start: 0,
                end: alias.len(),
                text: alias.to_owned(),
            },
            full_slug_span("comparator"),
            full_slug_span("reference_at"),
            full_slug_span("observation_at"),
            full_slug_span("resolution_oracle"),
        ],
    };

    Some(ExtractedCandidate {
        subject: MarketSubject::Crypto(subject),
        instrument_key: rule.instrument_key(),
        confidence: Probability::ONE,
        grounding,
    })
}

/// The window duration a tier-0 subject spans, when its slug is templated.
///
/// Exposed for tests and diagnostics; production consumers read the frozen
/// subject's instants directly.
#[must_use]
pub fn window_bounds(slug: &str) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let mut parts = slug.split('-');
    let _alias = parts.next()?;
    if parts.next()? != "updown" {
        return None;
    }
    let duration_secs = interval_secs(parts.next()?)?;
    let epoch: i64 = parts.next()?.parse().ok()?;
    let open = Utc.timestamp_opt(epoch, 0).single()?;
    Some((open, open + Duration::seconds(duration_secs)))
}

#[cfg(test)]
mod tests {
    use super::{Tier0SlugExtractor, window_bounds};
    use crate::linkage::extractor::{
        DefaultSubjectValidator, SubjectExtractor, SubjectValidator, ValidationOutcome,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::domain::{
        LinkageSourceMetadata, MarketSubject, PriceComparator, ResolutionOracle,
    };
    use quant_pivot_models::types::MarketId;

    fn metadata(slug: &str) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: slug.to_owned(),
            question: "Bitcoin Up or Down".to_owned(),
            description: None,
            series_slug: Some("btc-updown-5m".to_owned()),
            end_date: None,
        }
    }

    #[test]
    fn deterministic_updown_slug_parses_and_grounds() {
        // 1780319100 = 2026-06-01T13:05:00Z, aligned to 300s.
        let metadata = metadata("btc-updown-5m-1780319100");
        let candidate = Tier0SlugExtractor
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        assert_eq!(subject.asset.as_str(), "BTC");
        assert_eq!(subject.comparator, PriceComparator::UpVsReference);
        assert!(subject.strike.is_none());
        let open = Utc.timestamp_opt(1_780_319_100, 0).single().unwrap();
        assert_eq!(subject.reference_at, Some(open));
        assert_eq!(
            subject.observation_at,
            open + chrono::Duration::seconds(300)
        );
        assert!(matches!(
            subject.resolution_oracle,
            ResolutionOracle::ChainlinkDataStreams { .. }
        ));
        assert_eq!(candidate.instrument_key.as_str(), "BINANCE:BTCUSDT:1m");

        // The full candidate must clear the single grounding gate.
        assert_eq!(
            DefaultSubjectValidator.validate(&candidate, &metadata),
            ValidationOutcome::Accepted
        );
    }

    #[test]
    fn misaligned_or_unknown_slugs_fall_through() {
        // Epoch not aligned to the 5m boundary.
        assert!(
            Tier0SlugExtractor
                .extract(&metadata("btc-updown-5m-1780319101"))
                .expect("extract")
                .is_none()
        );
        // Unknown asset alias.
        assert!(
            Tier0SlugExtractor
                .extract(&metadata("pepe-updown-5m-1780319100"))
                .expect("extract")
                .is_none()
        );
        // Human-readable hourly slug is Tier 1 territory.
        assert!(
            Tier0SlugExtractor
                .extract(&metadata("bitcoin-up-or-down-july-7-3pm-et"))
                .expect("extract")
                .is_none()
        );
    }

    #[test]
    fn window_bounds_derive_from_the_epoch() {
        let (open, close) = window_bounds("eth-updown-15m-1780318800").expect("bounds");
        assert_eq!((close - open).num_seconds(), 900);
        assert_eq!(open.timestamp(), 1_780_318_800);
    }
}
