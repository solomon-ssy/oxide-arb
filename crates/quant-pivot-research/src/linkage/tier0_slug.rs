//! Tier 0: deterministic series-slug direct read (zero parsing ambiguity).
//!
//! Polymarket's short-period crypto up/down markets carry clock-templated
//! slugs: `{asset}-updown-{5m|15m|4h}-{epoch}` where `epoch` is the window
//! **open** aligned to the interval (`epoch % duration == 0`) and the window is
//! `[epoch, epoch + duration)`. These series settle against the asset's
//! Chainlink Data Streams feed, and the rules text always carries the literal
//! `data.chain.link/streams/{feed}` anchor — the slug template alone is
//! deterministic evidence for the *subject shape* (asset / comparator /
//! window), but the *settlement oracle* is independently grounded to that
//! literal description anchor via the oracle extractor, exactly like
//! Tier 1. A market whose description does not ground a recognized oracle
//! produces **no candidate** — never a guessed default.
//!
//! This tier covers the traded-volume bulk of the crypto vertical with
//! near-zero free-text parsing; anything that does not match the epoch
//! template exactly falls through to Tier 1.

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::{
        CryptoSubject, GroundingField, GroundingKind, GroundingProof, GroundingSpan,
        LinkageSourceMetadata, MarketSubject, PriceComparator,
    },
    enums::domain::ResolverTier,
    types::Probability,
};

use crate::linkage::{
    extractor::{ExtractedCandidate, SubjectExtractor},
    oracle::extract_oracle,
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

    // The oracle is independently grounded to the description's literal
    // anchor — never assumed from the ruleset, even though every observed
    // short-cycle market in production settles via Chainlink Data Streams.
    let (resolution_oracle, oracle_span) = extract_oracle(rule, metadata.description.as_deref())?;

    let subject = CryptoSubject {
        asset: rule.asset(),
        quote: rule.quote(),
        comparator: PriceComparator::UpVsReference,
        strike: None,
        reference_at: Some(window_open),
        observation_at: window_close,
        resolution_oracle,
    };

    // Grounding: the asset anchors to its literal alias span; comparator and
    // the window instants are entailed by the full template match (never
    // independently written anywhere in the slug); the oracle anchors to its
    // own literal span in the description.
    let template_entailed = |subject_field: &str| GroundingSpan {
        subject_field: subject_field.to_owned(),
        source: GroundingField::Slug,
        start: 0,
        end: slug.len(),
        text: slug.to_owned(),
        kind: GroundingKind::TemplateEntailed,
    };
    let grounding = GroundingProof {
        spans: vec![
            GroundingSpan {
                subject_field: "asset".to_owned(),
                source: GroundingField::Slug,
                start: 0,
                end: alias.len(),
                text: alias.to_owned(),
                kind: GroundingKind::LiteralSpan,
            },
            template_entailed("comparator"),
            template_entailed("reference_at"),
            template_entailed("observation_at"),
            oracle_span,
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
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{LinkageSourceMetadata, MarketSubject, PriceComparator, ResolutionOracle},
        types::MarketId,
    };

    use super::{Tier0SlugExtractor, window_bounds};
    use crate::linkage::{
        extractor::{
            DefaultSubjectValidator, SubjectExtractor, SubjectValidator, ValidationOutcome,
        },
        rules,
    };

    /// The literal Chainlink Data Streams rules-text anchor every observed
    /// short-cycle up/down market carries.
    const CHAINLINK_STREAM_RULES: &str = "This market will resolve to \"Up\" if the Bitcoin \
        price at the end of the time range is greater than or equal to the price at the \
        beginning. The resolution source for this market is the Chainlink BTC/USD data \
        stream, available at https://data.chain.link/streams/btc-usd.";

    fn metadata(slug: &str, description: Option<&str>) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: slug.to_owned(),
            question: "Bitcoin Up or Down".to_owned(),
            description: description.map(str::to_owned),
            series_slug: Some("btc-updown-5m".to_owned()),
            decision_group_market_ids: Vec::new(),
            end_date: None,
        }
    }

    fn grounded(slug: &str) -> LinkageSourceMetadata {
        metadata(slug, Some(CHAINLINK_STREAM_RULES))
    }

    #[test]
    fn deterministic_updown_slug_parses_and_grounds() {
        // 1780319100 = 2026-06-01T13:05:00Z, aligned to 300s.
        let source = grounded("btc-updown-5m-1780319100");
        let candidate = Tier0SlugExtractor
            .extract(&source)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
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
            DefaultSubjectValidator.validate(&candidate, &source),
            ValidationOutcome::Accepted
        );
    }

    #[test]
    fn missing_description_yields_no_candidate() {
        // Fail-open regression guard: without a literal oracle anchor in the
        // description, Tier 0 must produce NO candidate — never a guessed
        // Chainlink default from the ruleset.
        assert!(
            Tier0SlugExtractor
                .extract(&metadata("btc-updown-5m-1780319100", None))
                .expect("extract")
                .is_none()
        );
    }

    #[test]
    fn unrecognized_oracle_text_yields_no_candidate() {
        assert!(
            Tier0SlugExtractor
                .extract(&metadata(
                    "btc-updown-5m-1780319100",
                    Some("This market resolves via magic.")
                ))
                .expect("extract")
                .is_none()
        );
    }

    #[test]
    fn misaligned_or_unknown_slugs_fall_through() {
        // Epoch not aligned to the 5m boundary.
        assert!(
            Tier0SlugExtractor
                .extract(&grounded("btc-updown-5m-1780319101"))
                .expect("extract")
                .is_none()
        );
        // Unknown asset alias.
        assert!(
            Tier0SlugExtractor
                .extract(&grounded("pepe-updown-5m-1780319100"))
                .expect("extract")
                .is_none()
        );
        // Human-readable hourly slug is Tier 1 territory.
        assert!(
            Tier0SlugExtractor
                .extract(&grounded("bitcoin-up-or-down-july-7-3pm-et"))
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

    #[test]
    fn every_supported_asset_parses_every_epoch_interval() {
        const ALIGNED_EPOCH: i64 = 1_800_000_000;
        for rule in rules() {
            let feed = rule.chainlink_feed.to_ascii_lowercase();
            let description =
                format!("The resolution source is https://data.chain.link/streams/{feed}.");
            for interval in ["5m", "15m", "4h"] {
                let slug = format!(
                    "{}-updown-{interval}-{ALIGNED_EPOCH}",
                    rule.ticker.to_ascii_lowercase()
                );
                let source = metadata(&slug, Some(&description));
                let candidate = Tier0SlugExtractor
                    .extract(&source)
                    .expect("extract")
                    .expect("recognized asset/interval fixture");
                let MarketSubject::Crypto(subject) = &candidate.subject else {
                    panic!("crypto subject")
                };
                assert_eq!(subject.asset.as_str(), rule.ticker);
                assert_eq!(
                    DefaultSubjectValidator.validate(&candidate, &source),
                    ValidationOutcome::Accepted
                );
            }
        }
    }
}
