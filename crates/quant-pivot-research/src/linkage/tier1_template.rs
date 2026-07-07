//! Tier 1: deterministic template parsing over slug / question / description.
//!
//! Covers the crypto boards whose slugs are human-readable rather than
//! epoch-templated:
//!
//! - **hourly up/down**: `bitcoin-up-or-down-july-7-3pm-et` (a year segment
//!   appeared in some vintages: `...-july-7-2026-3pm-et` — both parse);
//! - **daily up/down**: `bitcoin-up-or-down-on-july-7` — a noon-ET → noon-ET
//!   window whose slug names the settlement day;
//! - **threshold / band questions**: "Will Bitcoin reach $150,000 …", "Will
//!   Ethereum dip to $2,000 …", "… between $95,000 and $105,000 …" — the
//!   settlement instant is the market's `end_date`.
//!
//! Eastern-Time instants convert through `America/New_York` (DST-correct);
//! ambiguous or nonexistent local times (transition hours) fail closed. The
//! settlement oracle is extracted from the **description** rules text: the
//! literal `data.chain.link/streams/{feed}` reference, a "Binance … 1-minute
//! candle" citation, or — recognized-but-unclassified — the literal
//! "resolution source" sentence ([`ResolutionOracle::Other`], basis
//! cross-check then fails closed). A market whose rules text grounds no oracle
//! produces no candidate at all.

use std::sync::LazyLock;

use chrono::{DateTime, Duration, LocalResult, TimeZone, Utc};
use chrono_tz::America::New_York;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        CryptoSubject, GroundingField, GroundingProof, GroundingSpan, LinkageSourceMetadata,
        MarketSubject, PriceComparator, ResolutionOracle,
    },
    enums::domain::{KlineInterval, ResolverTier},
    types::{ChainlinkFeedKey, Probability, Usd},
};
use regex::Regex;
use rust_decimal::Decimal;

use crate::linkage::{
    extractor::{ExtractedCandidate, SubjectExtractor},
    ruleset::{AssetRule, find_alias, rule_for_alias},
};

/// Hourly ET up/down slug, with or without a year segment.
static HOURLY_SLUG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([a-z]+)-up-or-down-([a-z]+)-(\d{1,2})(?:-(\d{4}))?-(\d{1,2})(am|pm)-et$")
        .expect("static regex")
});

/// Daily up/down slug (noon-ET → noon-ET; the slug names the settlement day).
static DAILY_SLUG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([a-z]+)-up-or-down-on-([a-z]+)-(\d{1,2})(?:-(\d{4}))?$").expect("static regex")
});

/// A dollar amount with optional thousands separators and `k`/`m` suffix.
static DOLLAR_AMOUNT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?").expect("static regex")
});

/// A band question: "between $X and $Y".
static DOLLAR_BAND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)between\s+\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?\s+and\s+\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?",
    )
    .expect("static regex")
});

/// The literal Chainlink Data Streams reference in the rules text.
static CHAINLINK_STREAM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"data\.chain\.link/streams/([a-z0-9]+-[a-z0-9]+)").expect("static regex")
});

/// A Binance 1-minute candle citation in the rules text.
static BINANCE_CANDLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Binance[^.]{0,120}?(?:1[- ]minute|one[- ]minute)[^.]{0,40}?candle")
        .expect("static regex")
});

/// A "resolution source" sentence (recognized-but-unclassified oracle).
static RESOLUTION_SENTENCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)resolution source[^.]*\.").expect("static regex"));

/// Upward comparators in threshold questions.
static ABOVE_PHRASE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(reach|hit|close above|above|exceed|surpass)\b").expect("static regex")
});

/// Downward comparators in threshold questions.
static BELOW_PHRASE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(dip to|drop to|fall to|close below|below|fall under)\b")
        .expect("static regex")
});

/// Tier 1 deterministic template parser.
pub struct CryptoSubjectParser;

impl SubjectExtractor for CryptoSubjectParser {
    fn tier(&self) -> ResolverTier {
        ResolverTier::Tier1Template
    }

    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>> {
        Ok(parse_hourly_updown(metadata)
            .or_else(|| parse_daily_updown(metadata))
            .or_else(|| parse_threshold_question(metadata)))
    }
}

// ── up/down (relative) templates ────────────────────────────────────────────

/// `{alias}-up-or-down-{month}-{day}(-{year})?-{hour}{am|pm}-et` → 1h window.
fn parse_hourly_updown(metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
    let slug = metadata.slug.as_str();
    let captures = HOURLY_SLUG.captures(slug)?;
    let rule = rule_for_alias(captures.get(1)?.as_str())?;
    let month = month_number(captures.get(2)?.as_str())?;
    let day: u32 = captures.get(3)?.as_str().parse().ok()?;
    let year = captures
        .get(4)
        .map_or_else(|| infer_year(metadata), |m| m.as_str().parse().ok())?;
    let hour_12: u32 = captures.get(5)?.as_str().parse().ok()?;
    let meridiem = captures.get(6)?.as_str();
    let hour = to_24h(hour_12, meridiem)?;

    let window_open = eastern_instant(year, month, day, hour, 0)?;
    let subject = updown_subject(rule, window_open, window_open + Duration::hours(1));
    Some(templated_candidate(
        rule,
        subject,
        metadata,
        captures.get(1)?.start(),
    ))
}

/// `{alias}-up-or-down-on-{month}-{day}(-{year})?` → noon-ET → noon-ET window.
fn parse_daily_updown(metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
    let slug = metadata.slug.as_str();
    let captures = DAILY_SLUG.captures(slug)?;
    let rule = rule_for_alias(captures.get(1)?.as_str())?;
    let month = month_number(captures.get(2)?.as_str())?;
    let day: u32 = captures.get(3)?.as_str().parse().ok()?;
    let year = captures
        .get(4)
        .map_or_else(|| infer_year(metadata), |m| m.as_str().parse().ok())?;

    // "… on March 5" spans Mar 4 noon ET → Mar 5 noon ET.
    let close = eastern_instant(year, month, day, 12, 0)?;
    let open = close - Duration::hours(24);
    let subject = updown_subject(rule, open, close);
    Some(templated_candidate(
        rule,
        subject,
        metadata,
        captures.get(1)?.start(),
    ))
}

/// A relative (price-to-beat) subject over `[open, close)`.
fn updown_subject(
    rule: &AssetRule,
    window_open: DateTime<Utc>,
    window_close: DateTime<Utc>,
) -> CryptoSubject {
    CryptoSubject {
        asset: rule.asset(),
        quote: rule.quote(),
        comparator: PriceComparator::UpVsReference,
        strike: None,
        reference_at: Some(window_open),
        observation_at: window_close,
        resolution_oracle: ResolutionOracle::ChainlinkDataStreams { feed: rule.feed() },
    }
}

/// Assemble a slug-templated candidate: the asset anchors to its alias span,
/// derived fields to the full template match, and the oracle to the rules
/// text when present (falling back to the template for the recurring series
/// whose slug alone entails the Chainlink stream).
fn templated_candidate(
    rule: &AssetRule,
    mut subject: CryptoSubject,
    metadata: &LinkageSourceMetadata,
    alias_start: usize,
) -> ExtractedCandidate {
    let slug = metadata.slug.as_str();
    let mut spans = vec![GroundingSpan {
        subject_field: "asset".to_owned(),
        source: GroundingField::Slug,
        start: alias_start,
        end: alias_start + alias_len(rule, slug, alias_start),
        text: slug[alias_start..alias_start + alias_len(rule, slug, alias_start)].to_owned(),
    }];
    let full = |subject_field: &str| GroundingSpan {
        subject_field: subject_field.to_owned(),
        source: GroundingField::Slug,
        start: 0,
        end: slug.len(),
        text: slug.to_owned(),
    };
    spans.push(full("comparator"));
    spans.push(full("reference_at"));
    spans.push(full("observation_at"));

    // Prefer the description's literal oracle citation when the rules text is
    // present; the slug template remains the deterministic fallback anchor.
    if let Some((oracle, span)) = extract_oracle(rule, metadata) {
        subject.resolution_oracle = oracle;
        spans.push(span);
    } else {
        spans.push(full("resolution_oracle"));
    }

    ExtractedCandidate {
        subject: MarketSubject::Crypto(subject),
        instrument_key: rule.instrument_key(),
        confidence: Probability::ONE,
        grounding: GroundingProof { spans },
    }
}

/// The matched alias length at `start` (longest alias of the rule matching).
fn alias_len(rule: &AssetRule, slug: &str, start: usize) -> usize {
    rule.aliases
        .iter()
        .filter(|alias| slug[start..].starts_with(*alias))
        .map(|alias| alias.len())
        .max()
        .unwrap_or(0)
}

// ── threshold / band questions ──────────────────────────────────────────────

/// "Will {asset} reach/dip to/close above … $X (… and $Y) …?" over `question`.
fn parse_threshold_question(metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
    let question = metadata.question.as_str();
    let lowered = question.to_lowercase();
    let (rule, alias, alias_offset) = find_alias(&lowered)?;
    // Threshold markets settle at the market's scheduled resolution instant.
    let observation_at = metadata.end_date?;
    let (oracle, oracle_span) = extract_oracle(rule, metadata)?;

    let mut spans = vec![
        GroundingSpan {
            subject_field: "asset".to_owned(),
            source: GroundingField::Question,
            start: alias_offset,
            end: alias_offset + alias.len(),
            text: question[alias_offset..alias_offset + alias.len()].to_owned(),
        },
        oracle_span,
    ];

    let (comparator, strike) = if let Some(captures) = DOLLAR_BAND.captures(question) {
        let lo = parse_dollars(captures.get(1)?.as_str(), captures.get(2))?;
        let hi = parse_dollars(captures.get(3)?.as_str(), captures.get(4))?;
        if hi <= lo {
            return None;
        }
        let full = captures.get(0)?;
        spans.push(question_span("strike", question, full.start(), full.end()));
        spans.push(question_span(
            "comparator",
            question,
            full.start(),
            full.end(),
        ));
        (
            PriceComparator::Between { hi: Usd::new(hi) },
            Some(Usd::new(lo)),
        )
    } else {
        let amount = DOLLAR_AMOUNT.captures(question)?;
        let strike = parse_dollars(amount.get(1)?.as_str(), amount.get(2))?;
        let amount_full = amount.get(0)?;
        spans.push(question_span(
            "strike",
            question,
            amount_full.start(),
            amount_full.end(),
        ));
        let comparator = if let Some(above) = ABOVE_PHRASE.find(question) {
            spans.push(question_span(
                "comparator",
                question,
                above.start(),
                above.end(),
            ));
            PriceComparator::Above
        } else if let Some(below) = BELOW_PHRASE.find(question) {
            spans.push(question_span(
                "comparator",
                question,
                below.start(),
                below.end(),
            ));
            PriceComparator::Below
        } else {
            // A dollar amount with no recognizable direction is ambiguous —
            // fail through, never guess a side.
            return None;
        };
        (comparator, Some(Usd::new(strike)))
    };

    let subject = CryptoSubject {
        asset: rule.asset(),
        quote: rule.quote(),
        comparator,
        strike,
        reference_at: None,
        observation_at,
        resolution_oracle: oracle,
    };
    Some(ExtractedCandidate {
        subject: MarketSubject::Crypto(subject),
        instrument_key: rule.instrument_key(),
        confidence: Probability::ONE,
        grounding: GroundingProof { spans },
    })
}

/// A grounding span over the question text.
fn question_span(field: &str, question: &str, start: usize, end: usize) -> GroundingSpan {
    GroundingSpan {
        subject_field: field.to_owned(),
        source: GroundingField::Question,
        start,
        end,
        text: question[start..end].to_owned(),
    }
}

/// Parse `$X` with thousands separators and `k`/`m` suffix into a decimal.
fn parse_dollars(digits: &str, suffix: Option<regex::Match<'_>>) -> Option<Decimal> {
    let cleaned = digits.replace(',', "");
    let base: Decimal = cleaned.parse().ok()?;
    let multiplier = match suffix.map(|m| m.as_str().to_ascii_lowercase()) {
        Some(ref s) if s == "k" => Decimal::from(1_000),
        Some(ref s) if s == "m" => Decimal::from(1_000_000),
        Some(_) | None => Decimal::ONE,
    };
    Some(base * multiplier)
}

// ── settlement-oracle extraction (description rules text) ───────────────────

/// Extract the settlement oracle from the rules text with its literal anchor.
fn extract_oracle(
    rule: &AssetRule,
    metadata: &LinkageSourceMetadata,
) -> Option<(ResolutionOracle, GroundingSpan)> {
    let description = metadata.description.as_deref()?;
    let span = |start: usize, end: usize| GroundingSpan {
        subject_field: "resolution_oracle".to_owned(),
        source: GroundingField::Description,
        start,
        end,
        text: description[start..end].to_owned(),
    };

    if let Some(captures) = CHAINLINK_STREAM.captures(description) {
        let feed_text = captures.get(1)?.as_str().to_uppercase();
        let feed = ChainlinkFeedKey::parse(feed_text).ok()?;
        let full = captures.get(0)?;
        return Some((
            ResolutionOracle::ChainlinkDataStreams { feed },
            span(full.start(), full.end()),
        ));
    }
    if let Some(matched) = BINANCE_CANDLE.find(description) {
        return Some((
            ResolutionOracle::BinanceKline {
                symbol: rule.symbol(),
                interval: KlineInterval::OneMinute,
            },
            span(matched.start(), matched.end()),
        ));
    }
    if let Some(matched) = RESOLUTION_SENTENCE.find(description) {
        return Some((
            ResolutionOracle::Other {
                descriptor: matched.as_str().to_owned(),
            },
            span(matched.start(), matched.end()),
        ));
    }
    None
}

// ── Eastern-Time helpers (DST-correct, fail-closed) ─────────────────────────

/// Month name → number.
fn month_number(name: &str) -> Option<u32> {
    Some(match name {
        "january" => 1,
        "february" => 2,
        "march" => 3,
        "april" => 4,
        "may" => 5,
        "june" => 6,
        "july" => 7,
        "august" => 8,
        "september" => 9,
        "october" => 10,
        "november" => 11,
        "december" => 12,
        _ => return None,
    })
}

/// 12-hour clock → 24-hour.
fn to_24h(hour_12: u32, meridiem: &str) -> Option<u32> {
    if hour_12 == 0 || hour_12 > 12 {
        return None;
    }
    Some(match (hour_12, meridiem) {
        (12, "am") => 0,
        (12, "pm") => 12,
        (h, "am") => h,
        (h, "pm") => h + 12,
        _ => return None,
    })
}

/// An unambiguous Eastern-Time instant in UTC.
///
/// DST transition hours are ambiguous or nonexistent locally — fail closed
/// (return `None`) rather than picking an offset.
fn eastern_instant(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Utc>> {
    match New_York.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(instant) => Some(instant.with_timezone(&Utc)),
        LocalResult::Ambiguous(_, _) | LocalResult::None => None,
    }
}

/// The slug year when the vintage omits it: taken from the market's scheduled
/// resolution instant in Eastern Time (the slug's own clock).
fn infer_year(metadata: &LinkageSourceMetadata) -> Option<i32> {
    use chrono::Datelike;
    Some(metadata.end_date?.with_timezone(&New_York).year())
}

#[cfg(test)]
mod tests {
    use super::CryptoSubjectParser;
    use crate::linkage::extractor::{
        DefaultSubjectValidator, SubjectExtractor, SubjectValidator, ValidationOutcome,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{LinkageSourceMetadata, MarketSubject, PriceComparator, ResolutionOracle},
        types::{MarketId, Usd},
    };
    use rust_decimal_macros::dec;

    const CHAINLINK_RULES: &str = "This market will resolve to \"Up\" if the Bitcoin price at \
         the end of the time range specified in the title is greater than or equal to the price \
         at the beginning of that range. The resolution source for this market is information \
         from Chainlink, specifically the BTC/USD data stream available at \
         https://data.chain.link/streams/btc-usd.";

    const BINANCE_RULES: &str = "This market will resolve according to the Binance BTCUSDT \
         1 minute candle closing price on the resolution date.";

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
    fn hourly_et_slug_parses_dst_correctly() {
        // July → EDT (UTC-4): 3pm ET = 19:00Z.
        let metadata = metadata(
            "bitcoin-up-or-down-july-7-3pm-et",
            "Bitcoin Up or Down - July 7, 3PM ET",
            Some(CHAINLINK_RULES),
        );
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        let open = Utc.with_ymd_and_hms(2026, 7, 7, 19, 0, 0).unwrap();
        assert_eq!(subject.reference_at, Some(open));
        assert_eq!(subject.observation_at, open + chrono::Duration::hours(1));
        assert!(matches!(
            &subject.resolution_oracle,
            ResolutionOracle::ChainlinkDataStreams { feed } if feed.as_str() == "BTC-USD"
        ));
        assert_eq!(
            DefaultSubjectValidator.validate(&candidate, &metadata),
            ValidationOutcome::Accepted
        );

        // The year-carrying vintage parses identically.
        let with_year = self::metadata(
            "bitcoin-up-or-down-july-7-2026-3pm-et",
            "Bitcoin Up or Down - July 7, 3PM ET",
            Some(CHAINLINK_RULES),
        );
        let candidate = CryptoSubjectParser
            .extract(&with_year)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        assert_eq!(subject.reference_at, Some(open));
    }

    #[test]
    fn daily_slug_spans_noon_et_to_noon_et() {
        let metadata = metadata(
            "bitcoin-up-or-down-on-july-7",
            "Bitcoin Up or Down on July 7",
            Some(CHAINLINK_RULES),
        );
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        // Noon EDT = 16:00Z; window opens the previous day.
        let close = Utc.with_ymd_and_hms(2026, 7, 7, 16, 0, 0).unwrap();
        assert_eq!(subject.observation_at, close);
        assert_eq!(
            subject.reference_at,
            Some(close - chrono::Duration::hours(24))
        );
    }

    #[test]
    fn threshold_question_extracts_comparator_strike_and_binance_oracle() {
        let metadata = metadata(
            "will-bitcoin-reach-150000-in-july",
            "Will Bitcoin reach $150,000 in July?",
            Some(BINANCE_RULES),
        );
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        assert_eq!(subject.comparator, PriceComparator::Above);
        assert_eq!(subject.strike, Some(Usd::new(dec!(150000))));
        assert!(matches!(
            &subject.resolution_oracle,
            ResolutionOracle::BinanceKline { symbol, .. } if symbol.as_str() == "BTCUSDT"
        ));
        assert_eq!(
            DefaultSubjectValidator.validate(&candidate, &metadata),
            ValidationOutcome::Accepted
        );
    }

    #[test]
    fn band_question_extracts_between() {
        let eth_rules = CHAINLINK_RULES.replace("btc-usd", "eth-usd");
        let metadata = metadata(
            "what-price-will-ethereum-hit-in-july",
            "Will Ethereum close between $2,500 and $3,000 on July 31?",
            Some(&eth_rules),
        );
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        assert_eq!(subject.strike, Some(Usd::new(dec!(2500))));
        assert_eq!(
            subject.comparator,
            PriceComparator::Between {
                hi: Usd::new(dec!(3000))
            }
        );
    }

    #[test]
    fn missing_oracle_or_direction_fails_through() {
        // No description ⇒ threshold questions cannot ground an oracle.
        let no_rules = metadata(
            "will-bitcoin-reach-150000-in-july",
            "Will Bitcoin reach $150,000 in July?",
            None,
        );
        assert!(
            CryptoSubjectParser
                .extract(&no_rules)
                .expect("extract")
                .is_none()
        );

        // A dollar amount with no direction phrase is ambiguous.
        let ambiguous = metadata(
            "bitcoin-price-july",
            "Bitcoin $150,000 in July?",
            Some(BINANCE_RULES),
        );
        assert!(
            CryptoSubjectParser
                .extract(&ambiguous)
                .expect("extract")
                .is_none()
        );
    }
}
