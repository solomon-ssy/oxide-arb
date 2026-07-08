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
//! settlement oracle is extracted from the **description** rules text via
//! [`crate::linkage::oracle::extract_oracle`] — the same function Tier 0
//! uses, so both tiers share one grounding contract. A market whose rules
//! text grounds no oracle produces no candidate at all (never a guessed
//! default from the ruleset).

use std::sync::LazyLock;

use chrono::{DateTime, Duration, LocalResult, TimeZone, Utc};
use chrono_tz::America::New_York;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        CryptoSubject, GroundingField, GroundingKind, GroundingProof, GroundingSpan,
        LinkageSourceMetadata, MarketSubject, PriceComparator,
    },
    enums::domain::ResolverTier,
    types::{Probability, Usd},
};
use regex::Regex;
use rust_decimal::Decimal;

use crate::linkage::{
    extractor::{ExtractedCandidate, SubjectExtractor},
    oracle::extract_oracle,
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
    let hour_12: u32 = captures.get(5)?.as_str().parse().ok()?;
    let meridiem = captures.get(6)?.as_str();
    let hour = to_24h(hour_12, meridiem)?;
    let year = captures.get(4).map_or_else(
        || infer_year(metadata, month, day, hour, 0),
        |m| m.as_str().parse().ok(),
    )?;

    let window_open = eastern_instant(year, month, day, hour, 0)?;
    let subject = updown_subject(
        rule,
        metadata,
        window_open,
        window_open + Duration::hours(1),
    )?;
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
    let year = captures.get(4).map_or_else(
        || infer_year(metadata, month, day, 12, 0),
        |m| m.as_str().parse().ok(),
    )?;

    // "… on March 5" spans Mar 4 noon ET → Mar 5 noon ET.
    let close = eastern_instant(year, month, day, 12, 0)?;
    let open = close - Duration::hours(24);
    let subject = updown_subject(rule, metadata, open, close)?;
    Some(templated_candidate(
        rule,
        subject,
        metadata,
        captures.get(1)?.start(),
    ))
}

/// A relative (price-to-beat) subject over `[open, close)`.
///
/// The oracle is independently grounded to the description's literal anchor
/// (never a ruleset default): `None` when the description does not ground a
/// recognized oracle, which propagates to "no candidate" at the call site —
/// fail through / `Unresolved`, never a guessed settlement source.
fn updown_subject(
    rule: &AssetRule,
    metadata: &LinkageSourceMetadata,
    window_open: DateTime<Utc>,
    window_close: DateTime<Utc>,
) -> Option<CryptoSubject> {
    let (resolution_oracle, _) = extract_oracle(rule, metadata.description.as_deref())?;
    Some(CryptoSubject {
        asset: rule.asset(),
        quote: rule.quote(),
        comparator: PriceComparator::UpVsReference,
        strike: None,
        reference_at: Some(window_open),
        observation_at: window_close,
        resolution_oracle,
    })
}

/// Assemble a slug-templated candidate: the asset anchors to its alias span,
/// window-derived fields to the full template match (template-entailed —
/// never independently written anywhere in the slug), and the oracle to its
/// own literal span in the description (re-derived here since `updown_subject`
/// only needed the oracle value, not its span).
fn templated_candidate(
    rule: &AssetRule,
    subject: CryptoSubject,
    metadata: &LinkageSourceMetadata,
    alias_start: usize,
) -> ExtractedCandidate {
    let slug = metadata.slug.as_str();
    let alias_end = alias_start + alias_len(rule, slug, alias_start);
    let mut spans = vec![GroundingSpan {
        subject_field: "asset".to_owned(),
        source: GroundingField::Slug,
        start: alias_start,
        end: alias_end,
        text: slug[alias_start..alias_end].to_owned(),
        kind: GroundingKind::LiteralSpan,
    }];
    let template_entailed = |subject_field: &str| GroundingSpan {
        subject_field: subject_field.to_owned(),
        source: GroundingField::Slug,
        start: 0,
        end: slug.len(),
        text: slug.to_owned(),
        kind: GroundingKind::TemplateEntailed,
    };
    spans.push(template_entailed("comparator"));
    spans.push(template_entailed("reference_at"));
    spans.push(template_entailed("observation_at"));

    // Re-derive the oracle's own literal span (the value was already resolved
    // by `updown_subject`; `extract_oracle` is a pure re-match, not a second
    // extraction decision).
    let (_, oracle_span) =
        extract_oracle(rule, metadata.description.as_deref()).expect("oracle already resolved");
    spans.push(oracle_span);

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
    let (oracle, oracle_span) = extract_oracle(rule, metadata.description.as_deref())?;

    let mut spans = vec![
        GroundingSpan {
            subject_field: "asset".to_owned(),
            source: GroundingField::Question,
            start: alias_offset,
            end: alias_offset + alias.len(),
            text: question[alias_offset..alias_offset + alias.len()].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        oracle_span,
        // `observation_at` is the market's own scheduled resolution instant —
        // not independently re-stated in the question/description text, so it
        // is template-entailed by the market record itself.
        GroundingSpan {
            subject_field: "observation_at".to_owned(),
            source: GroundingField::Question,
            start: 0,
            end: question.len(),
            text: question.to_owned(),
            kind: GroundingKind::TemplateEntailed,
        },
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

/// A literal grounding span over the question text.
fn question_span(field: &str, question: &str, start: usize, end: usize) -> GroundingSpan {
    GroundingSpan {
        subject_field: field.to_owned(),
        source: GroundingField::Question,
        start,
        end,
        text: question[start..end].to_owned(),
        kind: GroundingKind::LiteralSpan,
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

/// The maximum plausible drift between a yearless slug's inferred instant and
/// the market's `end_date` before the inference is rejected as ambiguous.
///
/// A genuine yearless slug's true instant is always within hours of
/// `end_date` for hourly markets, or within the ~24h daily window for daily
/// markets — never off by anything approaching a year. Three days is
/// generous headroom over both cases while decisively rejecting a
/// wrong-year inference (which is off by tens or hundreds of days, since the
/// three tested candidates are exactly a year apart): fail closed rather
/// than silently picking the "closest" wrong year.
const MAX_PLAUSIBLE_YEAR_DRIFT: Duration = Duration::days(3);

/// The slug year when the vintage omits it, resolved against `end_date`
/// (never assumed to share `end_date`'s calendar year outright — a slug near
/// a year boundary can name an instant in the year before or after
/// `end_date`'s own year).
///
/// Tries `end_date`'s Eastern-Time year and its immediate neighbors, and
/// accepts the reconstructed instant only when it lies within
/// [`MAX_PLAUSIBLE_YEAR_DRIFT`] of `end_date` — fail closed (`None`) when no
/// candidate is plausibly close, or when a valid local time cannot be built
/// for any candidate (DST transition).
fn infer_year(
    metadata: &LinkageSourceMetadata,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
) -> Option<i32> {
    use chrono::Datelike;

    let end_date = metadata.end_date?;
    let anchor_year = end_date.with_timezone(&New_York).year();
    let mut best: Option<(i32, Duration)> = None;
    for candidate_year in [anchor_year - 1, anchor_year, anchor_year + 1] {
        let Some(instant) = eastern_instant(candidate_year, month, day, hour, minute) else {
            continue;
        };
        let delta = (instant - end_date).abs();
        if best.is_none_or(|(_, best_delta)| delta < best_delta) {
            best = Some((candidate_year, delta));
        }
    }
    best.filter(|(_, delta)| *delta <= MAX_PLAUSIBLE_YEAR_DRIFT)
        .map(|(year, _)| year)
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

    #[test]
    fn up_down_template_without_description_fails_through_never_guesses_oracle() {
        // Regression guard for the audited fail-open bug: an hourly/daily
        // up/down slug with NO description must yield no candidate, never a
        // ruleset-default Chainlink oracle.
        let no_description = metadata(
            "bitcoin-up-or-down-july-7-3pm-et",
            "Bitcoin Up or Down - July 7, 3PM ET",
            None,
        );
        assert!(
            CryptoSubjectParser
                .extract(&no_description)
                .expect("extract")
                .is_none()
        );

        let daily_no_description = metadata(
            "bitcoin-up-or-down-on-july-7",
            "Bitcoin Up or Down on July 7",
            None,
        );
        assert!(
            CryptoSubjectParser
                .extract(&daily_no_description)
                .expect("extract")
                .is_none()
        );
    }

    #[test]
    fn year_boundary_slug_infers_the_correct_side_of_the_boundary() {
        // `end_date` lands just after New Year; the yearless December slug
        // must resolve to the PRIOR year, not `end_date`'s own year.
        let end_date = Utc.with_ymd_and_hms(2027, 1, 1, 5, 30, 0).unwrap(); // Jan 1 00:30 ET
        let metadata = LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: "bitcoin-up-or-down-december-31-11pm-et".to_owned(),
            question: "Bitcoin Up or Down - Dec 31, 11PM ET".to_owned(),
            description: Some(CHAINLINK_RULES.to_owned()),
            series_slug: None,
            end_date: Some(end_date),
        };
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject;
        // Dec 31, 2026, 11pm EST = Jan 1, 2027 04:00Z.
        let expected_open = Utc.with_ymd_and_hms(2027, 1, 1, 4, 0, 0).unwrap();
        assert_eq!(subject.reference_at, Some(expected_open));
    }

    #[test]
    fn implausible_year_drift_fails_closed() {
        // `end_date` is many months away from any calendar-year candidate for
        // this month/day — the inference must refuse to guess.
        let metadata = LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: "bitcoin-up-or-down-june-15-3pm-et".to_owned(),
            question: "Bitcoin Up or Down - June 15, 3PM ET".to_owned(),
            description: Some(CHAINLINK_RULES.to_owned()),
            series_slug: None,
            end_date: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
        };
        assert!(
            CryptoSubjectParser
                .extract(&metadata)
                .expect("extract")
                .is_none(),
            "June 15 candidates are ~165+ days from a Jan 1 end_date — must fail closed"
        );
    }
}
