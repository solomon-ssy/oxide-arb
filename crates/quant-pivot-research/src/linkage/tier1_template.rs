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

use chrono::{DateTime, Datelike, Duration, LocalResult, TimeZone, Utc};
use chrono_tz::America::New_York;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::{
        CryptoSubject, GroundingField, GroundingKind, GroundingProof, GroundingSpan,
        LinkageSourceMetadata, MarketSubject, PriceBoundaryInclusion, PriceComparator,
    },
    enums::domain::ResolverTier,
    types::{Probability, Usd},
};
use regex::{Match, Regex};
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

/// A band question: "between $X and $Y".
static DOLLAR_BAND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)between\s+\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?\s+and\s+\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?",
    )
    .expect("static regex")
});

/// A threshold phrase followed by an optional-dollar amount. Anchoring the
/// amount to the comparator prevents date/year digits from becoming strikes.
static THRESHOLD_PHRASE_AMOUNT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(reach|hit|at or above|at least|close above|greater than|above|exceed|surpass|dip to|drop to|fall to|at or below|at most|close below|less than|below|fall under)\s+\$?([0-9][0-9,]*(?:\.[0-9]+)?)\s*([kKmM])?\b",
    )
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
        let (lower, upper) = band_boundary_policy(metadata.description.as_deref()?)?;
        (
            PriceComparator::Between {
                hi: Usd::new(hi),
                lower,
                upper,
            },
            Some(Usd::new(lo)),
        )
    } else {
        let threshold = THRESHOLD_PHRASE_AMOUNT.captures(question)?;
        let strike = parse_dollars(threshold.get(2)?.as_str(), threshold.get(3))?;
        let amount_full = threshold.get(2)?;
        spans.push(question_span(
            "strike",
            question,
            amount_full.start(),
            amount_full.end(),
        ));
        let phrase = threshold.get(1)?;
        spans.push(question_span(
            "comparator",
            question,
            phrase.start(),
            phrase.end(),
        ));
        let comparator = threshold_comparator(phrase.as_str())?;
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

/// Freeze exact threshold semantics from the literal question phrase.
fn threshold_comparator(phrase: &str) -> Option<PriceComparator> {
    Some(match phrase.to_ascii_lowercase().as_str() {
        "above" | "close above" | "greater than" | "exceed" | "surpass" => {
            PriceComparator::GreaterThan
        }
        "reach" | "hit" | "at or above" | "at least" => PriceComparator::GreaterThanOrEqual,
        "below" | "close below" | "less than" | "fall under" => PriceComparator::LessThan,
        "dip to" | "drop to" | "fall to" | "at or below" | "at most" => {
            PriceComparator::LessThanOrEqual
        }
        _ => return None,
    })
}

/// Derive a band's exact endpoint ownership from its resolution rules.
/// Ambiguous prose fails closed because sibling neg-risk bands cannot both own
/// the same settlement value.
fn band_boundary_policy(
    description: &str,
) -> Option<(PriceBoundaryInclusion, PriceBoundaryInclusion)> {
    let rules = description.to_ascii_lowercase();
    if rules.contains("falls exactly between two brackets")
        && rules.contains("higher range bracket")
    {
        return Some((
            PriceBoundaryInclusion::Inclusive,
            PriceBoundaryInclusion::Exclusive,
        ));
    }
    if rules.contains("inclusive of both") || rules.contains("both endpoints are inclusive") {
        return Some((
            PriceBoundaryInclusion::Inclusive,
            PriceBoundaryInclusion::Inclusive,
        ));
    }
    None
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
fn parse_dollars(digits: &str, suffix: Option<Match<'_>>) -> Option<Decimal> {
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
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{
            LinkageSourceMetadata, MarketSubject, PriceBoundaryInclusion, PriceComparator,
            ResolutionOracle,
        },
        enums::domain::KlineInterval,
        types::{MarketId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::CryptoSubjectParser;
    use crate::linkage::{
        extractor::{
            DefaultSubjectValidator, SubjectExtractor, SubjectValidator, ValidationOutcome,
        },
        rules,
    };

    const CHAINLINK_RULES: &str = "This market will resolve to \"Up\" if the Bitcoin price at \
         the end of the time range specified in the title is greater than or equal to the price \
         at the beginning of that range. The resolution source for this market is information \
         from Chainlink, specifically the BTC/USD data stream available at \
         https://data.chain.link/streams/btc-usd.";

    const BINANCE_RULES: &str = "This market will resolve according to the Binance BTCUSDT \
         1 minute candle closing price on the resolution date.";

    const HIGHER_BRACKET_RULES: &str = "The resolution source is the Binance ETH/USDT 1 minute \
         candle close. If the reported value falls exactly between two brackets, then this \
         market will resolve to the higher range bracket.";

    fn metadata(slug: &str, question: &str, description: Option<&str>) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: slug.to_owned(),
            question: question.to_owned(),
            description: description.map(str::to_owned),
            series_slug: None,
            decision_group_market_ids: Vec::new(),
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
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
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
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
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
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
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
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
        assert_eq!(subject.comparator, PriceComparator::GreaterThanOrEqual);
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
        let metadata = metadata(
            "what-price-will-ethereum-hit-in-july",
            "Will Ethereum close between $2,500 and $3,000 on July 31?",
            Some(HIGHER_BRACKET_RULES),
        );
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
        assert_eq!(subject.strike, Some(Usd::new(dec!(2500))));
        assert_eq!(
            subject.comparator,
            PriceComparator::Between {
                hi: Usd::new(dec!(3000)),
                lower: PriceBoundaryInclusion::Inclusive,
                upper: PriceBoundaryInclusion::Exclusive,
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
    fn every_supported_asset_parses_hourly_and_full_name_above_templates() {
        for rule in rules() {
            let ticker = rule.ticker.to_ascii_lowercase();
            let feed = rule.chainlink_feed.to_ascii_lowercase();
            let chainlink_rules =
                format!("The resolution source is https://data.chain.link/streams/{feed}.");
            let hourly = metadata(
                &format!("{ticker}-up-or-down-july-7-3pm-et"),
                &format!("{} Up or Down - July 7, 3PM ET", rule.ticker),
                Some(&chainlink_rules),
            );
            let hourly_candidate = CryptoSubjectParser
                .extract(&hourly)
                .expect("hourly extract")
                .expect("hourly fixture");
            let MarketSubject::Crypto(hourly_subject) = &hourly_candidate.subject else {
                panic!("crypto subject")
            };
            assert_eq!(hourly_subject.asset.as_str(), rule.ticker);
            assert_eq!(
                DefaultSubjectValidator.validate(&hourly_candidate, &hourly),
                ValidationOutcome::Accepted
            );

            let full_name = rule.aliases[0];
            let resolution_rules = format!(
                "This market resolves per the Binance {} 1 minute candle close.",
                rule.binance_symbol
            );
            let threshold = metadata(
                &format!("will-{ticker}-close-above-100"),
                &format!("Will {full_name} close above $100?"),
                Some(&resolution_rules),
            );
            let threshold_candidate = CryptoSubjectParser
                .extract(&threshold)
                .expect("threshold extract")
                .expect("full-name above fixture");
            let MarketSubject::Crypto(threshold_subject) = &threshold_candidate.subject else {
                panic!("crypto subject")
            };
            assert_eq!(threshold_subject.asset.as_str(), rule.ticker);
            assert_eq!(threshold_subject.comparator, PriceComparator::GreaterThan);
            assert_eq!(
                DefaultSubjectValidator.validate(&threshold_candidate, &threshold),
                ValidationOutcome::Accepted
            );
        }
    }

    #[test]
    fn real_hourly_binance_templates_freeze_one_hour_and_optional_dollar_strike() {
        let updown_rules = "This market resolves using the Binance BTC/USDT 1 hour candle close.";
        let updown = metadata(
            "bitcoin-up-or-down-july-18-2026-8pm-et",
            "Bitcoin Up or Down - July 18, 2026, 8PM ET",
            Some(updown_rules),
        );
        let candidate = CryptoSubjectParser
            .extract(&updown)
            .expect("extract")
            .expect("hourly up/down");
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
        assert!(matches!(
            subject.resolution_oracle,
            ResolutionOracle::BinanceKline {
                interval: KlineInterval::OneHour,
                ..
            }
        ));

        let threshold = metadata(
            "bitcoin-above-61400-on-july-17-2026-1pm-et",
            "Will Bitcoin be above 61,400 on July 17, 2026 at 1PM ET?",
            Some(updown_rules),
        );
        let candidate = CryptoSubjectParser
            .extract(&threshold)
            .expect("extract")
            .expect("hourly threshold");
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
        assert_eq!(subject.strike, Some(Usd::new(dec!(61400))));
        assert_eq!(subject.comparator, PriceComparator::GreaterThan);
        assert!(matches!(
            subject.resolution_oracle,
            ResolutionOracle::BinanceKline {
                interval: KlineInterval::OneHour,
                ..
            }
        ));
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
            decision_group_market_ids: Vec::new(),
            end_date: Some(end_date),
        };
        let candidate = CryptoSubjectParser
            .extract(&metadata)
            .expect("extract")
            .expect("recognized");
        let MarketSubject::Crypto(subject) = &candidate.subject else {
            panic!("crypto subject")
        };
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
            decision_group_market_ids: Vec::new(),
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
