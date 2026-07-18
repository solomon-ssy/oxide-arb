//! Settlement-oracle extraction shared by every deterministic tier.
//!
//! Every crypto market's settlement oracle — regardless of which tier
//! resolved the rest of the subject — is grounded to a **literal** anchor in
//! the rules text (`description`): the `data.chain.link/streams/{feed}` URL
//! for Chainlink Data Streams, a "Binance … 1-minute candle" citation, or a
//! an explicitly recognized settlement citation. There is no
//! ruleset-default fallback: a market whose description cannot be matched
//! against one of these anchors yields no oracle, and therefore no candidate
//! at all (fail through / `Unresolved`) — never a guessed oracle. This is the
//! fix for the audited fail-open bug where the up/down template path used to
//! default to the ruleset's Chainlink feed when the description didn't
//! ground one.

use std::{collections::BTreeSet, sync::LazyLock};

use quant_pivot_models::{
    domain::{GroundingField, GroundingKind, GroundingSpan, ResolutionOracle},
    enums::domain::KlineInterval,
    types::ChainlinkFeedKey,
};
use regex::Regex;

use crate::linkage::ruleset::AssetRule;

/// The literal Chainlink Data Streams reference in the rules text.
static CHAINLINK_STREAM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"data\.chain\.link/streams/([a-z0-9]+-[a-z0-9]+)").expect("static regex")
});

/// Literal venue anchor around which pair/interval/candle evidence is bound.
static BINANCE_WORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bbinance\b").expect("static regex"));

static BINANCE_PAIR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b([a-z0-9]+)\s*/?\s*usdt\b").expect("static regex"));

static BINANCE_INTERVAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(1\s*(?:-| )?\s*minute|one[- ]minute|1m|1\s*(?:-| )?\s*hour|one[- ]hour|1h)\b",
    )
    .expect("static regex")
});

static CANDLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:candle|candlestick)\b").expect("static regex"));

/// Extract the settlement oracle from the rules text with its literal anchor.
///
/// `None` when the description is absent or grounds no recognized oracle —
/// callers must treat this as "no candidate", never fall back to a ruleset
/// default.
pub fn extract_oracle(
    rule: &AssetRule,
    description: Option<&str>,
) -> Option<(ResolutionOracle, GroundingSpan)> {
    let description = description?;
    let span = |start: usize, end: usize| GroundingSpan {
        subject_field: "resolution_oracle".to_owned(),
        source: GroundingField::Description,
        start,
        end,
        text: description[start..end].to_owned(),
        kind: GroundingKind::LiteralSpan,
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
    if let Some((interval, start, end)) = extract_binance_oracle(rule, description) {
        return Some((
            ResolutionOracle::BinanceKline {
                market: rule.binance_market,
                symbol: rule.symbol(),
                interval,
            },
            span(start, end),
        ));
    }
    None
}

/// Accept exactly one unambiguous Binance pair/interval settlement citation.
fn extract_binance_oracle(
    rule: &AssetRule,
    description: &str,
) -> Option<(KlineInterval, usize, usize)> {
    let symbol = rule.symbol();
    let mut candidate: Option<(KlineInterval, usize, usize)> = None;
    for venue in BINANCE_WORD.find_iter(description) {
        let mut window_start = venue.start().saturating_sub(500);
        while !description.is_char_boundary(window_start) {
            window_start += 1;
        }
        let mut window_end = (venue.end() + 500).min(description.len());
        while !description.is_char_boundary(window_end) {
            window_end -= 1;
        }
        let window = &description[window_start..window_end];
        let Some(candle) = CANDLE.find(window) else {
            continue;
        };
        let pairs = BINANCE_PAIR
            .captures_iter(window)
            .filter_map(|captures| captures.get(1))
            .map(|asset| format!("{}USDT", asset.as_str().to_ascii_uppercase()))
            .collect::<BTreeSet<_>>();
        if pairs.len() != 1 || !pairs.contains(symbol.as_str()) {
            return None;
        }
        let intervals = BINANCE_INTERVAL
            .captures_iter(window)
            .filter_map(|captures| captures.get(1))
            .filter_map(|interval| parse_interval(interval.as_str()))
            .collect::<BTreeSet<_>>();
        if intervals.len() != 1 {
            return None;
        }
        let interval = *intervals.first()?;
        let pair = BINANCE_PAIR.find(window)?;
        let interval_span = BINANCE_INTERVAL.find(window)?;
        let venue_start = venue.start() - window_start;
        let venue_end = venue.end() - window_start;
        let next = (
            interval,
            window_start + venue_start.min(pair.start()).min(interval_span.start()),
            window_start
                + venue_end
                    .max(pair.end())
                    .max(interval_span.end())
                    .max(candle.end()),
        );
        if candidate.is_some_and(|current| current.0 != interval) {
            return None;
        }
        candidate.get_or_insert(next);
    }
    candidate
}

fn parse_interval(value: &str) -> Option<KlineInterval> {
    let normalized = value.to_ascii_lowercase().replace([' ', '-'], "");
    match normalized.as_str() {
        "1minute" | "oneminute" | "1m" => Some(KlineInterval::OneMinute),
        "1hour" | "onehour" | "1h" => Some(KlineInterval::OneHour),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::extract_oracle;
    use crate::linkage::ruleset::rule_for_alias;
    use quant_pivot_models::{domain::ResolutionOracle, enums::domain::KlineInterval};

    #[test]
    fn no_description_grounds_no_oracle() {
        let rule = rule_for_alias("btc").expect("rule");
        assert!(extract_oracle(rule, None).is_none());
    }

    #[test]
    fn unrecognized_text_grounds_no_oracle() {
        let rule = rule_for_alias("btc").expect("rule");
        assert!(extract_oracle(rule, Some("This market resolves via magic.")).is_none());
    }

    #[test]
    fn chainlink_stream_url_grounds_data_streams() {
        let rule = rule_for_alias("btc").expect("rule");
        let (oracle, span) = extract_oracle(
            rule,
            Some("See https://data.chain.link/streams/btc-usd for the feed."),
        )
        .expect("oracle");
        assert!(matches!(
            oracle,
            ResolutionOracle::ChainlinkDataStreams { .. }
        ));
        assert_eq!(span.text, "data.chain.link/streams/btc-usd");
    }

    #[test]
    fn binance_candle_citation_grounds_binance_kline() {
        let rule = rule_for_alias("btc").expect("rule");
        let (oracle, _) = extract_oracle(
            rule,
            Some("Resolves per the Binance BTCUSDT 1 minute candle close."),
        )
        .expect("oracle");
        assert!(matches!(oracle, ResolutionOracle::BinanceKline { .. }));
    }

    #[test]
    fn binance_hourly_candle_freezes_pair_and_interval() {
        let rule = rule_for_alias("btc").expect("rule");
        let (oracle, span) = extract_oracle(
            rule,
            Some(
                "The close price is read from the BTC/USDT 1 hour candle. The resolution source \
                 is information from Binance, specifically the BTC/USDT pair.",
            ),
        )
        .expect("oracle");
        assert!(matches!(
            oracle,
            ResolutionOracle::BinanceKline {
                interval: KlineInterval::OneHour,
                ..
            }
        ));
        assert!(span.text.contains("BTC/USDT 1 hour candle"));
        assert!(span.text.contains("Binance"));
    }

    #[test]
    fn binance_citation_rejects_wrong_pair_or_conflicting_interval() {
        let rule = rule_for_alias("btc").expect("rule");
        assert!(
            extract_oracle(
                rule,
                Some("Resolution uses the Binance ETH/USDT 1 hour candle close."),
            )
            .is_none()
        );
        assert!(
            extract_oracle(
                rule,
                Some("Resolution uses the Binance BTC/USDT 1 minute and 1 hour candle close."),
            )
            .is_none()
        );
    }
}
