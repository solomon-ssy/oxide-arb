//! Settlement-oracle extraction shared by every deterministic tier.
//!
//! Every crypto market's settlement oracle — regardless of which tier
//! resolved the rest of the subject — is grounded to a **literal** anchor in
//! the rules text (`description`): the `data.chain.link/streams/{feed}` URL
//! for Chainlink Data Streams, a "Binance … 1-minute candle" citation, or a
//! recognized-but-unclassified "resolution source" sentence. There is no
//! ruleset-default fallback: a market whose description cannot be matched
//! against one of these anchors yields no oracle, and therefore no candidate
//! at all (fail through / `Unresolved`) — never a guessed oracle. This is the
//! fix for the audited fail-open bug where the up/down template path used to
//! default to the ruleset's Chainlink feed when the description didn't
//! ground one.

use quant_pivot_models::{
    domain::{GroundingField, GroundingKind, GroundingSpan, ResolutionOracle},
    enums::domain::KlineInterval,
    types::ChainlinkFeedKey,
};
use regex::Regex;
use std::sync::LazyLock;

use crate::linkage::ruleset::AssetRule;

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

#[cfg(test)]
mod tests {
    use super::extract_oracle;
    use crate::linkage::ruleset::rule_for_alias;
    use quant_pivot_models::domain::ResolutionOracle;

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
}
