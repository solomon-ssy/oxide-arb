//! The frozen, versioned crypto resolver ruleset (Phase 11.2.2).
//!
//! One table binds each supported asset's **slug aliases** (as they appear in
//! Polymarket slugs/questions) to its Binance spot symbol and its Chainlink
//! feed key. The table is data, not types: listing a new asset is an entry
//! here plus a [`CRYPTO_RESOLVER_VERSION`] bump — historical linkages remain
//! replayable because every ledger record carries the ruleset version that
//! produced it (the bitemporal ruleset axis).
//!
//! Any asset absent from this table fails closed to `Unresolved` — the
//! resolver never guesses a venue symbol or feed binding.

use quant_pivot_models::{
    enums::domain::KlineInterval,
    types::{
        BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
        ResolverVersion,
    },
};

/// The current deterministic resolver ruleset version.
///
/// Bump **every** time the alias / symbol / feed table below changes.
pub const DOMAIN_RESOLVER_VERSION: ResolverVersion = ResolverVersion::new(2);

/// One asset's frozen resolution bindings.
#[derive(Debug, Clone, Copy)]
pub struct AssetRule {
    /// Canonical ticker (uppercase).
    pub ticker: &'static str,
    /// Lowercase aliases as they appear in slugs / questions (`btc`,
    /// `bitcoin`, …). Order is by specificity (longest first) so alias
    /// scanning is deterministic.
    pub aliases: &'static [&'static str],
    /// Binance spot symbol (feature source).
    pub binance_symbol: &'static str,
    /// Chainlink feed key (`{ASSET}-USD`, deploy-config `feeds` map key).
    pub chainlink_feed: &'static str,
}

/// The launch asset set (ruleset v1): the boards Polymarket runs
/// recurring crypto series on, by traded volume.
const RULES: &[AssetRule] = &[
    AssetRule {
        ticker: "BTC",
        aliases: &["bitcoin", "btc"],
        binance_symbol: "BTCUSDT",
        chainlink_feed: "BTC-USD",
    },
    AssetRule {
        ticker: "ETH",
        aliases: &["ethereum", "eth"],
        binance_symbol: "ETHUSDT",
        chainlink_feed: "ETH-USD",
    },
    AssetRule {
        ticker: "SOL",
        aliases: &["solana", "sol"],
        binance_symbol: "SOLUSDT",
        chainlink_feed: "SOL-USD",
    },
    AssetRule {
        ticker: "XRP",
        aliases: &["ripple", "xrp"],
        binance_symbol: "XRPUSDT",
        chainlink_feed: "XRP-USD",
    },
    AssetRule {
        ticker: "DOGE",
        aliases: &["dogecoin", "doge"],
        binance_symbol: "DOGEUSDT",
        chainlink_feed: "DOGE-USD",
    },
];

/// The full frozen ruleset (deterministic order).
#[must_use]
pub const fn rules() -> &'static [AssetRule] {
    RULES
}

/// Look up the rule whose alias exactly matches `alias` (lowercase).
#[must_use]
pub fn rule_for_alias(alias: &str) -> Option<&'static AssetRule> {
    RULES.iter().find(|rule| rule.aliases.contains(&alias))
}

/// Find the first alias occurrence in lowercase `text`, longest-alias-first per
/// rule so `bitcoin` wins over `btc` when both could match.
///
/// Returns the rule plus the matched alias and its byte offset (grounding span).
#[must_use]
pub fn find_alias(text: &str) -> Option<(&'static AssetRule, &'static str, usize)> {
    let mut best: Option<(&'static AssetRule, &'static str, usize)> = None;
    for rule in RULES {
        for alias in rule.aliases {
            if let Some(offset) = text.find(alias) {
                let better = match best {
                    None => true,
                    Some((_, best_alias, best_offset)) => {
                        offset < best_offset
                            || (offset == best_offset && alias.len() > best_alias.len())
                    }
                };
                if better {
                    best = Some((rule, alias, offset));
                }
            }
        }
    }
    best
}

impl AssetRule {
    /// The validated ticker.
    ///
    /// # Panics
    ///
    /// Never: ruleset entries are validated by the `ruleset_entries_are_valid`
    /// test at build time.
    #[must_use]
    pub fn asset(&self) -> CryptoAsset {
        CryptoAsset::parse(self.ticker).expect("ruleset ticker is validated by tests")
    }

    /// The quote currency the Polymarket rules text prices in.
    #[must_use]
    pub fn quote(&self) -> CryptoQuote {
        CryptoQuote::parse("USD").expect("static quote is valid")
    }

    /// The validated Binance symbol.
    #[must_use]
    pub fn symbol(&self) -> BinanceSymbol {
        BinanceSymbol::parse(self.binance_symbol).expect("ruleset symbol is validated by tests")
    }

    /// The validated Chainlink feed key.
    #[must_use]
    pub fn feed(&self) -> ChainlinkFeedKey {
        ChainlinkFeedKey::parse(self.chainlink_feed).expect("ruleset feed is validated by tests")
    }

    /// The canonical feature-source instrument key (`BINANCE:{symbol}:1m`).
    #[must_use]
    pub fn instrument_key(&self) -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(&self.symbol(), KlineInterval::OneMinute)
    }

    /// Low-latency Binance event stream used only by Binance-bound markets.
    #[must_use]
    pub fn binance_event_instrument(&self) -> DomainInstrumentKey {
        DomainInstrumentKey::binance_agg_trade(&self.symbol())
    }

    /// Exact Chainlink Data Streams resolution/event feed.
    #[must_use]
    pub fn chainlink_instrument(&self) -> DomainInstrumentKey {
        DomainInstrumentKey::chainlink_data_streams(&self.feed())
    }
}

#[cfg(test)]
mod tests {
    use super::{find_alias, rule_for_alias, rules};

    #[test]
    fn ruleset_entries_are_valid() {
        for rule in rules() {
            // Every binding must parse through its validated newtype — a
            // malformed ruleset entry is a build-time failure, never a runtime
            // guess.
            let _ = rule.asset();
            let _ = rule.symbol();
            let _ = rule.feed();
            let _ = rule.instrument_key();
            let _ = rule.binance_event_instrument();
            let _ = rule.chainlink_instrument();
            assert!(!rule.aliases.is_empty());
            for alias in rule.aliases {
                assert_eq!(*alias, alias.to_lowercase(), "aliases must be lowercase");
            }
        }
    }

    #[test]
    fn alias_lookup_is_exact_and_scanning_prefers_earliest_longest() {
        assert_eq!(rule_for_alias("btc").expect("btc").ticker, "BTC");
        assert_eq!(rule_for_alias("bitcoin").expect("bitcoin").ticker, "BTC");
        assert!(rule_for_alias("bitcorn").is_none());

        let (rule, alias, offset) =
            find_alias("bitcoin-up-or-down-july-7-3pm-et").expect("alias found");
        assert_eq!(rule.ticker, "BTC");
        assert_eq!(alias, "bitcoin");
        assert_eq!(offset, 0);

        let (rule, alias, _) = find_alias("will ethereum reach $5,000?").expect("alias found");
        assert_eq!(rule.ticker, "ETH");
        assert_eq!(alias, "ethereum");
    }
}
