//! The frozen, versioned crypto resolver ruleset.
//!
//! One table binds each supported asset's **slug aliases** (as they appear in
//! Polymarket slugs/questions) to its Binance spot symbol and its Chainlink
//! feed key. The table is data, not types: listing a new asset is an entry
//! here plus a `CRYPTO_RESOLVER_VERSION` bump — historical linkages remain
//! replayable because every ledger record carries the ruleset version that
//! produced it (the bitemporal ruleset axis).
//!
//! Any asset absent from this table fails closed to `Unresolved` — the
//! resolver never guesses a venue symbol or feed binding.

use quant_pivot_models::{
    enums::domain::{BinanceMarketSegment, KlineInterval},
    types::{
        BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
        DomainSourceId, ResolverVersion,
    },
};

/// The current deterministic resolver ruleset version.
///
/// Bump **every** time the alias / symbol / feed table below changes.
pub const DOMAIN_RESOLVER_VERSION: ResolverVersion = ResolverVersion::new(1);

/// Symbols officially exposed by both public Polymarket RTDS Crypto topics.
pub const PUBLIC_RTDS_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP"];

/// Assets currently listed on the configured Binance Spot source.
pub const BINANCE_SPOT_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"];

/// Complete frozen price-contract scope.
pub const ALL_CRYPTO_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"];

/// Ruleset assets whose Chainlink settlement feed requires the authenticated
/// Data Streams adapter rather than public RTDS.
pub const CREDENTIAL_CHAINLINK_ASSETS: &[&str] = &["BNB", "DOGE", "HYPE"];

/// Credential-settled assets that still have a public Binance Spot feature plane.
pub const CREDENTIAL_BINANCE_ASSETS: &[&str] = &["BNB", "DOGE"];

/// Assets whose Binance contract cites USD-M Futures instead of Spot.
pub const BINANCE_USDM_FUTURES_ASSETS: &[&str] = &["HYPE"];

/// One asset's frozen resolution bindings.
#[derive(Debug, Clone, Copy)]
pub struct AssetRule {
    /// Canonical ticker (uppercase).
    pub ticker: &'static str,
    /// Lowercase aliases as they appear in slugs / questions (`btc`,
    /// `bitcoin`, …). Order is by specificity (longest first) so alias
    /// scanning is deterministic.
    pub aliases: &'static [&'static str],
    /// Exact Binance product used by Binance-cited contracts.
    pub binance_market: BinanceMarketSegment,
    /// Symbol within the exact Binance product.
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
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "BTCUSDT",
        chainlink_feed: "BTC-USD",
    },
    AssetRule {
        ticker: "ETH",
        aliases: &["ethereum", "eth"],
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "ETHUSDT",
        chainlink_feed: "ETH-USD",
    },
    AssetRule {
        ticker: "SOL",
        aliases: &["solana", "sol"],
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "SOLUSDT",
        chainlink_feed: "SOL-USD",
    },
    AssetRule {
        ticker: "XRP",
        aliases: &["ripple", "xrp"],
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "XRPUSDT",
        chainlink_feed: "XRP-USD",
    },
    AssetRule {
        ticker: "DOGE",
        aliases: &["dogecoin", "doge"],
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "DOGEUSDT",
        chainlink_feed: "DOGE-USD",
    },
    AssetRule {
        ticker: "BNB",
        aliases: &["binance coin", "bnb"],
        binance_market: BinanceMarketSegment::Spot,
        binance_symbol: "BNBUSDT",
        chainlink_feed: "BNB-USD",
    },
    AssetRule {
        ticker: "HYPE",
        aliases: &["hyperliquid", "hype"],
        binance_market: BinanceMarketSegment::UsdmFutures,
        binance_symbol: "HYPEUSDT",
        chainlink_feed: "HYPE-USD",
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
            for (offset, _) in text.match_indices(alias) {
                if !alias_has_token_boundaries(text, offset, alias.len()) {
                    continue;
                }
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

fn alias_has_token_boundaries(text: &str, offset: usize, length: usize) -> bool {
    let preceding = text[..offset].chars().next_back();
    let following = text[offset + length..].chars().next();
    preceding.is_none_or(|character| !character.is_ascii_alphanumeric())
        && following.is_none_or(|character| !character.is_ascii_alphanumeric())
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

    /// Canonical feature-source key in the exact Binance product.
    #[must_use]
    pub fn instrument_key(&self) -> DomainInstrumentKey {
        self.kline_instrument(KlineInterval::OneMinute)
    }

    #[must_use]
    pub fn kline_instrument(&self, interval: KlineInterval) -> DomainInstrumentKey {
        self.binance_market
            .kline_instrument(&self.symbol(), interval)
    }

    #[must_use]
    pub fn kline_source_id(&self) -> DomainSourceId {
        self.binance_market.kline_source()
    }

    /// Low-latency Binance event stream used only by Binance-bound markets.
    #[must_use]
    pub fn binance_event_instrument(&self) -> DomainInstrumentKey {
        self.binance_market.trade_instrument(&self.symbol())
    }

    #[must_use]
    pub fn binance_event_source_id(&self) -> DomainSourceId {
        self.binance_market.trade_source()
    }

    /// Whether both public RTDS topics document this asset.
    #[must_use]
    pub fn public_rtds_supported(&self) -> bool {
        PUBLIC_RTDS_ASSETS.contains(&self.ticker)
    }

    /// Exact public RTDS Binance event feed.
    #[must_use]
    pub fn rtds_binance_instrument(&self) -> DomainInstrumentKey {
        DomainInstrumentKey::polymarket_rtds_binance(&self.symbol())
    }

    /// Exact public RTDS Chainlink event/resolution feed.
    #[must_use]
    pub fn rtds_chainlink_instrument(&self) -> DomainInstrumentKey {
        DomainInstrumentKey::polymarket_rtds_chainlink(&self.feed())
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
    fn alias_lookup_exact_longest() {
        assert_eq!(rule_for_alias("btc").expect("btc").ticker, "BTC");
        assert_eq!(rule_for_alias("bitcoin").expect("bitcoin").ticker, "BTC");
        assert!(rule_for_alias("bitcorn").is_none());

        let (rule, alias, offset) =
            find_alias("bitcoin-up-or-down-july-7-3pm-et").expect("alias found");
        assert_eq!(rule.ticker, "BTC");
        assert_eq!(alias, "bitcoin");
        assert_eq!(offset, 0);
        assert!(find_alias("will this resolve without an asset").is_none());
        assert!(find_alias("whether the event occurs").is_none());

        let (rule, alias, _) = find_alias("will ethereum reach $5,000?").expect("alias found");
        assert_eq!(rule.ticker, "ETH");
        assert_eq!(alias, "ethereum");
    }
}
