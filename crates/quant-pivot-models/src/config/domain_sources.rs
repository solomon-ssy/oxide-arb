//! External-vertical domain data sources (`[domain_sources]`, deploy).
//!
//! Connection-level parameters for the Phase 11.2.2 crypto feature sources:
//! the Binance spot REST kline client and the Chainlink on-chain aggregator
//! reader (which reuses the Polygon RPC endpoint from `[polymarket.onchain]`).
//! Runtime tunables (per-family enablement, source delay, backfill depth,
//! basis thresholds) live in `runtime_config.domain` — never here.

use std::collections::BTreeMap;

use serde::Deserialize;

/// External domain data-source connections.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DomainSourcesConfig {
    /// Binance spot REST kline source (public market data, keyless).
    pub binance: BinanceSourceConfig,
    /// Chainlink on-chain aggregator source (Polygon `eth_call` reads).
    pub chainlink: ChainlinkSourceConfig,
}

/// Binance spot REST market-data client (`GET /api/v3/klines`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BinanceSourceConfig {
    /// Enable Binance kline ingestion. Default: true.
    pub enabled: bool,
    /// REST base URL. Default: `https://api.binance.com`.
    pub base_url: String,
    /// Proactive request-weight budget per minute (klines cost weight 2/req;
    /// the venue IP budget is 6000/min — stay far below it). Default: 1000.
    pub weight_budget_per_min: u32,
    /// Incremental poll cadence in seconds. Default: 30.
    pub poll_secs: u64,
    /// Maximum rows written per `ClickHouse` batch. Default: 5000.
    pub batch_size: usize,
}

impl Default for BinanceSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: default_binance_url(),
            weight_budget_per_min: 1_000,
            poll_secs: 30,
            batch_size: 5_000,
        }
    }
}

fn default_binance_url() -> String {
    "https://api.binance.com".into()
}

/// Chainlink on-chain aggregator reader (`AggregatorV3` `latestRoundData` /
/// `getRoundData` via the shared Polygon RPC).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainlinkSourceConfig {
    /// Enable Chainlink oracle-quote ingestion. Default: true.
    pub enabled: bool,
    /// Incremental poll cadence in seconds. Default: 15.
    pub poll_secs: u64,
    /// Maximum historical rounds back-scanned per feed on bootstrap (bounds
    /// `getRoundData` RPC volume; older basis history stays fail-closed
    /// missing). Default: 500.
    pub max_round_backscan: u32,
    /// Aggregator **proxy** addresses keyed by feed key (e.g. `BTC-USD`).
    /// Defaults cover the Polygon mainnet proxies for the launch asset set.
    pub feeds: BTreeMap<String, String>,
}

impl Default for ChainlinkSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_secs: 15,
            max_round_backscan: 500,
            feeds: default_polygon_feeds(),
        }
    }
}

/// Polygon mainnet aggregator proxy addresses for the launch asset set.
fn default_polygon_feeds() -> BTreeMap<String, String> {
    [
        ("BTC-USD", "0xc907E116054Ad103354f2D350FD2514433D57F6f"),
        ("ETH-USD", "0xF9680D99D6C9589e2a93a78A04A279e509205945"),
        ("SOL-USD", "0x16F8008c3e89f62e5e2b909Ce70999370D38F4F2"),
        ("XRP-USD", "0x979211Dfbc0738559B778a6a58a5b1bbbBe720f9"),
        ("DOGE-USD", "0x1c747D909102bfCdb305C54bDdDBdA3eF588B1d0"),
    ]
    .into_iter()
    .map(|(feed, address)| (feed.to_owned(), address.to_owned()))
    .collect()
}
