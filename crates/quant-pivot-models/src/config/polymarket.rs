//! Polymarket platform configuration.
//!
//! This is the sole venue. There is no abstraction layer for "multiple venues" —
//! quant-pivot operates exclusively on Polymarket (Polygon chain).

use super::fees::FeesConfig;
use serde::Deserialize;

/// Polymarket platform configuration. Mounted at `[polymarket]` in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketConfig {
    /// CLOB REST base URL. Default: `https://clob.polymarket.com`.
    pub clob_base_url: String,
    /// CLOB market-data WebSocket URL.
    /// Default: `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
    pub clob_ws_url: String,
    /// EVM chain ID; must be Polygon (`137`) — validated at startup.
    pub chain_id: u64,
    /// On-chain (Polygon RPC) parameters.
    pub onchain: OnchainConfig,
    /// Per-category fee schedule.
    pub fees: FeesConfig,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            clob_base_url: default_clob_url(),
            clob_ws_url: default_clob_ws_url(),
            chain_id: default_chain_id(),
            onchain: OnchainConfig::default(),
            fees: FeesConfig::default(),
        }
    }
}

fn default_clob_url() -> String {
    "https://clob.polymarket.com".into()
}
fn default_clob_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".into()
}
const fn default_chain_id() -> u64 {
    137
}

/// On-chain interaction parameters (Polygon RPC).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnchainConfig {
    /// Polygon JSON-RPC endpoint (CTF oracle + redeem transactions).
    /// Default: `https://polygon-rpc.com`.
    pub rpc_url: String,
    /// RPC request timeout (ms). Default: `10000`.
    pub rpc_timeout_ms: u64,
}

impl Default for OnchainConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_rpc_url(),
            rpc_timeout_ms: default_rpc_timeout(),
        }
    }
}

fn default_rpc_url() -> String {
    "https://polygon-rpc.com".into()
}
const fn default_rpc_timeout() -> u64 {
    10_000
}
