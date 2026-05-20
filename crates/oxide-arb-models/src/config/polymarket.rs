//! Polymarket platform configuration.
//!
//! This is the sole venue. There is no abstraction layer for "multiple venues" —
//! oxide-arb operates exclusively on Polymarket (Polygon chain).

use serde::Deserialize;
use validator::Validate;

/// Polymarket platform configuration. Mounted at `[polymarket]` in TOML.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PolymarketConfig {
    #[serde(default = "default_clob_url")]
    pub clob_base_url: String,
    #[serde(default = "default_clob_ws_url")]
    pub clob_ws_url: String,
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,
    #[serde(default)]
    pub onchain: OnchainConfig,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            clob_base_url: default_clob_url(),
            clob_ws_url: default_clob_ws_url(),
            chain_id: default_chain_id(),
            onchain: OnchainConfig::default(),
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
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct OnchainConfig {
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
    #[serde(default = "default_rpc_timeout")]
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
