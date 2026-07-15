//! Polymarket platform configuration.
//!
//! This is the sole venue. There is no abstraction layer for "multiple venues" —
//! quant-pivot operates exclusively on Polymarket (Polygon chain).

use serde::Deserialize;
use std::fmt;

/// Polymarket platform configuration. Mounted at `[polymarket]` in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketConfig {
    /// CLOB REST base URL. Default: `https://clob.polymarket.com`.
    pub clob_base_url: String,
    /// CLOB market-data WebSocket URL.
    /// Default: `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
    pub clob_ws_url: String,
    /// Hard timeout for the single `POST /order` attempt, in milliseconds.
    /// A timeout is ambiguous and must be reconciled; it is never retried.
    pub order_post_timeout_ms: u64,
    /// Refresh cadence for append-only CLOB market-info observations.
    pub clob_market_info_refresh_secs: u64,
    /// EVM chain ID; must be Polygon (`137`) — validated at startup.
    pub chain_id: u64,
    /// On-chain (Polygon RPC) parameters.
    pub onchain: OnchainConfig,
    /// Gasless relayer used for Proxy / Gnosis Safe money-moving transactions.
    pub relayer: RelayerConfig,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            clob_base_url: default_clob_url(),
            clob_ws_url: default_clob_ws_url(),
            order_post_timeout_ms: default_order_post_timeout(),
            clob_market_info_refresh_secs: default_clob_market_info_refresh_secs(),
            chain_id: default_chain_id(),
            onchain: OnchainConfig::default(),
            relayer: RelayerConfig::default(),
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
const fn default_order_post_timeout() -> u64 {
    15_000
}
const fn default_clob_market_info_refresh_secs() -> u64 {
    900
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

/// Polymarket gasless relayer parameters (`[polymarket.relayer]`).
///
/// The relayer submits money-moving transactions (e.g. CTF `redeemPositions`)
/// from a user's Proxy / Gnosis Safe wallet on the operator's behalf and pays
/// the gas. Authentication uses Relayer API keys (`RELAYER_API_KEY` +
/// `RELAYER_API_KEY_ADDRESS`). The key is a secret: supply it via
/// `QUANT_PIVOT__POLYMARKET__RELAYER__API_KEY` or the gitignored local TOML.
/// Only required when `quant.account.wallet_kind` is `proxy` or `gnosis_safe`;
/// EOA settlement signs and pays gas directly and ignores these fields.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelayerConfig {
    /// Relayer submit/status base URL. Default: `https://relayer-v2.polymarket.com`.
    pub base_url: String,
    /// Relayer API key (secret).
    pub api_key: Option<String>,
    /// Ethereum address that owns the relayer API key (the signer EOA address).
    pub api_key_address: Option<String>,
    /// HTTP request timeout (ms). Default: `15000`.
    pub request_timeout_ms: u64,
}

impl Default for RelayerConfig {
    fn default() -> Self {
        Self {
            base_url: default_relayer_url(),
            api_key: None,
            api_key_address: None,
            request_timeout_ms: default_relayer_timeout(),
        }
    }
}

impl fmt::Debug for RelayerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayerConfig")
            .field("base_url", &self.base_url)
            .field("api_key", &redacted(self.api_key.as_ref()))
            .field("api_key_address", &self.api_key_address)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .finish()
    }
}

impl RelayerConfig {
    /// Normalize credential fields: empty strings (`api_key = ""`) become unset.
    pub fn normalize(&mut self) {
        if self.api_key.as_deref().is_some_and(str::is_empty) {
            self.api_key = None;
        }
        if self.api_key_address.as_deref().is_some_and(str::is_empty) {
            self.api_key_address = None;
        }
    }

    /// API key with surrounding whitespace ignored, `None` when blank.
    #[must_use]
    pub fn api_key(&self) -> Option<&str> {
        self.api_key
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
    }

    /// API key owner address, `None` when blank.
    #[must_use]
    pub fn api_key_address(&self) -> Option<&str> {
        self.api_key_address
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
    }

    /// Whether both relayer credentials are present (required for proxy/safe).
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.api_key().is_some() && self.api_key_address().is_some()
    }
}

const fn redacted(value: Option<&String>) -> &'static str {
    if value.is_some() {
        "<redacted>"
    } else {
        "<unset>"
    }
}

fn default_relayer_url() -> String {
    "https://relayer-v2.polymarket.com".into()
}
const fn default_relayer_timeout() -> u64 {
    15_000
}
