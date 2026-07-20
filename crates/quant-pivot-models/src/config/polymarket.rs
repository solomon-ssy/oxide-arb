//! Polymarket platform configuration.
//!
//! This is the sole venue. There is no abstraction layer for "multiple venues" —
//! quant-pivot operates exclusively on Polymarket (Polygon chain).

use quant_pivot_error::config::ConfigError;
use serde::Deserialize;

use super::secret::SystemdCredentialRef;

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
    /// Typed Polygon JSON-RPC endpoint source.
    pub rpc_endpoint: PolygonRpcEndpoint,
    /// RPC request timeout (ms). Default: `10000`.
    pub rpc_timeout_ms: u64,
}

impl Default for OnchainConfig {
    fn default() -> Self {
        Self {
            rpc_endpoint: PolygonRpcEndpoint::default(),
            rpc_timeout_ms: default_rpc_timeout(),
        }
    }
}

impl OnchainConfig {
    /// Resolve a protected authenticated endpoint during deploy bootstrap.
    pub fn resolve_credentials(&mut self) -> Result<(), ConfigError> {
        self.rpc_endpoint.resolve()
    }

    /// Return the resolved URL at the Polygon adapter boundary.
    #[must_use]
    pub fn rpc_url(&self) -> &str {
        self.rpc_endpoint.resolved_url()
    }
}

/// Source of the Polygon JSON-RPC URL.
///
/// Public, non-secret endpoints may remain in source TOML. Authenticated URLs
/// (including provider keys embedded in path/query/user-info) are accepted only
/// through a protected systemd credential file and never rendered by `Debug`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolygonRpcEndpoint {
    Public { url: String },
    SystemdCredential { credential: SystemdCredentialRef },
}

impl Default for PolygonRpcEndpoint {
    fn default() -> Self {
        Self::Public {
            url: "https://polygon-rpc.com".to_owned(),
        }
    }
}

impl PolygonRpcEndpoint {
    fn resolve(&mut self) -> Result<(), ConfigError> {
        if let Self::SystemdCredential { credential } = self {
            credential.resolve("polymarket.onchain.rpc_endpoint.credential")?;
        }
        Ok(())
    }

    #[must_use]
    pub fn resolved_url(&self) -> &str {
        match self {
            Self::Public { url } => url,
            Self::SystemdCredential { credential } => credential.secret().expose_secret(),
        }
    }
}
const fn default_rpc_timeout() -> u64 {
    10_000
}

/// Polymarket gasless relayer parameters (`[polymarket.relayer]`).
///
/// The relayer submits money-moving transactions (e.g. CTF `redeemPositions`)
/// from a user's Proxy / Gnosis Safe wallet on the operator's behalf and pays
/// the gas. Authentication uses a typed systemd credential reference plus the
/// non-secret key-owner address. Plaintext credentials are never deploy values.
/// Only required when `quant.account.wallet_kind` is `proxy` or `gnosis_safe`;
/// EOA settlement signs and pays gas directly and ignores these fields.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelayerConfig {
    /// Relayer submit/status base URL. Default: `https://relayer-v2.polymarket.com`.
    pub base_url: String,
    /// Relayer API key (secret).
    pub api_key: Option<SystemdCredentialRef>,
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

impl RelayerConfig {
    /// Normalize empty credential references and address strings to unset.
    pub fn normalize(&mut self) {
        if self
            .api_key
            .as_ref()
            .is_some_and(|credential| !credential.is_configured())
        {
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
            .as_ref()
            .map(|credential| credential.secret().expose_secret())
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

fn default_relayer_url() -> String {
    "https://relayer-v2.polymarket.com".into()
}
const fn default_relayer_timeout() -> u64 {
    15_000
}

#[cfg(test)]
mod rpc_endpoint_tests {
    use super::{OnchainConfig, PolygonRpcEndpoint};
    use crate::config::secret::SystemdCredentialRef;

    #[test]
    fn endpoint_source_is_explicitly_tagged() {
        let public: OnchainConfig = toml::from_str(
            r#"
rpc_timeout_ms = 5000
rpc_endpoint = { source = "public", url = "https://polygon-rpc.com" }
"#,
        )
        .expect("deserialize public Polygon RPC endpoint");
        assert!(matches!(
            public.rpc_endpoint,
            PolygonRpcEndpoint::Public { .. }
        ));

        let protected: OnchainConfig = toml::from_str(
            r#"
rpc_endpoint = { source = "systemd_credential", credential = { name = "polygon-rpc-url" } }
"#,
        )
        .expect("deserialize protected Polygon RPC endpoint");
        assert!(matches!(
            protected.rpc_endpoint,
            PolygonRpcEndpoint::SystemdCredential { .. }
        ));
    }

    #[test]
    fn authenticated_endpoint_debug_is_redacted() {
        let endpoint = PolygonRpcEndpoint::SystemdCredential {
            credential: SystemdCredentialRef::from_resolved(
                "https://provider.invalid/v2/private-provider-key",
            ),
        };
        let debug = format!("{endpoint:?}");
        assert!(!debug.contains("private-provider-key"));
        assert!(debug.contains("<secret:redacted>"));
    }
}
