//! Polymarket platform configuration.
//!
//! This is the sole venue. There is no abstraction layer for "multiple venues" —
//! quant-pivot operates exclusively on Polymarket (Polygon chain).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::secret::SecretText;

/// Polymarket platform configuration. Mounted at `[polymarket]` in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// Settlement worker timing and bounded-work configuration.
    pub settlement: SettlementDeployConfig,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            clob_base_url: default_clob_url(),
            clob_ws_url: default_clob_ws_url(),
            order_post_timeout_ms: default_order_post_timeout(),
            clob_market_info_refresh_secs: default_market_refresh_secs(),
            chain_id: default_chain_id(),
            onchain: OnchainConfig::default(),
            relayer: RelayerConfig::default(),
            settlement: SettlementDeployConfig::default(),
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
    45_000
}
const fn default_market_refresh_secs() -> u64 {
    900
}

/// On-chain interaction parameters (Polygon RPC).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// Return the resolved URL at the Polygon adapter boundary.
    #[must_use]
    pub fn rpc_url(&self) -> &str {
        self.rpc_endpoint.resolved_url()
    }
}

/// Source of the Polygon JSON-RPC URL.
///
/// Public, non-secret endpoints may remain in source TOML. Authenticated URLs
/// (including provider keys embedded in path/query/user-info) use a protected
/// secret text and are never rendered by `Debug`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolygonRpcEndpoint {
    Public {
        url: String,
    },
    Protected {
        #[serde(serialize_with = "super::secret::serialize_empty")]
        url: SecretText,
    },
}

impl Default for PolygonRpcEndpoint {
    fn default() -> Self {
        Self::Public {
            url: "https://polygon-rpc.com".to_owned(),
        }
    }
}

impl PolygonRpcEndpoint {
    #[must_use]
    pub fn resolved_url(&self) -> &str {
        match self {
            Self::Public { url } => url,
            Self::Protected { url } => url.expose_secret(),
        }
    }
}
const fn default_rpc_timeout() -> u64 {
    10_000
}

/// Settlement submission rollout configuration (`[polymarket.settlement]`).
///
/// This switch is necessary but never sufficient: runtime mode, kill switch,
/// authorization, verified deployment capability and production evidence are
/// independent fail-closed gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SettlementDeployConfig {
    /// Exclusive case-claim lease duration. A crashed worker may be replaced
    /// only after this database-backed lease expires.
    pub claim_lease_secs: u64,
    /// Fixed TTL for a `SemiAuto` settlement authorization challenge.
    pub semi_auto_authorization_ttl_secs: u64,
    /// Durable discovery cadence; catalog events may only wake this poll early.
    pub discovery_poll_secs: u64,
    /// Recovery/submission polling cadence.
    pub submission_poll_secs: u64,
    /// Maximum cases processed by one worker tick.
    pub max_claims_per_tick: u64,
    /// Maximum concurrent signer-free settlement RPC operations.
    pub rpc_concurrency: usize,
    /// UI-only deployment-readiness cache TTL. Money-path preflight and
    /// admission always bypass this cache.
    pub readiness_ui_cache_secs: u64,
    /// Maximum contiguous finalized Polygon blocks read per external-evidence
    /// cursor pass.
    pub external_scan_block_span: u64,
    /// First durable retry delay before exponential backoff.
    pub retry_initial_secs: u64,
    /// Maximum durable delay between settlement retry attempts after exponential backoff.
    pub retry_max_secs: u64,
}

impl Default for SettlementDeployConfig {
    fn default() -> Self {
        Self {
            claim_lease_secs: 30,
            semi_auto_authorization_ttl_secs: 300,
            discovery_poll_secs: 30,
            submission_poll_secs: 5,
            max_claims_per_tick: 32,
            rpc_concurrency: 4,
            readiness_ui_cache_secs: 10,
            external_scan_block_span: 2_048,
            retry_initial_secs: 2,
            retry_max_secs: 300,
        }
    }
}

/// Polymarket gasless relayer parameters (`[polymarket.relayer]`).
///
/// The relayer submits explicitly authorized contract calls from a user's
/// Proxy, Gnosis Safe, or Deposit Wallet and pays the gas. Authentication uses
/// a deploy secret plus the non-secret key-owner address.
/// Required for every non-EOA wallet when order submission is enabled; EOA
/// settlement signs and pays gas directly and ignores these fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RelayerConfig {
    /// Relayer submit/status base URL. Default: `https://relayer-v2.polymarket.com`.
    pub base_url: String,
    /// Zeroizing Polymarket relayer API key used only by the authenticated submission adapter.
    #[serde(serialize_with = "super::secret::serialize_optional_empty")]
    pub api_key: Option<SecretText>,
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
    /// Normalize empty secrets and address strings to unset.
    pub fn normalize(&mut self) {
        if self.api_key.as_ref().is_some_and(SecretText::is_empty) {
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
            .map(SecretText::expose_secret)
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
    use super::{OnchainConfig, PolygonRpcEndpoint, PolymarketConfig};
    use crate::config::secret::SecretText;

    #[test]
    fn endpoint_source_explicitly_tagged() {
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
rpc_timeout_ms = 5000
rpc_endpoint = { source = "protected", url = "https://provider.invalid/v2/private-provider-key" }
"#,
        )
        .expect("deserialize protected Polygon RPC endpoint");
        assert!(matches!(
            protected.rpc_endpoint,
            PolygonRpcEndpoint::Protected { .. }
        ));
    }

    #[test]
    fn authenticated_endpoint_debug_redacted() {
        let endpoint = PolygonRpcEndpoint::Protected {
            url: SecretText::from("https://provider.invalid/v2/private-provider-key"),
        };
        let debug = format!("{endpoint:?}");
        assert!(!debug.contains("private-provider-key"));
        assert!(debug.contains("<secret:redacted>"));
    }

    #[test]
    fn settlement_governance_durations_defaults() {
        let config = PolymarketConfig::default().settlement;
        assert_eq!(config.claim_lease_secs, 30);
        assert_eq!(config.semi_auto_authorization_ttl_secs, 300);
        assert_eq!(config.readiness_ui_cache_secs, 10);
    }
}
