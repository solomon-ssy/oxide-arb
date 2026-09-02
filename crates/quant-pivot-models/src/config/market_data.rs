//! Market-data connection configuration (`[market_data]`, deploy).
//!
//! Connection-level parameters only: WebSocket reconnect/sharding policy and
//! the Gamma catalog client. Staleness thresholds are runtime configuration
//! (`runtime_config::MarketDataRuntimeConfig`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{polymarket::PolygonRpcEndpoint, secret::SecretText};

/// Market-data connections (CLOB WebSocket + Gamma catalog + Data API).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MarketDataDeployConfig {
    /// Polymarket CLOB WebSocket sharding and reconnect policy.
    pub websocket: WebSocketConfig,
    /// Polymarket Gamma API client.
    pub gamma: GammaConfig,
    /// Polymarket Data API client (keyless positions reads).
    pub data_api: DataApiConfig,
    /// Finalized exchange-history extraction and independent Polygon attestation.
    pub finalized_exchange_history: FinalizedExchangeHistoryConfig,
}

/// Polymarket CLOB WebSocket sharding and reconnect policy.
///
/// The transport adapter owns the market channel's official text heartbeat.
/// Its protocol cadence is not an application business-policy tunable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    /// Initial reconnect backoff (ms) after a dropped connection.
    /// Default: `1000`.
    pub reconnect_delay_ms: u64,
    /// Reconnect backoff ceiling (ms). Default: `30000`.
    pub max_reconnect_delay_ms: u64,
    /// Token subscriptions per WS connection before a new shard is opened.
    /// Affects connection count and per-connection event volume.
    /// Default: `200`.
    pub max_subscriptions_per_connection: usize,
    /// Maximum active engine tokens subscribed across all WS connections.
    /// Default: `2000`.
    pub engine_max_subscription_tokens: usize,
    /// Look-ahead window (hours) for engine WS subscription subscription. Default: `72`.
    pub engine_subscription_window_hours: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            reconnect_delay_ms: default_ws_reconnect(),
            max_reconnect_delay_ms: default_ws_max_reconnect(),
            max_subscriptions_per_connection: default_ws_max_subscriptions(),
            engine_max_subscription_tokens: default_subscription_limit(),
            engine_subscription_window_hours: default_subscription_window(),
        }
    }
}

const fn default_ws_reconnect() -> u64 {
    1_000
}
const fn default_ws_max_reconnect() -> u64 {
    30_000
}
const fn default_ws_max_subscriptions() -> usize {
    200
}
const fn default_subscription_limit() -> usize {
    2_000
}
const fn default_subscription_window() -> u64 {
    72
}

/// Polymarket Gamma API configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GammaConfig {
    /// Gamma REST base URL. Default: `https://gamma-api.polymarket.com`.
    pub base_url: String,
    /// Interval between complete keyset reconciliations. Default: `300` seconds.
    pub reconcile_interval_secs: u64,
    /// Page size for catalog pagination. Default: `100`.
    pub page_size: u32,
    /// Maximum successful keyset pages in one scan. Default: `10_000`.
    pub max_keyset_pages: u32,
    /// Maximum HTTP attempts, including retries, in one scan. Default: `50_000`.
    pub max_keyset_requests: u32,
    /// Closed-event identity horizon required by fresh-boot history projection. Default: `200` days.
    pub historical_identity_days: u32,
}

impl Default for GammaConfig {
    fn default() -> Self {
        Self {
            base_url: default_gamma_url(),
            reconcile_interval_secs: default_gamma_reconcile_interval(),
            page_size: default_gamma_page_size(),
            max_keyset_pages: default_gamma_pages(),
            max_keyset_requests: default_gamma_requests(),
            historical_identity_days: default_gamma_history_days(),
        }
    }
}

fn default_gamma_url() -> String {
    "https://gamma-api.polymarket.com".into()
}
const fn default_gamma_reconcile_interval() -> u64 {
    300
}
const fn default_gamma_page_size() -> u32 {
    100
}
const fn default_gamma_pages() -> u32 {
    10_000
}
const fn default_gamma_requests() -> u32 {
    50_000
}
const fn default_gamma_history_days() -> u32 {
    200
}
/// Polymarket Data API configuration (keyless positions reads).
///
/// The Data API serves the venue position ledger (`GET /positions?user=<funder>`)
/// used to mark the report capital base. No credentials are required — only the
/// proxy/funder address (configured under `[quant.account]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataApiConfig {
    /// Data API base URL. Default: `https://data-api.polymarket.com`.
    pub base_url: String,
    /// Page size for positions pagination (`1..=500`). Default: `500`.
    pub page_size: u32,
    /// Minimum token size to include (`sizeThreshold`). Default: `1`.
    pub size_threshold: u32,
}

impl Default for DataApiConfig {
    fn default() -> Self {
        Self {
            base_url: default_data_api_url(),
            page_size: default_api_page_size(),
            size_threshold: default_api_size_limit(),
        }
    }
}

fn default_data_api_url() -> String {
    "https://data-api.polymarket.com".into()
}
const fn default_api_page_size() -> u32 {
    500
}
const fn default_api_size_limit() -> u32 {
    1
}

/// `HyperSync` primary extraction endpoint and credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HyperSyncConfig {
    /// Stable identity of the primary extraction provider and failure domain.
    pub provider_id: String,
    /// Primary finalized Polygon exchange-history extraction endpoint.
    pub endpoint: String,
    /// Bearer credential used only by the primary historical extraction adapter.
    #[serde(serialize_with = "super::secret::serialize_empty")]
    pub api_token: SecretText,
}

impl Default for HyperSyncConfig {
    fn default() -> Self {
        Self {
            provider_id: "envio_hypersync_polygon".to_owned(),
            endpoint: "https://polygon.hypersync.xyz".to_owned(),
            api_token: SecretText::default(),
        }
    }
}

/// Independent archive-capable Polygon JSON-RPC witness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExchangeHistoryAttestorConfig {
    /// Stable identity of the independent archive witness provider.
    pub provider_id: String,
    /// JSON-RPC endpoint in a trust and failure domain independent of Envio.
    pub rpc_endpoint: PolygonRpcEndpoint,
    /// Maximum inclusive block span in one `eth_getLogs` request.
    pub max_blocks_per_log_request: u64,
    /// Maximum concurrent `eth_getLogs` subrequests within one logical chunk.
    pub max_concurrent_log_requests: usize,
}

impl Default for ExchangeHistoryAttestorConfig {
    fn default() -> Self {
        Self {
            provider_id: "publicnode_polygon_archive".to_owned(),
            rpc_endpoint: PolygonRpcEndpoint::Public {
                url: "https://polygon-bor-rpc.publicnode.com".to_owned(),
            },
            max_blocks_per_log_request: 10,
            max_concurrent_log_requests: 2,
        }
    }
}

impl ExchangeHistoryAttestorConfig {
    /// Resolve the endpoint only at the attestation adapter boundary.
    #[must_use]
    pub fn rpc_url(&self) -> &str {
        self.rpc_endpoint.resolved_url()
    }
}

/// Structural ceiling for one simultaneously resident provider chunk.
pub const EXCHANGE_HISTORY_MAX_BLOCKS_PER_CHUNK: u64 = 50_000;
/// Structural ceiling for sequential work admitted by one scheduling turn.
pub const EXCHANGE_HISTORY_MAX_BLOCKS_PER_TICK: u64 = 1_500_000;

/// Finalized Polygon exchange-history extraction, proof and projection policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinalizedExchangeHistoryConfig {
    /// Enable finalized history extraction and projection.
    pub enabled: bool,
    /// Poll interval after the activation frontier reaches the finalized head.
    pub poll_secs: u64,
    /// Primary history extractor.
    pub hypersync: HyperSyncConfig,
    /// Independent archive JSON-RPC witness.
    pub attestor: ExchangeHistoryAttestorConfig,
    /// HTTP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Per-request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Maximum streamed JSON response body accepted from `HyperSync`.
    pub max_hypersync_response_body_bytes: usize,
    /// Maximum streamed JSON-RPC response body accepted from the attestor.
    pub max_rpc_response_body_bytes: usize,
    /// Maximum max-width canonical JSON header/log array bytes retained for one chunk.
    pub max_canonical_chunk_bytes: usize,
    /// Minimum inclusive chunk span after adaptive contraction.
    pub min_blocks_per_chunk: u64,
    /// Maximum inclusive chunk span after adaptive expansion (at most 50,000).
    pub max_blocks_per_chunk: u64,
    /// First retry delay in milliseconds.
    pub retry_initial_ms: u64,
    /// Maximum retry delay in milliseconds.
    pub retry_max_ms: u64,
    /// Maximum attempts per provider pair before the frontier fails closed.
    pub retry_max_attempts: u32,
    /// Confirmation delay used to reconstruct `model_available_at`.
    pub model_confirmation_blocks: u64,
    /// Reconciliation buffer behind the accepted frontier.
    pub rollback_buffer_blocks: u64,
    /// Recent history window prioritized for the first pooled activation.
    pub activation_frontier_days: u32,
    /// Long-term raw history target filled after activation.
    pub retention_frontier_days: u32,
    /// Maximum blocks assigned to activation per turn (at most 1,500,000).
    pub hot_window_blocks_per_tick: u64,
    /// Maximum blocks assigned to retention per turn (at most 1,500,000).
    pub full_history_blocks_per_tick: u64,
    /// Maximum rows written in one fact batch.
    pub batch_size: usize,
}

impl Default for FinalizedExchangeHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_secs: 30,
            hypersync: HyperSyncConfig::default(),
            attestor: ExchangeHistoryAttestorConfig::default(),
            connect_timeout_ms: 10_000,
            request_timeout_ms: 60_000,
            max_hypersync_response_body_bytes: 64 * 1_024 * 1_024,
            max_rpc_response_body_bytes: 64 * 1_024 * 1_024,
            max_canonical_chunk_bytes: 64 * 1_024 * 1_024,
            min_blocks_per_chunk: 100,
            max_blocks_per_chunk: 2_000,
            retry_initial_ms: 500,
            retry_max_ms: 30_000,
            retry_max_attempts: 8,
            model_confirmation_blocks: 12,
            rollback_buffer_blocks: 200,
            activation_frontier_days: 33,
            retention_frontier_days: 200,
            hot_window_blocks_per_tick: 50_000,
            full_history_blocks_per_tick: 5_000,
            batch_size: 1_000,
        }
    }
}
