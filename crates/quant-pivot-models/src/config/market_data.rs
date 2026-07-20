//! Market-data connection configuration (`[market_data]`, deploy).
//!
//! Connection-level parameters only: WebSocket reconnect/sharding policy and
//! the Gamma catalog client. Staleness thresholds are runtime configuration
//! (`runtime_config::MarketDataRuntimeConfig`).

use serde::Deserialize;

/// Absolute reconciliation input cap enforced at config validation and the
/// native-SQL repository boundary.
pub const MAX_TRADE_TAPE_RECONCILIATION_ROWS: usize = 1_000_000;

/// Market-data connections (CLOB WebSocket + Gamma catalog + Data API).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MarketDataDeployConfig {
    /// Polymarket CLOB WebSocket sharding and reconnect policy.
    pub websocket: WebSocketConfig,
    /// Polymarket Gamma API client.
    pub gamma: GammaConfig,
    /// Polymarket Data API client (keyless positions reads).
    pub data_api: DataApiConfig,
    /// On-chain trade-tape ingestion for structural participant-concentration facts.
    pub trade_tape_on_chain: TradeTapeOnChainConfig,
}

/// Polymarket CLOB WebSocket sharding and reconnect policy.
///
/// Transport heartbeats are owned by `polymarket_client_sdk_v2` (workspace
/// feature `heartbeats`). This struct does not expose ping intervals — they
/// are not configurable at the application layer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
            engine_max_subscription_tokens: default_engine_max_subscription_tokens(),
            engine_subscription_window_hours: default_engine_subscription_window_hours(),
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
const fn default_engine_max_subscription_tokens() -> usize {
    2_000
}
const fn default_engine_subscription_window_hours() -> u64 {
    72
}

/// Polymarket Gamma API configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
}

impl Default for GammaConfig {
    fn default() -> Self {
        Self {
            base_url: default_gamma_url(),
            reconcile_interval_secs: default_gamma_reconcile_interval(),
            page_size: default_gamma_page_size(),
            max_keyset_pages: default_gamma_max_keyset_pages(),
            max_keyset_requests: default_gamma_max_keyset_requests(),
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
const fn default_gamma_max_keyset_pages() -> u32 {
    10_000
}
const fn default_gamma_max_keyset_requests() -> u32 {
    50_000
}
/// Polymarket Data API configuration (keyless positions reads).
///
/// The Data API serves the venue position ledger (`GET /positions?user=<funder>`)
/// used to mark the report capital base. No credentials are required — only the
/// proxy/funder address (configured under `[quant.account]`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
            page_size: default_data_api_page_size(),
            size_threshold: default_data_api_size_threshold(),
        }
    }
}

fn default_data_api_url() -> String {
    "https://data-api.polymarket.com".into()
}
const fn default_data_api_page_size() -> u32 {
    500
}
const fn default_data_api_size_threshold() -> u32 {
    1
}

/// On-chain trade-tape ingestion configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TradeTapeOnChainConfig {
    /// Enable periodic on-chain `OrderFilled` ingestion. Default: true.
    pub enabled: bool,
    /// Poll interval in seconds. Default: 30.
    pub poll_secs: u64,
    /// Block confirmations before a chunk is considered finalized. Default: 12.
    pub confirmations: u64,
    /// Maximum blocks scanned per contract per tick. Default: 2000.
    pub max_blocks_per_tick: u64,
    /// Maximum inclusive block range sent in one `eth_getLogs` request. Default: 2000.
    pub max_blocks_per_request: u64,
    /// Maximum rows written per `ClickHouse` batch. Default: 1000.
    pub batch_size: usize,
    /// Re-read horizon for WS/on-chain reconciliation. Default: 3600 seconds.
    pub reconciliation_lookback_secs: u64,
    /// Maximum absolute event-time distance for an exact match. Default: 2000 ms.
    pub reconciliation_match_window_ms: u64,
    /// Age after which a still-unmatched WS print becomes unavailable. Default: 600 seconds.
    pub reconciliation_terminal_age_secs: u64,
    /// Hard row cap per reconciliation cycle; overflow fails without truncation.
    pub reconciliation_max_rows: usize,
}

impl Default for TradeTapeOnChainConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_secs: 30,
            confirmations: 12,
            max_blocks_per_tick: 2_000,
            max_blocks_per_request: 2_000,
            batch_size: 1_000,
            reconciliation_lookback_secs: 3_600,
            reconciliation_match_window_ms: 2_000,
            reconciliation_terminal_age_secs: 600,
            reconciliation_max_rows: 100_000,
        }
    }
}
