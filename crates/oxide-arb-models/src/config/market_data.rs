//! Market-data connection configuration (`[market_data]`, deploy).
//!
//! Connection-level parameters only: WebSocket reconnect/sharding policy and
//! the Gamma catalog client. Staleness thresholds are runtime configuration
//! (`runtime_config::MarketDataRuntimeConfig`).

use serde::Deserialize;

/// Market-data connections (CLOB WebSocket + Gamma catalog).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MarketDataDeployConfig {
    /// Polymarket CLOB WebSocket sharding and reconnect policy.
    pub websocket: WebSocketConfig,
    /// Polymarket Gamma API client.
    pub gamma: GammaConfig,
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
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            reconnect_delay_ms: default_ws_reconnect(),
            max_reconnect_delay_ms: default_ws_max_reconnect(),
            max_subscriptions_per_connection: default_ws_max_subscriptions(),
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

/// Polymarket Gamma API configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GammaConfig {
    /// Gamma REST base URL. Default: `https://gamma-api.polymarket.com`.
    pub base_url: String,
    /// Interval (seconds) between full market-catalog syncs (also the gamma
    /// periodic task cadence). Default: `300`.
    pub full_sync_interval_secs: u64,
    /// Page size for catalog pagination. Default: `100`.
    pub page_size: u32,
}

impl Default for GammaConfig {
    fn default() -> Self {
        Self {
            base_url: default_gamma_url(),
            full_sync_interval_secs: default_gamma_full_sync_interval(),
            page_size: default_gamma_page_size(),
        }
    }
}

fn default_gamma_url() -> String {
    "https://gamma-api.polymarket.com".into()
}
const fn default_gamma_full_sync_interval() -> u64 {
    300
}
const fn default_gamma_page_size() -> u32 {
    100
}
