//! Market data pipeline configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct MarketDataConfig {
    #[serde(default = "default_staleness_fresh")]
    pub staleness_fresh_ms: u64,
    #[serde(default = "default_staleness_acceptable")]
    pub staleness_acceptable_ms: u64,
    #[serde(default = "default_staleness_stale")]
    pub staleness_stale_ms: u64,
    #[serde(default = "default_staleness_expired")]
    pub staleness_expired_ms: u64,
    #[serde(default)]
    pub websocket: WebSocketConfig,
    #[serde(default)]
    pub gamma: GammaConfig,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            staleness_fresh_ms: default_staleness_fresh(),
            staleness_acceptable_ms: default_staleness_acceptable(),
            staleness_stale_ms: default_staleness_stale(),
            staleness_expired_ms: default_staleness_expired(),
            websocket: WebSocketConfig::default(),
            gamma: GammaConfig::default(),
        }
    }
}

const fn default_staleness_fresh() -> u64 {
    2_000
}
const fn default_staleness_acceptable() -> u64 {
    5_000
}
const fn default_staleness_stale() -> u64 {
    15_000
}
const fn default_staleness_expired() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct WebSocketConfig {
    #[serde(default = "default_ws_reconnect")]
    pub reconnect_delay_ms: u64,
    #[serde(default = "default_ws_max_reconnect")]
    pub max_reconnect_delay_ms: u64,
    #[serde(default = "default_ws_ping_interval")]
    pub ping_interval_secs: u64,
    #[serde(default = "default_ws_max_subscriptions")]
    pub max_subscriptions_per_connection: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            reconnect_delay_ms: default_ws_reconnect(),
            max_reconnect_delay_ms: default_ws_max_reconnect(),
            ping_interval_secs: default_ws_ping_interval(),
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
const fn default_ws_ping_interval() -> u64 {
    30
}
const fn default_ws_max_subscriptions() -> usize {
    100
}

/// Polymarket Gamma API configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct GammaConfig {
    #[serde(default = "default_gamma_url")]
    pub base_url: String,
    #[serde(default = "default_gamma_poll")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_gamma_page_size")]
    pub page_size: u32,
}

impl Default for GammaConfig {
    fn default() -> Self {
        Self {
            base_url: default_gamma_url(),
            poll_interval_secs: default_gamma_poll(),
            page_size: default_gamma_page_size(),
        }
    }
}

fn default_gamma_url() -> String {
    "https://gamma-api.polymarket.com".into()
}
const fn default_gamma_poll() -> u64 {
    300
}
const fn default_gamma_page_size() -> u32 {
    100
}
