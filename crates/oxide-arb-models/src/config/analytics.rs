//! `ClickHouse` OLAP analytics configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct AnalyticsConfig {
    #[serde(default = "default_ch_url")]
    pub clickhouse_url: String,
    #[serde(default = "default_ch_database")]
    pub clickhouse_database: String,
    #[serde(default = "default_ch_user")]
    pub clickhouse_user: String,
    #[serde(default)]
    pub clickhouse_password: String,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum concurrent `ClickHouse` insert operations. Prevents overwhelming
    /// the server under high tick ingestion rates.
    #[serde(default = "default_max_concurrent_inserts")]
    pub max_concurrent_inserts: usize,

    /// Maximum acceptable replication/insert lag in seconds. When the CH server
    /// reports lag exceeding this value, writes are throttled via exponential
    /// back-off until lag subsides.
    #[serde(default = "default_max_lag_secs")]
    pub max_lag_secs: f64,

    /// Interval (seconds) between lag probes to the `ClickHouse` server.
    #[serde(default = "default_lag_probe_interval_secs")]
    pub lag_probe_interval_secs: u64,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            clickhouse_url: default_ch_url(),
            clickhouse_database: default_ch_database(),
            clickhouse_user: default_ch_user(),
            clickhouse_password: String::new(),
            flush_interval_secs: default_flush_interval(),
            batch_size: default_batch_size(),
            max_concurrent_inserts: default_max_concurrent_inserts(),
            max_lag_secs: default_max_lag_secs(),
            lag_probe_interval_secs: default_lag_probe_interval_secs(),
        }
    }
}

fn default_ch_url() -> String {
    "http://localhost:8123".into()
}
fn default_ch_database() -> String {
    "oxide_arb".into()
}
fn default_ch_user() -> String {
    "default".into()
}
const fn default_flush_interval() -> u64 {
    10
}
const fn default_batch_size() -> usize {
    1000
}
const fn default_max_concurrent_inserts() -> usize {
    4
}
const fn default_max_lag_secs() -> f64 {
    10.0
}
const fn default_lag_probe_interval_secs() -> u64 {
    5
}
