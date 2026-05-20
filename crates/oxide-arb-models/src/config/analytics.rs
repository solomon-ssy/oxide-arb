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
