//! Cache layer configuration.

use serde::Deserialize;
use std::collections::HashMap;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct CacheConfig {
    #[serde(default)]
    pub redis: RedisConfig,
    #[serde(default)]
    pub moka: MokaConfig,
    /// Global operation timeout (ms). Per-domain overrides take precedence.
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
    /// Whether cache failures are transparent to callers (true = never propagate errors).
    #[serde(default = "default_fail_open")]
    pub fail_open: bool,
    /// Disable the entire cache layer (all operations become no-ops).
    #[serde(default)]
    pub disabled: bool,
    /// Per-domain policy overrides. Key = domain name (e.g. "market", "config").
    #[serde(default)]
    pub domains: HashMap<String, DomainCacheConfig>,
}

/// Per-domain cache policy override.
#[derive(Debug, Clone, Deserialize)]
pub struct DomainCacheConfig {
    /// Override operation timeout for this domain (ms).
    pub timeout_ms: Option<u64>,
    /// Override fail-open for this domain.
    pub fail_open: Option<bool>,
    /// Disable caching for this domain entirely.
    #[serde(default)]
    pub disabled: bool,
}

const fn default_operation_timeout_ms() -> u64 {
    500
}
const fn default_fail_open() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RedisConfig {
    #[serde(default = "default_redis_url")]
    pub url: String,
    #[serde(default = "default_redis_pool")]
    pub pool_size: u32,
    #[serde(default = "default_redis_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "default_redis_key_prefix")]
    pub key_prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: default_redis_url(),
            pool_size: default_redis_pool(),
            timeout_ms: default_redis_timeout(),
            key_prefix: default_redis_key_prefix(),
        }
    }
}

fn default_redis_url() -> String {
    "redis://localhost:6379".into()
}
const fn default_redis_pool() -> u32 {
    8
}
const fn default_redis_timeout() -> u64 {
    1000
}
fn default_redis_key_prefix() -> String {
    "oarb:".into()
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct MokaConfig {
    #[serde(default = "default_moka_max_cap")]
    pub max_capacity: u64,
    #[serde(default = "default_moka_ttl")]
    pub time_to_live_secs: u64,
    #[serde(default = "default_moka_tti")]
    pub time_to_idle_secs: u64,
}

impl Default for MokaConfig {
    fn default() -> Self {
        Self {
            max_capacity: default_moka_max_cap(),
            time_to_live_secs: default_moka_ttl(),
            time_to_idle_secs: default_moka_tti(),
        }
    }
}

const fn default_moka_max_cap() -> u64 {
    10_000
}
const fn default_moka_ttl() -> u64 {
    300
}
const fn default_moka_tti() -> u64 {
    120
}
