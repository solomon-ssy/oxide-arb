//! Cache layer configuration.

use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct CacheConfig {
    #[serde(default)]
    pub redis: RedisConfig,
    #[serde(default)]
    pub moka: MokaConfig,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RedisConfig {
    #[serde(default = "default_redis_url")]
    pub url: String,
    #[serde(default = "default_redis_pool")]
    pub pool_size: u32,
    #[serde(default = "default_redis_timeout")]
    pub timeout_ms: u64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: default_redis_url(),
            pool_size: default_redis_pool(),
            timeout_ms: default_redis_timeout(),
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
