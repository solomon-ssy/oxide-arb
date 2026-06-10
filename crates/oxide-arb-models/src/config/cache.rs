//! Cache layer configuration (`[cache]`, deploy).

use serde::Deserialize;
use std::collections::HashMap;

/// Tiered cache (in-process Moka L1 + Redis L2) policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Redis (L2) connection.
    pub redis: RedisConfig,
    /// In-process Moka (L1) cache.
    pub moka: MokaConfig,
    /// Global operation timeout (ms). Per-domain overrides take precedence.
    /// Default: `500`.
    pub operation_timeout_ms: u64,
    /// Whether cache failures are transparent to callers (`true` = never
    /// propagate errors; callers fall through to the source of truth).
    /// Default: `true`.
    pub fail_open: bool,
    /// Disable the entire cache layer (all operations become no-ops).
    /// Default: `false`.
    pub disabled: bool,
    /// Per-domain policy overrides. Key = domain name (e.g. `market`).
    /// Default: empty.
    pub domains: HashMap<String, DomainCacheConfig>,
}

/// Per-domain cache policy override.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
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

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            redis: RedisConfig::default(),
            moka: MokaConfig::default(),
            operation_timeout_ms: default_operation_timeout_ms(),
            fail_open: default_fail_open(),
            disabled: false,
            domains: HashMap::new(),
        }
    }
}

/// Redis connection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    /// Connection URL. Default: `redis://localhost:6379`.
    pub url: String,
    /// Connection pool size. Default: `8`.
    pub pool_size: u32,
    /// Per-operation timeout (ms). Default: `1000`.
    pub timeout_ms: u64,
    /// Key namespace prefix. Default: `oarb:`.
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

/// In-process Moka (L1) cache.
///
/// TTLs are per-entry and chosen by each call site — there is no global
/// time-to-live/time-to-idle knob by design.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MokaConfig {
    /// Maximum number of cached entries. Default: `10000`.
    pub max_capacity: u64,
}

impl Default for MokaConfig {
    fn default() -> Self {
        Self {
            max_capacity: default_moka_max_cap(),
        }
    }
}

const fn default_moka_max_cap() -> u64 {
    10_000
}
