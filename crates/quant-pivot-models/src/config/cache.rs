//! Cache layer configuration (`[cache]`, deploy).

use super::secret::SystemdCredentialRef;
use serde::Deserialize;
use std::collections::HashMap;
use url::Url;

/// Tiered cache (in-process Moka L1 + Redis L2) policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Platform Redis connection. One shared pool backs both the cache L2 and
    /// the JWT revocation blacklist (split by key namespace), so this
    /// connection is always established even when `disabled = true`.
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

/// Platform Redis connection — a single shared pool serving the cache L2 and
/// the JWT revocation blacklist, separated only by key namespace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    /// Server host. Default: `localhost`.
    pub host: String,
    /// Server port. Default: `6379`.
    pub port: u16,
    /// ACL username. Leave empty when the server uses password-only auth
    /// (`requirepass`). Default: empty.
    pub user: String,
    /// systemd credential reference for Redis authentication.
    pub password: SystemdCredentialRef,
    /// Logical database index (`SELECT`). Default: `0`.
    pub database: u8,
    /// Connection pool size. Default: `8`.
    pub pool_size: u32,
    /// Per-operation timeout (ms): how long a caller waits for a pooled
    /// connection at steady state. Default: `1000`.
    pub timeout_ms: u64,
    /// Startup readiness budget (ms): how long pool creation may take to
    /// establish and PING the first connection before the process fails
    /// closed. Kept separate from [`timeout_ms`](Self::timeout_ms) so a tight
    /// per-operation deadline never starves initial connection establishment
    /// under load. Default: `5000`.
    pub connect_timeout_ms: u64,
    /// Key namespace prefix applied to every platform Redis key (cache keys
    /// directly; revoked JWT ids under `{key_prefix}jwt:blacklist:`).
    /// Default: `qp:`.
    pub key_prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            host: default_redis_host(),
            port: default_redis_port(),
            user: String::new(),
            password: SystemdCredentialRef::default(),
            database: 0,
            pool_size: default_redis_pool(),
            timeout_ms: default_redis_timeout(),
            connect_timeout_ms: default_redis_connect_timeout(),
            key_prefix: default_redis_key_prefix(),
        }
    }
}

impl RedisConfig {
    /// Human-readable endpoint for logs and diagnostics (never includes credentials).
    #[must_use]
    pub fn endpoint(&self) -> String {
        if self.database == 0 {
            format!("{}:{}", self.host, self.port)
        } else {
            format!("{}:{}/{}", self.host, self.port, self.database)
        }
    }

    /// Build the Redis connection URL consumed by deadpool-redis / redis-rs.
    ///
    /// Userinfo components are percent-encoded per RFC 3986.
    pub fn try_connection_url(&self) -> Result<String, url::ParseError> {
        let mut url = Url::parse("redis://localhost/")?;
        url.set_host(Some(self.host.as_str()))?;
        url.set_port(Some(self.port)).ok();
        if !self.user.is_empty() && url.set_username(&self.user).is_err() {
            return Err(url::ParseError::InvalidDomainCharacter);
        }
        if !self.password.is_empty() {
            url.set_password(Some(self.password.expose_secret())).ok();
        }
        if self.database != 0 {
            url.set_path(&format!("/{}", self.database));
        } else {
            url.set_path("");
        }
        Ok(url.to_string())
    }

    /// Build the Redis connection URL, panicking only when host/port are invalid.
    ///
    /// Callers should prefer [`Self::try_connection_url`] when surfacing errors
    /// to operators; this helper exists for call sites that already validated
    /// the deploy config at startup.
    #[must_use]
    pub fn connection_url(&self) -> String {
        self.try_connection_url().unwrap_or_else(|error| {
            panic!(
                "invalid redis config (host={:?}, port={}): {error}",
                self.host, self.port
            )
        })
    }
}

fn default_redis_host() -> String {
    "localhost".into()
}
const fn default_redis_port() -> u16 {
    6379
}
const fn default_redis_pool() -> u32 {
    8
}
const fn default_redis_timeout() -> u64 {
    1000
}
const fn default_redis_connect_timeout() -> u64 {
    5000
}
fn default_redis_key_prefix() -> String {
    "qp:".into()
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

#[cfg(test)]
mod tests {
    use super::RedisConfig;

    #[test]
    fn connection_url_without_auth_uses_host_and_port() {
        let cfg = RedisConfig::default();
        assert_eq!(cfg.connection_url(), "redis://localhost:6379");
        assert_eq!(cfg.endpoint(), "localhost:6379");
    }

    #[test]
    fn connection_url_password_only_auth() {
        let cfg = RedisConfig {
            password: "s3cret".into(),
            ..RedisConfig::default()
        };
        assert_eq!(cfg.connection_url(), "redis://:s3cret@localhost:6379");
    }

    #[test]
    fn connection_url_user_and_password_auth() {
        let cfg = RedisConfig {
            user: "oxide".into(),
            password: "s3cret".into(),
            ..RedisConfig::default()
        };
        assert_eq!(cfg.connection_url(), "redis://oxide:s3cret@localhost:6379");
    }

    #[test]
    fn connection_url_percent_encodes_special_characters() {
        let cfg = RedisConfig {
            password: "p@ss:w/rd".into(),
            ..RedisConfig::default()
        };
        assert_eq!(
            cfg.connection_url(),
            "redis://:p%40ss%3Aw%2Frd@localhost:6379"
        );
    }

    #[test]
    fn connection_url_includes_database_when_non_zero() {
        let cfg = RedisConfig {
            database: 2,
            ..RedisConfig::default()
        };
        assert_eq!(cfg.connection_url(), "redis://localhost:6379/2");
        assert_eq!(cfg.endpoint(), "localhost:6379/2");
    }
}
