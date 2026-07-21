//! Production-grade cache manager with per-domain routing, fail-open semantics,
//! read-through population, noop mode, and operation timeouts.
//!
//! All domain policies are driven from `CacheConfig` (deserialized from the
//! application config file). No hardcoded domain behavior.
//!
//! Design principles:
//! - **Fail-open reads**: a failed or timed-out `get` degrades to a miss so
//!   callers always fall through to the source of truth. Cache read failures
//!   never propagate.
//! - **Policy-driven writes**: `set` failures are swallowed (and logged) when
//!   the domain is fail-open, propagated otherwise.
//! - **Per-domain routing**: Each domain (market, config, calibration, etc.)
//!   can be independently configured or disabled via config.
//! - **Read-through**: On miss, an async loader populates the cache atomically.
//! - **Noop mode**: When `config.disabled = true`, all operations are no-ops.
//! - **Operation timeouts**: Every cache operation has a bounded deadline.

use std::{collections::HashMap, future::Future, time::Duration};

use bitcode::{Decode, Encode};
use prometheus::{Error, Registry};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::CacheConfig;
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::cache::{CacheKey, TieredCache};

/// Resolved per-domain cache behavior (computed from config at construction time).
#[derive(Debug, Clone)]
pub struct DomainCachePolicy {
    pub enabled: bool,
    pub operation_timeout: Duration,
    pub fail_open: bool,
}

/// The production cache manager wrapping `TieredCache` with operational controls.
///
/// This is the **only** cache surface the rest of the platform consumes; the
/// underlying [`TieredCache`] is never handed out, so every cache operation in
/// the process goes through the same policy gate.
pub struct CacheManager {
    cache: Option<TieredCache>,
    domain_policies: HashMap<String, DomainCachePolicy>,
    default_policy: DomainCachePolicy,
}

impl CacheManager {
    /// Build a fully operational `CacheManager` from config.
    ///
    /// Domain policies are resolved from `config.domains`. Any domain not
    /// explicitly configured uses the global defaults from `CacheConfig`.
    pub fn new(cache: TieredCache, config: &CacheConfig) -> Self {
        if config.disabled {
            return Self::noop();
        }

        let global_timeout = Duration::from_millis(config.operation_timeout_ms);
        let global_fail_open = config.fail_open;

        let default_policy = DomainCachePolicy {
            enabled: true,
            operation_timeout: global_timeout,
            fail_open: global_fail_open,
        };

        let domain_policies = config
            .domains
            .iter()
            .map(|(domain, domain_cfg)| {
                let policy = DomainCachePolicy {
                    enabled: !domain_cfg.disabled,
                    operation_timeout: domain_cfg
                        .timeout_ms
                        .map_or(global_timeout, Duration::from_millis),
                    fail_open: domain_cfg.fail_open.unwrap_or(global_fail_open),
                };
                (domain.clone(), policy)
            })
            .collect();

        Self {
            cache: Some(cache),
            domain_policies,
            default_policy,
        }
    }

    /// Create a noop cache manager that skips all operations.
    /// Used when `config.disabled = true` or in tests.
    pub fn noop() -> Self {
        Self {
            cache: None,
            domain_policies: HashMap::new(),
            default_policy: DomainCachePolicy {
                enabled: false,
                operation_timeout: Duration::from_millis(100),
                fail_open: true,
            },
        }
    }

    /// Get a bitcode-encoded value. Returns `None` on miss, domain disabled,
    /// backend error, or timeout (fail-open read).
    pub async fn get<T: for<'a> Decode<'a> + Send>(&self, key: &CacheKey) -> Option<T> {
        let (cache, policy) = self.active(key)?;
        read_with_policy(policy, key, cache.get::<T>(key)).await
    }

    /// Get a JSON-encoded value (for types that cannot derive `bitcode`
    /// codecs, e.g. models with `DateTime` fields). Same fail-open semantics
    /// as [`Self::get`].
    pub async fn get_json<T: DeserializeOwned + Send>(&self, key: &CacheKey) -> Option<T> {
        let (cache, policy) = self.active(key)?;
        read_with_policy(policy, key, cache.get_json::<T>(key)).await
    }

    /// Set a bitcode-encoded value. Errors are swallowed (and logged) when the
    /// domain is fail-open, propagated otherwise.
    pub async fn set<T: Encode + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let Some((cache, policy)) = self.active(key) else {
            return Ok(());
        };
        write_with_policy(policy, key, cache.set(key, value)).await
    }

    /// Set a JSON-encoded value. Same policy semantics as [`Self::set`].
    pub async fn set_json<T: Serialize + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let Some((cache, policy)) = self.active(key) else {
            return Ok(());
        };
        write_with_policy(policy, key, cache.set_json(key, value)).await
    }

    /// Read-through: attempt cache get; on miss, call `loader`, populate cache,
    /// then return the loaded value.
    ///
    /// The loader is only invoked on a cache miss. If the loader fails, the
    /// error propagates (the cache layer is fail-open, not the data layer).
    pub async fn get_or_load<T, F, Fut>(&self, key: &CacheKey, loader: F) -> Result<T, StorageError>
    where
        T: for<'a> Decode<'a> + Encode + Send + Sync + Clone,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, StorageError>>,
    {
        if let Some(cached) = self.get::<T>(key).await {
            return Ok(cached);
        }

        let value = loader().await?;
        // Fire-and-forget set (fail-open)
        let _ = self.set(key, &value).await;
        Ok(value)
    }

    /// Invalidate a cache entry (both L1 and L2). Always fail-open: a missed
    /// invalidation only delays freshness until the entry's TTL expires.
    pub async fn invalidate(&self, key: &CacheKey) {
        let Some((cache, policy)) = self.active(key) else {
            return;
        };

        match timeout(policy.operation_timeout, cache.invalidate(key)).await {
            Ok(Ok(())) => {
                debug!(key = %key.as_str(), "Cache entry invalidated");
            }
            Ok(Err(error)) => {
                warn!(
                    key = %key.as_str(),
                    %error,
                    "Cache invalidate failed (fail-open)"
                );
            }
            Err(_elapsed) => {
                warn!(
                    key = %key.as_str(),
                    timeout_ms = policy.operation_timeout.as_millis(),
                    "Cache invalidate timed out (fail-open)"
                );
            }
        }
    }

    /// Register the cache hit/miss counters into the process metrics registry.
    /// A noop manager registers nothing.
    pub fn register_metrics(&self, registry: &Registry) -> Result<(), Error> {
        self.cache
            .as_ref()
            .map_or(Ok(()), |cache| cache.metrics().register(registry))
    }

    /// Check if the cache is operating in noop mode.
    pub const fn is_noop(&self) -> bool {
        self.cache.is_none()
    }

    /// Resolve the live cache handle and policy for `key`'s domain.
    /// `None` means the operation must be skipped (noop mode or domain disabled).
    fn active(&self, key: &CacheKey) -> Option<(&TieredCache, &DomainCachePolicy)> {
        let cache = self.cache.as_ref()?;
        let policy = self
            .domain_policies
            .get(key.domain())
            .unwrap_or(&self.default_policy);
        policy.enabled.then_some((cache, policy))
    }
}

/// Run a cache read under `policy`: backend errors and timeouts are logged and
/// degrade to a miss, so callers always fall through to the source of truth.
async fn read_with_policy<T, Fut>(
    policy: &DomainCachePolicy,
    key: &CacheKey,
    operation: Fut,
) -> Option<T>
where
    Fut: Future<Output = Result<Option<T>, StorageError>>,
{
    match timeout(policy.operation_timeout, operation).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            warn!(
                domain = key.domain(),
                key = %key.as_str(),
                %error,
                "Cache get failed (fail-open)"
            );
            None
        }
        Err(_elapsed) => {
            warn!(
                domain = key.domain(),
                key = %key.as_str(),
                timeout_ms = policy.operation_timeout.as_millis(),
                "Cache get timed out (fail-open)"
            );
            None
        }
    }
}

/// Run a cache write under `policy`: failures are swallowed (and logged) when
/// the domain is fail-open, propagated otherwise.
async fn write_with_policy<Fut>(
    policy: &DomainCachePolicy,
    key: &CacheKey,
    operation: Fut,
) -> Result<(), StorageError>
where
    Fut: Future<Output = Result<(), StorageError>>,
{
    match timeout(policy.operation_timeout, operation).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            if policy.fail_open {
                warn!(
                    domain = key.domain(),
                    key = %key.as_str(),
                    %error,
                    "Cache set failed (fail-open)"
                );
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(_elapsed) => {
            if policy.fail_open {
                warn!(
                    domain = key.domain(),
                    key = %key.as_str(),
                    timeout_ms = policy.operation_timeout.as_millis(),
                    "Cache set timed out (fail-open)"
                );
                Ok(())
            } else {
                Err(StorageError::Timeout {
                    operation: format!("cache set {}", key.as_str()),
                    duration: policy.operation_timeout,
                })
            }
        }
    }
}
