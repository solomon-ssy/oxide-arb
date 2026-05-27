//! Production-grade cache manager with per-domain routing, fail-open semantics,
//! read-through population, noop mode, and operation timeouts.
//!
//! All domain policies are driven from `CacheConfig` (deserialized from the
//! application config file). No hardcoded domain behavior.
//!
//! Design principles:
//! - **Fail-open**: Cache failures never block or fail the application.
//!   A failed `get` returns `None`; a failed `set` is silently logged.
//! - **Per-domain routing**: Each domain (market, config, calibration, etc.)
//!   can be independently configured or disabled via config.
//! - **Read-through**: On miss, an async loader populates the cache atomically.
//! - **Noop mode**: When `config.disabled = true`, all operations are no-ops.
//! - **Operation timeouts**: Every cache operation has a bounded deadline.

use crate::cache::{CacheKey, CacheMetrics, TieredCache};
use bitcode::{Decode, Encode};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::CacheConfig;
use std::{collections::HashMap, future::Future, time::Duration};
use tracing::{debug, warn};

/// Resolved per-domain cache behavior (computed from config at construction time).
#[derive(Debug, Clone)]
pub struct DomainCachePolicy {
    pub enabled: bool,
    pub operation_timeout: Duration,
    pub fail_open: bool,
}

/// The production cache manager wrapping `TieredCache` with operational controls.
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

    /// Get a value from cache with fail-open semantics and timeout.
    /// Returns `None` on miss or any error (when `fail_open` = true).
    pub async fn get<T: for<'a> Decode<'a> + Send>(&self, key: &CacheKey) -> Option<T> {
        let policy = self.policy_for(key.domain());
        if !policy.enabled {
            return None;
        }

        let cache = self.cache.as_ref()?;
        let timeout = policy.operation_timeout;

        match tokio::time::timeout(timeout, cache.get::<T>(key)).await {
            Ok(Ok(value)) => value,
            Ok(Err(e)) => {
                if policy.fail_open {
                    warn!(
                        domain = key.domain(),
                        key = %key.as_str(),
                        error = %e,
                        "Cache get failed (fail-open)"
                    );
                }
                None
            }
            Err(_elapsed) => {
                warn!(
                    domain = key.domain(),
                    key = %key.as_str(),
                    timeout_ms = timeout.as_millis(),
                    "Cache get timed out (fail-open)"
                );
                None
            }
        }
    }

    /// Set a value in cache. Errors are logged but never propagated when fail-open.
    pub async fn set<T: Encode + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let policy = self.policy_for(key.domain());
        if !policy.enabled {
            return Ok(());
        }

        let Some(cache) = self.cache.as_ref() else {
            return Ok(());
        };

        let timeout = policy.operation_timeout;
        match tokio::time::timeout(timeout, cache.set(key, value)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                if policy.fail_open {
                    warn!(
                        domain = key.domain(),
                        key = %key.as_str(),
                        error = %e,
                        "Cache set failed (fail-open)"
                    );
                    Ok(())
                } else {
                    Err(e)
                }
            }
            Err(_elapsed) => {
                if policy.fail_open {
                    warn!(
                        domain = key.domain(),
                        key = %key.as_str(),
                        timeout_ms = timeout.as_millis(),
                        "Cache set timed out (fail-open)"
                    );
                    Ok(())
                } else {
                    Err(StorageError::Timeout {
                        operation: format!("cache set {}", key.as_str()),
                        duration: timeout,
                    })
                }
            }
        }
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

    /// Invalidate a cache entry (both L1 and L2). Fail-open.
    pub async fn invalidate(&self, key: &CacheKey) {
        let policy = self.policy_for(key.domain());
        if !policy.enabled {
            return;
        }

        let Some(cache) = self.cache.as_ref() else {
            return;
        };

        let timeout = policy.operation_timeout;
        match tokio::time::timeout(timeout, cache.invalidate(key)).await {
            Ok(Ok(())) => {
                debug!(key = %key.as_str(), "Cache entry invalidated");
            }
            Ok(Err(e)) => {
                warn!(
                    key = %key.as_str(),
                    error = %e,
                    "Cache invalidate failed (fail-open)"
                );
            }
            Err(_) => {
                warn!(
                    key = %key.as_str(),
                    "Cache invalidate timed out (fail-open)"
                );
            }
        }
    }

    /// Access the underlying metrics (if cache is active).
    pub fn metrics(&self) -> Option<&CacheMetrics> {
        self.cache.as_ref().map(TieredCache::metrics)
    }

    /// Check if the cache is operating in noop mode.
    pub const fn is_noop(&self) -> bool {
        self.cache.is_none()
    }

    fn policy_for(&self, domain: &str) -> &DomainCachePolicy {
        self.domain_policies
            .get(domain)
            .unwrap_or(&self.default_policy)
    }
}
