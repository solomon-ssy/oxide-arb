//! deadpool-redis pool factory and readiness helpers.
//!
//! Maps domain [`RedisConfig`] onto explicit deadpool [`PoolConfig`] (size +
//! timeouts) and blocks until a PING succeeds. Shared by the L2 cache backend
//! and the web-tier JWT revocation blacklist.
//!
//! Two distinct timeout semantics are kept separate on purpose:
//!
//! - [`RedisConfig::timeout_ms`] — steady-state **per-operation** pool wait.
//! - [`RedisConfig::connect_timeout_ms`] — **startup readiness budget**: the
//!   total time [`connect_pool`] may spend establishing and pinging the first
//!   connection before the caller fails closed.

use std::time::{Duration, Instant};

use deadpool_redis::{Config, Pool, PoolConfig, Runtime, Timeouts};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::RedisConfig;
use tokio::time::sleep;

/// Upper bound for establishing a single new pooled connection. Clamped to the
/// startup readiness budget so one create attempt can never consume more than
/// the whole budget (which would defeat the retry loop in [`connect_pool`]).
const CREATE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default timeout when recycling an existing pooled connection.
const RECYCLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Delay between readiness probe retries.
const READINESS_RETRY: Duration = Duration::from_millis(50);

/// Map [`RedisConfig`] onto a deadpool [`Config`] with explicit pool limits.
pub fn deadpool_config(redis: &RedisConfig) -> Result<Config, StorageError> {
    let url = redis.try_connection_url().map_err(|error| {
        StorageError::Connection(format!(
            "invalid Redis connection settings (endpoint={}): {error}",
            redis.endpoint()
        ))
    })?;
    let mut cfg = Config::from_url(url);
    let mut pool_config = PoolConfig::new(redis.pool_size as usize);
    pool_config.timeouts = Timeouts {
        wait: Some(Duration::from_millis(redis.timeout_ms)),
        create: Some(create_timeout(redis)),
        recycle: Some(RECYCLE_TIMEOUT),
    };
    cfg.pool = Some(pool_config);
    Ok(cfg)
}

/// Per-connection create timeout: the smaller of the deadpool default and the
/// startup readiness budget, so a hung TCP/TLS handshake cannot eat the whole
/// budget in a single attempt.
fn create_timeout(redis: &RedisConfig) -> Duration {
    CREATE_TIMEOUT.min(Duration::from_millis(redis.connect_timeout_ms.max(1)))
}

/// Create a pool from `redis` and block until PING succeeds (startup readiness).
///
/// The readiness loop retries within [`RedisConfig::connect_timeout_ms`] — the
/// per-operation `timeout_ms` only governs pooled-connection waits at steady
/// state and never constrains startup.
pub async fn connect_pool(redis: &RedisConfig) -> Result<Pool, StorageError> {
    let pool = deadpool_config(redis)?
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|error| {
            StorageError::Connection(format!("Redis pool creation failed: {error}"))
        })?;

    wait_until_ready(&pool, Duration::from_millis(redis.connect_timeout_ms)).await?;
    Ok(pool)
}

/// Ping Redis through an existing pool (readiness / health probes).
pub async fn ping(pool: &Pool) -> Result<(), StorageError> {
    let mut conn = pool.get().await?;
    redis::cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .map_err(StorageError::Redis)?;
    Ok(())
}

/// Retry PING until success or `budget` elapses, returning the last error.
async fn wait_until_ready(pool: &Pool, budget: Duration) -> Result<(), StorageError> {
    let deadline = Instant::now() + budget;

    loop {
        match ping(pool).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                sleep(READINESS_RETRY.min(remaining)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CREATE_TIMEOUT, create_timeout, deadpool_config};
    use quant_pivot_models::config::RedisConfig;
    use std::time::Duration;

    #[test]
    fn operation_and_startup_timeouts_are_independent() {
        let redis = RedisConfig {
            timeout_ms: 250,
            connect_timeout_ms: 9_000,
            ..RedisConfig::default()
        };
        let cfg = deadpool_config(&redis).expect("valid config");
        let timeouts = cfg.pool.expect("pool config").timeouts;
        // Steady-state pool wait follows timeout_ms ...
        assert_eq!(timeouts.wait, Some(Duration::from_millis(250)));
        // ... while connection establishment keeps the larger default budget.
        assert_eq!(timeouts.create, Some(CREATE_TIMEOUT));
    }

    #[test]
    fn create_timeout_is_clamped_to_the_startup_budget() {
        let redis = RedisConfig {
            connect_timeout_ms: 1_200,
            ..RedisConfig::default()
        };
        assert_eq!(create_timeout(&redis), Duration::from_millis(1_200));

        let redis = RedisConfig {
            connect_timeout_ms: 60_000,
            ..RedisConfig::default()
        };
        assert_eq!(create_timeout(&redis), CREATE_TIMEOUT);
    }
}
