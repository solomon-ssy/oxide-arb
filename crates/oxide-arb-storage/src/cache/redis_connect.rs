//! deadpool-redis pool factory and readiness helpers.
//!
//! Maps domain [`RedisConfig`] onto explicit deadpool [`PoolConfig`] (size +
//! timeouts) and blocks until a PING succeeds. Shared by the L2 cache backend
//! and the web-tier JWT revocation blacklist.

use std::time::{Duration, Instant};

use deadpool_redis::{Config, Pool, PoolConfig, Runtime, Timeouts};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::RedisConfig;
use tokio::time::sleep;

/// Default timeout for establishing a new pooled connection.
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
        create: Some(CREATE_TIMEOUT),
        recycle: Some(RECYCLE_TIMEOUT),
    };
    cfg.pool = Some(pool_config);
    Ok(cfg)
}

/// Create a pool from `redis` and block until PING succeeds (startup readiness).
pub async fn connect_pool(redis: &RedisConfig) -> Result<Pool, StorageError> {
    let pool = deadpool_config(redis)?
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|error| {
            StorageError::Connection(format!("Redis pool creation failed: {error}"))
        })?;

    wait_until_ready(&pool, Duration::from_millis(redis.timeout_ms)).await?;
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

async fn wait_until_ready(pool: &Pool, timeout: Duration) -> Result<(), StorageError> {
    let deadline = Instant::now() + timeout;

    loop {
        match ping(pool).await {
            Ok(()) => return Ok(()),
            Err(error) if Instant::now() < deadline => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                sleep(READINESS_RETRY.min(remaining)).await;
                if remaining.is_zero() {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
}
