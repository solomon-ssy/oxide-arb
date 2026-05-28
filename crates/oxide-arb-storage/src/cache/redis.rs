//! Distributed L2 cache using Redis via deadpool.

use crate::cache::backend::CacheBackend;
use async_trait::async_trait;
use deadpool_redis::{Config, Pool, Runtime};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::RedisConfig;
use redis::AsyncCommands;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

pub struct RedisBackend {
    pool: Pool,
    key_prefix: String,
}

impl RedisBackend {
    pub async fn new(config: &RedisConfig) -> Result<Self, StorageError> {
        let cfg = Config::from_url(&config.url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(format!("Redis pool creation failed: {e}")))?;

        Self::wait_until_ready(&pool, Duration::from_millis(config.timeout_ms)).await?;

        info!(url = %config.url, prefix = %config.key_prefix, "Redis cache connected");

        Ok(Self {
            pool,
            key_prefix: config.key_prefix.clone(),
        })
    }

    pub async fn health_check(&self) -> Result<(), StorageError> {
        Self::ping(&self.pool).await?;
        Ok(())
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    async fn wait_until_ready(pool: &Pool, timeout: Duration) -> Result<(), StorageError> {
        let deadline = Instant::now() + timeout;
        let retry_delay = Duration::from_millis(50);

        loop {
            match Self::ping(pool).await {
                Ok(()) => return Ok(()),
                Err(error) if Instant::now() < deadline => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    sleep(retry_delay.min(remaining)).await;
                    if remaining.is_zero() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ping(pool: &Pool) -> Result<(), StorageError> {
        let mut conn = pool.get().await?;
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(StorageError::Redis)?;
        Ok(())
    }
}

#[async_trait]
impl CacheBackend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut conn = self.pool.get().await?;
        let result: Option<Vec<u8>> = conn.get(self.prefixed(key)).await?;
        Ok(result)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        let mut conn = self.pool.get().await?;
        conn.set_ex::<_, _, ()>(self.prefixed(key), value, ttl.as_secs())
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        let mut conn = self.pool.get().await?;
        let removed: i64 = conn.del(self.prefixed(key)).await?;
        Ok(removed > 0)
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let mut conn = self.pool.get().await?;
        let exists: bool = conn.exists(self.prefixed(key)).await?;
        Ok(exists)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut conn = self.pool.get().await?;
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        let results: Vec<Option<Vec<u8>>> = conn.mget(prefixed).await?;
        Ok(results)
    }

    async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError> {
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        for (k, v) in entries {
            pipe.set_ex::<_, _>(self.prefixed(k), *v, ttl.as_secs());
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }
}
