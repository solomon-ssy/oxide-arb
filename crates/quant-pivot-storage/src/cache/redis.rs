//! Distributed L2 cache over a shared deadpool-redis pool.
//!
//! The pool is established once at the composition root (see
//! [`crate::cache::connect_pool`]) and shared with every Redis consumer in the
//! process (cache L2, JWT revocation blacklist). This backend only adds the
//! cache key namespace on top.

use crate::cache::{backend::CacheBackend, redis_connect};
use async_trait::async_trait;
use deadpool_redis::Pool;
use quant_pivot_error::storage::StorageError;
use redis::AsyncCommands;
use std::time::Duration;

pub struct RedisBackend {
    pool: Pool,
    key_prefix: String,
}

impl RedisBackend {
    /// Wrap a shared, already-verified connection pool.
    ///
    /// `key_prefix` is the platform namespace (e.g. `oarb:`) prepended to
    /// every cache key before it reaches Redis.
    #[must_use]
    pub fn new(pool: Pool, key_prefix: &str) -> Self {
        Self {
            pool,
            key_prefix: key_prefix.to_owned(),
        }
    }

    /// Ping Redis through the shared pool (readiness / health probes).
    pub async fn health_check(&self) -> Result<(), StorageError> {
        redis_connect::ping(&self.pool).await
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
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
