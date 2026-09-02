//! In-process L1 cache using Moka (`TinyLFU` eviction).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use moka::{Expiry, future::Cache};
use quant_pivot_error::storage::StorageError;

use crate::cache::backend::CacheBackend;

#[derive(Clone)]
struct CacheEntry {
    data: Vec<u8>,
    ttl: Duration,
}

struct PerEntryExpiry;

impl Expiry<String, CacheEntry> for PerEntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &CacheEntry,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

pub struct MokaBackend {
    cache: Cache<String, CacheEntry>,
}

impl MokaBackend {
    pub fn new(max_capacity: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .expire_after(PerEntryExpiry)
            .build();
        Self { cache }
    }
}

#[async_trait]
impl CacheBackend for MokaBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.cache.get(key).await.map(|e| e.data))
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        self.cache
            .insert(
                key.to_string(),
                CacheEntry {
                    data: value.to_vec(),
                    ttl,
                },
            )
            .await;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        self.cache.remove(key).await;
        Ok(true)
    }

    async fn delete_many(&self, keys: &[&str]) -> Result<usize, StorageError> {
        for key in keys {
            self.cache.remove(*key).await;
        }
        Ok(keys.len())
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        Ok(self.cache.contains_key(key))
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.cache.get(*key).await.map(|e| e.data));
        }
        Ok(results)
    }

    async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError> {
        for (k, v) in entries {
            self.cache
                .insert(
                    (*k).to_string(),
                    CacheEntry {
                        data: v.to_vec(),
                        ttl,
                    },
                )
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CacheBackend, MokaBackend};

    #[tokio::test]
    async fn batch_delete_removes_keys() {
        let cache = MokaBackend::new(16);
        cache
            .set("first", b"1", Duration::from_mins(1))
            .await
            .expect("seed first cache key");
        cache
            .set("second", b"2", Duration::from_mins(1))
            .await
            .expect("seed second cache key");

        let removed = cache
            .delete_many(&["first", "second"])
            .await
            .expect("delete cache batch");

        assert_eq!(removed, 2);
        assert!(!cache.exists("first").await.expect("inspect first key"));
        assert!(!cache.exists("second").await.expect("inspect second key"));
    }
}
