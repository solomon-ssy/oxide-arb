//! In-process L1 cache using Moka (`TinyLFU` eviction).

use crate::cache::backend::CacheBackend;
use async_trait::async_trait;
use moka::{Expiry, future::Cache};
use oxide_arb_error::storage::StorageError;
use std::time::{Duration, Instant};

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
