//! Tiered cache: L1 (Moka) → L2 (Redis) fallthrough.

use crate::cache::{CacheBackend, CacheKey, CacheMetrics, MokaBackend, RedisBackend};
use bitcode::{Decode, Encode};
use oxide_arb_error::storage::StorageError;
use tracing::trace;

pub struct TieredCache {
    l1: MokaBackend,
    l2: RedisBackend,
    metrics: CacheMetrics,
}

impl TieredCache {
    pub fn new(l1: MokaBackend, l2: RedisBackend) -> Self {
        Self {
            l1,
            l2,
            metrics: CacheMetrics::new(),
        }
    }

    pub const fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }

    pub async fn get<T: for<'a> Decode<'a> + Send>(
        &self,
        key: &CacheKey,
    ) -> Result<Option<T>, StorageError> {
        let key_str = key.as_str();

        if let Some(bytes) = self.l1.get(&key_str).await? {
            self.metrics.record_hit("l1", key.domain());
            return bitcode::decode(&bytes).map(Some).map_err(Into::into);
        }

        if let Some(bytes) = self.l2.get(&key_str).await? {
            self.metrics.record_hit("l2", key.domain());
            let l1_ttl = key.ttl() / 4;
            self.l1.set(&key_str, &bytes, l1_ttl).await?;
            return bitcode::decode(&bytes).map(Some).map_err(Into::into);
        }

        self.metrics.record_miss(key.domain());
        trace!(key = %key_str, "Cache miss (L1 + L2)");
        Ok(None)
    }

    pub async fn set<T: Encode + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let bytes = bitcode::encode(value);
        let ttl = key.ttl();
        let l1_ttl = ttl / 4;
        let key_str = key.as_str();

        let (r1, r2) = tokio::join!(
            self.l1.set(&key_str, &bytes, l1_ttl),
            self.l2.set(&key_str, &bytes, ttl),
        );
        r1?;
        r2?;
        Ok(())
    }

    pub async fn invalidate(&self, key: &CacheKey) -> Result<(), StorageError> {
        let key_str = key.as_str();
        let (r1, r2) = tokio::join!(self.l1.delete(&key_str), self.l2.delete(&key_str),);
        r1?;
        r2?;
        Ok(())
    }

    /// Get a value using `serde_json` for deserialization.
    ///
    /// Use this for types that cannot derive `bitcode::Decode` (e.g. `SeaORM`
    /// entity models with `DateTime` fields).
    pub async fn get_json<T: serde::de::DeserializeOwned + Send>(
        &self,
        key: &CacheKey,
    ) -> Result<Option<T>, StorageError> {
        let key_str = key.as_str();

        if let Some(bytes) = self.l1.get(&key_str).await? {
            self.metrics.record_hit("l1", key.domain());
            return serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| StorageError::Codec(e.to_string()));
        }

        if let Some(bytes) = self.l2.get(&key_str).await? {
            self.metrics.record_hit("l2", key.domain());
            let l1_ttl = key.ttl() / 4;
            self.l1.set(&key_str, &bytes, l1_ttl).await?;
            return serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| StorageError::Codec(e.to_string()));
        }

        self.metrics.record_miss(key.domain());
        trace!(key = %key_str, "Cache miss (L1 + L2)");
        Ok(None)
    }

    /// Set a value using `serde_json` for serialization.
    ///
    /// Use this for types that cannot derive `bitcode::Encode` (e.g. `SeaORM`
    /// entity models with `DateTime` fields).
    pub async fn set_json<T: serde::Serialize + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(value).map_err(|e| StorageError::Codec(e.to_string()))?;
        let ttl = key.ttl();
        let l1_ttl = ttl / 4;
        let key_str = key.as_str();

        let (r1, r2) = tokio::join!(
            self.l1.set(&key_str, &bytes, l1_ttl),
            self.l2.set(&key_str, &bytes, ttl),
        );
        r1?;
        r2?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use oxide_arb_models::types::EventId;
    use oxide_arb_models::types::MarketId;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct MockL2 {
        data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    #[async_trait]
    impl CacheBackend for MockL2 {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.lock().await.get(key).cloned())
        }

        async fn set(&self, key: &str, value: &[u8], _ttl: Duration) -> Result<(), StorageError> {
            self.data
                .lock()
                .await
                .insert(key.to_owned(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<bool, StorageError> {
            Ok(self.data.lock().await.remove(key).is_some())
        }

        async fn exists(&self, key: &str) -> Result<bool, StorageError> {
            Ok(self.data.lock().await.contains_key(key))
        }

        async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
            let map = self.data.lock().await;
            Ok(keys.iter().map(|k| map.get(*k).cloned()).collect())
        }

        async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError> {
            for (k, v) in entries {
                self.set(k, v, ttl).await?;
            }
            Ok(())
        }
    }

    struct MockTiered {
        l1: MokaBackend,
        l2: MockL2,
        metrics: CacheMetrics,
    }

    impl MockTiered {
        fn new(l1: MokaBackend, l2: MockL2) -> Self {
            Self {
                l1,
                l2,
                metrics: CacheMetrics::new(),
            }
        }

        async fn get<T: for<'a> Decode<'a> + Send>(
            &self,
            key: &CacheKey,
        ) -> Result<Option<T>, StorageError> {
            let key_str = key.as_str();
            if let Some(bytes) = self.l1.get(&key_str).await? {
                self.metrics.record_hit("l1", key.domain());
                return bitcode::decode(&bytes).map(Some).map_err(Into::into);
            }
            if let Some(bytes) = self.l2.get(&key_str).await? {
                self.metrics.record_hit("l2", key.domain());
                let l1_ttl = key.ttl() / 4;
                self.l1.set(&key_str, &bytes, l1_ttl).await?;
                return bitcode::decode(&bytes).map(Some).map_err(Into::into);
            }
            self.metrics.record_miss(key.domain());
            Ok(None)
        }

        async fn set<T: Encode + Send + Sync>(
            &self,
            key: &CacheKey,
            value: &T,
        ) -> Result<(), StorageError> {
            let bytes = bitcode::encode(value);
            let ttl = key.ttl();
            let l1_ttl = ttl / 4;
            let key_str = key.as_str();
            let (r1, r2) = tokio::join!(
                self.l1.set(&key_str, &bytes, l1_ttl),
                self.l2.set(&key_str, &bytes, ttl),
            );
            r1?;
            r2?;
            Ok(())
        }
    }

    #[derive(bitcode::Encode, bitcode::Decode, Debug, PartialEq, Eq, Clone)]
    struct CachedStub {
        id: String,
    }

    #[tokio::test]
    async fn tiered_l2_hit_backfills_l1_without_redis() {
        let l2 = MockL2::default();
        let writer = MockTiered::new(MokaBackend::new(100), l2);
        let key = CacheKey::MarketInfo {
            market_id: MarketId::new("0xmock"),
        };
        let value = CachedStub {
            id: "0xmock".into(),
        };
        writer.set(&key, &value).await.unwrap();

        let reader = MockTiered::new(MokaBackend::new(100), MockL2::default());
        // Seed L2 only (simulate another process writer).
        let bytes = bitcode::encode(&value);
        reader
            .l2
            .set(&key.as_str(), &bytes, key.ttl())
            .await
            .unwrap();

        let first: Option<CachedStub> = reader.get(&key).await.unwrap();
        assert_eq!(first, Some(value.clone()));

        reader.l2.delete(&key.as_str()).await.unwrap();
        let second: Option<CachedStub> = reader.get(&key).await.unwrap();
        assert_eq!(second, Some(value));
    }

    #[tokio::test]
    async fn tiered_both_miss_without_redis() {
        let cache = MockTiered::new(MokaBackend::new(100), MockL2::default());
        let key = CacheKey::EventInfo {
            event_id: EventId::new("missing"),
        };
        let missing: Option<CachedStub> = cache.get(&key).await.unwrap();
        assert!(missing.is_none());
    }
}
