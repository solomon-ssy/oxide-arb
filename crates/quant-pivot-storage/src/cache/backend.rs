//! Cache backend trait.

use async_trait::async_trait;
use oxide_arb_error::storage::StorageError;
use std::time::Duration;

#[async_trait]
pub trait CacheBackend: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError>;
    async fn delete(&self, key: &str) -> Result<bool, StorageError>;
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;
    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError>;
    async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError>;
}
