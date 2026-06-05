//! Persistence layer errors — `PostgreSQL`, `ClickHouse`, and cache.

use sea_orm::DbErr;
use std::time::Duration;
use thiserror::Error;

/// Errors from the storage subsystem (DB, analytics, cache).
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    #[error("Database transaction failed: {0}")]
    Transaction(String),

    #[cfg(feature = "storage")]
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[cfg(not(feature = "storage"))]
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[cfg(feature = "storage")]
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[cfg(feature = "storage")]
    #[error("Redis pool error: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),

    #[cfg(not(feature = "storage"))]
    #[error("Cache error: {0}")]
    Cache(String),

    #[cfg(feature = "storage")]
    #[error("Serialization error: {0}")]
    Serialization(#[from] bitcode::Error),

    #[error("Codec error: {0}")]
    Codec(String),

    #[cfg(not(feature = "storage"))]
    #[error("Serialization error: {0}")]
    SerializationStr(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("Stale data: {0}")]
    StaleData(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Operation `{operation}` timed out after {duration:?}")]
    Timeout {
        operation: String,
        duration: Duration,
    },

    #[error("ClickHouse write semaphore closed (system shutting down)")]
    ClickHouseWriteSemaphoreClosed,

    #[error("Timed out waiting for ClickHouse lag to recover")]
    ClickHouseLagTimeout,
}
