//! Persistence layer errors — `PostgreSQL`, `ClickHouse`, and cache.

use sea_orm::DbErr;
use thiserror::Error;

/// Errors from the storage subsystem (DB, analytics, cache).
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    #[error("Database transaction failed: {0}")]
    Transaction(String),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("Stale data: {0}")]
    StaleData(String),
}
