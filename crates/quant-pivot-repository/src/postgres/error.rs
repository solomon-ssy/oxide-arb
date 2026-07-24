//! Shared error constructors for Postgres repositories.

use quant_pivot_error::storage::StorageError;
use sea_orm::{DbErr, SqlErr};

/// Map a `SeaORM` error, translating a unique-constraint violation into
/// [`StorageError::Duplicate`].
pub fn map_unique(err: DbErr, entity: &'static str, key: &str) -> StorageError {
    match err.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => StorageError::duplicate(entity, key),
        _ => StorageError::from(err),
    }
}
