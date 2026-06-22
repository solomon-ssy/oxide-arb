//! Shared error-mapping helpers for the RBAC Postgres repositories.

use std::fmt::Display;

use oxide_arb_error::storage::StorageError;
use sea_orm::{DbErr, SqlErr};

/// Map a `SeaORM` error, translating a unique-constraint violation into a
/// `StorageError::Conflict` that carries the logical entity and conflicting key.
/// Any other database error is forwarded verbatim.
pub fn map_unique(err: DbErr, entity: &str, key: &str) -> StorageError {
    match err.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => {
            StorageError::Conflict(format!("{entity} already exists: {key}"))
        }
        _ => StorageError::from(err),
    }
}

/// Construct a `StorageError::NotFound` for the given entity and identifier.
pub fn not_found(entity: &'static str, id: impl Display) -> StorageError {
    StorageError::NotFound {
        entity,
        id: id.to_string(),
    }
}
