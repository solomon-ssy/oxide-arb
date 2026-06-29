//! Shared error constructors for Postgres repositories.

use std::fmt::Display;

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

/// Construct an explicit duplicate-key error (non-DB guard).
pub fn duplicate(entity: &'static str, key: impl Display) -> StorageError {
    StorageError::duplicate(entity, key)
}

/// Construct a [`StorageError::NotFound`] for the given entity and identifier.
pub fn not_found(entity: &'static str, id: impl Display) -> StorageError {
    StorageError::not_found(entity, id)
}

/// Construct an [`StorageError::IllegalTransition`].
pub fn illegal_transition(
    entity: &'static str,
    id: Option<impl Display>,
    from: impl Display,
    to: impl Display,
) -> StorageError {
    StorageError::illegal_transition(entity, id, from, to)
}

/// Construct a [`StorageError::StateConflict`].
pub fn state_conflict(
    entity: &'static str,
    id: Option<impl Display>,
    detail: impl Display,
) -> StorageError {
    StorageError::state_conflict(entity, id, detail)
}

/// Construct an [`StorageError::InvariantViolation`].
pub fn invariant_violation(entity: Option<&'static str>, detail: impl Display) -> StorageError {
    StorageError::invariant_violation(entity, detail)
}
