//! Shared chunked id-lookup helpers for Gamma catalog repositories.
//!
//! Batch write primitives (`insert_many_chunked` / `upsert_many_chunked`) live
//! in the shared Postgres write helpers.

use std::collections::HashSet;

use quant_pivot_error::storage::StorageError;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

use crate::batch::chunk_for_in_clause;

/// Load all rows whose string id column matches any of `ids`, chunking the `IN`
/// list to stay under the Postgres bind-parameter limit.
pub async fn find_str_id_chunks<E, C, Id, F>(
    db: &impl ConnectionTrait,
    ids: &[Id],
    id_column: C,
    as_str: F,
) -> Result<Vec<E::Model>, StorageError>
where
    E: EntityTrait,
    C: ColumnTrait,
    F: Copy + Fn(&Id) -> &str,
{
    let mut rows = Vec::with_capacity(ids.len());
    for chunk in chunk_for_in_clause(ids) {
        let batch = E::find()
            .filter(id_column.is_in(chunk.iter().map(as_str)))
            .all(db)
            .await
            .map_err(StorageError::from)?;
        rows.extend(batch);
    }
    Ok(rows)
}

/// Project only the string id column for existence checks, chunked.
pub async fn find_existing_chunks<E, C, Id, F>(
    db: &impl ConnectionTrait,
    ids: &[Id],
    id_column: C,
    as_str: F,
) -> Result<HashSet<String>, StorageError>
where
    E: EntityTrait,
    C: ColumnTrait,
    F: Copy + Fn(&Id) -> &str,
{
    let mut existing = HashSet::with_capacity(ids.len());
    for chunk in chunk_for_in_clause(ids) {
        let batch = E::find()
            .filter(id_column.is_in(chunk.iter().map(as_str)))
            .select_only()
            .column(id_column)
            .into_tuple::<String>()
            .all(db)
            .await
            .map_err(StorageError::from)?;
        existing.extend(batch);
    }
    Ok(existing)
}
