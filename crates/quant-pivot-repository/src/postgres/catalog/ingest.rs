//! Shared chunked ingest helpers for Gamma catalog repositories.

use std::collections::HashSet;

use num_traits::ToPrimitive;
use quant_pivot_error::storage::StorageError;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, IntoActiveModel, Iterable, QueryFilter, QuerySelect,
    sea_query::OnConflict,
};

use crate::batch::{chunk_for_in_clause, chunk_for_insert};

/// Load all rows whose string id column matches any of `ids`, chunking the `IN`
/// list to stay under the Postgres bind-parameter limit.
pub async fn find_models_by_str_id_chunks<E, C, Id, F>(
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
pub async fn find_existing_str_id_chunks<E, C, Id, F>(
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

/// Multi-row upsert split into bind-safe chunks.
pub async fn upsert_many_chunked<E, A>(
    db: &impl ConnectionTrait,
    dtos: Vec<A>,
    on_conflict: OnConflict,
) -> Result<u64, StorageError>
where
    E: EntityTrait,
    A: IntoActiveModel<E::ActiveModel>,
    E::Column: Iterable,
{
    if dtos.is_empty() {
        return Ok(0);
    }
    let count = ToPrimitive::to_u64(&dtos.len()).unwrap_or(u64::MAX);
    let models: Vec<E::ActiveModel> = dtos
        .into_iter()
        .map(IntoActiveModel::into_active_model)
        .collect();
    let rows_per_insert = crate::batch::max_rows_per_insert(E::Column::iter().count());
    for chunk in chunk_for_insert(&models, rows_per_insert) {
        E::insert_many(chunk.to_vec())
            .on_conflict(on_conflict.clone())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(count)
}
