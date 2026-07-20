//! Shared junction-table replace-set helpers for RBAC repositories.

use std::{
    collections::HashSet,
    hash::{BuildHasher, Hash},
};

use quant_pivot_error::storage::StorageError;
use sea_orm::{ConnectionTrait, EntityTrait, IntoActiveModel, Iterable, sea_query::OnConflict};

use crate::postgres::write::upsert_many_chunked;

/// Compute added and removed ids between a target set and the current junction rows.
pub fn replace_set_diff<T, S>(target: &HashSet<T, S>, current: &HashSet<T, S>) -> (Vec<T>, Vec<T>)
where
    T: Eq + Hash + Clone,
    S: BuildHasher,
{
    (
        target.difference(current).cloned().collect(),
        current.difference(target).cloned().collect(),
    )
}

/// Insert junction rows, ignoring duplicates via `ON CONFLICT DO NOTHING`.
pub async fn insert_junction_rows<E>(
    db: &impl ConnectionTrait,
    rows: impl IntoIterator<Item = E::ActiveModel>,
    on_conflict: OnConflict,
) -> Result<(), StorageError>
where
    E: EntityTrait,
    E::Model: IntoActiveModel<E::ActiveModel>,
    E::Column: Iterable,
{
    let rows: Vec<E::ActiveModel> = rows.into_iter().collect();
    if rows.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<E, _>(db, rows, on_conflict).await?;
    Ok(())
}
