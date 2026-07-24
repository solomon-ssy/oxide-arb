//! Shared [`SeaORM`] query helpers for Postgres repositories.
//!
//! ## Optional filters
//!
//! Prefer [`Condition::add_option`] when building reusable filter expressions
//! (equality, ranges, time windows). Use [`QueryTrait::apply_if`] on a
//! [`Select`] when chaining optional operations (keyword OR, joins, limits).
//!
//! ## Pagination
//!
//! All list endpoints use [`SeaORM`] [`PaginatorTrait::paginate`] via
//! [`paginate_mapped`] (single-entity rows) or [`paginate_into_model`]
//! (custom [`FromQueryResult`] projections, including N:1 JOIN selects).
//!
//! ### Join pagination rules
//!
//! - **Allowed:** N:1 `INNER JOIN` filter/projection (one primary row per join).
//! - **Forbidden:** 1:N join pagination — `COUNT(*)` and page rows inflate.
//! - `total` always comes from the primary paginator, never from a second
//!   enrich query.
//!
//! ## Batch id loads
//!
//! Use [`find_id_chunks`] / [`map_by_key`] / [`group_by_key`] with
//! [`crate::batch::chunk_for_in_clause`] so hot-path enrichers stay under the
//! Postgres bind-parameter limit and avoid N+1 lookups.

use std::{collections::HashMap, hash::Hash};

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::pagination::{PageWindow, Paginated};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter,
    QueryOrder, Select, Value,
};

use crate::batch::chunk_for_in_clause;

/// Return `Some` only when `value` is present and non-empty after trim is not
/// required — empty strings are treated as absent filters.
#[must_use]
pub fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Paginate a select query, map models to domain types, and return a
/// [`Paginated`] envelope using the hardened [`PageWindow`].
pub async fn paginate_mapped<'db, E, D, F>(
    select: Select<E>,
    db: &'db impl ConnectionTrait,
    window: PageWindow,
    map: F,
) -> Result<Paginated<D>, StorageError>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Send + Sync + 'db,
    F: FnMut(E::Model) -> D,
{
    let paginator = select.paginate(db, window.size());
    let total = paginator.num_items().await.map_err(StorageError::from)?;
    let rows = paginator
        .fetch_page(window.seaorm_index())
        .await
        .map_err(StorageError::from)?;
    let items = rows.into_iter().map(map).collect();
    Ok(Paginated::from_window(items, total, window))
}

/// Paginate a select into a custom [`FromQueryResult`] projection.
///
/// Use after N:1 `JOIN` + `column_as` (or `select_only`) when the row type is
/// not `E::Model`. Do **not** use with 1:N joins — page totals inflate.
pub async fn paginate_into_model<'db, E, M>(
    select: Select<E>,
    db: &'db impl ConnectionTrait,
    window: PageWindow,
) -> Result<Paginated<M>, StorageError>
where
    E: EntityTrait,
    M: FromQueryResult + Send + Sync + 'db,
{
    let paginator = select.into_model::<M>().paginate(db, window.size());
    let total = paginator.num_items().await.map_err(StorageError::from)?;
    let items = paginator
        .fetch_page(window.seaorm_index())
        .await
        .map_err(StorageError::from)?;
    Ok(Paginated::from_window(items, total, window))
}

/// List rows for a foreign-key value, newest [`created_at_column`] first.
pub async fn list_fk_desc<E, Fk, Created, Item>(
    db: &impl ConnectionTrait,
    fk_column: Fk,
    fk_value: impl Into<Value>,
    created_at_column: Created,
    map: impl Fn(E::Model) -> Item,
) -> Result<Vec<Item>, StorageError>
where
    E: EntityTrait,
    Fk: ColumnTrait,
    Created: ColumnTrait,
{
    E::find()
        .filter(fk_column.eq(fk_value))
        .order_by_desc(created_at_column)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(map).collect())
}

/// Load entity models whose id column is in `ids`, chunking the `IN` list.
pub async fn find_id_chunks<E, C, Id>(
    db: &impl ConnectionTrait,
    ids: &[Id],
    id_column: C,
) -> Result<Vec<E::Model>, StorageError>
where
    E: EntityTrait,
    C: ColumnTrait + Copy,
    Id: Clone + Into<Value>,
{
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(ids.len());
    for chunk in chunk_for_in_clause(ids) {
        let batch = E::find()
            .filter(id_column.is_in(chunk.to_vec()))
            .all(db)
            .await
            .map_err(StorageError::from)?;
        rows.extend(batch);
    }
    Ok(rows)
}

/// Index items by a key extracted from each value (last write wins on collision).
#[must_use]
pub fn map_by_key<K, V, F>(items: impl IntoIterator<Item = V>, mut key_fn: F) -> HashMap<K, V>
where
    K: Eq + Hash,
    F: FnMut(&V) -> K,
{
    items
        .into_iter()
        .map(|item| {
            let key = key_fn(&item);
            (key, item)
        })
        .collect()
}

/// Group items by a key extracted from each value.
#[must_use]
pub fn group_by_key<K, V, F>(
    items: impl IntoIterator<Item = V>,
    mut key_fn: F,
) -> HashMap<K, Vec<V>>
where
    K: Eq + Hash,
    F: FnMut(&V) -> K,
{
    let mut grouped: HashMap<K, Vec<V>> = HashMap::new();
    for item in items {
        let key = key_fn(&item);
        grouped.entry(key).or_default().push(item);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::{group_by_key, map_by_key, non_empty};

    #[test]
    fn non_empty_rejects_strings() {
        assert_eq!(non_empty(None), None);
        assert_eq!(non_empty(Some("")), None);
        assert_eq!(non_empty(Some("x")), Some("x"));
    }

    #[test]
    fn map_key_indexes_write() {
        let map = map_by_key(vec![("a", 1), ("b", 2), ("a", 3)], |(k, _)| *k);
        assert_eq!(map.get("a"), Some(&("a", 3)));
        assert_eq!(map.get("b"), Some(&("b", 2)));
    }

    #[test]
    fn group_key_collects_buckets() {
        let grouped = group_by_key(vec![("a", 1), ("b", 2), ("a", 3)], |(k, _)| *k);
        assert_eq!(grouped.get("a").map(Vec::len), Some(2));
        assert_eq!(grouped.get("b").map(Vec::len), Some(1));
    }
}
