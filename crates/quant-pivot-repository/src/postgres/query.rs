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
//! [`paginate_mapped`] so count + fetch share one code path.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{PageRequest, Paginated};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter,
    QueryOrder, Select,
};

/// Return `Some` only when `value` is present and non-empty after trim is not
/// required — empty strings are treated as absent filters.
#[must_use]
pub fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Paginate a select query, map models to domain types, and return a
/// [`Paginated`] envelope using the normalized [`PageRequest`] window.
pub async fn paginate_mapped<'db, E, D, F>(
    select: Select<E>,
    db: &'db impl ConnectionTrait,
    page: &PageRequest,
    map: F,
) -> Result<Paginated<D>, StorageError>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Send + Sync + 'db,
    F: FnMut(E::Model) -> D,
{
    let window = page.normalized();
    let paginator = select.paginate(db, window.size);
    let total = paginator.num_items().await.map_err(StorageError::from)?;
    let rows = paginator
        .fetch_page(window.page.saturating_sub(1))
        .await
        .map_err(StorageError::from)?;
    let items = rows.into_iter().map(map).collect();
    Ok(Paginated::from_request(items, total, &window))
}

/// List rows for a foreign-key value, newest [`created_at_column`] first.
pub async fn list_by_fk_ordered_desc<E, Fk, Created, Item>(
    db: &impl ConnectionTrait,
    fk_column: Fk,
    fk_value: impl Into<sea_orm::Value>,
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

#[cfg(test)]
mod tests {
    use super::non_empty;

    #[test]
    fn non_empty_rejects_blank_strings() {
        assert_eq!(non_empty(None), None);
        assert_eq!(non_empty(Some("")), None);
        assert_eq!(non_empty(Some("x")), Some("x"));
    }
}
