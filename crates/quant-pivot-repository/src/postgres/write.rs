//! Context-neutral batch-write primitives shared by every `Pg*Repository`.
//!
//! All multi-row INSERT / UPSERT paths must funnel through [`insert_many_chunked`]
//! or [`upsert_many_chunked`] rather than calling [`EntityTrait::insert_many`]
//! directly. Both helpers (a) chunk under the Postgres bind-parameter limit and
//! (b) run [`align_partial_columns`], which is what keeps nullable native-`enum`
//! columns type-consistent across a heterogeneous batch.

use num_traits::ToPrimitive;
use quant_pivot_error::storage::StorageError;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, EntityTrait, IntoActiveModel, Iterable,
    sea_query::{OnConflict, Value},
};

use crate::batch::chunk_for_insert;
#[cfg(test)]
use crate::batch::max_rows_per_insert;

/// Multi-row INSERT split into bind-safe chunks (no `ON CONFLICT`).
pub async fn insert_many_chunked<E, A>(
    db: &impl ConnectionTrait,
    dtos: Vec<A>,
) -> Result<u64, StorageError>
where
    E: EntityTrait,
    E::Model: IntoActiveModel<E::ActiveModel>,
    A: IntoActiveModel<E::ActiveModel>,
    E::Column: Iterable,
{
    let (count, models) = prepare_batch::<E, A>(dtos);
    if models.is_empty() {
        return Ok(0);
    }
    let columns_per_row = E::Column::iter().count();
    for chunk in chunk_for_insert(&models, columns_per_row) {
        E::insert_many(chunk.to_vec())
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(count)
}

/// Multi-row `INSERT ... RETURNING` split into bind-safe chunks.
///
/// Callers that require the entire batch to commit atomically must pass an
/// explicit transaction as `db`; this helper deliberately does not invent a
/// transaction boundary for its caller.
pub async fn insert_many_returning_chunked<E, A>(
    db: &impl ConnectionTrait,
    dtos: Vec<A>,
) -> Result<Vec<E::Model>, StorageError>
where
    E: EntityTrait,
    E::Model: IntoActiveModel<E::ActiveModel>,
    A: IntoActiveModel<E::ActiveModel>,
    E::Column: Iterable,
{
    let (_, models) = prepare_batch::<E, A>(dtos);
    if models.is_empty() {
        return Ok(Vec::new());
    }
    let columns_per_row = E::Column::iter().count();
    let mut inserted = Vec::with_capacity(models.len());
    for chunk in chunk_for_insert(&models, columns_per_row) {
        inserted.extend(
            E::insert_many(chunk.to_vec())
                .exec_with_returning(db)
                .await
                .map_err(StorageError::from)?,
        );
    }
    Ok(inserted)
}

/// Multi-row UPSERT (`ON CONFLICT DO …`) split into bind-safe chunks.
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
    let (count, models) = prepare_batch::<E, A>(dtos);
    if models.is_empty() {
        return Ok(0);
    }
    let columns_per_row = E::Column::iter().count();
    for chunk in chunk_for_insert(&models, columns_per_row) {
        E::insert_many(chunk.to_vec())
            .on_conflict(on_conflict.clone())
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(count)
}

/// Deterministic upper bound on statements emitted by the shared batch-write
/// helpers for `row_count` rows of entity `E`.
#[cfg(test)]
#[must_use]
fn batch_statement_budget<E>(row_count: usize) -> usize
where
    E: EntityTrait,
    E::Column: Iterable,
{
    let rows_per_statement = max_rows_per_insert(E::Column::iter().count());
    row_count.div_ceil(rows_per_statement)
}

/// Lower DTOs to active models and align partially-set columns across the batch.
fn prepare_batch<E, A>(dtos: Vec<A>) -> (u64, Vec<E::ActiveModel>)
where
    E: EntityTrait,
    A: IntoActiveModel<E::ActiveModel>,
    E::Column: Iterable,
{
    let count = ToPrimitive::to_u64(&dtos.len()).unwrap_or(u64::MAX);
    let mut models: Vec<E::ActiveModel> = dtos
        .into_iter()
        .map(IntoActiveModel::into_active_model)
        .collect();
    align_partial_columns::<E>(&mut models);
    (count, models)
}

/// Make every column that is `Set` in *any* row of a batch also `Set` in *every*
/// row, filling gaps with a type-correct SQL `NULL`.
///
/// [`SeaORM`](sea_orm)'s `insert_many` fills a `NotSet` hole — a nullable field
/// left `None`, which `IntoActiveValue` lowers to [`ActiveValue::NotSet`] — with
/// an *uncast* typed-null bind, while a `Set` value on a native Postgres `enum`
/// column is wrapped in `CAST($n AS qp_enum)`. A batch mixing the two makes
/// Postgres resolve that `VALUES` column — and therefore `EXCLUDED.<col>` in an
/// `ON CONFLICT DO UPDATE` clause — to `text`, which rejects the enum insert /
/// conflict assignment. Rewriting each hole to `Set(NULL)` routes it through the
/// same `CAST`, so the column stays homogeneously enum-typed regardless of
/// per-row `Option` nullability — no per-repository partitioning required.
///
/// [`ActiveValue::NotSet`]: sea_orm::ActiveValue::NotSet
fn align_partial_columns<E>(models: &mut [E::ActiveModel])
where
    E: EntityTrait,
    E::Column: Iterable,
{
    for col in E::Column::iter() {
        // A column left `NotSet` in *all* rows keeps its DB default; only
        // columns present in at least one row must be aligned across the batch.
        let typed_null = models
            .iter()
            .find_map(|model| model.get(col).into_value())
            .map(|value| Value::as_null(&value));
        let Some(typed_null) = typed_null else {
            continue;
        };
        for model in models.iter_mut() {
            if model.is_not_set(col) {
                model.set(col, typed_null.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::entities::casbin_rule;

    use super::batch_statement_budget;

    #[test]
    fn batch_statement_budget_uses_columns_once() {
        assert_eq!(batch_statement_budget::<casbin_rule::Entity>(0), 0);
        assert_eq!(batch_statement_budget::<casbin_rule::Entity>(8_191), 1);
        assert_eq!(batch_statement_budget::<casbin_rule::Entity>(8_192), 2);
        assert_eq!(batch_statement_budget::<casbin_rule::Entity>(16_382), 2);
    }
}
