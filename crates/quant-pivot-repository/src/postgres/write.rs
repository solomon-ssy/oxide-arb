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

use crate::batch::{chunk_for_insert, max_rows_per_insert};

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
    let rows_per_insert = max_rows_per_insert(E::Column::iter().count());
    for chunk in chunk_for_insert(&models, rows_per_insert) {
        E::insert_many(chunk.to_vec())
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(count)
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
    let rows_per_insert = max_rows_per_insert(E::Column::iter().count());
    for chunk in chunk_for_insert(&models, rows_per_insert) {
        E::insert_many(chunk.to_vec())
            .on_conflict(on_conflict.clone())
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
    }
    Ok(count)
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
