//! Postgres-backed favorite-longshot bias-table ledger repository (append-only).

use crate::traits::FavoriteLongshotBiasTableRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        BiasTableListQuery, FavoriteLongshotBiasTableInfo, NewFavoriteLongshotBiasTable,
        PageWindow, Paginated,
    },
    entities::quant_favorite_longshot_bias_table,
    types::FavoriteLongshotBiasTableId,
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
};

use crate::postgres::query::paginate_mapped;

/// Postgres-backed favorite-longshot bias-table ledger repository.
pub struct PgFavoriteLongshotBiasTableRepository {
    db: DatabaseConnection,
}

impl PgFavoriteLongshotBiasTableRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FavoriteLongshotBiasTableRepository for PgFavoriteLongshotBiasTableRepository {
    async fn create(
        &self,
        table: NewFavoriteLongshotBiasTable,
    ) -> Result<FavoriteLongshotBiasTableInfo, StorageError> {
        quant_favorite_longshot_bias_table::Entity::insert(table.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        bias_table_id: &FavoriteLongshotBiasTableId,
    ) -> Result<Option<FavoriteLongshotBiasTableInfo>, StorageError> {
        quant_favorite_longshot_bias_table::Entity::find_by_id(bias_table_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: BiasTableListQuery,
    ) -> Result<Paginated<FavoriteLongshotBiasTableInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .from
                    .map(|from| quant_favorite_longshot_bias_table::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_favorite_longshot_bias_table::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_favorite_longshot_bias_table::Entity::find()
                .filter(condition)
                .order_by_desc(quant_favorite_longshot_bias_table::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
