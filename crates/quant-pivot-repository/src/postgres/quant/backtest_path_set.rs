//! Postgres-backed CPCV path-set ledger repository (append-only, Phase 11.5).

use crate::{
    postgres::query::{list_by_fk_ordered_desc, paginate_mapped},
    traits::BacktestPathSetRepository,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        BacktestPathSetInfo, BacktestPathSetListQuery, NewBacktestPathSet, PageWindow, Paginated,
    },
    entities::quant_backtest_path_set,
    types::{BacktestPathSetId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
};

/// Postgres-backed CPCV path-set ledger repository.
pub struct PgBacktestPathSetRepository {
    db: DatabaseConnection,
}

impl PgBacktestPathSetRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl BacktestPathSetRepository for PgBacktestPathSetRepository {
    async fn create(
        &self,
        path_set: NewBacktestPathSet,
    ) -> Result<BacktestPathSetInfo, StorageError> {
        quant_backtest_path_set::Entity::insert(path_set.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> Result<Option<BacktestPathSetInfo>, StorageError> {
        quant_backtest_path_set::Entity::find_by_id(path_set_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestPathSetInfo>, StorageError> {
        list_by_fk_ordered_desc::<quant_backtest_path_set::Entity, _, _, _>(
            &self.db,
            quant_backtest_path_set::Column::ModelVersionId,
            model_version_id.clone(),
            quant_backtest_path_set::Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: BacktestPathSetListQuery,
    ) -> Result<Paginated<BacktestPathSetInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_version_id
                    .clone()
                    .map(|id| quant_backtest_path_set::Column::ModelVersionId.eq(id)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_backtest_path_set::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_backtest_path_set::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_backtest_path_set::Entity::find()
                .filter(condition)
                .order_by_desc(quant_backtest_path_set::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
