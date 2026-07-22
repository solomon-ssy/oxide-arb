//! Postgres-backed backtest-report ledger repository (append-only).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::BacktestReportListQuery,
        pagination::{PageWindow, Paginated},
        quant::{BacktestReportInfo, NewBacktestReport},
    },
    entities::quant_backtest_report::{Column, Entity},
    types::{BacktestReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
};

use crate::{
    postgres::query::{list_by_fk_ordered_desc, paginate_mapped},
    traits::BacktestReportRepository,
};

/// Postgres-backed backtest-report ledger repository.
pub struct PgBacktestReportRepository {
    db: DatabaseConnection,
}

impl PgBacktestReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl BacktestReportRepository for PgBacktestReportRepository {
    async fn create(&self, report: NewBacktestReport) -> Result<BacktestReportInfo, StorageError> {
        Entity::insert(report.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<BacktestReportInfo>, StorageError> {
        Entity::find_by_id(*backtest_report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestReportInfo>, StorageError> {
        list_by_fk_ordered_desc::<Entity, _, _, _>(
            &self.db,
            Column::ModelVersionId,
            *model_version_id,
            Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: BacktestReportListQuery,
    ) -> Result<Paginated<BacktestReportInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_version_id
                    .map(|id| Column::ModelVersionId.eq(id)),
            )
            .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
            .add_option(query.to.map(|to| Column::CreatedAt.lt(to)));
        paginate_mapped(
            Entity::find()
                .filter(condition)
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
