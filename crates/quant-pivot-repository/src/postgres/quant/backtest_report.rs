//! Postgres-backed backtest-report ledger repository (append-only).

use crate::traits::BacktestReportRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BacktestReportInfo, NewBacktestReport},
    entities::quant_backtest_report,
    types::{BacktestReportId, ModelVersionId},
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

use crate::postgres::query::list_by_fk_ordered_desc;

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
        quant_backtest_report::Entity::insert(report.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<BacktestReportInfo>, StorageError> {
        quant_backtest_report::Entity::find_by_id(backtest_report_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestReportInfo>, StorageError> {
        list_by_fk_ordered_desc::<quant_backtest_report::Entity, _, _, _>(
            &self.db,
            quant_backtest_report::Column::ModelVersionId,
            model_version_id.clone(),
            quant_backtest_report::Column::CreatedAt,
            Into::into,
        )
        .await
    }
}
