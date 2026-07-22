//! Postgres-backed pairwise model-comparison report ledger repository (append-only).

use std::collections::HashMap;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::ComparisonReportListQuery,
        pagination::{PageWindow, Paginated},
        quant::{ModelComparisonReportInfo, NewModelComparisonReport},
    },
    entities::quant_model_comparison_report::{Column, Entity},
    types::{BacktestReportId, ModelComparisonReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
};

use crate::{
    postgres::query::{list_by_fk_ordered_desc, paginate_mapped},
    traits::ModelComparisonReportRepository,
};

/// Postgres-backed comparison-report ledger repository.
pub struct PgModelComparisonReportRepository {
    db: DatabaseConnection,
}

impl PgModelComparisonReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ModelComparisonReportRepository for PgModelComparisonReportRepository {
    async fn create(
        &self,
        report: NewModelComparisonReport,
    ) -> Result<ModelComparisonReportInfo, StorageError> {
        Entity::insert(report.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError> {
        Entity::find_by_id(*comparison_report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_candidate_version(
        &self,
        candidate_model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelComparisonReportInfo>, StorageError> {
        list_by_fk_ordered_desc::<Entity, _, _, _>(
            &self.db,
            Column::CandidateModelVersionId,
            *candidate_model_version_id,
            Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: ComparisonReportListQuery,
    ) -> Result<Paginated<ModelComparisonReportInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .candidate_model_version_id
                    .map(|id| Column::CandidateModelVersionId.eq(id)),
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

    async fn find_by_backtest_report_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError> {
        Entity::find()
            .filter(
                Condition::any()
                    .add(Column::CandidateReportId.eq(*backtest_report_id))
                    .add(Column::BaselineReportId.eq(*backtest_report_id)),
            )
            .order_by_desc(Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn comparison_ids_for_backtest_reports(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> Result<HashMap<BacktestReportId, ModelComparisonReportId>, StorageError> {
        if backtest_report_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = Entity::find()
            .filter(
                Condition::any()
                    .add(Column::CandidateReportId.is_in(backtest_report_ids.to_vec()))
                    .add(Column::BaselineReportId.is_in(backtest_report_ids.to_vec())),
            )
            .order_by_desc(Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut map = HashMap::new();
        for row in rows {
            let info = ModelComparisonReportInfo::from(row);
            map.entry(info.candidate_report_id)
                .or_insert_with(|| info.comparison_report_id);
            map.entry(info.baseline_report_id)
                .or_insert_with(|| info.comparison_report_id);
        }
        Ok(map)
    }
}
