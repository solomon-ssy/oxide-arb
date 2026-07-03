//! Postgres-backed pairwise model-comparison report ledger repository (append-only).

use crate::traits::ModelComparisonReportRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ComparisonReportListQuery, ModelComparisonReportInfo, NewModelComparisonReport, PageWindow,
        Paginated,
    },
    entities::quant_model_comparison_report,
    types::{ModelComparisonReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
};

use crate::postgres::query::{list_by_fk_ordered_desc, paginate_mapped};

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
        quant_model_comparison_report::Entity::insert(report.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError> {
        quant_model_comparison_report::Entity::find_by_id(comparison_report_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_candidate_version(
        &self,
        candidate_model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelComparisonReportInfo>, StorageError> {
        list_by_fk_ordered_desc::<quant_model_comparison_report::Entity, _, _, _>(
            &self.db,
            quant_model_comparison_report::Column::CandidateModelVersionId,
            candidate_model_version_id.clone(),
            quant_model_comparison_report::Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: ComparisonReportListQuery,
    ) -> Result<Paginated<ModelComparisonReportInfo>, StorageError> {
        let condition =
            Condition::all()
                .add_option(query.candidate_model_version_id.clone().map(|id| {
                    quant_model_comparison_report::Column::CandidateModelVersionId.eq(id)
                }))
                .add_option(
                    query
                        .from
                        .map(|from| quant_model_comparison_report::Column::CreatedAt.gte(from)),
                )
                .add_option(
                    query
                        .to
                        .map(|to| quant_model_comparison_report::Column::CreatedAt.lt(to)),
                );
        paginate_mapped(
            quant_model_comparison_report::Entity::find()
                .filter(condition)
                .order_by_desc(quant_model_comparison_report::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
