//! Postgres-backed pairwise model-comparison report ledger repository (append-only).

use crate::traits::ModelComparisonReportRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelComparisonReportInfo, NewModelComparisonReport},
    entities::quant_model_comparison_report,
    types::{ModelComparisonReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
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
        quant_model_comparison_report::Entity::find()
            .filter(
                quant_model_comparison_report::Column::CandidateModelVersionId
                    .eq(candidate_model_version_id.clone()),
            )
            .order_by_desc(quant_model_comparison_report::Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
