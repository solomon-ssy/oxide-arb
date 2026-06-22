//! Postgres-backed recommendation report repository.

use crate::traits::RecommendationReportRepository;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewRecommendation, NewRecommendationReport, RecommendationReportInfo},
    entities::{quant_recommendation, quant_recommendation_report},
    enums::quant::{RecommendationReportStatus, ReportKind},
    types::RecommendationReportId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, TransactionTrait,
};

/// Postgres-backed recommendation report repository.
pub struct PgRecommendationReportRepository {
    db: DatabaseConnection,
}

impl PgRecommendationReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationReportRepository for PgRecommendationReportRepository {
    async fn create_report(
        &self,
        report: NewRecommendationReport,
        recommendations: Vec<NewRecommendation>,
    ) -> Result<RecommendationReportInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let report_model = quant_recommendation_report::Entity::insert(report.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        if !recommendations.is_empty() {
            let rows = recommendations
                .into_iter()
                .map(IntoActiveModel::into_active_model)
                .collect::<Vec<quant_recommendation::ActiveModel>>();
            quant_recommendation::Entity::insert_many(rows)
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(report_model.into())
    }

    async fn latest_published(
        &self,
        kind: ReportKind,
    ) -> Result<Option<RecommendationReportInfo>, StorageError> {
        quant_recommendation_report::Entity::find()
            .filter(quant_recommendation_report::Column::ReportKind.eq(kind))
            .filter(quant_recommendation_report::Column::Status.is_in([
                RecommendationReportStatus::Published,
                RecommendationReportStatus::PublishedEmpty,
            ]))
            .order_by_desc(quant_recommendation_report::Column::PublishedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        _reason: &str,
    ) -> Result<RecommendationReportInfo, StorageError> {
        let Some(row) = quant_recommendation_report::Entity::find_by_id(report_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "recommendation report not found: {report_id}"
            )));
        };
        if !matches!(
            row.status,
            RecommendationReportStatus::Published | RecommendationReportStatus::PublishedEmpty
        ) {
            return Err(StorageError::Conflict(format!(
                "cannot revoke report {report_id} from status {}",
                row.status.as_str()
            )));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(RecommendationReportStatus::Revoked);
        active.revoked_at = ActiveValue::Set(Some(Utc::now()));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
