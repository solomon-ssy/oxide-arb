//! Postgres-backed read-only recommendation repository.

use crate::traits::RecommendationRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::RecommendationInfo,
    entities::quant_recommendation,
    types::{RecommendationId, RecommendationReportId},
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};

/// Postgres-backed read-only recommendation repository.
pub struct PgRecommendationRepository {
    db: DatabaseConnection,
}

impl PgRecommendationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationRepository for PgRecommendationRepository {
    async fn find_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        quant_recommendation::Entity::find()
            .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
            .order_by_asc(quant_recommendation::Column::Rank)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        quant_recommendation::Entity::find_by_id(recommendation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
