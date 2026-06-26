//! Postgres-backed recommendation attribution repository.

use crate::traits::AttributionRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewRecommendationAttribution, RecommendationAttributionInfo},
    entities::quant_recommendation_attribution,
    types::RecommendationId,
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter};

/// Postgres-backed recommendation attribution repository.
pub struct PgAttributionRepository {
    db: DatabaseConnection,
}

impl PgAttributionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl AttributionRepository for PgAttributionRepository {
    async fn create(
        &self,
        attribution: NewRecommendationAttribution,
    ) -> Result<RecommendationAttributionInfo, StorageError> {
        quant_recommendation_attribution::Entity::insert(attribution.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Vec<RecommendationAttributionInfo>, StorageError> {
        quant_recommendation_attribution::Entity::find()
            .filter(
                quant_recommendation_attribution::Column::RecommendationId
                    .eq(recommendation_id.clone()),
            )
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
