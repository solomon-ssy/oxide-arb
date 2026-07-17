//! Postgres-backed feature-vector repository.

use crate::{postgres::query::find_models_by_id_chunks, traits::FeatureRepository};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{FeatureVectorInfo, NewFeatureVector},
    entities::quant_feature_vector,
    types::FeatureVectorId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

/// Postgres-backed feature-vector repository (insert-only ledger).
pub struct PgFeatureRepository {
    db: DatabaseConnection,
}

impl PgFeatureRepository {
    /// Build a repository over a database connection.
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FeatureRepository for PgFeatureRepository {
    async fn create(&self, vector: NewFeatureVector) -> Result<FeatureVectorInfo, StorageError> {
        let model = quant_feature_vector::Entity::insert(vector.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(model.into())
    }

    async fn create_batch(
        &self,
        vectors: Vec<NewFeatureVector>,
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        if vectors.is_empty() {
            return Ok(Vec::new());
        }
        // One `INSERT ... RETURNING` statement — atomic by itself and a single
        // round-trip. Postgres returns rows in insertion (VALUES) order, so the
        // result aligns with the input.
        let models = quant_feature_vector::Entity::insert_many(
            vectors.into_iter().map(IntoActiveModel::into_active_model),
        )
        .exec_with_returning(&self.db)
        .await
        .map_err(StorageError::from)?;
        Ok(models.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError> {
        quant_feature_vector::Entity::find_by_id(id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        ids: &[FeatureVectorId],
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        find_models_by_id_chunks::<quant_feature_vector::Entity, _, _>(
            &self.db,
            ids,
            quant_feature_vector::Column::FeatureVectorId,
        )
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
