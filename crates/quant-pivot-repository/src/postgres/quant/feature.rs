//! Postgres-backed feature-vector repository.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{FeatureVectorInfo, NewFeatureVector},
    entities::quant_feature_vector::{Column, Entity},
    types::FeatureVectorId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel, TransactionTrait};

use crate::{
    postgres::{query::find_models_by_id_chunks, write::insert_many_returning_chunked},
    traits::FeatureRepository,
};

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
        let model = Entity::insert(vector.into_active_model())
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
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let models = insert_many_returning_chunked::<Entity, _>(&txn, vectors).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(models.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError> {
        Entity::find_by_id(id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        ids: &[FeatureVectorId],
    ) -> Result<Vec<FeatureVectorInfo>, StorageError> {
        find_models_by_id_chunks::<Entity, _, _>(&self.db, ids, Column::FeatureVectorId)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
