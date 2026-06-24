//! Postgres-backed model registry repository.

use crate::traits::ModelRegistryRepository;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion},
    entities::{quant_model_spec, quant_model_version},
    enums::quant::ModelPublicationStatus,
    types::ModelVersionId,
};
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};

/// Postgres-backed model registry repository.
pub struct PgModelRegistryRepository {
    db: DatabaseConnection,
}

impl PgModelRegistryRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ModelRegistryRepository for PgModelRegistryRepository {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        quant_model_spec::Entity::insert(spec.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn create_model_version(
        &self,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        quant_model_version::Entity::insert(version.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_model_version_by_id(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        quant_model_version::Entity::find_by_id(model_version_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn publish_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        update_model_version_status(
            &self.db,
            model_version_id,
            ModelPublicationStatus::Published,
        )
        .await
    }

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        update_model_version_status(&self.db, model_version_id, ModelPublicationStatus::Retired)
            .await
    }
}

async fn update_model_version_status(
    db: &DatabaseConnection,
    model_version_id: &ModelVersionId,
    next: ModelPublicationStatus,
) -> Result<ModelVersionInfo, StorageError> {
    let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(StorageError::Conflict(format!(
            "model version not found: {model_version_id}"
        )));
    };
    let valid = matches!(
        (row.publication_status, next),
        (
            ModelPublicationStatus::Candidate | ModelPublicationStatus::Shadow,
            ModelPublicationStatus::Published
        ) | (
            ModelPublicationStatus::Published,
            ModelPublicationStatus::Retired
        )
    );
    if !valid {
        return Err(StorageError::Conflict(format!(
            "cannot transition model version {model_version_id} from {} to {}",
            row.publication_status.as_str(),
            next.as_str()
        )));
    }
    let mut active = row.into_active_model();
    active.publication_status = ActiveValue::Set(next);
    match next {
        ModelPublicationStatus::Published => {
            active.published_at = ActiveValue::Set(Some(Utc::now()));
        }
        ModelPublicationStatus::Retired => {
            active.retired_at = ActiveValue::Set(Some(Utc::now()));
        }
        _ => {}
    }
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}
