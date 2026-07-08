//! Postgres-backed unified calibration-artifact ledger repository (append-only
//! identity; `active` is the sole mutable column — Phase 11.3 §3.4).

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::CalibrationArtifactRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        CalibrationArtifactInfo, CalibrationArtifactListQuery, NewCalibrationArtifact, PageWindow,
        Paginated,
    },
    entities::quant_calibration_artifact,
    types::CalibrationArtifactId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder,
};

/// Postgres-backed unified calibration-artifact ledger repository.
pub struct PgCalibrationArtifactRepository {
    db: DatabaseConnection,
}

impl PgCalibrationArtifactRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl CalibrationArtifactRepository for PgCalibrationArtifactRepository {
    async fn create(
        &self,
        artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        quant_calibration_artifact::Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        quant_calibration_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .kind
                    .map(|kind| quant_calibration_artifact::Column::Kind.eq(kind)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_calibration_artifact::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_calibration_artifact::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_calibration_artifact::Entity::find()
                .filter(condition)
                .order_by_desc(quant_calibration_artifact::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        let Some(row) = quant_calibration_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_CALIBRATION_ARTIFACT,
                artifact_id,
            ));
        };
        let mut active = row.into_active_model();
        active.active = ActiveValue::Set(true);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
