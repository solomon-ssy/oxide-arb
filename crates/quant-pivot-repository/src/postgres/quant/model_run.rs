//! Postgres-backed model-run repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_MODEL_RUN};
use quant_pivot_models::{
    domain::quant::{ModelRunInfo, NewModelRun},
    entities::quant_model_run::{Column, Entity, Model},
    enums::quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{ContentHash, ModelRunId, ModelVersionId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder,
};

use crate::{postgres::error, traits::ModelRunRepository};

/// Postgres-backed model-run repository: create a `Running` run, then finalize it
/// to a terminal state through the guarded [`Self::succeed`] / [`Self::fail`]
/// transitions.
pub struct PgModelRunRepository {
    db: DatabaseConnection,
}

impl PgModelRunRepository {
    /// Build a repository over a database connection.
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Load a `Running` run for finalization, rejecting a missing row or an
    /// already-terminal transition.
    async fn load_running(&self, model_run_id: &ModelRunId) -> Result<Model, StorageError> {
        let Some(row) = Entity::find_by_id(*model_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(QUANT_MODEL_RUN, model_run_id));
        };
        if row.status != ModelRunStatus::Running {
            return Err(error::state_conflict(
                QUANT_MODEL_RUN,
                Some(model_run_id),
                format!(
                    "cannot finalize from non-running status {}",
                    row.status.as_str()
                ),
            ));
        }
        Ok(row)
    }
}

#[async_trait::async_trait]
impl ModelRunRepository for PgModelRunRepository {
    async fn create(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError> {
        Entity::insert(run.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<ModelRunInfo>, StorageError> {
        Entity::find_by_id(*model_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_succeeded_live_between(
        &self,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<ModelRunInfo>, StorageError> {
        Entity::find()
            .filter(Column::RunKind.eq(ModelRunKind::LiveInference))
            .filter(Column::Status.eq(ModelRunStatus::Succeeded))
            .filter(Column::WindowStart.gte(from))
            .filter(Column::WindowStart.lt(until))
            .order_by_asc(Column::WindowStart)
            .order_by_asc(Column::ModelRunId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn succeed(
        &self,
        model_run_id: &ModelRunId,
        output_hash: ContentHash,
        finished_at: DateTime<Utc>,
        model_version_id: Option<ModelVersionId>,
    ) -> Result<ModelRunInfo, StorageError> {
        let mut active = self.load_running(model_run_id).await?.into_active_model();
        active.status = ActiveValue::Set(ModelRunStatus::Succeeded);
        active.output_hash = ActiveValue::Set(Some(output_hash));
        active.finished_at = ActiveValue::Set(Some(finished_at));
        if let Some(version_id) = model_version_id {
            active.model_version_id = ActiveValue::Set(Some(version_id));
        }
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn fail(
        &self,
        model_run_id: &ModelRunId,
        error_code: ModelRunErrorCode,
        error_message: String,
        finished_at: DateTime<Utc>,
    ) -> Result<ModelRunInfo, StorageError> {
        let mut active = self.load_running(model_run_id).await?.into_active_model();
        active.status = ActiveValue::Set(ModelRunStatus::Failed);
        active.error_code = ActiveValue::Set(Some(error_code));
        active.error_message = ActiveValue::Set(Some(error_message));
        active.finished_at = ActiveValue::Set(Some(finished_at));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
