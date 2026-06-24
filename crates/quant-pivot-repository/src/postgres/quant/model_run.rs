//! Postgres-backed model-run repository.

use crate::traits::ModelRunRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelRunInfo, NewModelRun},
    entities::quant_model_run,
    enums::quant::{ModelRunErrorCode, ModelRunStatus},
    types::{ContentHash, ModelRunId},
};
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};

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
    async fn load_running(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<quant_model_run::Model, StorageError> {
        let Some(row) = quant_model_run::Entity::find_by_id(model_run_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "model run not found: {model_run_id}"
            )));
        };
        if row.status != ModelRunStatus::Running {
            return Err(StorageError::Conflict(format!(
                "cannot finalize model run {model_run_id} from non-running status {}",
                row.status.as_str()
            )));
        }
        Ok(row)
    }
}

#[async_trait::async_trait]
impl ModelRunRepository for PgModelRunRepository {
    async fn create(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError> {
        quant_model_run::Entity::insert(run.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<ModelRunInfo>, StorageError> {
        quant_model_run::Entity::find_by_id(model_run_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn succeed(
        &self,
        model_run_id: &ModelRunId,
        output_hash: ContentHash,
        metrics_json: serde_json::Value,
        finished_at: DateTime<Utc>,
    ) -> Result<ModelRunInfo, StorageError> {
        let mut active = self.load_running(model_run_id).await?.into_active_model();
        active.status = ActiveValue::Set(ModelRunStatus::Succeeded);
        active.output_hash = ActiveValue::Set(Some(output_hash));
        active.metrics_json = ActiveValue::Set(metrics_json);
        active.finished_at = ActiveValue::Set(Some(finished_at));
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
