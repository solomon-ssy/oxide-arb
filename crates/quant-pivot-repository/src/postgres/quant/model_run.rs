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
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::Expr,
};

use crate::{postgres::primitives, traits::ModelRunRepository};

/// Postgres-backed model-run repository: create a `Running` run, then finalize it
/// to a terminal state through guarded success, failure, or cancellation
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

    async fn terminal_result(
        &self,
        model_run_id: &ModelRunId,
        mut rows: Vec<Model>,
    ) -> Result<ModelRunInfo, StorageError> {
        if let Some(row) = rows.pop() {
            if !rows.is_empty() {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_MODEL_RUN),
                    "model-run compare-and-set finalized more than one row",
                ));
            }
            return Ok(row.into());
        }
        let Some(row) = Entity::find_by_id(*model_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(QUANT_MODEL_RUN, model_run_id));
        };
        Err(StorageError::state_conflict(
            QUANT_MODEL_RUN,
            Some(model_run_id),
            format!(
                "cannot finalize from non-running status {}",
                row.status.as_str()
            ),
        ))
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
        model_version_id: Option<ModelVersionId>,
    ) -> Result<ModelRunInfo, StorageError> {
        let mut update = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ModelRunStatus::Succeeded),
            )
            .col_expr(Column::OutputHash, Expr::value(Some(output_hash)))
            .col_expr(
                Column::ErrorCode,
                Expr::value(Option::<ModelRunErrorCode>::None),
            )
            .col_expr(Column::ErrorMessage, Expr::value(Option::<String>::None))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .filter(Column::ModelRunId.eq(*model_run_id))
            .filter(Column::Status.eq(ModelRunStatus::Running));
        if let Some(version_id) = model_version_id {
            update = update.col_expr(Column::ModelVersionId, Expr::value(Some(version_id)));
        }
        let rows = update
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        self.terminal_result(model_run_id, rows).await
    }

    async fn fail(
        &self,
        model_run_id: &ModelRunId,
        error_code: ModelRunErrorCode,
        error_message: String,
    ) -> Result<ModelRunInfo, StorageError> {
        let rows = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ModelRunStatus::Failed),
            )
            .col_expr(Column::OutputHash, Expr::value(Option::<ContentHash>::None))
            .col_expr(Column::ErrorCode, primitives::enum_value(&error_code))
            .col_expr(Column::ErrorMessage, Expr::value(Some(error_message)))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .filter(Column::ModelRunId.eq(*model_run_id))
            .filter(Column::Status.eq(ModelRunStatus::Running))
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        self.terminal_result(model_run_id, rows).await
    }

    async fn cancel(
        &self,
        model_run_id: &ModelRunId,
        reason: String,
    ) -> Result<ModelRunInfo, StorageError> {
        let rows = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ModelRunStatus::Cancelled),
            )
            .col_expr(Column::OutputHash, Expr::value(Option::<ContentHash>::None))
            .col_expr(
                Column::ErrorCode,
                primitives::enum_value(&ModelRunErrorCode::CancelledByOperator),
            )
            .col_expr(Column::ErrorMessage, Expr::value(Some(reason)))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .filter(Column::ModelRunId.eq(*model_run_id))
            .filter(Column::Status.eq(ModelRunStatus::Running))
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        self.terminal_result(model_run_id, rows).await
    }
}
