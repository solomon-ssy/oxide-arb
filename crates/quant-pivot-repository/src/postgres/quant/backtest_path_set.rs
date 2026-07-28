//! Postgres-backed append-only CPCV path-set ledger repository.

use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_BACKTEST_PATH_SET, QUANT_MODEL_RUN},
};
use quant_pivot_models::{
    domain::{
        api::BacktestPathSetListQuery,
        pagination::{PageWindow, Paginated},
        quant::{BacktestPathSetInfo, NewBacktestPathSet},
    },
    entities::{
        quant_backtest_path_set::{Column as PathSetColumn, Entity as PathSetEntity},
        quant_model_run::{Column as ModelRunColumn, Entity as ModelRunEntity},
    },
    enums::quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{BacktestPathSetId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, OnConflict},
};

use crate::{
    postgres::{
        primitives,
        query::{list_fk_desc, paginate_mapped},
    },
    traits::{BacktestPathSetRepository, CpcvPathSetCommit},
};

/// Postgres-backed CPCV path-set ledger repository.
pub struct PgBacktestPathSetRepository {
    db: DatabaseConnection,
}

impl PgBacktestPathSetRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl BacktestPathSetRepository for PgBacktestPathSetRepository {
    async fn create(
        &self,
        path_set: NewBacktestPathSet,
    ) -> Result<BacktestPathSetInfo, StorageError> {
        path_set
            .verify_hash()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some("quant_backtest_path_set"),
                detail: error.to_string(),
            })?;
        let info = PathSetEntity::insert(path_set.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)?;
        verify_path_set(info)
    }

    async fn commit_cpcv(
        &self,
        commit: CpcvPathSetCommit,
    ) -> Result<BacktestPathSetInfo, StorageError> {
        commit
            .path_set
            .verify_hash()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(QUANT_BACKTEST_PATH_SET),
                detail: error.to_string(),
            })?;
        let path_set_hash = commit.path_set.path_set_hash();
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let run = ModelRunEntity::find_by_id(commit.path_set.model_run_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_MODEL_RUN, commit.path_set.model_run_id)
            })?;
        let exact_subject = run.run_kind == ModelRunKind::Cpcv
            && run.model_version_id == Some(commit.path_set.model_version_id)
            && run.decision_policy_snapshot_id == commit.path_set.decision_policy_snapshot_id
            && run.market_selection_id.is_none()
            && run.window_start == commit.path_set.window_start
            && run.window_end == commit.path_set.window_end
            && run.input_hash == commit.input_hash;
        if !exact_subject {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(&commit.path_set.model_run_id),
                "CPCV run differs from the canonical path-set subject",
            ));
        }
        let already_succeeded = matches!(
            run.status,
            ModelRunStatus::Succeeded
                if run.output_hash == Some(path_set_hash)
                    && run.error_code.is_none()
                    && run.error_message.is_none()
                    && run.finished_at.is_some()
        );
        let clean_running = matches!(
            run.status,
            ModelRunStatus::Running
                if run.output_hash.is_none()
                    && run.error_code.is_none()
                    && run.error_message.is_none()
                    && run.finished_at.is_none()
        );
        if !already_succeeded && !clean_running {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(&commit.path_set.model_run_id),
                "CPCV run is neither Running nor an exact Succeeded replay",
            ));
        }

        PathSetEntity::insert(commit.path_set.clone().into_active_model())
            .on_conflict(
                OnConflict::column(PathSetColumn::PathSetId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let stored = PathSetEntity::find_by_id(commit.path_set.path_set_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_BACKTEST_PATH_SET,
                    Some(&commit.path_set.path_set_id),
                    "CPCV commit completed without an observable path set",
                )
            })
            .and_then(verify_path_set)?;
        if stored.path_set_hash != path_set_hash
            || stored.model_run_id != commit.path_set.model_run_id
        {
            return Err(StorageError::state_conflict(
                QUANT_BACKTEST_PATH_SET,
                Some(&commit.path_set.path_set_id),
                "CPCV path-set collision is not an exact immutable replay",
            ));
        }
        if !already_succeeded {
            let terminal = ModelRunEntity::update_many()
                .col_expr(
                    ModelRunColumn::Status,
                    primitives::enum_value(&ModelRunStatus::Succeeded),
                )
                .col_expr(ModelRunColumn::OutputHash, Expr::value(Some(path_set_hash)))
                .col_expr(
                    ModelRunColumn::ErrorCode,
                    Expr::value(Option::<ModelRunErrorCode>::None),
                )
                .col_expr(
                    ModelRunColumn::ErrorMessage,
                    Expr::value(Option::<String>::None),
                )
                .col_expr(
                    ModelRunColumn::FinishedAt,
                    Expr::cust("statement_timestamp()"),
                )
                .filter(ModelRunColumn::ModelRunId.eq(commit.path_set.model_run_id))
                .filter(ModelRunColumn::Status.eq(ModelRunStatus::Running))
                .exec_with_returning(&transaction)
                .await
                .map_err(StorageError::from)?;
            if terminal.len() != 1 {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_MODEL_RUN),
                    format!(
                        "CPCV commit finalized {} model runs; expected one",
                        terminal.len()
                    ),
                ));
            }
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(stored)
    }

    async fn find_by_id(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> Result<Option<BacktestPathSetInfo>, StorageError> {
        PathSetEntity::find_by_id(*path_set_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .and_then(|row| row.map(Into::into).map(verify_path_set).transpose())
    }

    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestPathSetInfo>, StorageError> {
        let rows = list_fk_desc::<PathSetEntity, _, _, _>(
            &self.db,
            PathSetColumn::ModelVersionId,
            *model_version_id,
            PathSetColumn::CreatedAt,
            Into::into,
        )
        .await?;
        rows.into_iter().map(verify_path_set).collect()
    }

    async fn page(
        &self,
        query: BacktestPathSetListQuery,
    ) -> Result<Paginated<BacktestPathSetInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_version_id
                    .map(|id| PathSetColumn::ModelVersionId.eq(id)),
            )
            .add_option(query.from.map(|from| PathSetColumn::CreatedAt.gte(from)))
            .add_option(query.to.map(|to| PathSetColumn::CreatedAt.lt(to)));
        let page = paginate_mapped(
            PathSetEntity::find()
                .filter(condition)
                .order_by_desc(PathSetColumn::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(verify_path_set)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }
}

fn verify_path_set(info: BacktestPathSetInfo) -> Result<BacktestPathSetInfo, StorageError> {
    info.verify_hash()
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some("quant_backtest_path_set"),
            detail: error.to_string(),
        })?;
    Ok(info)
}
