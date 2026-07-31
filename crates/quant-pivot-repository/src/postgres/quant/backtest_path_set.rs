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
    enums::{
        model::ModelFamily,
        quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    },
    types::{
        BacktestPathSetId, ModelVersionId, backtest::CpcvFoldCalibrationPolicy,
        model_lineage::ModelVersionDerivation,
    },
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, OnConflict},
};

use crate::{
    postgres::{
        primitives, quant::model_registry::PgModelRegistryRepository, query::paginate_mapped,
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

    async fn validate_subject(
        transaction: &impl ConnectionTrait,
        path_set: &NewBacktestPathSet,
    ) -> Result<(), StorageError> {
        let model = PgModelRegistryRepository::require_version_info(
            transaction,
            &path_set.model_version_id,
        )
        .await?;
        let bindings = model
            .verified_serving_contract()
            .map_err(|error| {
                StorageError::invariant_violation(Some(QUANT_BACKTEST_PATH_SET), error.to_string())
            })?
            .bindings();
        let subject = &path_set.subject;
        let exact_subject = model.training_dataset_id == Some(path_set.training_dataset_id)
            && bindings.policy_snapshot.decision_policy_snapshot_id
                == path_set.decision_policy_snapshot_id
            && subject.model_artifact_hash == model.artifact_hash
            && subject.serving_contract_hash == model.serving_contract_hash
            && subject.training_dataset_hash == bindings.transform.training_dataset_hash
            && subject.dataset_manifest_hash == bindings.dataset.manifest_hash
            && subject.dataset_artifact_bytes_hash == bindings.dataset.artifact_bytes_hash
            && subject.policy_snapshot_hash == bindings.policy_snapshot.snapshot_hash;
        if !exact_subject {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_PATH_SET),
                "CPCV path-set subject differs from the immutable model serving graph",
            ));
        }
        let derivation = model.verified_derivation().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_BACKTEST_PATH_SET), error.to_string())
        })?;
        let fold_policy = &path_set.methodology.fold_calibration;
        let calibration = bindings.model.calibration.as_ref();
        let valid_fold_policy = match (model.model_family, derivation, calibration, fold_policy) {
            (
                ModelFamily::ClassicalGradientBoostedTrees,
                ModelVersionDerivation::Training,
                None,
                CpcvFoldCalibrationPolicy::NotApplicable,
            )
            | (
                ModelFamily::WeightedFactor,
                ModelVersionDerivation::Training,
                None,
                CpcvFoldCalibrationPolicy::SubjectHeuristic { .. },
            ) => true,
            (
                ModelFamily::WeightedFactor,
                ModelVersionDerivation::ReturnCalibration {
                    parent_model_version_id,
                    calibration_artifact_id,
                },
                Some(binding),
                CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                    calibration_artifact_id: fold_calibration_id,
                    calibration_hash,
                    parent_model_version_id: fold_parent_id,
                    parent_artifact_hash,
                    parent_serving_contract_hash,
                    ..
                },
            ) if calibration_artifact_id == binding.artifact_id
                && calibration_artifact_id == *fold_calibration_id
                && binding.content_hash == *calibration_hash
                && parent_model_version_id == *fold_parent_id =>
            {
                let parent = PgModelRegistryRepository::require_version_info(
                    transaction,
                    &parent_model_version_id,
                )
                .await?;
                parent.artifact_hash == *parent_artifact_hash
                    && parent.serving_contract_hash == *parent_serving_contract_hash
                    && parent
                        .verified_serving_contract()
                        .map_err(|error| {
                            StorageError::invariant_violation(
                                Some(QUANT_BACKTEST_PATH_SET),
                                error.to_string(),
                            )
                        })?
                        .bindings()
                        .model
                        .calibration
                        .is_none()
            }
            _ => false,
        };
        if !valid_fold_policy {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_PATH_SET),
                "CPCV fold-calibration policy differs from model family or derivation lineage",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BacktestPathSetRepository for PgBacktestPathSetRepository {
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
        Self::validate_subject(&transaction, &commit.path_set).await?;
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
        PathSetEntity::find()
            .filter(PathSetColumn::ModelVersionId.eq(*model_version_id))
            .order_by_desc(PathSetColumn::CreatedAt)
            .order_by_desc(PathSetColumn::PathSetId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .map(verify_path_set)
            .collect()
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
