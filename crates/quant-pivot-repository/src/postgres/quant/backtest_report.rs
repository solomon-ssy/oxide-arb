//! Postgres-backed backtest-report ledger repository (append-only).

use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_BACKTEST_REPORT, QUANT_MODEL_RUN, QUANT_MODEL_VERSION, QUANT_TRAINING_DATASET},
};
use quant_pivot_models::{
    domain::{
        api::BacktestReportListQuery,
        pagination::{PageWindow, Paginated},
        quant::{BacktestReportInfo, NewBacktestReport},
    },
    entities::{
        quant_backtest_report::{Column, Entity},
        quant_model_run::Entity as ModelRunEntity,
        quant_model_version::Entity as ModelVersionEntity,
        quant_training_dataset::Entity as TrainingDatasetEntity,
    },
    enums::quant::{DatasetPurpose, ModelRunKind, ModelRunStatus, TrainingDatasetStatus},
    types::{BacktestReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{
        error,
        query::{list_fk_desc, paginate_mapped},
    },
    traits::BacktestReportRepository,
};

/// Postgres-backed backtest-report ledger repository.
pub struct PgBacktestReportRepository {
    db: DatabaseConnection,
}

impl PgBacktestReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn validate_lineage<C>(
        &self,
        db: &C,
        report: &NewBacktestReport,
    ) -> Result<(), StorageError>
    where
        C: ConnectionTrait,
    {
        let model_version = ModelVersionEntity::find_by_id(report.model_version_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_VERSION, report.model_version_id))?;
        let dataset = TrainingDatasetEntity::find_by_id(report.evaluation_dataset_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_TRAINING_DATASET, report.evaluation_dataset_id)
            })?;
        let model_run = ModelRunEntity::find_by_id(report.model_run_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, report.model_run_id))?;

        let run_matches = model_run.run_kind == ModelRunKind::Backtest
            && model_run.status == ModelRunStatus::Running
            && model_run.model_version_id == Some(report.model_version_id)
            && model_run.decision_policy_snapshot_id == report.decision_policy_snapshot_id
            && model_run.window_start == report.window_start
            && model_run.window_end == report.window_end;
        if !run_matches {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report does not match its Running Backtest model run",
            ));
        }

        let dataset_matches = dataset.status == TrainingDatasetStatus::Ready
            && dataset.purpose == DatasetPurpose::Evaluation
            && dataset.model_spec_id == model_version.model_spec_id
            && dataset.research_profile_artifact_id == model_version.research_profile_artifact_id
            && dataset.decision_policy_snapshot_id == report.decision_policy_snapshot_id
            && dataset.window_start == report.window_start
            && dataset.window_end == report.window_end
            && dataset.manifest.is_some()
            && dataset.manifest_hash.is_some()
            && dataset.artifact_bytes_hash.is_some()
            && dataset.parquet_uri.is_some()
            && dataset.coverage.is_some();
        if !dataset_matches {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report does not match a Ready Evaluation dataset with exact model, profile, policy, window, and artifact lineage",
            ));
        }

        let dataset_sample_count = dataset.sample_count.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "Ready Evaluation dataset has no sealed sample count",
            )
        })?;
        if report.sample_count > dataset_sample_count {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                format!(
                    "backtest report sample count {} exceeds Evaluation dataset count {dataset_sample_count}",
                    report.sample_count
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BacktestReportRepository for PgBacktestReportRepository {
    async fn create(&self, report: NewBacktestReport) -> Result<BacktestReportInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        self.validate_lineage(&transaction, &report).await?;
        let duplicate_key = report.backtest_report_id.to_string();
        let inserted = Entity::insert(report.into_active_model())
            .exec_with_returning(&transaction)
            .await
            .map_err(|source| {
                error::map_unique(source, QUANT_BACKTEST_REPORT, duplicate_key.as_str())
            })?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }

    async fn find_by_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<BacktestReportInfo>, StorageError> {
        Entity::find_by_id(*backtest_report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestReportInfo>, StorageError> {
        list_fk_desc::<Entity, _, _, _>(
            &self.db,
            Column::ModelVersionId,
            *model_version_id,
            Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: BacktestReportListQuery,
    ) -> Result<Paginated<BacktestReportInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_version_id
                    .map(|id| Column::ModelVersionId.eq(id)),
            )
            .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
            .add_option(query.to.map(|to| Column::CreatedAt.lt(to)));
        paginate_mapped(
            Entity::find()
                .filter(condition)
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
