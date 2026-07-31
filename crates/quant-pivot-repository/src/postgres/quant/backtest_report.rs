//! Postgres-backed backtest-report ledger repository (append-only).

use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_BACKTEST_REPORT, QUANT_MODEL_RUN},
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
    },
    enums::quant::{DatasetPurpose, ModelRunKind, ModelRunStatus},
    types::{BacktestReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{
        error,
        quant::integrity::{load_dataset, load_model_lineage, verify_replay_dataset},
        query::paginate_mapped,
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
        report.verify_hash().map_err(|detail| {
            StorageError::invariant_violation(Some(QUANT_BACKTEST_REPORT), detail)
        })?;
        if report.parquet_uri.is_some() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report parquet_uri is not part of the canonical report artifact",
            ));
        }
        let model = load_model_lineage(db, report.model_version_id).await?;
        let contract = model.version.serving_contract.bindings();
        if contract.policy_snapshot.decision_policy_snapshot_id
            != report.decision_policy_snapshot_id
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report policy differs from the source model serving contract",
            ));
        }
        let dataset = load_dataset(db, report.evaluation_dataset_id).await?;
        let materialization =
            verify_replay_dataset(db, &dataset, DatasetPurpose::Evaluation, &model).await?;
        let model_run = ModelRunEntity::find_by_id(report.model_run_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, report.model_run_id))?;

        let run_identity_matches = model_run.run_kind == ModelRunKind::Backtest
            && model_run.model_version_id == Some(report.model_version_id)
            && model_run.decision_policy_snapshot_id == report.decision_policy_snapshot_id
            && model_run.market_selection_id.is_none();
        let run_window_matches = model_run.window_start == report.window_start
            && model_run.window_end == report.window_end;
        let run_state_matches = model_run.status == ModelRunStatus::Running
            && model_run.input_hash == *materialization.dataset_hash
            && model_run.output_hash.is_none()
            && model_run.error_code.is_none()
            && model_run.error_message.is_none()
            && model_run.finished_at.is_none();
        if !(run_identity_matches && run_window_matches && run_state_matches) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report does not match its exact Running Backtest model run",
            ));
        }

        let dataset_matches = dataset.decision_policy_snapshot_id
            == report.decision_policy_snapshot_id
            && dataset.window_start == report.window_start
            && dataset.window_end == report.window_end;
        if !dataset_matches {
            return Err(StorageError::invariant_violation(
                Some(QUANT_BACKTEST_REPORT),
                "backtest report does not match the exact Evaluation Dataset policy/window",
            ));
        }

        let dataset_sample_count = materialization.sample_count;
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
        Box::pin(self.validate_lineage(&transaction, &report)).await?;
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
        Entity::find()
            .filter(Column::ModelVersionId.eq(*model_version_id))
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::BacktestReportId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
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
