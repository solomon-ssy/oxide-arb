//! Postgres-backed pairwise model-comparison report ledger repository (append-only).

use std::collections::{HashMap, HashSet};

use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_BACKTEST_REPORT, QUANT_MODEL_RUN},
};
use quant_pivot_models::{
    domain::{
        api::ComparisonReportListQuery,
        pagination::{PageWindow, Paginated},
        quant::{BacktestReportInfo, ModelComparisonReportInfo, NewModelComparisonReport},
    },
    entities::{
        quant_backtest_report::Entity as BacktestReportEntity,
        quant_model_comparison_report::{Column, Entity},
        quant_model_run::{Entity as ModelRunEntity, Model as ModelRunModel},
    },
    enums::quant::{DatasetPurpose, ModelRunKind, ModelRunStatus},
    types::{BacktestReportId, ContentHash, ModelComparisonReportId, ModelVersionId},
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{
        quant::integrity::{load_dataset, load_model_lineage, verify_replay_dataset},
        query::{list_fk_desc, paginate_mapped},
    },
    traits::ModelComparisonReportRepository,
};

/// Postgres-backed comparison-report ledger repository.
pub struct PgModelComparisonReportRepository {
    db: DatabaseConnection,
}

impl PgModelComparisonReportRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn validate_lineage<C>(
        &self,
        db: &C,
        report: &NewModelComparisonReport,
    ) -> Result<(), StorageError>
    where
        C: ConnectionTrait,
    {
        let baseline = BacktestReportEntity::find_by_id(report.baseline_report_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::not_found(QUANT_BACKTEST_REPORT, report.baseline_report_id)
            })?;
        let candidate = BacktestReportEntity::find_by_id(report.candidate_report_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::not_found(QUANT_BACKTEST_REPORT, report.candidate_report_id)
            })?;
        report
            .validate_against_reports(&baseline, &candidate)
            .map_err(|detail| {
                StorageError::invariant_violation(Some("quant_model_comparison_report"), detail)
            })?;
        if report.model_run_id != candidate.model_run_id {
            return Err(comparison_invariant(
                "comparison model_run_id must be the exact candidate report run",
            ));
        }

        let baseline_model = load_model_lineage(db, baseline.model_version_id).await?;
        let candidate_model = load_model_lineage(db, candidate.model_version_id).await?;
        let dataset = load_dataset(db, candidate.evaluation_dataset_id).await?;
        let candidate_materialization =
            verify_replay_dataset(db, &dataset, DatasetPurpose::Evaluation, &candidate_model)
                .await?;
        let baseline_materialization =
            verify_replay_dataset(db, &dataset, DatasetPurpose::Evaluation, &baseline_model)
                .await?;
        if candidate_materialization.dataset_hash != baseline_materialization.dataset_hash {
            return Err(comparison_invariant(
                "comparison models do not resolve the same Evaluation Dataset bytes",
            ));
        }

        let baseline_run = ModelRunEntity::find_by_id(baseline.model_run_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, baseline.model_run_id))?;
        let candidate_run = ModelRunEntity::find_by_id(candidate.model_run_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, candidate.model_run_id))?;
        Self::verify_succeeded_run(
            &baseline_run,
            &baseline,
            candidate_materialization.dataset_hash,
        )?;
        Self::verify_succeeded_run(
            &candidate_run,
            &candidate,
            candidate_materialization.dataset_hash,
        )
    }

    fn verify_succeeded_run(
        run: &ModelRunModel,
        report: &BacktestReportInfo,
        dataset_hash: &ContentHash,
    ) -> Result<(), StorageError> {
        if run.run_kind != ModelRunKind::Backtest
            || run.status != ModelRunStatus::Succeeded
            || run.model_version_id != Some(report.model_version_id)
            || run.decision_policy_snapshot_id != report.decision_policy_snapshot_id
            || run.market_selection_id.is_some()
            || run.window_start != report.window_start
            || run.window_end != report.window_end
            || run.input_hash != *dataset_hash
            || run.output_hash != Some(report.report_hash)
            || run.error_code.is_some()
            || run.error_message.is_some()
            || run.finished_at.is_none()
            || run
                .finished_at
                .is_some_and(|finished_at| finished_at < run.started_at)
        {
            return Err(comparison_invariant(
                "comparison report is not backed by an exact Succeeded Backtest run",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ModelComparisonReportRepository for PgModelComparisonReportRepository {
    async fn create(
        &self,
        report: NewModelComparisonReport,
    ) -> Result<ModelComparisonReportInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        Box::pin(self.validate_lineage(&transaction, &report)).await?;
        let inserted = Entity::insert(report.into_active_model())
            .exec_with_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }

    async fn find_by_id(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError> {
        Entity::find_by_id(*comparison_report_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_by_candidate_version(
        &self,
        candidate_model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelComparisonReportInfo>, StorageError> {
        list_fk_desc::<Entity, _, _, _>(
            &self.db,
            Column::CandidateModelVersionId,
            *candidate_model_version_id,
            Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn page(
        &self,
        query: ComparisonReportListQuery,
    ) -> Result<Paginated<ModelComparisonReportInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .candidate_model_version_id
                    .map(|id| Column::CandidateModelVersionId.eq(id)),
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

    async fn find_by_backtest_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> Result<Option<ModelComparisonReportInfo>, StorageError> {
        Entity::find()
            .filter(
                Condition::any()
                    .add(Column::CandidateReportId.eq(*backtest_report_id))
                    .add(Column::BaselineReportId.eq(*backtest_report_id)),
            )
            .order_by_desc(Column::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn backtest_comparison_ids(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> Result<HashMap<BacktestReportId, ModelComparisonReportId>, StorageError> {
        if backtest_report_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let requested = backtest_report_ids.iter().copied().collect::<HashSet<_>>();
        let rows = Entity::find()
            .filter(
                Condition::any()
                    .add(Column::CandidateReportId.is_in(backtest_report_ids.to_vec()))
                    .add(Column::BaselineReportId.is_in(backtest_report_ids.to_vec())),
            )
            .order_by_desc(Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut map = HashMap::new();
        for row in rows {
            let info = ModelComparisonReportInfo::from(row);
            if requested.contains(&info.candidate_report_id) {
                map.entry(info.candidate_report_id)
                    .or_insert(info.comparison_report_id);
            }
            if requested.contains(&info.baseline_report_id) {
                map.entry(info.baseline_report_id)
                    .or_insert(info.comparison_report_id);
            }
        }
        Ok(map)
    }
}

fn comparison_invariant(detail: impl Into<String>) -> StorageError {
    StorageError::invariant_violation(Some("quant_model_comparison_report"), detail.into())
}
