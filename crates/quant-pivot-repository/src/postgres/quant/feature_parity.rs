//! Postgres feature-parity run lifecycle and append-only latch ledger.

use std::collections::BTreeSet;

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityRunListQuery,
        FeatureParityStateInfo, NewFeatureParityRun, NewFeatureParityState, NewResearchJob,
        PageWindow, Paginated, ResearchJobInfo,
    },
    entities::{quant_feature_parity_run, quant_feature_parity_state, quant_research_job},
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
        FeatureParityStateTransition, ResearchJobKind,
    },
    schema::column,
    types::{
        FeatureParityRunId, FeatureParityStateId, ModelVersionId, RecommendationReportId,
        TrainingDatasetId,
    },
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, Statement,
    TransactionTrait, sea_query::Expr,
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::{FeatureParityLatchActor, FeatureParityRepository},
};

pub(super) const LATCH_ADVISORY_LOCK_KEY: i64 = 0x_11_06_50_41;

/// Postgres-backed parity run and latch repository.
pub struct PgFeatureParityRepository {
    db: DatabaseConnection,
}

impl PgFeatureParityRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FeatureParityRepository for PgFeatureParityRepository {
    async fn create_run(
        &self,
        run: NewFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        validate_new_run(&run)?;
        let run_id = run.run_id.to_string();
        quant_feature_parity_run::Entity::insert(run.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| error::map_unique(error, entity::QUANT_FEATURE_PARITY_RUN, &run_id))
            .map(Into::into)
    }

    async fn enqueue_run(
        &self,
        run: NewFeatureParityRun,
        job: NewResearchJob,
    ) -> Result<(FeatureParityRunInfo, ResearchJobInfo), StorageError> {
        validate_new_run(&run)?;
        if job.kind != ResearchJobKind::FeatureParity {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_JOB),
                "parity run must be paired with a feature_parity research job",
            ));
        }
        let expected_run_id = run.run_id.to_string();
        if job
            .params_json
            .get("parity_run_id")
            .and_then(serde_json::Value::as_str)
            != Some(expected_run_id.as_str())
        {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_JOB),
                "feature_parity job params must reference the same parity_run_id",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let run_model = quant_feature_parity_run::Entity::insert(run.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|error| {
                error::map_unique(error, entity::QUANT_FEATURE_PARITY_RUN, &expected_run_id)
            })?;
        let job_model = quant_research_job::Entity::insert(job.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((run_model.into(), job_model.into()))
    }

    async fn find_run(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        find_run_on(&self.db, run_id).await
    }

    async fn page_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> Result<Paginated<FeatureParityRunInfo>, StorageError> {
        let mut condition = Condition::all();
        if let Some(kind) = query.kind {
            condition = condition.add(quant_feature_parity_run::Column::Kind.eq(kind));
        }
        if let Some(status) = query.status {
            condition = condition.add(quant_feature_parity_run::Column::Status.eq(status));
        }
        if let Some(report_id) = query.report_id.as_ref() {
            condition =
                condition.add(quant_feature_parity_run::Column::ReportId.eq(report_id.clone()));
        }
        if let Some(model_version_id) = query.model_version_id.as_ref() {
            condition = condition
                .add(quant_feature_parity_run::Column::ModelVersionId.eq(model_version_id.clone()));
        }
        if let Some(training_dataset_id) = query.training_dataset_id.as_ref() {
            condition = condition.add(
                quant_feature_parity_run::Column::TrainingDatasetId.eq(training_dataset_id.clone()),
            );
        }
        if let Some(from) = query.from {
            condition = condition.add(quant_feature_parity_run::Column::CreatedAt.gte(from));
        }
        if let Some(to) = query.to {
            condition = condition.add(quant_feature_parity_run::Column::CreatedAt.lt(to));
        }
        paginate_mapped(
            quant_feature_parity_run::Entity::find()
                .filter(condition)
                .order_by_desc(quant_feature_parity_run::Column::CreatedAt)
                .order_by_desc(quant_feature_parity_run::Column::RunId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn latest_run(
        &self,
        kind: FeatureParityRunKind,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        quant_feature_parity_run::Entity::find()
            .filter(quant_feature_parity_run::Column::Kind.eq(kind))
            .order_by_desc(quant_feature_parity_run::Column::CreatedAt)
            .order_by_desc(quant_feature_parity_run::Column::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_unbound_full(&self) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        quant_feature_parity_run::Entity::find()
            .filter(quant_feature_parity_run::Column::Kind.eq(FeatureParityRunKind::Full))
            .filter(quant_feature_parity_run::Column::ReportId.is_null())
            .filter(quant_feature_parity_run::Column::ModelVersionId.is_null())
            .filter(quant_feature_parity_run::Column::TrainingDatasetId.is_null())
            .order_by_desc(quant_feature_parity_run::Column::CreatedAt)
            .order_by_desc(quant_feature_parity_run::Column::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_full_window(
        &self,
        window_start: chrono::DateTime<chrono::Utc>,
        window_end: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        quant_feature_parity_run::Entity::find()
            .filter(quant_feature_parity_run::Column::Kind.eq(FeatureParityRunKind::Full))
            .filter(quant_feature_parity_run::Column::WindowStart.eq(window_start))
            .filter(quant_feature_parity_run::Column::WindowEnd.eq(window_end))
            .filter(quant_feature_parity_run::Column::ReportId.is_null())
            .filter(quant_feature_parity_run::Column::ModelVersionId.is_null())
            .filter(quant_feature_parity_run::Column::TrainingDatasetId.is_null())
            .filter(quant_feature_parity_run::Column::Status.is_in([
                FeatureParityRunStatus::Queued,
                FeatureParityRunStatus::Running,
                FeatureParityRunStatus::PendingMaterialization,
            ]))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_sampled_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        quant_feature_parity_run::Entity::find()
            .filter(quant_feature_parity_run::Column::Kind.eq(FeatureParityRunKind::Sampled))
            .filter(quant_feature_parity_run::Column::ReportId.eq(report_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_full_for_model(
        &self,
        model_version_id: &ModelVersionId,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
        quant_feature_parity_run::Entity::find()
            .filter(quant_feature_parity_run::Column::Kind.eq(FeatureParityRunKind::Full))
            .filter(quant_feature_parity_run::Column::ModelVersionId.eq(model_version_id.clone()))
            .filter(
                quant_feature_parity_run::Column::TrainingDatasetId.eq(training_dataset_id.clone()),
            )
            .order_by_desc(quant_feature_parity_run::Column::CreatedAt)
            .order_by_desc(quant_feature_parity_run::Column::RunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn mark_running(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        let result = quant_feature_parity_run::Entity::update_many()
            .col_expr(
                quant_feature_parity_run::Column::Status,
                column::pg_enum_value(&FeatureParityRunStatus::Running),
            )
            .col_expr(
                quant_feature_parity_run::Column::StartedAt,
                Expr::cust("COALESCE(started_at, NOW())"),
            )
            .filter(quant_feature_parity_run::Column::RunId.eq(run_id.clone()))
            .filter(
                Condition::any()
                    .add(
                        quant_feature_parity_run::Column::Status.eq(FeatureParityRunStatus::Queued),
                    )
                    .add(
                        quant_feature_parity_run::Column::Status
                            .eq(FeatureParityRunStatus::PendingMaterialization),
                    ),
            )
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            return Err(run_transition_conflict(&self.db, run_id, "running").await);
        }
        require_run_on(&self.db, run_id).await
    }

    async fn complete_run(
        &self,
        run_id: &FeatureParityRunId,
        result: CompleteFeatureParityRun,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        validate_completion(&result)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let finished_at = result.status.is_terminal().then(Utc::now);
        let mut statement = quant_feature_parity_run::Entity::update_many()
            .col_expr(
                quant_feature_parity_run::Column::Status,
                column::pg_enum_value(&result.status),
            )
            .col_expr(
                quant_feature_parity_run::Column::TotalCount,
                Expr::value(result.total_count),
            )
            .col_expr(
                quant_feature_parity_run::Column::ComparedCount,
                Expr::value(result.compared_count),
            )
            .col_expr(
                quant_feature_parity_run::Column::MatchedCount,
                Expr::value(result.matched_count),
            )
            .col_expr(
                quant_feature_parity_run::Column::MismatchedCount,
                Expr::value(result.mismatched_count),
            )
            .col_expr(
                quant_feature_parity_run::Column::PendingMaterializationCount,
                Expr::value(result.pending_materialization_count),
            )
            .col_expr(
                quant_feature_parity_run::Column::FeatureContractHash,
                Expr::value(result.feature_contract_hash.clone()),
            )
            .col_expr(
                quant_feature_parity_run::Column::TransformHash,
                Expr::value(result.transform_hash.clone()),
            )
            .col_expr(
                quant_feature_parity_run::Column::FailureCode,
                Expr::value(result.failure_code.clone()),
            )
            .col_expr(
                quant_feature_parity_run::Column::FailureDetail,
                Expr::value(result.failure_detail.clone()),
            )
            .col_expr(
                quant_feature_parity_run::Column::FinishedAt,
                Expr::value(finished_at),
            );
        if result.status == FeatureParityRunStatus::PendingMaterialization {
            statement = statement.col_expr(
                quant_feature_parity_run::Column::PendingSince,
                Expr::cust("COALESCE(pending_since, NOW())"),
            );
        }
        let update = statement
            .filter(quant_feature_parity_run::Column::RunId.eq(run_id.clone()))
            .filter(quant_feature_parity_run::Column::Status.eq(FeatureParityRunStatus::Running))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if update.rows_affected == 0 {
            let error = run_transition_conflict(&txn, run_id, result.status.as_str()).await;
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(error);
        }
        if result.status == FeatureParityRunStatus::Mismatched {
            acquire_latch_lock(&txn).await?;
            append_open_state(
                &txn,
                run_id,
                FeatureParityStateTransition::DeterministicMismatch,
                "deterministic online/replay mismatch",
            )
            .await?;
        }
        let completed = require_run_on(&txn, run_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(completed)
    }

    async fn mark_containment_complete(
        &self,
        run_id: &FeatureParityRunId,
    ) -> Result<FeatureParityRunInfo, StorageError> {
        let result = quant_feature_parity_run::Entity::update_many()
            .col_expr(
                quant_feature_parity_run::Column::ContainmentCompletedAt,
                Expr::cust("COALESCE(containment_completed_at, NOW())"),
            )
            .filter(quant_feature_parity_run::Column::RunId.eq(run_id.clone()))
            .filter(quant_feature_parity_run::Column::Status.is_in([
                FeatureParityRunStatus::Mismatched,
                FeatureParityRunStatus::Failed,
            ]))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            let current = require_run_on(&self.db, run_id).await?;
            if current.containment_completed_at.is_some()
                && matches!(
                    current.status,
                    FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed
                )
            {
                return Ok(current);
            }
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                format!(
                    "containment completion requires mismatched/failed status, found {}",
                    current.status.as_str()
                ),
            ));
        }
        require_run_on(&self.db, run_id).await
    }

    async fn current_state(&self) -> Result<Option<FeatureParityStateInfo>, StorageError> {
        current_state_on(&self.db).await
    }

    async fn open_latch(
        &self,
        cause_run_id: &FeatureParityRunId,
        transition: FeatureParityStateTransition,
        reason: String,
    ) -> Result<FeatureParityStateInfo, StorageError> {
        if transition == FeatureParityStateTransition::GovernedAcknowledge {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_FEATURE_PARITY_STATE),
                "opening the latch cannot use governed_acknowledge",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_latch_lock(&txn).await?;
        let cause = require_run_on(&txn, cause_run_id).await?;
        let expected = match transition {
            FeatureParityStateTransition::DeterministicMismatch => {
                FeatureParityRunStatus::Mismatched
            }
            FeatureParityStateTransition::IntegrityFailure => FeatureParityRunStatus::Failed,
            FeatureParityStateTransition::GovernedAcknowledge => unreachable!(),
        };
        if cause.status != expected {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(cause_run_id),
                format!(
                    "latch transition {} requires run status {}, found {}",
                    transition.as_str(),
                    expected.as_str(),
                    cause.status.as_str()
                ),
            ));
        }
        let state = append_open_state(&txn, cause_run_id, transition, &reason).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(state)
    }

    async fn record_integrity_failure_and_open_latch(
        &self,
        source_run_id: &FeatureParityRunId,
        reason: String,
    ) -> Result<(FeatureParityRunInfo, FeatureParityStateInfo), StorageError> {
        if reason.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_FEATURE_PARITY_STATE),
                "governance integrity failure reason must not be empty",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_latch_lock(&txn).await?;
        let source = require_run_on(&txn, source_run_id).await?;
        if source.kind != FeatureParityRunKind::Full
            || source.status != FeatureParityRunStatus::Passed
        {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(source_run_id),
                "governance integrity incident must derive from the passed full permit used by the switch",
            ));
        }
        let now = Utc::now();
        let incident_id = FeatureParityRunId::from_v7();
        let incident = NewFeatureParityRun {
            run_id: incident_id.clone(),
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Failed,
            window_start: source.window_start,
            window_end: source.window_end,
            report_id: source.report_id,
            model_version_id: source.model_version_id,
            training_dataset_id: source.training_dataset_id,
            triggered_by: "system:model_governance".to_owned(),
            requested_by: None,
            acting_role: "system".to_owned(),
            reason: reason.clone(),
            total_count: 0,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: source.feature_contract_hash,
            transform_hash: source.transform_hash,
            failure_code: Some("rollback_pointer_recovery_failed".to_owned()),
            failure_detail: Some(reason.clone()),
            started_at: Some(now),
            pending_since: None,
            containment_completed_at: Some(now),
            finished_at: Some(now),
        };
        let incident: FeatureParityRunInfo =
            quant_feature_parity_run::Entity::insert(incident.into_active_model())
                .exec_with_returning(&txn)
                .await
                .map_err(StorageError::from)?
                .into();
        let state = append_open_state(
            &txn,
            &incident_id,
            FeatureParityStateTransition::IntegrityFailure,
            &reason,
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((incident, state))
    }

    async fn acknowledge_latch(
        &self,
        recovery_run_id: &FeatureParityRunId,
        actor: FeatureParityLatchActor,
    ) -> Result<FeatureParityStateInfo, StorageError> {
        if actor.reason.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_FEATURE_PARITY_STATE),
                "acknowledgement reason must not be empty",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_latch_lock(&txn).await?;
        let recovery = require_run_on(&txn, recovery_run_id).await?;
        validate_recovery_run(&recovery)?;
        let current = current_state_on(&txn).await?;
        if let Some(current) = current.as_ref() {
            if current.state == FeatureParityLatchState::Clear {
                validate_clear_acknowledgement(current, recovery_run_id)?;
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(current.clone());
            }
            let incident_states = open_incident_states_on(&txn, current).await?;
            let mut causes = Vec::with_capacity(incident_states.len());
            for incident_state in incident_states {
                let cause_run_id = incident_state.cause_run_id.as_ref().ok_or_else(|| {
                    StorageError::state_conflict(
                        entity::QUANT_FEATURE_PARITY_STATE,
                        Some(&incident_state.state_id),
                        "open parity incident has no causal run",
                    )
                })?;
                let cause = require_run_on(&txn, cause_run_id).await?;
                causes.push((incident_state, cause));
            }
            validate_open_latch_recoveries(&causes, &recovery)?;
        } else {
            validate_bootstrap_recovery(&recovery)?;
        }
        let next = NewFeatureParityState {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: current.as_ref().and_then(|row| row.cause_run_id.clone()),
            recovery_run_id: Some(recovery_run_id.clone()),
            previous_state_id: current.as_ref().map(|row| row.state_id.clone()),
            actor: actor.actor,
            acting_role: Some(actor.acting_role),
            reason: actor.reason,
        };
        let inserted = quant_feature_parity_state::Entity::insert(next.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted.into())
    }
}

fn validate_new_run(run: &NewFeatureParityRun) -> Result<(), StorageError> {
    if run.status != FeatureParityRunStatus::Queued {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "new parity run must start queued",
        ));
    }
    if run.window_end <= run.window_start {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "window_end must be later than window_start",
        ));
    }
    if run.reason.trim().is_empty()
        || run.acting_role.trim().is_empty()
        || run.triggered_by.trim().is_empty()
        || run.feature_contract_hash.is_none()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "reason, acting_role, triggered_by, and feature_contract_hash are required",
        ));
    }
    if run.total_count != 0
        || run.compared_count != 0
        || run.matched_count != 0
        || run.mismatched_count != 0
        || run.pending_materialization_count != 0
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "queued parity run counters must start at zero",
        ));
    }
    if run.started_at.is_some()
        || run.pending_since.is_some()
        || run.containment_completed_at.is_some()
        || run.finished_at.is_some()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "queued parity run cannot pre-populate execution timestamps",
        ));
    }
    Ok(())
}

fn validate_completion(result: &CompleteFeatureParityRun) -> Result<(), StorageError> {
    if result.total_count < 0
        || result.compared_count < 0
        || result.matched_count < 0
        || result.mismatched_count < 0
        || result.pending_materialization_count < 0
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "parity counters must be non-negative",
        ));
    }
    if result.compared_count != result.matched_count + result.mismatched_count
        || result.total_count != result.compared_count + result.pending_materialization_count
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "compared_count must equal matched_count + mismatched_count and total_count must equal compared_count + pending_materialization_count",
        ));
    }
    if result.feature_contract_hash.is_none() {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "feature_contract_hash is required for every parity result",
        ));
    }
    match result.status {
        FeatureParityRunStatus::Passed
            if result.total_count > 0
                && result.matched_count == result.total_count
                && result.mismatched_count == 0
                && result.pending_materialization_count == 0
                && result.transform_hash.is_some()
                && result.failure_code.is_none()
                && result.failure_detail.is_none() =>
        {
            Ok(())
        }
        FeatureParityRunStatus::Mismatched if result.mismatched_count > 0 => Ok(()),
        FeatureParityRunStatus::PendingMaterialization
            if result.pending_materialization_count > 0 =>
        {
            Ok(())
        }
        FeatureParityRunStatus::Failed
            if result
                .failure_code
                .as_deref()
                .is_some_and(|code| !code.is_empty())
                && result
                    .failure_detail
                    .as_deref()
                    .is_some_and(|detail| !detail.is_empty()) =>
        {
            Ok(())
        }
        _ => Err(StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            "completion counters/details do not satisfy the target status",
        )),
    }
}

fn validate_recovery_run(run: &FeatureParityRunInfo) -> Result<(), StorageError> {
    if run.kind != FeatureParityRunKind::Full
        || run.status != FeatureParityRunStatus::Passed
        || run.total_count <= 0
        || run.compared_count != run.total_count
        || run.matched_count != run.total_count
        || run.mismatched_count != 0
        || run.pending_materialization_count != 0
        || run.feature_contract_hash.is_none()
        || run.transform_hash.is_none()
        || run.finished_at.is_none()
    {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "latch recovery requires a finished, non-empty full pass with feature/transform commitments and zero mismatch/pending rows",
        ));
    }
    Ok(())
}

/// The guarded bootstrap state has no causal serving incident. Its first clear
/// transition therefore accepts only a subject-bound proof over one immutable
/// model and its exact frozen dataset; an unbound runtime replay cannot
/// initialize production admission. The run must also have completed strictly
/// after its own durable initialization, so a pre-existing/backfilled terminal
/// row cannot be used as a bootstrap permit.
fn validate_bootstrap_recovery(run: &FeatureParityRunInfo) -> Result<(), StorageError> {
    if run.report_id.is_some()
        || run.model_version_id.is_none()
        || run.training_dataset_id.is_none()
    {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "uninitialized latch recovery requires a frozen model+dataset-bound full proof",
        ));
    }
    let finished_at = run.finished_at.ok_or_else(|| {
        StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "bootstrap recovery run has no completion timestamp",
        )
    })?;
    if finished_at <= run.created_at {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&run.run_id),
            "bootstrap recovery run must complete after it was initialized",
        ));
    }
    Ok(())
}

/// Acknowledge is idempotent only for the exact recovery proof that minted the
/// current clear generation. The advisory latch lock serializes concurrent
/// cold-start requests; a waiter presenting a different run observes the first
/// clear row and is rejected instead of being reported as accepted.
fn validate_clear_acknowledgement(
    current: &FeatureParityStateInfo,
    recovery_run_id: &FeatureParityRunId,
) -> Result<(), StorageError> {
    if current.recovery_run_id.as_ref() == Some(recovery_run_id) {
        return Ok(());
    }
    Err(StorageError::state_conflict(
        entity::QUANT_FEATURE_PARITY_STATE,
        Some(&current.state_id),
        format!(
            "parity latch is already clear from recovery run {}; acknowledgement with different run {recovery_run_id} is not idempotent",
            current
                .recovery_run_id
                .as_ref()
                .map_or_else(|| "<missing>".to_owned(), ToString::to_string)
        ),
    ))
}

#[cfg(test)]
fn validate_open_latch_recovery(
    state: &FeatureParityStateInfo,
    cause: &FeatureParityRunInfo,
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    validate_open_latch_recoveries(&[(state.clone(), cause.clone())], recovery)
}

/// Validate one governed proof against the complete unresolved incident set.
///
/// Every open transition since the latest clear generation remains an
/// independent deterministic cause. A later incident never replaces an older
/// one: the recovery proof must cover their window union, satisfy every subject
/// scope, and complete after the most recent incident was durably opened.
fn validate_open_latch_recoveries(
    incidents: &[(FeatureParityStateInfo, FeatureParityRunInfo)],
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    let Some((first_state, first_cause)) = incidents.first() else {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_STATE,
            Option::<&str>::None,
            "open parity latch has no unresolved deterministic causes",
        ));
    };
    let mut window_start = first_cause.window_start;
    let mut window_end = first_cause.window_end;
    let mut latest_opened_at = first_state.created_at;
    let mut cause_ids = Vec::with_capacity(incidents.len());

    for (state, cause) in incidents {
        if !matches!(
            cause.status,
            FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed
        ) {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                format!(
                    "open latch causal run must be mismatched/failed, found {}",
                    cause.status.as_str()
                ),
            ));
        }
        if cause.containment_completed_at.is_none() {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                "causal parity run has not completed report/intent containment",
            ));
        }
        validate_recovery_scope(cause, recovery)?;
        window_start = window_start.min(cause.window_start);
        window_end = window_end.max(cause.window_end);
        latest_opened_at = latest_opened_at.max(state.created_at);
        cause_ids.push(cause.run_id.to_string());
    }

    if recovery.window_start > window_start || recovery.window_end < window_end {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&recovery.run_id),
            format!(
                "recovery window [{}, {}) must cover unresolved cause union [{}, {}) for runs [{}]",
                recovery.window_start,
                recovery.window_end,
                window_start,
                window_end,
                cause_ids.join(", ")
            ),
        ));
    }
    let finished_at = recovery.finished_at.ok_or_else(|| {
        StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(&recovery.run_id),
            "passed full parity run has no completion timestamp",
        )
    })?;
    if finished_at <= latest_opened_at {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_STATE,
            Some(&first_state.state_id),
            "recovery full run must complete after every unresolved incident was opened",
        ));
    }
    Ok(())
}

fn validate_recovery_scope(
    cause: &FeatureParityRunInfo,
    recovery: &FeatureParityRunInfo,
) -> Result<(), StorageError> {
    match (
        cause.report_id.as_ref(),
        cause.model_version_id.as_ref(),
        cause.training_dataset_id.as_ref(),
    ) {
        // Serving runtime incidents (report-bound sampled or unbound scheduled
        // full) may only be cleared by an unbound runtime full replay. An
        // offline frozen-artifact proof is a different evidence population.
        (Some(_), Some(_), None) | (None, None, None) => {
            if recovery.report_id.is_some()
                || recovery.model_version_id.is_some()
                || recovery.training_dataset_id.is_some()
            {
                return Err(StorageError::state_conflict(
                    entity::QUANT_FEATURE_PARITY_RUN,
                    Some(&recovery.run_id),
                    "serving parity latch recovery requires an unbound runtime full run",
                ));
            }
        }
        // Offline model/dataset integrity incidents must be recovered against
        // that exact immutable subject, never by unrelated live traffic.
        (None, Some(model_version_id), Some(training_dataset_id)) => {
            if recovery.report_id.is_some()
                || recovery.model_version_id.as_ref() != Some(model_version_id)
                || recovery.training_dataset_id.as_ref() != Some(training_dataset_id)
            {
                return Err(StorageError::state_conflict(
                    entity::QUANT_FEATURE_PARITY_RUN,
                    Some(&recovery.run_id),
                    format!(
                        "offline parity recovery must bind exact model {model_version_id} and training dataset {training_dataset_id}"
                    ),
                ));
            }
        }
        _ => {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(&cause.run_id),
                "causal parity run has an invalid report/model/dataset scope",
            ));
        }
    }
    Ok(())
}

async fn acquire_latch_lock(txn: &DatabaseTransaction) -> Result<(), StorageError> {
    txn.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT pg_advisory_xact_lock($1)",
        [LATCH_ADVISORY_LOCK_KEY.into()],
    ))
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

pub(super) async fn verify_clear_latch_generation(
    txn: &DatabaseTransaction,
    expected_state_id: &FeatureParityStateId,
) -> Result<(), StorageError> {
    acquire_latch_lock(txn).await?;
    let current = current_state_on(txn).await?.ok_or_else(|| {
        StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_STATE,
            Option::<&str>::None,
            "feature parity latch is uninitialized at risk-increasing commit",
        )
    })?;
    if current.state != FeatureParityLatchState::Clear || &current.state_id != expected_state_id {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_STATE,
            Some(&current.state_id),
            format!(
                "parity commit permit {} is stale; current latch state is {} generation {}",
                expected_state_id,
                current.state.as_str(),
                current.state_id
            ),
        ));
    }
    Ok(())
}

async fn append_open_state(
    txn: &DatabaseTransaction,
    cause_run_id: &FeatureParityRunId,
    transition: FeatureParityStateTransition,
    reason: &str,
) -> Result<FeatureParityStateInfo, StorageError> {
    let current = current_state_on(txn).await?;
    if let Some(current) = current.as_ref()
        && current.state == FeatureParityLatchState::Open
        && current.cause_run_id.as_ref() == Some(cause_run_id)
    {
        return Ok(current.clone());
    }
    let next = NewFeatureParityState {
        state_id: FeatureParityStateId::from_v7(),
        state: FeatureParityLatchState::Open,
        transition,
        cause_run_id: Some(cause_run_id.clone()),
        recovery_run_id: None,
        previous_state_id: current.as_ref().map(|row| row.state_id.clone()),
        actor: None,
        acting_role: None,
        reason: reason.to_owned(),
    };
    quant_feature_parity_state::Entity::insert(next.into_active_model())
        .exec_with_returning(txn)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

/// Walk the append-only `previous_state_id` chain for the current open
/// generation and return each unique deterministic cause. Traversing the
/// explicit chain avoids timestamp ties and guarantees that a later incident
/// cannot hide an earlier, still-unresolved cause.
async fn open_incident_states_on<C>(
    db: &C,
    current: &FeatureParityStateInfo,
) -> Result<Vec<FeatureParityStateInfo>, StorageError>
where
    C: ConnectionTrait,
{
    let mut cursor = Some(current.clone());
    let mut seen_state_ids = BTreeSet::new();
    let mut seen_cause_ids = BTreeSet::new();
    let mut incidents = Vec::new();

    while let Some(state) = cursor {
        if !seen_state_ids.insert(state.state_id.to_string()) {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                "feature parity state ledger contains a previous_state_id cycle",
            ));
        }
        if state.state == FeatureParityLatchState::Clear {
            break;
        }
        if !matches!(
            state.transition,
            FeatureParityStateTransition::DeterministicMismatch
                | FeatureParityStateTransition::IntegrityFailure
        ) {
            return Err(StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                format!(
                    "open parity generation contains non-incident transition {}",
                    state.transition.as_str()
                ),
            ));
        }
        let cause_run_id = state.cause_run_id.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_STATE,
                Some(&state.state_id),
                "open parity incident has no causal run",
            )
        })?;
        if seen_cause_ids.insert(cause_run_id.to_string()) {
            incidents.push(state.clone());
        }

        cursor = match state.previous_state_id {
            Some(previous_state_id) => Some(
                quant_feature_parity_state::Entity::find_by_id(previous_state_id.clone())
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .map(Into::into)
                    .ok_or_else(|| {
                        StorageError::state_conflict(
                            entity::QUANT_FEATURE_PARITY_STATE,
                            Some(&state.state_id),
                            format!(
                                "previous parity state {previous_state_id} is missing from the append-only ledger"
                            ),
                        )
                    })?,
            ),
            None => None,
        };
    }

    if incidents.is_empty() {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_STATE,
            Some(&current.state_id),
            "open parity latch has no unresolved deterministic causes",
        ));
    }
    Ok(incidents)
}

async fn current_state_on<C>(db: &C) -> Result<Option<FeatureParityStateInfo>, StorageError>
where
    C: ConnectionTrait,
{
    quant_feature_parity_state::Entity::find()
        .order_by_desc(quant_feature_parity_state::Column::CreatedAt)
        .order_by_desc(quant_feature_parity_state::Column::StateId)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn find_run_on<C>(
    db: &C,
    run_id: &FeatureParityRunId,
) -> Result<Option<FeatureParityRunInfo>, StorageError>
where
    C: ConnectionTrait,
{
    quant_feature_parity_run::Entity::find_by_id(run_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn require_run_on<C>(
    db: &C,
    run_id: &FeatureParityRunId,
) -> Result<FeatureParityRunInfo, StorageError>
where
    C: ConnectionTrait,
{
    find_run_on(db, run_id)
        .await?
        .ok_or_else(|| StorageError::not_found(entity::QUANT_FEATURE_PARITY_RUN, run_id))
}

async fn run_transition_conflict<C>(
    db: &C,
    run_id: &FeatureParityRunId,
    target: &str,
) -> StorageError
where
    C: ConnectionTrait,
{
    match find_run_on(db, run_id).await {
        Ok(Some(run)) => StorageError::illegal_transition(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(run_id),
            run.status.as_str(),
            target,
        ),
        Ok(None) => StorageError::not_found(entity::QUANT_FEATURE_PARITY_RUN, run_id),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};
    use quant_pivot_models::types::{
        ContentHash, ModelVersionId, RecommendationReportId, TrainingDatasetId,
    };

    use super::*;

    fn hash() -> ContentHash {
        ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("content hash")
    }

    fn parity_run(
        kind: FeatureParityRunKind,
        status: FeatureParityRunStatus,
        window_start: chrono::DateTime<chrono::Utc>,
        window_end: chrono::DateTime<chrono::Utc>,
    ) -> FeatureParityRunInfo {
        let now = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        FeatureParityRunInfo {
            run_id: FeatureParityRunId::from_v7(),
            kind,
            status,
            window_start,
            window_end,
            report_id: None,
            model_version_id: None,
            training_dataset_id: None,
            triggered_by: "test".to_owned(),
            requested_by: Some("risk-owner".to_owned()),
            acting_role: "risk_owner".to_owned(),
            reason: "test".to_owned(),
            total_count: 2,
            compared_count: 2,
            matched_count: i64::from(status == FeatureParityRunStatus::Passed) * 2,
            mismatched_count: i64::from(status == FeatureParityRunStatus::Mismatched),
            pending_materialization_count: 0,
            feature_contract_hash: Some(hash()),
            transform_hash: (status == FeatureParityRunStatus::Passed).then(hash),
            failure_code: (status == FeatureParityRunStatus::Failed)
                .then(|| "integrity_failure".to_owned()),
            failure_detail: (status == FeatureParityRunStatus::Failed).then(|| "failed".to_owned()),
            started_at: Some(now - Duration::minutes(1)),
            pending_since: None,
            containment_completed_at: None,
            finished_at: status.is_terminal().then_some(now),
            created_at: now - Duration::minutes(2),
            updated_at: now,
        }
    }

    fn open_state(
        cause_run_id: &FeatureParityRunId,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> FeatureParityStateInfo {
        FeatureParityStateInfo {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Open,
            transition: FeatureParityStateTransition::DeterministicMismatch,
            cause_run_id: Some(cause_run_id.clone()),
            recovery_run_id: None,
            previous_state_id: None,
            actor: None,
            acting_role: None,
            reason: "deterministic mismatch".to_owned(),
            created_at,
        }
    }

    fn clear_state(recovery_run_id: &FeatureParityRunId) -> FeatureParityStateInfo {
        FeatureParityStateInfo {
            state_id: FeatureParityStateId::from_v7(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: Some(recovery_run_id.clone()),
            previous_state_id: None,
            actor: Some("risk-owner".to_owned()),
            acting_role: Some("risk_owner".to_owned()),
            reason: "bootstrap parity verified".to_owned(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 11, 12, 1, 0).unwrap(),
        }
    }

    #[test]
    fn recovery_requires_completed_containment_full_window_coverage_and_later_finish() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut cause = parity_run(
            FeatureParityRunKind::Sampled,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(1),
            base,
        );
        cause.report_id = Some(RecommendationReportId::from_v7());
        cause.model_version_id = Some(ModelVersionId::from_v7());
        let state = open_state(&cause.run_id, base + Duration::minutes(1));
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(2),
            base + Duration::minutes(2),
        );
        recovery.finished_at = Some(base + Duration::minutes(3));

        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("containment is mandatory");
        assert!(error.to_string().contains("containment"));

        cause.containment_completed_at = Some(base + Duration::seconds(30));
        recovery.window_start = cause.window_start + Duration::seconds(1);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must cover the causal start");
        assert!(error.to_string().contains("must cover"));

        recovery.window_start = cause.window_start;
        recovery.window_end = cause.window_end - Duration::seconds(1);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must cover the causal end");
        assert!(error.to_string().contains("must cover"));

        recovery.window_end = cause.window_end;
        recovery.finished_at = Some(state.created_at);
        let error = validate_open_latch_recovery(&state, &cause, &recovery)
            .expect_err("recovery must finish after latch open");
        assert!(
            error
                .to_string()
                .contains("after every unresolved incident")
        );

        recovery.finished_at = Some(state.created_at + Duration::milliseconds(1));
        validate_open_latch_recovery(&state, &cause, &recovery)
            .expect("covered, contained, later full run is valid");
    }

    #[test]
    fn recovery_covers_union_of_every_unresolved_cause_not_only_latest() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut older = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(4),
            base - Duration::hours(3),
        );
        older.containment_completed_at = Some(base - Duration::hours(2));
        let mut latest = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Failed,
            base - Duration::hours(1),
            base,
        );
        latest.containment_completed_at = Some(base + Duration::seconds(1));
        let older_state = open_state(&older.run_id, base - Duration::hours(2));
        let mut latest_state = open_state(&latest.run_id, base + Duration::seconds(2));
        latest_state.transition = FeatureParityStateTransition::IntegrityFailure;

        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            latest.window_start,
            latest.window_end,
        );
        recovery.finished_at = Some(base + Duration::minutes(1));
        let incidents = vec![(latest_state, latest), (older_state, older)];

        let error = validate_open_latch_recoveries(&incidents, &recovery)
            .expect_err("latest-only proof must not erase the older cause");
        assert!(error.to_string().contains("unresolved cause union"));

        recovery.window_start = base - Duration::hours(4);
        validate_open_latch_recoveries(&incidents, &recovery)
            .expect("one full replay covers the complete pending window union");
    }

    #[test]
    fn recovery_run_must_be_nonempty_full_pass_with_no_pending_or_mismatch() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(1),
            base,
        );
        validate_recovery_run(&recovery).expect("complete full pass");

        recovery.kind = FeatureParityRunKind::Sampled;
        assert!(validate_recovery_run(&recovery).is_err());
        recovery.kind = FeatureParityRunKind::Full;
        recovery.pending_materialization_count = 1;
        assert!(validate_recovery_run(&recovery).is_err());

        recovery.pending_materialization_count = 0;
        recovery.feature_contract_hash = None;
        assert!(validate_recovery_run(&recovery).is_err());

        recovery.feature_contract_hash = Some(hash());
        recovery.finished_at = None;
        assert!(validate_recovery_run(&recovery).is_err());
    }

    #[test]
    fn uninitialized_latch_requires_later_frozen_model_dataset_proof() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            base - Duration::hours(1),
            base,
        );

        let error = validate_bootstrap_recovery(&recovery)
            .expect_err("unbound runtime full cannot initialize the latch");
        assert!(error.to_string().contains("model+dataset-bound"));

        recovery.model_version_id = Some(ModelVersionId::from_v7());
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        validate_bootstrap_recovery(&recovery).expect("frozen subject proof is valid");

        recovery.report_id = Some(RecommendationReportId::from_v7());
        assert!(validate_bootstrap_recovery(&recovery).is_err());
        recovery.report_id = None;

        recovery.finished_at = Some(recovery.created_at);
        let error = validate_bootstrap_recovery(&recovery)
            .expect_err("proof must finish after durable initialization");
        assert!(error.to_string().contains("after it was initialized"));
    }

    #[test]
    fn clear_acknowledgement_is_idempotent_only_for_the_minting_recovery_run() {
        let recovery_run_id = FeatureParityRunId::from_v7();
        let state = clear_state(&recovery_run_id);
        validate_clear_acknowledgement(&state, &recovery_run_id)
            .expect("same recovery acknowledgement is idempotent");

        let different = FeatureParityRunId::from_v7();
        let error = validate_clear_acknowledgement(&state, &different)
            .expect_err("concurrent different proof must not be accepted");
        assert!(error.to_string().contains("different run"));
    }

    #[test]
    fn serving_recovery_cannot_use_subject_bound_offline_full() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let mut cause = parity_run(
            FeatureParityRunKind::Sampled,
            FeatureParityRunStatus::Mismatched,
            base - Duration::hours(1),
            base,
        );
        cause.report_id = Some(RecommendationReportId::from_v7());
        cause.model_version_id = Some(ModelVersionId::from_v7());
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            cause.window_start,
            cause.window_end,
        );

        validate_recovery_scope(&cause, &recovery).expect("unbound runtime full");
        recovery.model_version_id = Some(ModelVersionId::from_v7());
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        let error = validate_recovery_scope(&cause, &recovery)
            .expect_err("offline subject proof cannot clear serving latch");
        assert!(error.to_string().contains("unbound runtime full"));
    }

    #[test]
    fn offline_recovery_requires_exact_model_and_dataset_subject() {
        let base = Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap();
        let model_version_id = ModelVersionId::from_v7();
        let training_dataset_id = TrainingDatasetId::from_v7();
        let mut cause = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Failed,
            base - Duration::hours(1),
            base,
        );
        cause.model_version_id = Some(model_version_id.clone());
        cause.training_dataset_id = Some(training_dataset_id.clone());
        let mut recovery = parity_run(
            FeatureParityRunKind::Full,
            FeatureParityRunStatus::Passed,
            cause.window_start,
            cause.window_end,
        );
        recovery.model_version_id = Some(model_version_id);
        recovery.training_dataset_id = Some(training_dataset_id);

        validate_recovery_scope(&cause, &recovery).expect("exact offline subject");
        recovery.training_dataset_id = Some(TrainingDatasetId::from_v7());
        let error = validate_recovery_scope(&cause, &recovery)
            .expect_err("different dataset cannot clear offline latch");
        assert!(error.to_string().contains("exact model"));
    }
}
