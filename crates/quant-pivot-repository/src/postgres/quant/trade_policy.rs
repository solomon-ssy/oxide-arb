//! Postgres trade-policy artifact catalog and WORM governance transitions.

use std::collections::BTreeSet;

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        CompleteTradePolicyValidation, FailTradePolicyValidation, NewTradePolicyArtifact,
        NewTradePolicyGovernanceAudit, NewTradePolicyTrialAttempt, NewTradePolicyValidationRow,
        NewTradePolicyValidationRun, PageWindow, Paginated, TradePolicyArtifactInfo,
        TradePolicyAuditListQuery, TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicyTrialAttemptInfo, TradePolicyValidationListQuery, TradePolicyValidationRowInfo,
        TradePolicyValidationRowListQuery, TradePolicyValidationRunInfo,
    },
    entities::{
        quant_trade_policy_artifact, quant_trade_policy_governance_audit,
        quant_trade_policy_trial_attempt, quant_trade_policy_validation,
        quant_trade_policy_validation_row,
    },
    enums::quant::{
        TradePolicyStatus, TradePolicyTrialScope, TradePolicyTrialStatus,
        TradePolicyValidationStatus,
    },
    types::{ResearchJobId, TradePolicyArtifactId, TradePolicyValidationRunId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::OnConflict,
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::TradePolicyRepository,
};

pub struct PgTradePolicyRepository {
    db: DatabaseConnection,
}

impl PgTradePolicyRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl TradePolicyRepository for PgTradePolicyRepository {
    async fn insert(
        &self,
        artifact: NewTradePolicyArtifact,
    ) -> Result<TradePolicyArtifactInfo, StorageError> {
        quant_trade_policy_artifact::Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| {
                error::map_unique(error, entity::QUANT_TRADE_POLICY_ARTIFACT, "content_hash")
            })
            .map(Into::into)
    }

    async fn append_trial_attempt(
        &self,
        attempt: NewTradePolicyTrialAttempt,
    ) -> Result<TradePolicyTrialAttemptInfo, StorageError> {
        validate_trial_attempt(&attempt)?;
        let trial_attempt_id = attempt.trial_attempt_id.clone();
        quant_trade_policy_trial_attempt::Entity::insert(attempt.clone().into_active_model())
            .on_conflict(
                OnConflict::column(quant_trade_policy_trial_attempt::Column::TrialAttemptId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(|error| {
                error::map_unique(
                    error,
                    "quant_trade_policy_trial_attempt",
                    "fit_job_id_attempt_ordinal",
                )
            })?;
        let stored = quant_trade_policy_trial_attempt::Entity::find_by_id(trial_attempt_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::invariant_violation(
                    Some("quant_trade_policy_trial_attempt"),
                    "trial attempt was not visible after append",
                )
            })?;
        if !trial_attempt_matches(&stored, &attempt) {
            return Err(error::state_conflict(
                "quant_trade_policy_trial_attempt",
                Some(&attempt.trial_attempt_id),
                "trial attempt id is already bound to different immutable content",
            ));
        }
        Ok(stored.into())
    }

    async fn list_trial_attempts(
        &self,
        fit_job_id: &ResearchJobId,
        cutoff: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<TradePolicyTrialAttemptInfo>, StorageError> {
        let condition = Condition::all()
            .add(quant_trade_policy_trial_attempt::Column::FitJobId.eq(fit_job_id.clone()))
            .add_option(
                cutoff.map(|value| quant_trade_policy_trial_attempt::Column::CreatedAt.lte(value)),
            );
        quant_trade_policy_trial_attempt::Entity::find()
            .filter(condition)
            .order_by_asc(quant_trade_policy_trial_attempt::Column::AttemptOrdinal)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyArtifactInfo>, StorageError> {
        quant_trade_policy_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> Result<Paginated<TradePolicyArtifactInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .status
                    .map(|status| quant_trade_policy_artifact::Column::Status.eq(status)),
            )
            .add_option(query.source_dataset_id.as_ref().map(|dataset_id| {
                quant_trade_policy_artifact::Column::SourceDatasetId.eq(dataset_id.clone())
            }))
            .add_option(
                query
                    .from
                    .map(|from| quant_trade_policy_artifact::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_trade_policy_artifact::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_trade_policy_artifact::Entity::find()
                .filter(condition)
                .order_by_desc(quant_trade_policy_artifact::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        expected: TradePolicyStatus,
        target: TradePolicyStatus,
        audit: NewTradePolicyGovernanceAudit,
    ) -> Result<TradePolicyArtifactInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_trade_policy_artifact::Entity::find_by_id(artifact_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(entity::QUANT_TRADE_POLICY_ARTIFACT, artifact_id))?;
        if row.status != expected || !row.status.allows_transition_to(target) {
            return Err(error::illegal_transition(
                entity::QUANT_TRADE_POLICY_ARTIFACT,
                Some(artifact_id),
                row.status.as_str(),
                target.as_str(),
            ));
        }
        if audit.artifact_id != *artifact_id
            || audit.from_status != row.status
            || audit.to_status != target
            || audit.content_hash != row.content_hash
        {
            return Err(error::invariant_violation(
                Some(entity::QUANT_TRADE_POLICY_ARTIFACT),
                "trade-policy governance audit does not match the locked artifact transition",
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(target);
        active.updated_at = ActiveValue::Set(Utc::now());
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        quant_trade_policy_governance_audit::Entity::insert(audit.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> Result<Paginated<TradePolicyGovernanceAuditInfo>, StorageError> {
        paginate_mapped(
            quant_trade_policy_governance_audit::Entity::find()
                .filter(
                    quant_trade_policy_governance_audit::Column::ArtifactId.eq(artifact_id.clone()),
                )
                .order_by_desc(quant_trade_policy_governance_audit::Column::CreatedAt)
                .order_by_desc(quant_trade_policy_governance_audit::Column::AuditId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn begin_validation(
        &self,
        run: NewTradePolicyValidationRun,
    ) -> Result<TradePolicyValidationRunInfo, StorageError> {
        validate_new_validation(&run)?;
        let validation_run_id = run.validation_run_id.clone();
        quant_trade_policy_validation::Entity::insert(run.clone().into_active_model())
            .on_conflict(
                OnConflict::column(quant_trade_policy_validation::Column::ValidationRunId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(|error| {
                error::map_unique(
                    error,
                    "quant_trade_policy_validation",
                    "validation_run_id_or_running_artifact",
                )
            })?;
        let stored = quant_trade_policy_validation::Entity::find_by_id(validation_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::invariant_violation(
                    Some("quant_trade_policy_validation"),
                    "validation run was not visible after insert",
                )
            })?;
        ensure_validation_identity(&stored, &run)?;
        Ok(stored.into())
    }

    async fn append_validation_rows(
        &self,
        rows: Vec<NewTradePolicyValidationRow>,
    ) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        validate_validation_rows(&rows)?;
        let validation_run_id = rows[0].validation_run_id.clone();
        let ordinals = rows.iter().map(|row| row.row_ordinal).collect::<Vec<_>>();
        quant_trade_policy_validation_row::Entity::insert_many(
            rows.iter().cloned().map(IntoActiveModel::into_active_model),
        )
        .on_conflict(
            OnConflict::columns([
                quant_trade_policy_validation_row::Column::ValidationRunId,
                quant_trade_policy_validation_row::Column::RowOrdinal,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec_without_returning(&self.db)
        .await
        .map_err(StorageError::from)?;
        let stored = quant_trade_policy_validation_row::Entity::find()
            .filter(
                quant_trade_policy_validation_row::Column::ValidationRunId.eq(validation_run_id),
            )
            .filter(quant_trade_policy_validation_row::Column::RowOrdinal.is_in(ordinals))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        if stored.len() != rows.len()
            || stored.iter().any(|stored| {
                rows.iter()
                    .find(|row| row.row_ordinal == stored.row_ordinal)
                    .is_none_or(|expected| expected.row_hash != stored.row_hash)
            })
        {
            return Err(error::state_conflict(
                "quant_trade_policy_validation_row",
                Some(&rows[0].validation_run_id),
                "validation row ordinal is already bound to different content",
            ));
        }
        Ok(())
    }

    async fn complete_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        completion: CompleteTradePolicyValidation,
    ) -> Result<(TradePolicyValidationRunInfo, TradePolicyArtifactInfo), StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let run = validation_run_for_update(&transaction, validation_run_id).await?;
        if run.status != TradePolicyValidationStatus::Running {
            return Err(error::illegal_transition(
                "quant_trade_policy_validation",
                Some(validation_run_id),
                run.status,
                TradePolicyValidationStatus::Succeeded,
            ));
        }
        let (total_rows, passed_rows, failed_rows) =
            validation_row_counts(&transaction, validation_run_id).await?;
        if total_rows != completion.total_rows
            || passed_rows != completion.passed_rows
            || failed_rows != 0
            || total_rows == 0
        {
            return Err(error::invariant_violation(
                Some("quant_trade_policy_validation"),
                "validation completion counts do not match persisted row diagnostics",
            ));
        }
        let artifact = quant_trade_policy_artifact::Entity::find_by_id(run.artifact_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(entity::QUANT_TRADE_POLICY_ARTIFACT, &run.artifact_id)
            })?;
        if artifact.status != TradePolicyStatus::Draft || artifact.content_hash != run.artifact_hash
        {
            return Err(error::state_conflict(
                entity::QUANT_TRADE_POLICY_ARTIFACT,
                Some(&run.artifact_id),
                "validation completion requires the exact immutable Draft",
            ));
        }
        let audit = completion.audit;
        if audit.artifact_id != artifact.artifact_id
            || audit.from_status != TradePolicyStatus::Draft
            || audit.to_status != TradePolicyStatus::Validated
            || audit.content_hash != artifact.content_hash
        {
            return Err(error::invariant_violation(
                Some(entity::QUANT_TRADE_POLICY_ARTIFACT),
                "validation audit does not bind the locked Draft",
            ));
        }
        let mut run_active = run.into_active_model();
        run_active.status = ActiveValue::Set(TradePolicyValidationStatus::Succeeded);
        run_active.total_rows = ActiveValue::Set(total_rows);
        run_active.passed_rows = ActiveValue::Set(passed_rows);
        run_active.failed_rows = ActiveValue::Set(0);
        run_active.validation_hash = ActiveValue::Set(Some(completion.validation_hash));
        run_active.completed_at = ActiveValue::Set(Some(Utc::now()));
        let completed_run = run_active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let mut artifact_active = artifact.into_active_model();
        artifact_active.status = ActiveValue::Set(TradePolicyStatus::Validated);
        artifact_active.updated_at = ActiveValue::Set(Utc::now());
        let validated = artifact_active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        quant_trade_policy_governance_audit::Entity::insert(audit.into_active_model())
            .exec(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok((completed_run.into(), validated.into()))
    }

    async fn fail_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        failure: FailTradePolicyValidation,
    ) -> Result<TradePolicyValidationRunInfo, StorageError> {
        if !matches!(
            failure.status,
            TradePolicyValidationStatus::Failed | TradePolicyValidationStatus::Cancelled
        ) || failure.failure_detail.trim().is_empty()
        {
            return Err(error::invariant_violation(
                Some("quant_trade_policy_validation"),
                "validation failure requires a terminal failure status and detail",
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let run = validation_run_for_update(&transaction, validation_run_id).await?;
        if run.status != TradePolicyValidationStatus::Running {
            return Err(error::illegal_transition(
                "quant_trade_policy_validation",
                Some(validation_run_id),
                run.status,
                failure.status,
            ));
        }
        let (total_rows, passed_rows, failed_rows) =
            validation_row_counts(&transaction, validation_run_id).await?;
        let mut active = run.into_active_model();
        active.status = ActiveValue::Set(failure.status);
        active.total_rows = ActiveValue::Set(total_rows);
        active.passed_rows = ActiveValue::Set(passed_rows);
        active.failed_rows = ActiveValue::Set(failed_rows);
        active.validation_hash = ActiveValue::Set(Some(failure.validation_hash));
        active.failure_detail = ActiveValue::Set(Some(failure.failure_detail));
        active.completed_at = ActiveValue::Set(Some(Utc::now()));
        let failed = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(failed.into())
    }

    async fn find_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
    ) -> Result<Option<TradePolicyValidationRunInfo>, StorageError> {
        quant_trade_policy_validation::Entity::find_by_id(validation_run_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_successful_validation(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyValidationRunInfo>, StorageError> {
        quant_trade_policy_validation::Entity::find()
            .filter(quant_trade_policy_validation::Column::ArtifactId.eq(artifact_id.clone()))
            .filter(
                quant_trade_policy_validation::Column::Status
                    .eq(TradePolicyValidationStatus::Succeeded),
            )
            .order_by_desc(quant_trade_policy_validation::Column::CompletedAt)
            .order_by_desc(quant_trade_policy_validation::Column::ValidationRunId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page_validations(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyValidationListQuery,
    ) -> Result<Paginated<TradePolicyValidationRunInfo>, StorageError> {
        let condition = Condition::all()
            .add(quant_trade_policy_validation::Column::ArtifactId.eq(artifact_id.clone()))
            .add_option(
                query
                    .status
                    .map(|status| quant_trade_policy_validation::Column::Status.eq(status)),
            );
        paginate_mapped(
            quant_trade_policy_validation::Entity::find()
                .filter(condition)
                .order_by_desc(quant_trade_policy_validation::Column::CreatedAt)
                .order_by_desc(quant_trade_policy_validation::Column::ValidationRunId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn page_validation_rows(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        query: TradePolicyValidationRowListQuery,
    ) -> Result<Paginated<TradePolicyValidationRowInfo>, StorageError> {
        let condition = Condition::all()
            .add(
                quant_trade_policy_validation_row::Column::ValidationRunId
                    .eq(validation_run_id.clone()),
            )
            .add_option(
                query
                    .passed
                    .map(|passed| quant_trade_policy_validation_row::Column::Passed.eq(passed)),
            )
            .add_option(query.evidence_kind.as_ref().map(|kind| {
                quant_trade_policy_validation_row::Column::EvidenceKind.eq(kind.clone())
            }))
            .add_option(query.diagnostic_kind.as_ref().map(|kind| {
                quant_trade_policy_validation_row::Column::DiagnosticKind.eq(kind.clone())
            }));
        paginate_mapped(
            quant_trade_policy_validation_row::Entity::find()
                .filter(condition)
                .order_by_asc(quant_trade_policy_validation_row::Column::RowOrdinal),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}

fn validate_new_validation(run: &NewTradePolicyValidationRun) -> Result<(), StorageError> {
    if run.status != TradePolicyValidationStatus::Running || run.reason.trim().is_empty() {
        return Err(error::invariant_violation(
            Some("quant_trade_policy_validation"),
            "new validation run must be Running with a non-empty reason",
        ));
    }
    Ok(())
}

fn validate_trial_attempt(attempt: &NewTradePolicyTrialAttempt) -> Result<(), StorageError> {
    let scope_shape_valid = match attempt.scope {
        TradePolicyTrialScope::Candidate | TradePolicyTrialScope::LatencyStress => {
            attempt.fold_index.is_none() && attempt.path_index.is_none()
        }
        TradePolicyTrialScope::Fold => {
            attempt.fold_index.is_some_and(|index| index >= 0) && attempt.path_index.is_none()
        }
        TradePolicyTrialScope::Path => {
            attempt.fold_index.is_none() && attempt.path_index.is_some_and(|index| index >= 0)
        }
    };
    let evidence_shape_valid = match (
        attempt.evidence_uri.as_ref(),
        attempt.evidence_hash.as_ref(),
        attempt.evidence_row_count,
    ) {
        (None, None, None) => true,
        (Some(_), Some(_), Some(count)) => count >= 0,
        _ => false,
    };
    let terminal_shape_valid = match attempt.status {
        TradePolicyTrialStatus::Succeeded => {
            attempt
                .metrics_json
                .as_ref()
                .is_some_and(|metrics| metrics.validate().is_ok())
                && attempt.failure_detail.is_none()
                && attempt.evidence_uri.is_some()
        }
        TradePolicyTrialStatus::Failed | TradePolicyTrialStatus::Cancelled => {
            attempt.metrics_json.is_none()
                && attempt.failure_detail.as_ref().is_some_and(|detail| {
                    let length = detail.trim().chars().count();
                    (1..=8_192).contains(&length)
                })
        }
    };
    let expected_hash = attempt.expected_row_hash().map_err(|error| {
        error::invariant_violation(
            Some("quant_trade_policy_trial_attempt"),
            format!("trial attempt cannot be canonically hashed: {error}"),
        )
    })?;
    if attempt.attempt_ordinal < 0
        || attempt.candidate_id.trim().is_empty()
        || attempt.candidate_id.chars().count() > 128
        || !scope_shape_valid
        || !evidence_shape_valid
        || !terminal_shape_valid
        || expected_hash != attempt.row_hash
    {
        return Err(error::invariant_violation(
            Some("quant_trade_policy_trial_attempt"),
            "trial attempt scope, terminal payload, evidence, ordinal, or row hash is invalid",
        ));
    }
    Ok(())
}

fn trial_attempt_matches(
    stored: &quant_trade_policy_trial_attempt::Model,
    expected: &NewTradePolicyTrialAttempt,
) -> bool {
    stored.fit_job_id == expected.fit_job_id
        && stored.attempt_ordinal == expected.attempt_ordinal
        && stored.experiment_family_hash == expected.experiment_family_hash
        && stored.research_program_hash == expected.research_program_hash
        && stored.candidate_id == expected.candidate_id
        && stored.candidate_hash == expected.candidate_hash
        && stored.scope == expected.scope
        && stored.fold_index == expected.fold_index
        && stored.path_index == expected.path_index
        && stored.status == expected.status
        && stored.metrics_json == expected.metrics_json
        && stored.evidence_uri == expected.evidence_uri
        && stored.evidence_hash == expected.evidence_hash
        && stored.evidence_row_count == expected.evidence_row_count
        && stored.failure_detail == expected.failure_detail
        && stored.row_hash == expected.row_hash
}

fn ensure_validation_identity(
    stored: &quant_trade_policy_validation::Model,
    expected: &NewTradePolicyValidationRun,
) -> Result<(), StorageError> {
    if stored.artifact_id != expected.artifact_id
        || stored.artifact_hash != expected.artifact_hash
        || stored.source_dataset_id != expected.source_dataset_id
        || stored.source_dataset_hash != expected.source_dataset_hash
        || stored.source_slice_manifest_hash != expected.source_slice_manifest_hash
        || stored.evidence_manifest_hash != expected.evidence_manifest_hash
        || stored.actor_id != expected.actor_id
        || stored.reason != expected.reason
    {
        return Err(error::state_conflict(
            "quant_trade_policy_validation",
            Some(&expected.validation_run_id),
            "validation run id is already bound to a different immutable contract",
        ));
    }
    Ok(())
}

fn validate_validation_rows(rows: &[NewTradePolicyValidationRow]) -> Result<(), StorageError> {
    let run_id = &rows[0].validation_run_id;
    let mut ordinals = BTreeSet::new();
    for row in rows {
        let diagnostic_shape_valid =
            row.passed && row.diagnostic_kind.is_none() && row.detail.is_none()
                || !row.passed && row.diagnostic_kind.is_some() && row.detail.is_some();
        let hash_shape_valid = row.expected_row_hash.is_some() || row.actual_row_hash.is_some();
        let hash_match =
            row.expected_row_hash.is_some() && row.expected_row_hash == row.actual_row_hash;
        let evidence_kind_valid = matches!(
            row.evidence_kind.as_str(),
            "observation_eligibility"
                | "fills"
                | "candidate_trials"
                | "cohort_trials"
                | "cpcv_paths"
                | "coverage_gaps"
                | "statistical_summaries"
        );
        if row.validation_run_id != *run_id
            || row.row_ordinal < 0
            || !ordinals.insert(row.row_ordinal)
            || !diagnostic_shape_valid
            || !hash_shape_valid
            || row.passed != hash_match
            || !evidence_kind_valid
            || row.record_key.trim().is_empty()
        {
            return Err(error::invariant_violation(
                Some("quant_trade_policy_validation_row"),
                "validation row batch has mixed runs, duplicate ordinals, or invalid diagnostics",
            ));
        }
    }
    Ok(())
}

async fn validation_run_for_update<C>(
    db: &C,
    validation_run_id: &TradePolicyValidationRunId,
) -> Result<quant_trade_policy_validation::Model, StorageError>
where
    C: sea_orm::ConnectionTrait,
{
    quant_trade_policy_validation::Entity::find_by_id(validation_run_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found("quant_trade_policy_validation", validation_run_id))
}

async fn validation_row_counts<C>(
    db: &C,
    validation_run_id: &TradePolicyValidationRunId,
) -> Result<(i64, i64, i64), StorageError>
where
    C: sea_orm::ConnectionTrait,
{
    let base = || {
        quant_trade_policy_validation_row::Entity::find().filter(
            quant_trade_policy_validation_row::Column::ValidationRunId
                .eq(validation_run_id.clone()),
        )
    };
    let total = base().count(db).await.map_err(StorageError::from)?;
    let passed = base()
        .filter(quant_trade_policy_validation_row::Column::Passed.eq(true))
        .count(db)
        .await
        .map_err(StorageError::from)?;
    let failed = total.checked_sub(passed).ok_or_else(|| {
        error::invariant_violation(
            Some("quant_trade_policy_validation_row"),
            "passed row count exceeds total",
        )
    })?;
    Ok((exact_i64(total)?, exact_i64(passed)?, exact_i64(failed)?))
}

fn exact_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|error| {
        error::invariant_violation(
            Some("quant_trade_policy_validation"),
            format!("validation row count exceeds Postgres bigint: {error}"),
        )
    })
}
