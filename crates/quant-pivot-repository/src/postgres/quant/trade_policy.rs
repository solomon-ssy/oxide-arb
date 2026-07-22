//! Postgres trade-policy artifact catalog and WORM governance transitions.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_TRADE_POLICY_ARTIFACT};
use quant_pivot_models::{
    domain::{
        api::{
            TradePolicyAuditListQuery, TradePolicyListQuery, TradePolicyValidationListQuery,
            TradePolicyValidationRowListQuery,
        },
        pagination::{PageWindow, Paginated},
        quant::{
            CompleteTradePolicyValidation, FailTradePolicyValidation, NewTradePolicyArtifact,
            NewTradePolicyGovernanceAudit, NewTradePolicyTrialAttempt, NewTradePolicyValidationRow,
            NewTradePolicyValidationRun, TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo,
            TradePolicyTrialAttemptInfo, TradePolicyValidationRowInfo,
            TradePolicyValidationRunInfo,
        },
    },
    entities::{
        quant_trade_policy_artifact::{Column as QuantTradePolicyArtifactColumn, Entity},
        quant_trade_policy_governance_audit::{
            Column as QuantTradePolicyGovernanceAuditColumn,
            Entity as QuantTradePolicyGovernanceAuditEntity,
        },
        quant_trade_policy_trial_attempt::{
            Column, Entity as QuantTradePolicyTrialAttemptEntity, Model,
        },
        quant_trade_policy_validation::{
            Column as QuantTradePolicyValidationColumn, Entity as QuantTradePolicyValidationEntity,
            Model as QuantTradePolicyValidationModel,
        },
        quant_trade_policy_validation_row::{
            Column as QuantTradePolicyValidationRowColumn,
            Entity as QuantTradePolicyValidationRowEntity,
        },
    },
    enums::quant::{
        TradePolicyStatus, TradePolicyTrialScope, TradePolicyTrialStatus,
        TradePolicyValidationStatus,
    },
    types::{ResearchJobId, TradePolicyArtifactId, TradePolicyValidationRunId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::{error, query::paginate_mapped, write::upsert_many_chunked},
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
        Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| error::map_unique(error, QUANT_TRADE_POLICY_ARTIFACT, "content_hash"))
            .map(Into::into)
    }

    async fn append_trial_attempt(
        &self,
        attempt: NewTradePolicyTrialAttempt,
    ) -> Result<TradePolicyTrialAttemptInfo, StorageError> {
        validate_trial_attempt(&attempt)?;
        let trial_attempt_id = attempt.trial_attempt_id;
        QuantTradePolicyTrialAttemptEntity::insert(attempt.clone().into_active_model())
            .on_conflict(
                OnConflict::column(Column::TrialAttemptId)
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
        let stored = QuantTradePolicyTrialAttemptEntity::find_by_id(trial_attempt_id)
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
        cutoff: Option<DateTime<Utc>>,
    ) -> Result<Vec<TradePolicyTrialAttemptInfo>, StorageError> {
        let condition = Condition::all()
            .add(Column::FitJobId.eq(*fit_job_id))
            .add_option(cutoff.map(|value| Column::CreatedAt.lte(value)));
        QuantTradePolicyTrialAttemptEntity::find()
            .filter(condition)
            .order_by_asc(Column::AttemptOrdinal)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyArtifactInfo>, StorageError> {
        Entity::find_by_id(*artifact_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> Result<Paginated<TradePolicyArtifactInfo>, StorageError> {
        let condition =
            Condition::all()
                .add_option(
                    query
                        .status
                        .map(|status| QuantTradePolicyArtifactColumn::Status.eq(status)),
                )
                .add_option(query.source_dataset_id.as_ref().map(|dataset_id| {
                    QuantTradePolicyArtifactColumn::SourceDatasetId.eq(*dataset_id)
                }))
                .add_option(
                    query
                        .from
                        .map(|from| QuantTradePolicyArtifactColumn::CreatedAt.gte(from)),
                )
                .add_option(
                    query
                        .to
                        .map(|to| QuantTradePolicyArtifactColumn::CreatedAt.lt(to)),
                );
        paginate_mapped(
            Entity::find()
                .filter(condition)
                .order_by_desc(QuantTradePolicyArtifactColumn::CreatedAt),
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
        let row = Entity::find_by_id(*artifact_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(QUANT_TRADE_POLICY_ARTIFACT, artifact_id))?;
        if row.status != expected || !row.status.allows_transition_to(target) {
            return Err(error::illegal_transition(
                QUANT_TRADE_POLICY_ARTIFACT,
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
                Some(QUANT_TRADE_POLICY_ARTIFACT),
                "trade-policy governance audit does not match the locked artifact transition",
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(target);
        active.updated_at = ActiveValue::Set(Utc::now());
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        QuantTradePolicyGovernanceAuditEntity::insert(audit.into_active_model())
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
            QuantTradePolicyGovernanceAuditEntity::find()
                .filter(QuantTradePolicyGovernanceAuditColumn::ArtifactId.eq(*artifact_id))
                .order_by_desc(QuantTradePolicyGovernanceAuditColumn::CreatedAt)
                .order_by_desc(QuantTradePolicyGovernanceAuditColumn::AuditId),
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
        let validation_run_id = run.validation_run_id;
        QuantTradePolicyValidationEntity::insert(run.clone().into_active_model())
            .on_conflict(
                OnConflict::column(QuantTradePolicyValidationColumn::ValidationRunId)
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
        let stored = QuantTradePolicyValidationEntity::find_by_id(validation_run_id)
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
        let validation_run_id = rows[0].validation_run_id;
        let ordinals = rows.iter().map(|row| row.row_ordinal).collect::<Vec<_>>();
        upsert_many_chunked::<QuantTradePolicyValidationRowEntity, _>(
            &self.db,
            rows.clone(),
            OnConflict::columns([
                QuantTradePolicyValidationRowColumn::ValidationRunId,
                QuantTradePolicyValidationRowColumn::RowOrdinal,
            ])
            .do_nothing()
            .to_owned(),
        )
        .await?;
        let stored = QuantTradePolicyValidationRowEntity::find()
            .filter(QuantTradePolicyValidationRowColumn::ValidationRunId.eq(validation_run_id))
            .filter(QuantTradePolicyValidationRowColumn::RowOrdinal.is_in(ordinals))
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
        let artifact = Entity::find_by_id(run.artifact_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(QUANT_TRADE_POLICY_ARTIFACT, run.artifact_id))?;
        if artifact.status != TradePolicyStatus::Draft || artifact.content_hash != run.artifact_hash
        {
            return Err(error::state_conflict(
                QUANT_TRADE_POLICY_ARTIFACT,
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
                Some(QUANT_TRADE_POLICY_ARTIFACT),
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
        QuantTradePolicyGovernanceAuditEntity::insert(audit.into_active_model())
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
        QuantTradePolicyValidationEntity::find_by_id(*validation_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_successful_validation(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyValidationRunInfo>, StorageError> {
        QuantTradePolicyValidationEntity::find()
            .filter(QuantTradePolicyValidationColumn::ArtifactId.eq(*artifact_id))
            .filter(
                QuantTradePolicyValidationColumn::Status.eq(TradePolicyValidationStatus::Succeeded),
            )
            .order_by_desc(QuantTradePolicyValidationColumn::CompletedAt)
            .order_by_desc(QuantTradePolicyValidationColumn::ValidationRunId)
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
            .add(QuantTradePolicyValidationColumn::ArtifactId.eq(*artifact_id))
            .add_option(
                query
                    .status
                    .map(|status| QuantTradePolicyValidationColumn::Status.eq(status)),
            );
        paginate_mapped(
            QuantTradePolicyValidationEntity::find()
                .filter(condition)
                .order_by_desc(QuantTradePolicyValidationColumn::CreatedAt)
                .order_by_desc(QuantTradePolicyValidationColumn::ValidationRunId),
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
        let condition =
            Condition::all()
                .add(QuantTradePolicyValidationRowColumn::ValidationRunId.eq(*validation_run_id))
                .add_option(
                    query
                        .passed
                        .map(|passed| QuantTradePolicyValidationRowColumn::Passed.eq(passed)),
                )
                .add_option(
                    query.evidence_kind.as_ref().map(|kind| {
                        QuantTradePolicyValidationRowColumn::EvidenceKind.eq(kind.clone())
                    }),
                )
                .add_option(query.diagnostic_kind.as_ref().map(|kind| {
                    QuantTradePolicyValidationRowColumn::DiagnosticKind.eq(kind.clone())
                }));
        paginate_mapped(
            QuantTradePolicyValidationRowEntity::find()
                .filter(condition)
                .order_by_asc(QuantTradePolicyValidationRowColumn::RowOrdinal),
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
        || attempt.candidate_id.as_str().chars().count() > 128
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

fn trial_attempt_matches(stored: &Model, expected: &NewTradePolicyTrialAttempt) -> bool {
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
    stored: &QuantTradePolicyValidationModel,
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
) -> Result<QuantTradePolicyValidationModel, StorageError>
where
    C: ConnectionTrait,
{
    QuantTradePolicyValidationEntity::find_by_id(*validation_run_id)
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
    C: ConnectionTrait,
{
    let base = || {
        QuantTradePolicyValidationRowEntity::find()
            .filter(QuantTradePolicyValidationRowColumn::ValidationRunId.eq(*validation_run_id))
    };
    let total = base().count(db).await.map_err(StorageError::from)?;
    let passed = base()
        .filter(QuantTradePolicyValidationRowColumn::Passed.eq(true))
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
