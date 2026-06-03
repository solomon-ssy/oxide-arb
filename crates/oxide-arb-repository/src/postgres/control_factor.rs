use crate::traits::ControlFactorRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo,
        ControlFactorPublicationInfo, ControlFactorPublicationRowInfo,
        ControlFactorStageReportInfo, ControlFactorValue, ControlFactorValueInfo,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        MaterializationRunStatusPatch, NewControlFactorAuditEvent,
        NewControlFactorMaterializationRun, NewControlFactorPublication,
        NewControlFactorPublicationFactor, NewControlFactorPublicationRow,
        NewControlFactorStageReport, NewControlFactorValue, RunTransitionOutcome,
    },
    entities::{
        control_factor_audit_event::Entity as AuditEntity,
        control_factor_materialization_run::Column as RunColumn,
        control_factor_materialization_run::Entity as RunEntity,
        control_factor_publication::{
            Column as PublicationColumn, Entity as PublicationEntity, Model as PublicationModel,
        },
        control_factor_publication_factor::{
            Column as PublicationFactorColumn, Entity as PublicationFactorEntity,
        },
        control_factor_stage_report::{Column as StageReportColumn, Entity as StageReportEntity},
        control_factor_value::{Column as FactorColumn, Entity as FactorEntity},
    },
    enums::control_factor::{
        ControlAuditEventType, FactorStatus, MaterializationRunStatus, MaterializationStageName,
        PublicationMode, PublicationStatus,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId},
};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, TransactionTrait,
    sea_query::{Expr, OnConflict},
};

pub struct PgControlFactorRepository {
    db: DatabaseConnection,
}

impl PgControlFactorRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn load_materialization_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
    RunEntity::find_by_id(run_id.clone())
        .one(db)
        .await
        .map(|row| row.map(Into::into))
        .map_err(StorageError::from)
}

async fn find_materialization_run_by_dedupe_key_q(
    db: &impl ConnectionTrait,
    dedupe_key: &str,
) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
    RunEntity::find()
        .filter(RunColumn::RunDedupeKey.eq(dedupe_key))
        .one(db)
        .await
        .map(|row| row.map(Into::into))
        .map_err(StorageError::from)
}

async fn enqueue_materialization_run_q(
    db: &impl ConnectionTrait,
    run: NewControlFactorMaterializationRun,
    options: EnqueueMaterializationRunOptions,
) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
    if options.force_new_run && options.reason.as_deref().unwrap_or("").trim().is_empty() {
        return Err(StorageError::Codec(
            "force_new_run materialization enqueue requires a non-empty reason".into(),
        ));
    }

    if !options.force_new_run
        && let Some(dedupe_key) = run.run_dedupe_key.as_deref()
        && let Some(existing) = find_materialization_run_by_dedupe_key_q(db, dedupe_key).await?
    {
        return Ok(if is_active_run_status(existing.status) {
            EnqueueMaterializationRunOutcome::DuplicateActive(existing)
        } else {
            EnqueueMaterializationRunOutcome::DuplicateCompleted(existing)
        });
    }

    let mut active_model = run.into_active_model();
    if options.force_new_run {
        active_model.run_dedupe_key = Set(None);
    }
    RunEntity::insert(active_model)
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map(EnqueueMaterializationRunOutcome::Created)
        .map_err(StorageError::from)
}

async fn try_acquire_materialization_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
    started_at: DateTime<Utc>,
) -> Result<AcquireMaterializationRunOutcome, StorageError> {
    let Some(existing) = load_materialization_run_q(db, run_id).await? else {
        return Ok(AcquireMaterializationRunOutcome::NotFound);
    };
    if existing.status != MaterializationRunStatus::Queued {
        return Ok(AcquireMaterializationRunOutcome::NotQueued(existing));
    }
    RunEntity::update_many()
        .col_expr(
            RunColumn::Status,
            Expr::value(MaterializationRunStatus::Running),
        )
        .col_expr(RunColumn::StartedAt, Expr::value(started_at))
        .filter(RunColumn::MaterializationRunId.eq(run_id.clone()))
        .filter(RunColumn::Status.eq(MaterializationRunStatus::Queued))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    let Some(updated) = load_materialization_run_q(db, run_id).await? else {
        return Ok(AcquireMaterializationRunOutcome::NotFound);
    };
    if updated.status == MaterializationRunStatus::Running {
        Ok(AcquireMaterializationRunOutcome::Acquired(updated))
    } else {
        Ok(AcquireMaterializationRunOutcome::NotQueued(updated))
    }
}

async fn transition_materialization_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
    expected_from: MaterializationRunStatus,
    target: MaterializationRunStatus,
    patch: MaterializationRunStatusPatch,
) -> Result<RunTransitionOutcome, StorageError> {
    if target == MaterializationRunStatus::Failed && patch.failure_code.is_none() {
        return Err(StorageError::Codec(
            "failed materialization transition requires failure_code".into(),
        ));
    }
    let Some(existing) = load_materialization_run_q(db, run_id).await? else {
        return Ok(RunTransitionOutcome::NotFound);
    };
    if existing.status != expected_from {
        return Ok(RunTransitionOutcome::InvalidTransition {
            current_status: existing.status,
        });
    }

    let mut update = RunEntity::update_many()
        .col_expr(RunColumn::Status, Expr::value(target))
        .filter(RunColumn::MaterializationRunId.eq(run_id.clone()))
        .filter(RunColumn::Status.eq(expected_from));
    if let Some(finished_at) = patch.finished_at {
        update = update.col_expr(RunColumn::FinishedAt, Expr::value(finished_at));
    }
    if let Some(failure_code) = patch.failure_code {
        update = update.col_expr(RunColumn::FailureCode, Expr::value(failure_code));
    }
    if let Some(failure_detail) = patch.failure_detail {
        update = update.col_expr(RunColumn::FailureDetail, Expr::value(failure_detail));
    }
    if let Some(report) = patch.report {
        update = update.col_expr(RunColumn::Report, Expr::value(report));
    }
    if let Some(report_uri) = patch.report_uri {
        update = update.col_expr(RunColumn::ReportUri, Expr::value(report_uri));
    }
    update.exec(db).await.map_err(StorageError::from)?;
    let Some(updated) = load_materialization_run_q(db, run_id).await? else {
        return Ok(RunTransitionOutcome::NotFound);
    };
    Ok(RunTransitionOutcome::Transitioned(Box::new(updated)))
}

async fn retry_materialization_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
) -> Result<RunTransitionOutcome, StorageError> {
    let Some(existing) = load_materialization_run_q(db, run_id).await? else {
        return Ok(RunTransitionOutcome::NotFound);
    };
    if !matches!(
        existing.status,
        MaterializationRunStatus::Failed | MaterializationRunStatus::Cancelled
    ) {
        return Ok(RunTransitionOutcome::InvalidTransition {
            current_status: existing.status,
        });
    }
    RunEntity::update_many()
        .col_expr(
            RunColumn::Status,
            Expr::value(MaterializationRunStatus::Queued),
        )
        .col_expr(
            RunColumn::StartedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            RunColumn::FinishedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(RunColumn::FailureCode, Expr::value(Option::<String>::None))
        .col_expr(
            RunColumn::FailureDetail,
            Expr::value(Option::<String>::None),
        )
        .filter(RunColumn::MaterializationRunId.eq(run_id.clone()))
        .filter(RunColumn::Status.eq(existing.status))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    let Some(updated) = load_materialization_run_q(db, run_id).await? else {
        return Ok(RunTransitionOutcome::NotFound);
    };
    Ok(RunTransitionOutcome::Transitioned(Box::new(updated)))
}

async fn cancel_materialization_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
    reason: &str,
    cancelled_at: DateTime<Utc>,
) -> Result<CancelMaterializationRunOutcome, StorageError> {
    let Some(existing) = load_materialization_run_q(db, run_id).await? else {
        return Ok(CancelMaterializationRunOutcome::NotFound);
    };
    if is_terminal_run_status(existing.status) {
        return Ok(CancelMaterializationRunOutcome::AlreadyTerminal(existing));
    }
    let patch = MaterializationRunStatusPatch {
        finished_at: Some(cancelled_at),
        failure_code: Some("run.cancelled".into()),
        failure_detail: Some(reason.to_owned()),
        report: None,
        report_uri: None,
    };
    match transition_materialization_run_q(
        db,
        run_id,
        existing.status,
        MaterializationRunStatus::Cancelled,
        patch,
    )
    .await?
    {
        RunTransitionOutcome::Transitioned(run) => {
            Ok(CancelMaterializationRunOutcome::Cancelled(*run))
        }
        RunTransitionOutcome::InvalidTransition { .. } => {
            let Some(run) = load_materialization_run_q(db, run_id).await? else {
                return Ok(CancelMaterializationRunOutcome::NotFound);
            };
            Ok(CancelMaterializationRunOutcome::AlreadyTerminal(run))
        }
        RunTransitionOutcome::NotFound => Ok(CancelMaterializationRunOutcome::NotFound),
    }
}

async fn upsert_stage_report_q(
    db: &impl ConnectionTrait,
    report: NewControlFactorStageReport,
) -> Result<ControlFactorStageReportInfo, StorageError> {
    StageReportEntity::insert(report.into_active_model())
        .on_conflict(
            OnConflict::columns([
                StageReportColumn::MaterializationRunId,
                StageReportColumn::StageName,
            ])
            .update_columns([
                StageReportColumn::Status,
                StageReportColumn::FinishedAt,
                StageReportColumn::InputArtifactHashes,
                StageReportColumn::OutputArtifactHash,
                StageReportColumn::Coverage,
                StageReportColumn::Metrics,
                StageReportColumn::RecordsRead,
                StageReportColumn::RecordsWritten,
                StageReportColumn::Warnings,
                StageReportColumn::Errors,
                StageReportColumn::QueryFingerprints,
            ])
            .to_owned(),
        )
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn load_stage_report_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
    stage_name: MaterializationStageName,
) -> Result<Option<ControlFactorStageReportInfo>, StorageError> {
    StageReportEntity::find()
        .filter(StageReportColumn::MaterializationRunId.eq(run_id.clone()))
        .filter(StageReportColumn::StageName.eq(stage_name))
        .one(db)
        .await
        .map(|row| row.map(Into::into))
        .map_err(StorageError::from)
}

async fn list_stage_reports_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
) -> Result<Vec<ControlFactorStageReportInfo>, StorageError> {
    StageReportEntity::find()
        .filter(StageReportColumn::MaterializationRunId.eq(run_id.clone()))
        .order_by_asc(StageReportColumn::CreatedAt)
        .all(db)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
        .map_err(StorageError::from)
}

async fn create_factor_q(
    db: &impl ConnectionTrait,
    factor: NewControlFactorValue,
) -> Result<ControlFactorValueInfo, StorageError> {
    FactorEntity::insert(factor.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn transition_factor_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
    status: FactorStatus,
) -> Result<Option<ControlFactorValueInfo>, StorageError> {
    FactorEntity::update_many()
        .col_expr(FactorColumn::Status, Expr::value(status))
        .filter(FactorColumn::FactorId.eq(factor_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    load_factor_q(db, factor_id).await
}

async fn load_factor_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
) -> Result<Option<ControlFactorValueInfo>, StorageError> {
    FactorEntity::find_by_id(factor_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn create_publication_q(
    db: &impl ConnectionTrait,
    publication: NewControlFactorPublication,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    if publication.factor_ids.is_empty() {
        return Err(StorageError::Codec(
            "control-factor publication must contain at least one factor".into(),
        ));
    }
    let row = NewControlFactorPublicationRow::from(&publication);
    let model = PublicationEntity::insert(row.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    let memberships = publication
        .factor_ids
        .iter()
        .map(|factor_id| {
            NewControlFactorPublicationFactor {
                publication_id: publication.publication_id.clone(),
                factor_id: factor_id.clone(),
            }
            .into_active_model()
        })
        .collect::<Vec<_>>();
    PublicationFactorEntity::insert_many(memberships)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    enrich_publication_q(db, model).await
}

async fn activate_publication_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
    actor: &str,
    reason: &str,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let publication = load_publication_model_q(db, publication_id).await?;
    let factor_ids = load_publication_factor_ids_q(db, publication_id).await?;
    let target_status = factor_status_for_mode(publication.mode);
    for factor_id in &factor_ids {
        validate_factor_for_publication_q(db, factor_id, target_status).await?;
    }
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Superseded),
        )
        .filter(PublicationColumn::Mode.eq(publication.mode))
        .filter(PublicationColumn::Status.eq(PublicationStatus::Active))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Active),
        )
        .filter(PublicationColumn::PublicationId.eq(publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    for factor_id in &factor_ids {
        transition_factor_q(db, factor_id, target_status).await?;
    }
    append_audit_event_q(
        db,
        NewControlFactorAuditEvent {
            event_type: ControlAuditEventType::PublicationActivated,
            factor_id: None,
            publication_id: Some(publication_id.clone()),
            actor: actor.to_owned(),
            reason: reason.to_owned(),
            payload: serde_json::json!({ "mode": publication.mode.as_str() }),
        },
    )
    .await?;
    load_publication_info_q(db, publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: publication_id.to_string(),
        })
}

async fn rollback_publication_q(
    db: &impl ConnectionTrait,
    active_publication_id: &FactorPublicationId,
    target_publication_id: &FactorPublicationId,
    actor: &str,
    reason: &str,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let target = load_publication_model_q(db, target_publication_id).await?;
    let factor_ids = load_publication_factor_ids_q(db, target_publication_id).await?;
    let target_factor_status = factor_status_for_mode(target.mode);
    for factor_id in &factor_ids {
        validate_factor_for_publication_q(db, factor_id, target_factor_status).await?;
    }
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::RolledBack),
        )
        .filter(PublicationColumn::PublicationId.eq(active_publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    PublicationEntity::update_many()
        .col_expr(
            PublicationColumn::Status,
            Expr::value(PublicationStatus::Active),
        )
        .filter(PublicationColumn::PublicationId.eq(target_publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    for factor_id in &factor_ids {
        transition_factor_q(db, factor_id, target_factor_status).await?;
    }
    append_audit_event_q(
        db,
        NewControlFactorAuditEvent {
            event_type: ControlAuditEventType::PublicationRolledBack,
            factor_id: None,
            publication_id: Some(active_publication_id.clone()),
            actor: actor.to_owned(),
            reason: reason.to_owned(),
            payload: serde_json::json!({ "target_publication_id": target_publication_id }),
        },
    )
    .await?;
    load_publication_info_q(db, target_publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: target_publication_id.to_string(),
        })
}

async fn expire_factors_q(
    db: &impl ConnectionTrait,
    now: DateTime<Utc>,
) -> Result<u64, StorageError> {
    FactorEntity::update_many()
        .col_expr(FactorColumn::Status, Expr::value(FactorStatus::Expired))
        .filter(FactorColumn::ExpiresAt.lte(now))
        .filter(FactorColumn::Status.is_in([
            FactorStatus::Candidate,
            FactorStatus::Shadow,
            FactorStatus::Published,
            FactorStatus::ReportOnly,
        ]))
        .exec(db)
        .await
        .map(|result| result.rows_affected)
        .map_err(StorageError::from)
}

async fn append_audit_event_q(
    db: &impl ConnectionTrait,
    event: NewControlFactorAuditEvent,
) -> Result<ControlFactorAuditEventInfo, StorageError> {
    AuditEntity::insert(event.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn load_active_publication_q(
    db: &impl ConnectionTrait,
    mode: PublicationMode,
) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
    let model = PublicationEntity::find()
        .filter(PublicationColumn::Mode.eq(mode))
        .filter(PublicationColumn::Status.eq(PublicationStatus::Active))
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match model {
        Some(model) => enrich_publication_q(db, model).await.map(Some),
        None => Ok(None),
    }
}

async fn load_publication_info_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
    let model = PublicationEntity::find_by_id(publication_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match model {
        Some(model) => enrich_publication_q(db, model).await.map(Some),
        None => Ok(None),
    }
}

async fn load_publication_model_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<PublicationModel, StorageError> {
    PublicationEntity::find_by_id(publication_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: publication_id.to_string(),
        })
}

async fn enrich_publication_q(
    db: &impl ConnectionTrait,
    model: PublicationModel,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let publication_id = model.publication_id.clone();
    let factor_ids = load_publication_factor_ids_q(db, &publication_id).await?;
    Ok(ControlFactorPublicationRowInfo::from(model).with_factor_ids(factor_ids))
}

async fn load_publication_factor_ids_q(
    db: &impl ConnectionTrait,
    publication_id: &FactorPublicationId,
) -> Result<Vec<ControlFactorId>, StorageError> {
    PublicationFactorEntity::find()
        .filter(PublicationFactorColumn::PublicationId.eq(publication_id.clone()))
        .all(db)
        .await
        .map(|rows| rows.into_iter().map(|row| row.factor_id).collect())
        .map_err(StorageError::from)
}

async fn validate_factor_for_publication_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
    target_status: FactorStatus,
) -> Result<(), StorageError> {
    let info = load_factor_q(db, factor_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_value",
            id: factor_id.to_string(),
        })?;
    let typed = ControlFactorValue::from_info(&info)
        .map_err(|error| StorageError::Codec(error.to_string()))?;
    typed
        .validate_for_transition(target_status, None)
        .map_err(|error| StorageError::Codec(error.to_string()))
}

const fn factor_status_for_mode(mode: PublicationMode) -> FactorStatus {
    match mode {
        PublicationMode::Shadow => FactorStatus::Shadow,
        PublicationMode::Published => FactorStatus::Published,
    }
}

const fn is_active_run_status(status: MaterializationRunStatus) -> bool {
    matches!(
        status,
        MaterializationRunStatus::Queued | MaterializationRunStatus::Running
    )
}

const fn is_terminal_run_status(status: MaterializationRunStatus) -> bool {
    matches!(
        status,
        MaterializationRunStatus::Completed
            | MaterializationRunStatus::CompletedWithRejectedFactors
            | MaterializationRunStatus::ReportOnly
            | MaterializationRunStatus::Failed
            | MaterializationRunStatus::Cancelled
    )
}

#[async_trait::async_trait]
impl ControlFactorRepository for PgControlFactorRepository {
    async fn enqueue_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
        options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
        enqueue_materialization_run_q(&self.db, run, options).await
    }

    async fn load_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        load_materialization_run_q(&self.db, run_id).await
    }

    async fn find_materialization_run_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        find_materialization_run_by_dedupe_key_q(&self.db, dedupe_key).await
    }

    async fn try_acquire_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let outcome = try_acquire_materialization_run_q(&txn, run_id, started_at).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn retry_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let outcome = retry_materialization_run_q(&txn, run_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn transition_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        expected_from: MaterializationRunStatus,
        target: MaterializationRunStatus,
        patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let outcome =
            transition_materialization_run_q(&txn, run_id, expected_from, target, patch).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn cancel_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        reason: &str,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let outcome = cancel_materialization_run_q(&txn, run_id, reason, cancelled_at).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn upsert_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        upsert_stage_report_q(&self.db, report).await
    }

    async fn load_stage_report(
        &self,
        run_id: &MaterializationRunId,
        stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError> {
        load_stage_report_q(&self.db, run_id, stage_name).await
    }

    async fn list_stage_reports(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError> {
        list_stage_reports_q(&self.db, run_id).await
    }

    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        create_factor_q(&self.db, factor).await
    }

    async fn transition_factor(
        &self,
        factor_id: &ControlFactorId,
        status: FactorStatus,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        transition_factor_q(&self.db, factor_id, status).await
    }

    async fn create_publication(
        &self,
        publication: NewControlFactorPublication,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        create_publication_q(&self.db, publication).await
    }

    async fn activate_publication(
        &self,
        publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let publication = activate_publication_q(&txn, publication_id, actor, reason).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(publication)
    }

    async fn rollback_publication(
        &self,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let publication = rollback_publication_q(
            &txn,
            active_publication_id,
            target_publication_id,
            actor,
            reason,
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(publication)
    }

    async fn expire_factors(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        expire_factors_q(&self.db, now).await
    }

    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        append_audit_event_q(&self.db, event).await
    }

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        load_active_publication_q(&self.db, mode).await
    }
}
