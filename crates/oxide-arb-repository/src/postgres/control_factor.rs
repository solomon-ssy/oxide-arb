use crate::traits::ControlFactorRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, AuditActor, AuditEventContent,
        CancelMaterializationRunOutcome, ControlFactorAuditEventInfo,
        ControlFactorMaterializationRunInfo, ControlFactorPublication,
        ControlFactorPublicationInfo, ControlFactorPublicationRowInfo,
        ControlFactorStageReportInfo, ControlFactorValue, ControlFactorValueInfo,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome, ExpireFactorsOutcome,
        MaterializationRunStatusPatch, NewControlFactorAuditEvent,
        NewControlFactorMaterializationRun, NewControlFactorPublication,
        NewControlFactorPublicationFactor, NewControlFactorPublicationRow,
        NewControlFactorStageReport, NewControlFactorValue, PublishPublicationOutcome,
        RunTransitionOutcome,
    },
    entities::{
        control_factor_audit_event::{
            ActiveModel as AuditActiveModel, Column as AuditColumn, Entity as AuditEntity,
        },
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
        AuditResourceType, ControlAuditEventType, ControlFactorType, FactorStatus,
        MaterializationRunStatus, MaterializationStageName, PublicationMode, PublicationStatus,
    },
    types::{AuditEventId, ControlFactorId, FactorPublicationId, MaterializationRunId},
};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, OnConflict},
};

/// Advisory-lock key serializing all appends to the single global audit chain.
const AUDIT_CHAIN_LOCK_KEY: i64 = 5_500_000_000_000_001;
/// Advisory-lock key base for serializing publication activation per mode.
const PUBLICATION_LOCK_BASE: i64 = 5_500_000_000_000_100;

pub struct PgControlFactorRepository {
    db: DatabaseConnection,
}

impl PgControlFactorRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

const fn publication_lock_key(mode: PublicationMode) -> i64 {
    match mode {
        PublicationMode::Shadow => PUBLICATION_LOCK_BASE + 1,
        PublicationMode::Published => PUBLICATION_LOCK_BASE + 2,
    }
}

async fn advisory_xact_lock(db: &impl ConnectionTrait, key: i64) -> Result<(), StorageError> {
    db.execute_unprepared(&format!("SELECT pg_advisory_xact_lock({key})"))
        .await
        .map(|_| ())
        .map_err(StorageError::from)
}

// ── Materialization runs ────────────────────────────────────────────────

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

async fn latest_run_for_schedule_q(
    db: &impl ConnectionTrait,
    schedule_id: &str,
    statuses: &[MaterializationRunStatus],
) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
    let mut query = RunEntity::find().filter(RunColumn::TriggerRef.eq(schedule_id));
    if !statuses.is_empty() {
        query = query.filter(RunColumn::Status.is_in(statuses.iter().copied()));
    }
    query
        .order_by_desc(RunColumn::CreatedAt)
        .one(db)
        .await
        .map(|row| row.map(Into::into))
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

// ── Stage reports ─────────────────────────────────────────────────────────

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

// ── Factor values ──────────────────────────────────────────────────────────

async fn insert_factor_q(
    db: &impl ConnectionTrait,
    factor: NewControlFactorValue,
) -> Result<ControlFactorValueInfo, StorageError> {
    FactorEntity::insert(factor.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

/// Sets a factor status (and optionally its `status_reason`). Internal helper;
/// callers must validate the lifecycle transition and append a chained audit.
async fn set_factor_status_q(
    db: &impl ConnectionTrait,
    factor_id: &ControlFactorId,
    status: FactorStatus,
    status_reason: Option<&str>,
) -> Result<(), StorageError> {
    let mut update =
        FactorEntity::update_many().col_expr(FactorColumn::Status, Expr::value(status));
    if let Some(reason) = status_reason {
        update = update.col_expr(FactorColumn::StatusReason, Expr::value(reason));
    }
    update
        .filter(FactorColumn::FactorId.eq(factor_id.clone()))
        .exec(db)
        .await
        .map(|_| ())
        .map_err(StorageError::from)
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

async fn list_factors_by_run_q(
    db: &impl ConnectionTrait,
    run_id: &MaterializationRunId,
) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
    FactorEntity::find()
        .filter(FactorColumn::RunId.eq(run_id.clone()))
        .order_by_asc(FactorColumn::GeneratedAt)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|models| models.into_iter().map(Into::into).collect())
}

async fn list_factors_by_status_q(
    db: &impl ConnectionTrait,
    status: FactorStatus,
    factor_type: Option<ControlFactorType>,
) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
    let mut query = FactorEntity::find().filter(FactorColumn::Status.eq(status));
    if let Some(factor_type) = factor_type {
        query = query.filter(FactorColumn::FactorType.eq(factor_type));
    }
    query
        .order_by_asc(FactorColumn::ExpiresAt)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|models| models.into_iter().map(Into::into).collect())
}

fn typed_factor(info: &ControlFactorValueInfo) -> Result<ControlFactorValue, StorageError> {
    ControlFactorValue::from_info(info).map_err(|error| StorageError::Codec(error.to_string()))
}

// ── Audit chain ─────────────────────────────────────────────────────────────

async fn find_audit_event_by_idempotency_q(
    db: &impl ConnectionTrait,
    request_id: &str,
    event_type: ControlAuditEventType,
    resource_id: &str,
) -> Result<Option<ControlFactorAuditEventInfo>, StorageError> {
    AuditEntity::find()
        .filter(AuditColumn::RequestId.eq(request_id))
        .filter(AuditColumn::EventType.eq(event_type))
        .filter(AuditColumn::ResourceId.eq(resource_id))
        .one(db)
        .await
        .map(|row| row.map(Into::into))
        .map_err(StorageError::from)
}

/// Appends one event to the global audit hash chain. Caller must already be in a
/// transaction; this acquires the chain advisory lock, enforces
/// `(request_id, event_type, resource_id)` idempotency, links `prev_event_hash`,
/// and computes the tamper-evident `event_hash`. Shared with runtime-config
/// governance so its activations participate in the same global chain.
pub(crate) async fn append_audit_event_chained_q(
    db: &impl ConnectionTrait,
    event: NewControlFactorAuditEvent,
    now: DateTime<Utc>,
) -> Result<ControlFactorAuditEventInfo, StorageError> {
    advisory_xact_lock(db, AUDIT_CHAIN_LOCK_KEY).await?;

    if let Some(existing) = find_audit_event_by_idempotency_q(
        db,
        &event.request_id,
        event.event_type,
        &event.resource_id,
    )
    .await?
    {
        return Ok(existing);
    }

    let tip = AuditEntity::find()
        .order_by_desc(AuditColumn::Sequence)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    let (sequence, prev_event_hash) = match tip {
        Some(tip) => (tip.sequence + 1, Some(tip.event_hash)),
        None => (1, None),
    };

    let event_hash = AuditEventContent {
        sequence,
        event_type: event.event_type,
        actor: event.actor.as_str(),
        actor_role: event.actor_role,
        resource_type: event.resource_type,
        resource_id: event.resource_id.as_str(),
        request_id: event.request_id.as_str(),
        reason: event.reason.as_str(),
        before_hash: event.before_hash.as_deref(),
        after_hash: event.after_hash.as_deref(),
        diff: &event.diff,
        prev_event_hash: prev_event_hash.as_deref(),
        created_at: now,
    }
    .event_hash()
    .map_err(|error| StorageError::Codec(error.to_string()))?;

    let model = AuditActiveModel {
        event_id: Set(AuditEventId::new_v7()),
        sequence: Set(sequence),
        event_type: Set(event.event_type),
        actor: Set(event.actor),
        actor_role: Set(event.actor_role),
        resource_type: Set(event.resource_type),
        resource_id: Set(event.resource_id),
        request_id: Set(event.request_id),
        reason: Set(event.reason),
        before_hash: Set(event.before_hash),
        after_hash: Set(event.after_hash),
        diff: Set(event.diff),
        prev_event_hash: Set(prev_event_hash),
        event_hash: Set(event_hash),
        created_at: Set(now),
    };
    AuditEntity::insert(model)
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn load_audit_chain_q(
    db: &impl ConnectionTrait,
    from_sequence: i64,
    limit: u64,
) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
    AuditEntity::find()
        .filter(AuditColumn::Sequence.gte(from_sequence))
        .order_by_asc(AuditColumn::Sequence)
        .limit(limit)
        .all(db)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
        .map_err(StorageError::from)
}

// ── Publications ────────────────────────────────────────────────────────────

async fn find_publication_by_idempotency_q(
    db: &impl ConnectionTrait,
    idempotency_key: &str,
) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
    let model = PublicationEntity::find()
        .filter(PublicationColumn::IdempotencyKey.eq(idempotency_key))
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match model {
        Some(model) => enrich_publication_q(db, model).await.map(Some),
        None => Ok(None),
    }
}

async fn publish_publication_q(
    db: &impl ConnectionTrait,
    publication: NewControlFactorPublication,
    audit: NewControlFactorAuditEvent,
    now: DateTime<Utc>,
) -> Result<PublishPublicationOutcome, StorageError> {
    if publication.factor_ids.is_empty() {
        return Err(StorageError::Codec(
            "control-factor publication must contain at least one factor".into(),
        ));
    }

    if let Some(existing) =
        find_publication_by_idempotency_q(db, &publication.idempotency_key).await?
    {
        return Ok(PublishPublicationOutcome::AlreadyApplied(existing));
    }

    advisory_xact_lock(db, publication_lock_key(publication.mode)).await?;

    // Re-check after acquiring the lock to resolve concurrent first-writer races.
    if let Some(existing) =
        find_publication_by_idempotency_q(db, &publication.idempotency_key).await?
    {
        return Ok(PublishPublicationOutcome::AlreadyApplied(existing));
    }

    // Re-read members under the lock and validate the activation (TOCTOU guard).
    let mut factors = Vec::with_capacity(publication.factor_ids.len());
    for factor_id in &publication.factor_ids {
        let info = load_factor_q(db, factor_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "control_factor_value",
                id: factor_id.to_string(),
            })?;
        factors.push(typed_factor(&info)?);
    }
    let domain = publication_for_validation(&publication);
    domain
        .validate_for_activation(&factors)
        .map_err(|error| StorageError::Conflict(error.to_string()))?;
    let target_status = domain.target_factor_status();

    // Insert the publication header (Pending) and membership.
    let mut row = NewControlFactorPublicationRow::from(&publication).into_active_model();
    row.status = Set(PublicationStatus::Pending);
    PublicationEntity::insert(row)
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

    // Supersede the current active publication for this mode, then activate.
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
        .filter(PublicationColumn::PublicationId.eq(publication.publication_id.clone()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    for factor_id in &publication.factor_ids {
        set_factor_status_q(db, factor_id, target_status, None).await?;
    }

    append_audit_event_chained_q(db, audit, now).await?;

    let info = load_publication_info_q(db, &publication.publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: publication.publication_id.to_string(),
        })?;
    Ok(PublishPublicationOutcome::Published(info))
}

async fn rollback_publication_q(
    db: &impl ConnectionTrait,
    active_publication_id: &FactorPublicationId,
    target_publication_id: &FactorPublicationId,
    audit: NewControlFactorAuditEvent,
    now: DateTime<Utc>,
) -> Result<ControlFactorPublicationInfo, StorageError> {
    let target = load_publication_model_q(db, target_publication_id).await?;
    advisory_xact_lock(db, publication_lock_key(target.mode)).await?;

    let factor_ids = load_publication_factor_ids_q(db, target_publication_id).await?;
    let mut factors = Vec::with_capacity(factor_ids.len());
    for factor_id in &factor_ids {
        let info = load_factor_q(db, factor_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "control_factor_value",
                id: factor_id.to_string(),
            })?;
        factors.push(typed_factor(&info)?);
    }
    let target_factor_status = factor_status_for_mode(target.mode);
    for factor in &factors {
        factor
            .validate_for_transition(target_factor_status, None)
            .map_err(|error| StorageError::Conflict(error.to_string()))?;
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
        set_factor_status_q(db, factor_id, target_factor_status, None).await?;
    }

    append_audit_event_chained_q(db, audit, now).await?;

    load_publication_info_q(db, target_publication_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "control_factor_publication",
            id: target_publication_id.to_string(),
        })
}

async fn list_publications_q(
    db: &impl ConnectionTrait,
    mode: PublicationMode,
    status: Option<PublicationStatus>,
    limit: u64,
) -> Result<Vec<ControlFactorPublicationInfo>, StorageError> {
    let mut query = PublicationEntity::find().filter(PublicationColumn::Mode.eq(mode));
    if let Some(status) = status {
        query = query.filter(PublicationColumn::Status.eq(status));
    }
    let models = query
        .order_by_desc(PublicationColumn::EffectiveFrom)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let mut out = Vec::with_capacity(models.len());
    for model in models {
        out.push(enrich_publication_q(db, model).await?);
    }
    Ok(out)
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

fn publication_for_validation(
    publication: &NewControlFactorPublication,
) -> ControlFactorPublication {
    ControlFactorPublication {
        publication_id: publication.publication_id.clone(),
        mode: publication.mode,
        factor_ids: publication.factor_ids.clone(),
        previous_publication_id: publication.previous_publication_id.clone(),
        status: PublicationStatus::Pending,
        effective_from: publication.effective_from,
        expires_at: publication.expires_at,
        approved_by: publication.approved_by.clone(),
        approval_reason: publication.approval_reason.clone(),
        publication_hash: publication.publication_hash.clone(),
    }
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

    async fn latest_run_for_schedule(
        &self,
        schedule_id: &str,
        statuses: &[MaterializationRunStatus],
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        latest_run_for_schedule_q(&self.db, schedule_id, statuses).await
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
        audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info = insert_factor_q(&txn, factor).await?;
        append_audit_event_chained_q(&txn, audit, Utc::now()).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn load_factor(
        &self,
        factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        load_factor_q(&self.db, factor_id).await
    }

    async fn list_factors_by_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        list_factors_by_run_q(&self.db, run_id).await
    }

    async fn list_factors_by_status(
        &self,
        status: FactorStatus,
        factor_type: Option<ControlFactorType>,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        list_factors_by_status_q(&self.db, status, factor_type).await
    }

    async fn reject_factor(
        &self,
        factor_id: &ControlFactorId,
        status_reason: &str,
        audit: NewControlFactorAuditEvent,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(info) = load_factor_q(&txn, factor_id).await? else {
            txn.rollback().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let typed = typed_factor(&info)?;
        typed
            .validate_for_transition(FactorStatus::Rejected, None)
            .map_err(|error| StorageError::Conflict(error.to_string()))?;
        set_factor_status_q(&txn, factor_id, FactorStatus::Rejected, Some(status_reason)).await?;
        append_audit_event_chained_q(&txn, audit, Utc::now()).await?;
        let updated = load_factor_q(&txn, factor_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated)
    }

    async fn expire_factors(
        &self,
        now: DateTime<Utc>,
        actor: AuditActor,
    ) -> Result<ExpireFactorsOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let due = FactorEntity::find()
            .filter(FactorColumn::ExpiresAt.lte(now))
            .filter(FactorColumn::Status.is_in([
                FactorStatus::Candidate,
                FactorStatus::Shadow,
                FactorStatus::Published,
                FactorStatus::ReportOnly,
            ]))
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut expired = Vec::with_capacity(due.len());
        for model in due {
            let from_status = model.status;
            set_factor_status_q(&txn, &model.factor_id, FactorStatus::Expired, None).await?;
            let audit = NewControlFactorAuditEvent {
                event_type: ControlAuditEventType::FactorExpired,
                actor: actor.actor.clone(),
                actor_role: actor.actor_role,
                resource_type: AuditResourceType::Factor,
                resource_id: model.factor_id.as_str().to_owned(),
                request_id: actor.request_id.clone(),
                reason: actor.reason.clone(),
                before_hash: None,
                after_hash: None,
                diff: serde_json::json!({
                    "from_status": from_status,
                    "to_status": FactorStatus::Expired,
                    "expires_at": model.expires_at,
                }),
            };
            append_audit_event_chained_q(&txn, audit, now).await?;
            expired.push(model.factor_id);
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(ExpireFactorsOutcome { expired })
    }

    async fn publish_publication(
        &self,
        publication: NewControlFactorPublication,
        audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let outcome = publish_publication_q(&txn, publication, audit, Utc::now()).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn load_publication(
        &self,
        publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        load_publication_info_q(&self.db, publication_id).await
    }

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        load_active_publication_q(&self.db, mode).await
    }

    async fn list_publications(
        &self,
        mode: PublicationMode,
        status: Option<PublicationStatus>,
        limit: u64,
    ) -> Result<Vec<ControlFactorPublicationInfo>, StorageError> {
        list_publications_q(&self.db, mode, status, limit).await
    }

    async fn rollback_publication(
        &self,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let publication = rollback_publication_q(
            &txn,
            active_publication_id,
            target_publication_id,
            audit,
            Utc::now(),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(publication)
    }

    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info = append_audit_event_chained_q(&txn, event, Utc::now()).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn load_audit_chain(
        &self,
        from_sequence: i64,
        limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
        load_audit_chain_q(&self.db, from_sequence, limit).await
    }
}
