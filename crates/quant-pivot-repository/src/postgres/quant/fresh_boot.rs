//! `PostgreSQL` event-sourced repository for fresh-boot orchestration.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        AdvanceFreshBootRun, BlockFreshBootRun, DelayFreshBootRun, FRESH_BOOT_MAX_RETRY_COUNT,
        FreshBootRunEventInfo, FreshBootRunEventInput, FreshBootRunInfo, NewFreshBootRun,
        NewFreshBootRunEvent, SupersedeFreshBootRun,
    },
    entities::{
        quant_fresh_boot_run::{
            ActiveModel as FreshBootActiveModel, Column as FreshBootColumn,
            Entity as FreshBootEntity, Model as FreshBootModel,
        },
        quant_fresh_boot_run_event::{Column as EventColumn, Entity as EventEntity},
    },
    enums::quant::{FreshBootBlockedReason, FreshBootEventKind, FreshBootStage, FreshBootStatus},
    types::{FreshBootRunId, PolicyIdempotencyKey, ResearchJobId, WorkerId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, Condition, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait, sea_query::OnConflict,
};
use uuid::Uuid;

use crate::traits::FreshBootRepository;

const ENTITY: &str = "quant_fresh_boot_run";
const ORCHESTRATOR_ACTOR: &str = "fresh_boot_orchestrator";
const MAX_CLAIM_LIMIT: u64 = 100;

/// Sole relational persistence adapter for the fresh-boot state machine.
pub struct PgFreshBootRepository {
    db: DatabaseConnection,
}

impl PgFreshBootRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn verify_exact(
        existing: &FreshBootRunInfo,
        requested: &NewFreshBootRun,
    ) -> Result<(), StorageError> {
        if existing.run_id != requested.run_id
            || existing.supersedes_run_id != requested.supersedes_run_id
            || existing.research_profile_artifact_id != requested.research_profile_artifact_id
            || existing.profile_hash != requested.profile_hash
            || existing.route != requested.route
            || existing.decision_policy_snapshot_id != requested.decision_policy_snapshot_id
            || existing.idempotency_key != requested.idempotency_key
        {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(existing.run_id),
                "fresh-boot idempotency key was replayed with a different immutable preimage",
            ));
        }
        Ok(())
    }

    async fn lock(
        transaction: &DatabaseTransaction,
        run_id: FreshBootRunId,
    ) -> Result<FreshBootModel, StorageError> {
        FreshBootEntity::find_by_id(run_id)
            .lock_exclusive()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(ENTITY, run_id))
    }

    fn ensure_revision(current: &FreshBootModel, expected: i64) -> Result<(), StorageError> {
        if current.revision != expected {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(current.run_id),
                "fresh-boot revision changed before transition",
            ));
        }
        Ok(())
    }

    fn ensure_time(
        current: &FreshBootModel,
        occurred_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        if occurred_at < current.updated_at {
            return Err(StorageError::invariant_violation(
                Some(ENTITY),
                "fresh-boot event time precedes the durable projection",
            ));
        }
        Ok(())
    }

    async fn persist_event(
        transaction: &DatabaseTransaction,
        input: FreshBootRunEventInput,
    ) -> Result<(), StorageError> {
        let event = NewFreshBootRunEvent::try_seal(input)?;
        EventEntity::insert(event.into_active_model())
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    fn apply_patch(active: &mut FreshBootActiveModel, command: &AdvanceFreshBootRun) {
        let patch = &command.patch;
        if let Some(value) = patch.source_coverage_manifest.clone() {
            active.source_coverage_manifest = Set(Some(value));
        }
        if let Some(value) = patch.source_coverage_hash {
            active.source_coverage_hash = Set(Some(value));
        }
        if let Some(value) = patch.source_slice_id {
            active.source_slice_id = Set(Some(value));
        }
        if let Some(value) = patch.source_slice_hash {
            active.source_slice_hash = Set(Some(value));
        }
        if let Some(value) = patch.model_spec_id {
            active.model_spec_id = Set(Some(value));
        }
        if let Some(value) = patch.training_dataset_id {
            active.training_dataset_id = Set(Some(value));
        }
        if let Some(value) = patch.calibration_dataset_id {
            active.calibration_dataset_id = Set(Some(value));
        }
        if let Some(value) = patch.source_model_version_id {
            active.source_model_version_id = Set(Some(value));
        }
        if let Some(value) = patch.model_version_id {
            active.model_version_id = Set(Some(value));
        }
        if let Some(value) = patch.path_set_id {
            active.path_set_id = Set(Some(value));
        }
        if let Some(value) = patch.calibration_id {
            active.calibration_id = Set(Some(value));
        }
        if let Some(value) = patch.parity_run_id {
            active.parity_run_id = Set(Some(value));
        }
        if let Some(value) = patch.scenario_artifact_id {
            active.scenario_artifact_id = Set(Some(value));
        }
        if let Some(value) = patch.scenario_artifact_hash {
            active.scenario_artifact_hash = Set(Some(value));
        }
        if let Some(value) = patch.bootstrap_preflight.clone() {
            active.bootstrap_preflight = Set(Some(value));
        }
        if let Some(value) = patch.bootstrap_preflight_hash {
            active.bootstrap_preflight_hash = Set(Some(value));
        }
        if let Some(value) = patch.active_job_id {
            active.active_job_id = Set(value);
        }
        if let Some(value) = patch.last_job_id {
            active.last_job_id = Set(Some(value));
        }
        if let Some(value) = patch.bootstrap_policy_activation_id {
            active.bootstrap_policy_activation_id = Set(Some(value));
        }
        if let Some(value) = patch.manual_report_ready_at {
            active.manual_report_ready_at = Set(Some(value));
        }
        if let Some(value) = patch.first_report_run_id {
            active.first_report_run_id = Set(Some(value));
        }
        if let Some(value) = patch.first_report_id {
            active.first_report_id = Set(Some(value));
        }
        if let Some(value) = patch.next_scheduled_report_at {
            active.next_scheduled_report_at = Set(Some(value));
        }
        if let Some(value) = patch.retry_count {
            active.retry_count = Set(value);
        }
    }

    fn event_job(
        command: &AdvanceFreshBootRun,
        current: &FreshBootRunInfo,
    ) -> Option<ResearchJobId> {
        command
            .patch
            .active_job_id
            .flatten()
            .or(command.patch.last_job_id)
            .or(current.active_job_id)
    }

    fn event_result(command: &AdvanceFreshBootRun, current: &FreshBootRunInfo) -> Option<Uuid> {
        let patch = &command.patch;
        match command.event {
            FreshBootEventKind::SourceCoverageSatisfied => {
                patch.training_dataset_id.map(|id| *id.as_uuid_ref())
            }
            FreshBootEventKind::DatasetCompleted => {
                patch.source_slice_id.map(|id| *id.as_uuid_ref())
            }
            FreshBootEventKind::TrainingEnqueued | FreshBootEventKind::TrainingCompleted => patch
                .source_model_version_id
                .or(current.source_model_version_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::CalibrationDatasetEnqueued
            | FreshBootEventKind::CalibrationDatasetCompleted => patch
                .calibration_dataset_id
                .or(current.calibration_dataset_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::CalibrationCompleted => patch
                .model_version_id
                .or(current.model_version_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::CpcvEnqueued | FreshBootEventKind::CpcvCompleted => patch
                .path_set_id
                .or(current.path_set_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::ParityVerified => patch.parity_run_id.map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::ScenarioBound
            | FreshBootEventKind::BootstrapPrepared
            | FreshBootEventKind::PreflightRefreshed => patch
                .scenario_artifact_id
                .or(current.scenario_artifact_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::BootstrapCommitted => patch
                .bootstrap_policy_activation_id
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::ReportEnabled | FreshBootEventKind::ReportRetried => patch
                .first_report_run_id
                .or(current.first_report_run_id)
                .map(|id| *id.as_uuid_ref()),
            FreshBootEventKind::ReportPublished => {
                patch.first_report_id.map(|id| *id.as_uuid_ref())
            }
            _ => None,
        }
    }

    async fn create_event_if_missing(
        transaction: &DatabaseTransaction,
        run: &FreshBootRunInfo,
        actor: &str,
        detail: Option<String>,
        result_ref: Option<Uuid>,
    ) -> Result<(), StorageError> {
        let existing = EventEntity::find()
            .filter(EventColumn::RunId.eq(run.run_id))
            .filter(EventColumn::EventSequence.eq(0_i64))
            .one(transaction)
            .await
            .map_err(StorageError::from)?;
        if existing.is_none() {
            Self::persist_event(
                transaction,
                FreshBootRunEventInput {
                    run_id: run.run_id,
                    event_sequence: 0,
                    from_stage: run.stage,
                    to_stage: run.stage,
                    from_status: run.status,
                    to_status: run.status,
                    event_kind: FreshBootEventKind::RunCreated,
                    research_job_id: None,
                    result_ref,
                    evidence_hash: Some(run.profile_hash),
                    attempt: run.retry_count,
                    actor: actor.to_owned(),
                    detail,
                    occurred_at: run.created_at,
                },
            )
            .await?;
        }
        Ok(())
    }

    const fn retryable_status(status: FreshBootStatus) -> bool {
        matches!(
            status,
            FreshBootStatus::WaitingEvidence | FreshBootStatus::RetryScheduled
        )
    }

    fn operator_reason(reason: &str) -> Result<String, StorageError> {
        let reason = reason.trim();
        if reason.is_empty() || reason.len() > 1_024 {
            return Err(StorageError::invariant_violation(
                Some(ENTITY),
                "fresh-boot operator reason must contain 1..=1024 bytes",
            ));
        }
        Ok(reason.to_owned())
    }
}

#[async_trait::async_trait]
impl FreshBootRepository for PgFreshBootRepository {
    async fn create_or_load(&self, run: NewFreshBootRun) -> Result<FreshBootRunInfo, StorageError> {
        let requested = run.clone();
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        FreshBootEntity::insert(run.into_active_model())
            .on_conflict(
                OnConflict::column(FreshBootColumn::RunId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let locked = Self::lock(&transaction, requested.run_id).await?;
        let existing = FreshBootRunInfo::from(locked);
        Self::verify_exact(&existing, &requested)?;
        Self::create_event_if_missing(&transaction, &existing, ORCHESTRATOR_ACTOR, None, None)
            .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(existing)
    }

    async fn find(
        &self,
        run_id: &FreshBootRunId,
    ) -> Result<Option<FreshBootRunInfo>, StorageError> {
        FreshBootEntity::find_by_id(*run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_key(
        &self,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> Result<Option<FreshBootRunInfo>, StorageError> {
        FreshBootEntity::find()
            .filter(FreshBootColumn::IdempotencyKey.eq(idempotency_key.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_latest(&self) -> Result<Vec<FreshBootRunInfo>, StorageError> {
        FreshBootEntity::find()
            .filter(FreshBootColumn::Status.ne(FreshBootStatus::Superseded))
            .order_by_asc(FreshBootColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn list_events(
        &self,
        run_id: FreshBootRunId,
    ) -> Result<Vec<FreshBootRunEventInfo>, StorageError> {
        EventEntity::find()
            .filter(EventColumn::RunId.eq(run_id))
            .order_by_asc(EventColumn::EventSequence)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn claim_due(
        &self,
        worker_id: WorkerId,
        claimed_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<FreshBootRunInfo>, StorageError> {
        if limit == 0 || limit > MAX_CLAIM_LIMIT || lease_expires_at <= claimed_at {
            return Err(StorageError::invariant_violation(
                Some(ENTITY),
                "claim limit must be 1..=100 and lease must end after claim time",
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let running_due = Condition::all()
            .add(FreshBootColumn::Status.eq(FreshBootStatus::Running))
            .add(
                Condition::any()
                    .add(FreshBootColumn::LeaseExpiresAt.is_null())
                    .add(FreshBootColumn::LeaseExpiresAt.lte(claimed_at)),
            );
        let delayed_due = Condition::all()
            .add(FreshBootColumn::Status.is_in([
                FreshBootStatus::WaitingEvidence,
                FreshBootStatus::RetryScheduled,
            ]))
            .add(FreshBootColumn::NextAttemptAt.lte(claimed_at));
        let rows = FreshBootEntity::find()
            .filter(Condition::any().add(running_due).add(delayed_due))
            .order_by_asc(FreshBootColumn::NextAttemptAt)
            .order_by_asc(FreshBootColumn::CreatedAt)
            .limit(limit)
            .lock_exclusive()
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let previous = FreshBootRunInfo::from(row.clone());
            let mut active = row.into_active_model();
            let reclaims_expired_lease =
                previous.status == FreshBootStatus::Running && previous.lease_expires_at.is_some();
            if reclaims_expired_lease && previous.retry_count >= FRESH_BOOT_MAX_RETRY_COUNT {
                let detail = format!(
                    "fresh-boot worker lease expired {} times at stage {}",
                    previous.retry_count, previous.stage
                );
                let next_revision = previous.revision.saturating_add(1);
                active.status = Set(FreshBootStatus::BlockedTerminal);
                active.blocked_reason = Set(Some(FreshBootBlockedReason::RetryBudgetExhausted));
                active.blocked_detail = Set(Some(detail.clone()));
                active.lease_owner = Set(None);
                active.lease_expires_at = Set(None);
                active.revision = Set(next_revision);
                active.completed_at = Set(Some(claimed_at));
                active.updated_at = Set(claimed_at);
                active
                    .update(&transaction)
                    .await
                    .map_err(StorageError::from)?;
                Self::persist_event(
                    &transaction,
                    FreshBootRunEventInput {
                        run_id: previous.run_id,
                        event_sequence: next_revision,
                        from_stage: previous.stage,
                        to_stage: previous.stage,
                        from_status: previous.status,
                        to_status: FreshBootStatus::BlockedTerminal,
                        event_kind: FreshBootEventKind::TerminalBlocked,
                        research_job_id: previous.active_job_id,
                        result_ref: None,
                        evidence_hash: None,
                        attempt: previous.retry_count,
                        actor: ORCHESTRATOR_ACTOR.to_owned(),
                        detail: Some(detail),
                        occurred_at: claimed_at,
                    },
                )
                .await?;
                continue;
            }
            active.lease_owner = Set(Some(worker_id));
            active.lease_expires_at = Set(Some(lease_expires_at));
            if Self::retryable_status(previous.status) || reclaims_expired_lease {
                let next_retry_count = if reclaims_expired_lease {
                    previous.retry_count.saturating_add(1)
                } else {
                    previous.retry_count
                };
                active.status = Set(FreshBootStatus::Running);
                active.retry_reason = Set(None);
                active.retry_detail = Set(None);
                active.next_attempt_at = Set(None);
                active.retry_count = Set(next_retry_count);
                active.revision = Set(previous.revision.saturating_add(1));
                active.updated_at = Set(claimed_at);
                Self::persist_event(
                    &transaction,
                    FreshBootRunEventInput {
                        run_id: previous.run_id,
                        event_sequence: previous.revision.saturating_add(1),
                        from_stage: previous.stage,
                        to_stage: previous.stage,
                        from_status: previous.status,
                        to_status: FreshBootStatus::Running,
                        event_kind: FreshBootEventKind::RetryStarted,
                        research_job_id: previous.active_job_id,
                        result_ref: None,
                        evidence_hash: None,
                        attempt: next_retry_count,
                        actor: ORCHESTRATOR_ACTOR.to_owned(),
                        detail: reclaims_expired_lease
                            .then(|| "expired worker lease was reclaimed".to_owned()),
                        occurred_at: claimed_at,
                    },
                )
                .await?;
            }
            let updated = active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
            claimed.push(updated.into());
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(claimed)
    }

    async fn advance(
        &self,
        command: AdvanceFreshBootRun,
    ) -> Result<FreshBootRunInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::lock(&transaction, command.run_id).await?;
        Self::ensure_revision(&current, command.expected_revision)?;
        Self::ensure_time(&current, command.occurred_at)?;
        let info = FreshBootRunInfo::from(current.clone());
        if info.status != FreshBootStatus::Running {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(command.run_id),
                "only a running fresh-boot run can advance its stage",
            ));
        }
        let next_stage = info.next_stage(command.event)?;
        let next_status = if next_stage == FreshBootStage::FirstReportPublished {
            FreshBootStatus::Succeeded
        } else {
            FreshBootStatus::Running
        };
        let next_revision = info.revision.saturating_add(1);
        let research_job_id = Self::event_job(&command, &info);
        let result_ref = Self::event_result(&command, &info);
        let attempt = command.patch.retry_count.unwrap_or(info.retry_count);
        let mut active = current.into_active_model();
        active.stage = Set(next_stage);
        active.status = Set(next_status);
        active.retry_reason = Set(None);
        active.retry_detail = Set(None);
        active.next_attempt_at = Set(None);
        active.blocked_reason = Set(None);
        active.blocked_detail = Set(None);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.revision = Set(next_revision);
        active.stage_entered_at = Set(command.occurred_at);
        active.updated_at = Set(command.occurred_at);
        if next_status == FreshBootStatus::Succeeded {
            active.completed_at = Set(Some(command.occurred_at));
        }
        Self::apply_patch(&mut active, &command);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::persist_event(
            &transaction,
            FreshBootRunEventInput {
                run_id: info.run_id,
                event_sequence: next_revision,
                from_stage: info.stage,
                to_stage: next_stage,
                from_status: info.status,
                to_status: next_status,
                event_kind: command.event,
                research_job_id,
                result_ref,
                evidence_hash: command.evidence_hash,
                attempt,
                actor: command.actor,
                detail: command.detail,
                occurred_at: command.occurred_at,
            },
        )
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn delay(&self, command: DelayFreshBootRun) -> Result<FreshBootRunInfo, StorageError> {
        if !Self::retryable_status(command.status) || command.next_attempt_at <= command.occurred_at
        {
            return Err(StorageError::invariant_violation(
                Some(ENTITY),
                "delay requires a retryable status and a future attempt time",
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::lock(&transaction, command.run_id).await?;
        Self::ensure_revision(&current, command.expected_revision)?;
        Self::ensure_time(&current, command.occurred_at)?;
        if current.status != FreshBootStatus::Running {
            return Err(StorageError::illegal_transition(
                ENTITY,
                Some(command.run_id),
                current.status,
                command.status,
            ));
        }
        let next_retry_count = if command.consume_retry {
            current.retry_count.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(Some(ENTITY), "retry count overflow")
            })?
        } else {
            current.retry_count
        };
        let next_revision = current.revision.saturating_add(1);
        let event_kind = if command.status == FreshBootStatus::WaitingEvidence {
            FreshBootEventKind::EvidenceWaitScheduled
        } else {
            FreshBootEventKind::RetryScheduled
        };
        let mut active = current.clone().into_active_model();
        active.status = Set(command.status);
        active.retry_reason = Set(Some(command.reason));
        active.retry_detail = Set(Some(command.detail.clone()));
        active.retry_count = Set(next_retry_count);
        active.next_attempt_at = Set(Some(command.next_attempt_at));
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.revision = Set(next_revision);
        active.updated_at = Set(command.occurred_at);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::persist_event(
            &transaction,
            FreshBootRunEventInput {
                run_id: current.run_id,
                event_sequence: next_revision,
                from_stage: current.stage,
                to_stage: current.stage,
                from_status: current.status,
                to_status: command.status,
                event_kind,
                research_job_id: current.active_job_id,
                result_ref: None,
                evidence_hash: None,
                attempt: next_retry_count,
                actor: command.actor,
                detail: Some(command.detail),
                occurred_at: command.occurred_at,
            },
        )
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn block_terminal(
        &self,
        command: BlockFreshBootRun,
    ) -> Result<FreshBootRunInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::lock(&transaction, command.run_id).await?;
        Self::ensure_revision(&current, command.expected_revision)?;
        Self::ensure_time(&current, command.occurred_at)?;
        if current.status != FreshBootStatus::Running {
            return Err(StorageError::illegal_transition(
                ENTITY,
                Some(command.run_id),
                current.status,
                FreshBootStatus::BlockedTerminal,
            ));
        }
        let next_revision = current.revision.saturating_add(1);
        let mut active = current.clone().into_active_model();
        active.status = Set(FreshBootStatus::BlockedTerminal);
        active.blocked_reason = Set(Some(command.reason));
        active.blocked_detail = Set(Some(command.detail.clone()));
        active.retry_reason = Set(None);
        active.retry_detail = Set(None);
        active.next_attempt_at = Set(None);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.revision = Set(next_revision);
        active.completed_at = Set(Some(command.occurred_at));
        active.updated_at = Set(command.occurred_at);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::persist_event(
            &transaction,
            FreshBootRunEventInput {
                run_id: current.run_id,
                event_sequence: next_revision,
                from_stage: current.stage,
                to_stage: current.stage,
                from_status: current.status,
                to_status: FreshBootStatus::BlockedTerminal,
                event_kind: FreshBootEventKind::TerminalBlocked,
                research_job_id: current.active_job_id,
                result_ref: None,
                evidence_hash: None,
                attempt: current.retry_count,
                actor: command.actor,
                detail: Some(command.detail),
                occurred_at: command.occurred_at,
            },
        )
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn retry_now(
        &self,
        run_id: FreshBootRunId,
        expected_revision: i64,
        actor: String,
        reason: String,
        occurred_at: DateTime<Utc>,
    ) -> Result<FreshBootRunInfo, StorageError> {
        let reason = Self::operator_reason(&reason)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::lock(&transaction, run_id).await?;
        Self::ensure_revision(&current, expected_revision)?;
        Self::ensure_time(&current, occurred_at)?;
        if !Self::retryable_status(current.status) {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(run_id),
                "retry-now is allowed only for retryable fresh-boot states",
            ));
        }
        let next_revision = current.revision.saturating_add(1);
        let mut active = current.clone().into_active_model();
        active.next_attempt_at = Set(Some(occurred_at));
        active.revision = Set(next_revision);
        active.updated_at = Set(occurred_at);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::persist_event(
            &transaction,
            FreshBootRunEventInput {
                run_id,
                event_sequence: next_revision,
                from_stage: current.stage,
                to_stage: current.stage,
                from_status: current.status,
                to_status: current.status,
                event_kind: FreshBootEventKind::RetryAccelerated,
                research_job_id: current.active_job_id,
                result_ref: None,
                evidence_hash: None,
                attempt: current.retry_count,
                actor,
                detail: Some(reason),
                occurred_at,
            },
        )
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn supersede(
        &self,
        command: SupersedeFreshBootRun,
        replacement: NewFreshBootRun,
    ) -> Result<FreshBootRunInfo, StorageError> {
        let reason = Self::operator_reason(&command.reason)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::lock(&transaction, command.run_id).await?;
        Self::ensure_revision(&current, command.expected_revision)?;
        Self::ensure_time(&current, command.occurred_at)?;
        if current.status != FreshBootStatus::BlockedTerminal {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(command.run_id),
                "only a terminal blocker can be superseded",
            ));
        }
        if replacement.run_id != command.replacement_run_id
            || replacement.supersedes_run_id != Some(current.run_id)
            || replacement.route != current.route
            || replacement.research_profile_artifact_id != current.research_profile_artifact_id
        {
            return Err(StorageError::state_conflict(
                ENTITY,
                Some(command.run_id),
                "replacement run does not form an exact supersede lineage",
            ));
        }
        let requested = replacement.clone();
        FreshBootEntity::insert(replacement.into_active_model())
            .on_conflict(
                OnConflict::column(FreshBootColumn::RunId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let replacement = Self::lock(&transaction, command.replacement_run_id).await?;
        let replacement_info = FreshBootRunInfo::from(replacement);
        Self::verify_exact(&replacement_info, &requested)?;
        let replacement_detail = format!("supersedes_run_id={}; reason={}", current.run_id, reason);
        Self::create_event_if_missing(
            &transaction,
            &replacement_info,
            &command.actor,
            Some(replacement_detail),
            Some(*current.run_id.as_uuid_ref()),
        )
        .await?;
        let detail = format!(
            "replacement_run_id={}; reason={}",
            command.replacement_run_id, reason
        );
        let next_revision = current.revision.saturating_add(1);
        let mut active = current.clone().into_active_model();
        active.status = Set(FreshBootStatus::Superseded);
        active.revision = Set(next_revision);
        active.updated_at = Set(command.occurred_at);
        active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::persist_event(
            &transaction,
            FreshBootRunEventInput {
                run_id: current.run_id,
                event_sequence: next_revision,
                from_stage: current.stage,
                to_stage: current.stage,
                from_status: current.status,
                to_status: FreshBootStatus::Superseded,
                event_kind: FreshBootEventKind::Superseded,
                research_job_id: current.active_job_id,
                result_ref: Some(*command.replacement_run_id.as_uuid_ref()),
                evidence_hash: None,
                attempt: current.retry_count,
                actor: command.actor,
                detail: Some(detail),
                occurred_at: command.occurred_at,
            },
        )
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(replacement_info)
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::enums::quant::FreshBootStatus;

    use super::PgFreshBootRepository;

    #[test]
    fn retryable_status_is_explicit() {
        assert!(PgFreshBootRepository::retryable_status(
            FreshBootStatus::WaitingEvidence
        ));
        assert!(PgFreshBootRepository::retryable_status(
            FreshBootStatus::RetryScheduled
        ));
        assert!(!PgFreshBootRepository::retryable_status(
            FreshBootStatus::BlockedTerminal
        ));
    }
}
