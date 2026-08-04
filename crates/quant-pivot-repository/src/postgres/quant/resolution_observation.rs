//! `PostgreSQL` resolution inbox, projection queue, and truth-freeze watermark.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    QuantError,
    storage::{
        StorageError,
        entity::{
            QUANT_DOMAIN_SOURCE_CURSOR, QUANT_RESOLUTION_OBSERVATION_INBOX,
            QUANT_RESOLUTION_OBSERVATION_PROJECTION,
        },
    },
};
use quant_pivot_models::{
    domain::{
        data_plane::{DomainSourceCursorInfo, UpsertDomainSourceCursor},
        quant::{
            NewResolutionObservationInbox, RemediateResolutionProjection,
            ResolutionObservationInboxInfo, ResolutionObservationProjectionInfo,
            ResolutionProjectionAttentionItem, ResolutionProjectionBarrier,
            ResolutionProjectionClaim, ResolutionProjectionRemediationInfo,
            ResolutionProjectionSettlement, ResolutionRemediationCommit,
            ResolutionScanCommitOutcome,
        },
    },
    entities::{
        quant_domain_source_cursor::{
            ActiveModel as CursorActiveModel, Column as CursorColumn, Entity as CursorEntity,
            Model as CursorModel,
        },
        quant_resolution_observation_inbox::{
            ActiveModel as InboxActiveModel, Column as InboxColumn, Entity as InboxEntity,
            Model as InboxModel,
        },
        quant_resolution_observation_projection::{
            ActiveModel as ProjectionActiveModel, Column as ProjectionColumn,
            Entity as ProjectionEntity, Model as ProjectionModel,
        },
        quant_resolution_projection_remediation::{
            ActiveModel as RemediationActiveModel, Column as RemediationColumn,
            Entity as RemediationEntity,
        },
    },
    enums::{
        quant::{ResolutionProjectionStatus, ResolutionRemediationAction},
        rbac::{Operation, ResourceType},
    },
    types::{ContentHash, ResolutionObservationId, ResolutionRemediationId, WorkerId},
};
use sea_orm::{
    AccessMode, ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    IsolationLevel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{LockBehavior, LockType},
};

use crate::{
    postgres::{authorization, primitives},
    traits::ResolutionObservationRepository,
};

const MAX_ERROR_CHARS: usize = 4_096;
const MAX_CLAIM_LIMIT: u64 = 4_096;
const MAX_LEASE_SECS: u64 = 3_600;
const MAX_RETRY_DELAY_SECS: u64 = 86_400;

enum ObservationInsertOutcome {
    Inserted,
    Existing,
}

/// `PostgreSQL` owner for the immutable source inbox and its mutable delivery queue.
pub struct PgResolutionObservationRepository {
    db: DatabaseConnection,
}

impl PgResolutionObservationRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn validate_cursor(cursor: &UpsertDomainSourceCursor) -> Result<(), StorageError> {
        cursor.validate().map_err(|detail| {
            StorageError::invariant_violation(Some(QUANT_DOMAIN_SOURCE_CURSOR), detail)
        })
    }

    fn validate_observation(
        observation: &NewResolutionObservationInbox,
    ) -> Result<(), StorageError> {
        observation.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                error.to_string(),
            )
        })?;
        i64::try_from(observation.block_number).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                format!("block_number exceeds PostgreSQL bigint: {error}"),
            )
        })?;
        i64::try_from(observation.log_index).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                format!("log_index exceeds PostgreSQL bigint: {error}"),
            )
        })?;
        Ok(())
    }

    fn cursor_info(row: CursorModel) -> Result<DomainSourceCursorInfo, StorageError> {
        let info: DomainSourceCursorInfo = row.into();
        info.validate().map_err(|detail| {
            StorageError::invariant_violation(
                Some(QUANT_DOMAIN_SOURCE_CURSOR),
                format!("stored cursor failed validation: {detail}"),
            )
        })?;
        Ok(info)
    }

    fn inbox_info(row: InboxModel) -> Result<ResolutionObservationInboxInfo, StorageError> {
        let info: ResolutionObservationInboxInfo = row.into();
        info.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                format!("stored observation failed validation: {error}"),
            )
        })?;
        Ok(info)
    }

    async fn load_claim(
        db: &impl ConnectionTrait,
        projection: ProjectionModel,
    ) -> Result<ResolutionProjectionClaim, StorageError> {
        let observation = InboxEntity::find_by_id(projection.resolution_observation_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RESOLUTION_OBSERVATION_INBOX,
                    projection.resolution_observation_id,
                )
            })?;
        if observation.source_checkpoint_hash != projection.source_checkpoint_hash {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "projection checkpoint differs from its immutable inbox row",
            ));
        }
        Ok(ResolutionProjectionClaim {
            observation: Self::inbox_info(observation)?,
            projection: projection.into(),
        })
    }

    async fn insert_observation(
        db: &impl ConnectionTrait,
        observation: NewResolutionObservationInbox,
        available_at: DateTime<Utc>,
    ) -> Result<ObservationInsertOutcome, StorageError> {
        let observation_id =
            ResolutionObservationId::from_checkpoint_hash(&observation.source_checkpoint_hash);
        if let Some(row) = InboxEntity::find()
            .filter(InboxColumn::SourceCheckpointHash.eq(observation.source_checkpoint_hash))
            .one(db)
            .await
            .map_err(StorageError::from)?
        {
            let info = Self::inbox_info(row)?;
            if info.resolution_observation_id != observation_id || !info.matches(&observation) {
                return Err(StorageError::state_conflict(
                    QUANT_RESOLUTION_OBSERVATION_INBOX,
                    Some(observation.source_checkpoint_hash),
                    "checkpoint identity is already bound to different immutable content",
                ));
            }
            return Ok(ObservationInsertOutcome::Existing);
        }

        InboxEntity::insert(InboxActiveModel {
            resolution_observation_id: Set(observation_id),
            source_checkpoint_hash: Set(observation.source_checkpoint_hash),
            source_id: Set(observation.source_id),
            instrument_key: Set(observation.instrument_key),
            market_id: Set(observation.market_id),
            denominator: Set(observation.denominator),
            yes_numerator: Set(observation.yes_numerator),
            no_numerator: Set(observation.no_numerator),
            yes_payout_ratio: Set(observation.yes_payout_ratio),
            no_payout_ratio: Set(observation.no_payout_ratio),
            oracle: Set(observation.oracle),
            question_id: Set(observation.question_id),
            transaction_hash: Set(observation.transaction_hash),
            block_number: Set(i64::try_from(observation.block_number).map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                    format!("block_number overflow after validation: {error}"),
                )
            })?),
            block_hash: Set(observation.block_hash),
            log_index: Set(i64::try_from(observation.log_index).map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                    format!("log_index overflow after validation: {error}"),
                )
            })?),
            resolved_at: Set(observation.resolved_at),
            raw_payload_hash: Set(observation.raw_payload_hash),
            raw_uri: Set(observation.raw_uri),
            provider_revision: Set(observation.provider_revision),
            available_at: Set(available_at),
            created_at: Set(available_at),
        })
        .exec_without_returning(db)
        .await
        .map_err(StorageError::from)?;
        ProjectionEntity::insert(ProjectionActiveModel {
            resolution_observation_id: Set(observation_id),
            source_checkpoint_hash: Set(observation.source_checkpoint_hash),
            status: Set(ResolutionProjectionStatus::Pending),
            revision: Set(0),
            attempt_count: Set(0),
            claim_owner: Set(None),
            lease_expires_at: Set(None),
            next_attempt_at: Set(Some(available_at)),
            last_error_code: Set(None),
            last_error: Set(None),
            canonical_fact_hash: Set(None),
            verified_at: Set(None),
            created_at: Set(available_at),
            updated_at: Set(available_at),
        })
        .exec_without_returning(db)
        .await
        .map_err(StorageError::from)?;
        Ok(ObservationInsertOutcome::Inserted)
    }

    async fn load_remediation_replay(
        db: &impl ConnectionTrait,
        command: &RemediateResolutionProjection,
        request_hash: ContentHash,
    ) -> Result<Option<ResolutionRemediationCommit>, QuantError> {
        let Some(existing) = RemediationEntity::find()
            .filter(RemediationColumn::IdempotencyKey.eq(command.idempotency_key.clone()))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        if existing.request_hash != request_hash
            || existing.resolution_observation_id != command.resolution_observation_id
            || existing.expected_revision != command.expected_revision
            || existing.action != command.action
        {
            return Err(StorageError::state_conflict(
                QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                Some(command.resolution_observation_id),
                "resolution remediation idempotency key is bound to different content",
            )
            .into());
        }
        let projection = ProjectionEntity::find_by_id(existing.resolution_observation_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                    existing.resolution_observation_id,
                )
            })?;
        if projection.revision < existing.committed_revision {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "projection revision regressed behind remediation evidence",
            )
            .into());
        }
        Ok(Some(ResolutionRemediationCommit {
            projection: projection.into(),
            remediation: existing.into(),
            replayed: true,
        }))
    }

    fn validate_claim(lease_secs: u64, limit: u64) -> Result<Duration, StorageError> {
        if lease_secs == 0 || lease_secs > MAX_LEASE_SECS {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                format!("lease_secs must be within 1..={MAX_LEASE_SECS}"),
            ));
        }
        if limit == 0 || limit > MAX_CLAIM_LIMIT {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                format!("claim limit must be within 1..={MAX_CLAIM_LIMIT}"),
            ));
        }
        let seconds = i64::try_from(lease_secs).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                format!("lease seconds overflow: {error}"),
            )
        })?;
        Ok(Duration::seconds(seconds))
    }

    fn validate_error(error: &str) -> Result<(), StorageError> {
        if error.trim().is_empty() || error.chars().count() > MAX_ERROR_CHARS {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                format!("projection error must contain 1..={MAX_ERROR_CHARS} characters"),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ResolutionObservationRepository for PgResolutionObservationRepository {
    async fn commit_scan(
        &self,
        expected_cursor_hash: ContentHash,
        cursor: UpsertDomainSourceCursor,
        observations: Vec<NewResolutionObservationInbox>,
    ) -> Result<ResolutionScanCommitOutcome, StorageError> {
        Self::validate_cursor(&cursor)?;
        let mut identities = HashSet::with_capacity(observations.len());
        for observation in &observations {
            Self::validate_observation(observation)?;
            if observation.source_id != cursor.source_id
                || observation.instrument_key != cursor.instrument_key
                || !identities.insert(observation.source_checkpoint_hash)
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                    "scan observations must have unique checkpoints bound to the cursor source",
                ));
            }
        }

        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let current = CursorEntity::find()
            .filter(CursorColumn::SourceId.eq(cursor.source_id.clone()))
            .filter(CursorColumn::InstrumentKey.eq(cursor.instrument_key.clone()))
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_DOMAIN_SOURCE_CURSOR,
                    format!("{}/{}", cursor.source_id, cursor.instrument_key),
                )
            })?;
        if current.checkpoint_hash != expected_cursor_hash {
            let winner = Self::cursor_info(current)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(ResolutionScanCommitOutcome::Conflict(winner));
        }

        let available_at = primitives::statement_timestamp(&transaction).await?;
        let mut inserted = 0_u64;
        let mut existing = 0_u64;
        for observation in observations {
            match Self::insert_observation(&transaction, observation, available_at).await? {
                ObservationInsertOutcome::Inserted => {
                    inserted = inserted.checked_add(1).ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                            "inserted observation count overflow",
                        )
                    })?;
                }
                ObservationInsertOutcome::Existing => {
                    existing = existing.checked_add(1).ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_RESOLUTION_OBSERVATION_INBOX),
                            "existing observation count overflow",
                        )
                    })?;
                }
            }
        }

        let mut active: CursorActiveModel = current.into_active_model();
        active.checkpoint_json = Set(cursor.checkpoint_json);
        active.checkpoint_hash = Set(cursor.checkpoint_hash);
        active.status = Set(cursor.status);
        active.last_error = Set(cursor.last_error);
        active.updated_at = Set(available_at);
        let advanced = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let advanced = Self::cursor_info(advanced)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(ResolutionScanCommitOutcome::Committed {
            cursor: advanced,
            inserted,
            existing,
        })
    }

    async fn claim_pending(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<ResolutionProjectionClaim>, StorageError> {
        let lease = Self::validate_claim(lease_secs, limit)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let lease_expires_at = now + lease;
        let due = Condition::any()
            .add(
                Condition::all()
                    .add(ProjectionColumn::Status.is_in([
                        ResolutionProjectionStatus::Pending,
                        ResolutionProjectionStatus::RetryScheduled,
                    ]))
                    .add(
                        Condition::any()
                            .add(ProjectionColumn::NextAttemptAt.is_null())
                            .add(ProjectionColumn::NextAttemptAt.lte(now)),
                    ),
            )
            .add(
                Condition::all()
                    .add(ProjectionColumn::Status.eq(ResolutionProjectionStatus::Delivering))
                    .add(ProjectionColumn::LeaseExpiresAt.lte(now)),
            );
        let rows = ProjectionEntity::find()
            .filter(due)
            .order_by_asc(ProjectionColumn::NextAttemptAt)
            .order_by_asc(ProjectionColumn::CreatedAt)
            .order_by_asc(ProjectionColumn::ResolutionObservationId)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let attempt_count = row.attempt_count.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                    "projection attempt count overflow",
                )
            })?;
            let revision = row.revision.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                    "projection revision overflow",
                )
            })?;
            let mut active = row.into_active_model();
            active.status = Set(ResolutionProjectionStatus::Delivering);
            active.revision = Set(revision);
            active.attempt_count = Set(attempt_count);
            active.claim_owner = Set(Some(worker_id));
            active.lease_expires_at = Set(Some(lease_expires_at));
            active.next_attempt_at = Set(None);
            active.last_error_code = Set(None);
            active.last_error = Set(None);
            active.updated_at = Set(now);
            let projection = active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
            claims.push(Self::load_claim(&transaction, projection).await?);
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(claims)
    }

    async fn settle(
        &self,
        observation_id: ResolutionObservationId,
        worker_id: WorkerId,
        settlement: ResolutionProjectionSettlement,
    ) -> Result<ResolutionObservationProjectionInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let row = ProjectionEntity::find_by_id(observation_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RESOLUTION_OBSERVATION_PROJECTION, observation_id)
            })?;
        if row.status != ResolutionProjectionStatus::Delivering
            || row.claim_owner != Some(worker_id)
            || row.lease_expires_at.is_none_or(|expiry| expiry <= now)
        {
            return Err(StorageError::state_conflict(
                QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                Some(observation_id),
                "projection settlement requires a live lease owned by the worker",
            ));
        }
        let mut active = row.into_active_model();
        let next_revision = active.revision.as_ref().checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "projection revision overflow",
            )
        })?;
        match settlement {
            ResolutionProjectionSettlement::Verified {
                canonical_fact_hash,
            } => {
                active.status = Set(ResolutionProjectionStatus::Verified);
                active.canonical_fact_hash = Set(Some(canonical_fact_hash));
                active.verified_at = Set(Some(now));
                active.next_attempt_at = Set(None);
                active.last_error_code = Set(None);
                active.last_error = Set(None);
            }
            ResolutionProjectionSettlement::RetryScheduled {
                retry_delay_secs,
                error_code,
                error,
            } => {
                Self::validate_error(&error)?;
                if retry_delay_secs == 0 || retry_delay_secs > MAX_RETRY_DELAY_SECS {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                        format!("retry_delay_secs must be within 1..={MAX_RETRY_DELAY_SECS}"),
                    ));
                }
                let retry_delay =
                    Duration::seconds(i64::try_from(retry_delay_secs).map_err(|error| {
                        StorageError::invariant_violation(
                            Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                            format!("resolution retry delay overflow: {error}"),
                        )
                    })?);
                let retry_at = now.checked_add_signed(retry_delay).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                        "resolution retry timestamp overflow",
                    )
                })?;
                active.status = Set(ResolutionProjectionStatus::RetryScheduled);
                active.canonical_fact_hash = Set(None);
                active.verified_at = Set(None);
                active.next_attempt_at = Set(Some(retry_at));
                active.last_error_code = Set(Some(error_code));
                active.last_error = Set(Some(error));
            }
            ResolutionProjectionSettlement::MappingBlocked { error_code, error } => {
                Self::validate_error(&error)?;
                active.status = Set(ResolutionProjectionStatus::MappingBlocked);
                active.canonical_fact_hash = Set(None);
                active.verified_at = Set(None);
                active.next_attempt_at = Set(None);
                active.last_error_code = Set(Some(error_code));
                active.last_error = Set(Some(error));
            }
            ResolutionProjectionSettlement::Quarantined { error_code, error } => {
                Self::validate_error(&error)?;
                active.status = Set(ResolutionProjectionStatus::Quarantined);
                active.canonical_fact_hash = Set(None);
                active.verified_at = Set(None);
                active.next_attempt_at = Set(None);
                active.last_error_code = Set(Some(error_code));
                active.last_error = Set(Some(error));
            }
        }
        active.revision = Set(next_revision);
        active.claim_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.updated_at = Set(now);
        let settled = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(settled.into())
    }

    async fn remediate(
        &self,
        command: RemediateResolutionProjection,
    ) -> Result<ResolutionRemediationCommit, QuantError> {
        command.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                error.to_string(),
            )
        })?;
        let request_hash = command.request_hash().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                error.to_string(),
            )
        })?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<QuantError>(
            &transaction,
            command.actor_user_id,
            &command.actor_role,
            ResourceType::Reconciliation,
            Operation::Resolve,
        )
        .await?;
        if let Some(replayed) =
            Self::load_remediation_replay(&transaction, &command, request_hash).await?
        {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(replayed);
        }

        let row = ProjectionEntity::find_by_id(command.resolution_observation_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                    command.resolution_observation_id,
                )
            })?;
        if row.revision != command.expected_revision
            || !matches!(
                row.status,
                ResolutionProjectionStatus::MappingBlocked
                    | ResolutionProjectionStatus::Quarantined
            )
            || row.claim_owner.is_some()
            || row.lease_expires_at.is_some()
            || row.next_attempt_at.is_some()
        {
            return Err(StorageError::state_conflict(
                QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                Some(command.resolution_observation_id),
                "remediation requires the exact revision of a blocked or quarantined projection",
            )
            .into());
        }
        let prior_error_code = row.last_error_code.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "blocked projection has no typed error code",
            )
        })?;
        let prior_error = row.last_error.clone().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "blocked projection has no error detail",
            )
        })?;
        let committed_revision = row.revision.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "projection revision overflow",
            )
        })?;
        let resulting_status = match command.action {
            ResolutionRemediationAction::Requeue => ResolutionProjectionStatus::Pending,
            ResolutionRemediationAction::Exclude => ResolutionProjectionStatus::Excluded,
        };
        let now = primitives::statement_timestamp(&transaction).await?;
        let remediation_id = ResolutionRemediationId::from_request_hash(&request_hash);
        let remediation = RemediationEntity::insert(RemediationActiveModel {
            remediation_id: Set(remediation_id),
            resolution_observation_id: Set(command.resolution_observation_id),
            expected_revision: Set(command.expected_revision),
            committed_revision: Set(committed_revision),
            action: Set(command.action),
            prior_status: Set(row.status),
            prior_error_code: Set(prior_error_code),
            prior_error: Set(prior_error),
            resulting_status: Set(resulting_status),
            idempotency_key: Set(command.idempotency_key),
            request_hash: Set(request_hash),
            reason_code: Set(command.reason_code),
            operator_note: Set(command.operator_note),
            actor_user_id: Set(authorized.user_id),
            actor_username: Set(authorized.username),
            actor_role: Set(authorized.role),
            created_at: Set(now),
        })
        .exec_with_returning(&transaction)
        .await
        .map_err(StorageError::from)?;
        let mut active = row.into_active_model();
        active.status = Set(resulting_status);
        active.revision = Set(committed_revision);
        active.claim_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.next_attempt_at = Set(match command.action {
            ResolutionRemediationAction::Requeue => Some(now),
            ResolutionRemediationAction::Exclude => None,
        });
        if command.action == ResolutionRemediationAction::Requeue {
            active.last_error_code = Set(None);
            active.last_error = Set(None);
        }
        active.canonical_fact_hash = Set(None);
        active.verified_at = Set(None);
        active.updated_at = Set(now);
        let projection = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(ResolutionRemediationCommit {
            projection: projection.into(),
            remediation: remediation.into(),
            replayed: false,
        })
    }

    async fn list_attention(
        &self,
        limit: u64,
    ) -> Result<Vec<ResolutionProjectionAttentionItem>, StorageError> {
        if limit == 0 || limit > 100 {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESOLUTION_OBSERVATION_PROJECTION),
                "attention limit must be within 1..=100",
            ));
        }
        let projections = ProjectionEntity::find()
            .filter(ProjectionColumn::Status.is_in([
                ResolutionProjectionStatus::MappingBlocked,
                ResolutionProjectionStatus::Quarantined,
                ResolutionProjectionStatus::Excluded,
            ]))
            .order_by_desc(ProjectionColumn::UpdatedAt)
            .order_by_asc(ProjectionColumn::ResolutionObservationId)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let observation_ids = projections
            .iter()
            .map(|projection| projection.resolution_observation_id)
            .collect::<Vec<_>>();
        let observations = InboxEntity::find()
            .filter(InboxColumn::ResolutionObservationId.is_in(observation_ids.clone()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|observation| {
                Ok((
                    observation.resolution_observation_id,
                    Self::inbox_info(observation)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, StorageError>>()?;
        let mut histories =
            HashMap::<ResolutionObservationId, Vec<ResolutionProjectionRemediationInfo>>::new();
        for remediation in RemediationEntity::find()
            .filter(RemediationColumn::ResolutionObservationId.is_in(observation_ids))
            .order_by_asc(RemediationColumn::CreatedAt)
            .order_by_asc(RemediationColumn::RemediationId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
        {
            histories
                .entry(remediation.resolution_observation_id)
                .or_default()
                .push(remediation.into());
        }
        projections
            .into_iter()
            .map(|projection| {
                let observation = observations
                    .get(&projection.resolution_observation_id)
                    .cloned()
                    .ok_or_else(|| {
                        StorageError::not_found(
                            QUANT_RESOLUTION_OBSERVATION_INBOX,
                            projection.resolution_observation_id,
                        )
                    })?;
                let remediations = histories
                    .remove(&projection.resolution_observation_id)
                    .unwrap_or_default();
                Ok(ResolutionProjectionAttentionItem {
                    observation,
                    projection: projection.into(),
                    remediations,
                })
            })
            .collect()
    }

    async fn find_by_checkpoint(
        &self,
        checkpoint_hash: ContentHash,
    ) -> Result<Option<ResolutionProjectionClaim>, StorageError> {
        let observation = InboxEntity::find()
            .filter(InboxColumn::SourceCheckpointHash.eq(checkpoint_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        let Some(observation) = observation else {
            return Ok(None);
        };
        let projection = ProjectionEntity::find_by_id(observation.resolution_observation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_RESOLUTION_OBSERVATION_PROJECTION,
                    observation.resolution_observation_id,
                )
            })?;
        Ok(Some(ResolutionProjectionClaim {
            observation: Self::inbox_info(observation)?,
            projection: projection.into(),
        }))
    }

    async fn barrier(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<ResolutionProjectionBarrier, StorageError> {
        let transaction = self
            .db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
            .map_err(StorageError::from)?;
        let visible = InboxEntity::find()
            .inner_join(ProjectionEntity)
            .filter(InboxColumn::AvailableAt.lte(cutoff));
        let unresolved_count = visible
            .clone()
            .filter(ProjectionColumn::Status.is_not_in([
                ResolutionProjectionStatus::Verified,
                ResolutionProjectionStatus::Excluded,
            ]))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let mapping_blocked_count = visible
            .clone()
            .filter(ProjectionColumn::Status.eq(ResolutionProjectionStatus::MappingBlocked))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let quarantined_count = visible
            .clone()
            .filter(ProjectionColumn::Status.eq(ResolutionProjectionStatus::Quarantined))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let excluded_count = visible
            .clone()
            .filter(ProjectionColumn::Status.eq(ResolutionProjectionStatus::Excluded))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let oldest_unresolved_at = visible
            .filter(ProjectionColumn::Status.is_not_in([
                ResolutionProjectionStatus::Verified,
                ResolutionProjectionStatus::Excluded,
            ]))
            .order_by_asc(InboxColumn::AvailableAt)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(|row| row.available_at);
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(ResolutionProjectionBarrier {
            cutoff,
            unresolved_count,
            mapping_blocked_count,
            quarantined_count,
            excluded_count,
            oldest_unresolved_at,
            terminal_through: oldest_unresolved_at.unwrap_or(cutoff),
        })
    }
}
