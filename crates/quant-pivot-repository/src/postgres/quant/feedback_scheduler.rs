//! PostgreSQL-authoritative feedback scheduler state and lease repository.

use std::fmt::Display;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_FEEDBACK_SCHEDULER_STATE};
use quant_pivot_models::{
    domain::quant::{
        FeedbackSchedulerClaim, FeedbackSchedulerControl, FeedbackSchedulerLease,
        FeedbackSchedulerRetry, FeedbackSchedulerStateInfo, FeedbackSchedulerSuccess,
        NewFeedbackSchedulerState, cadence_cutoff, next_cadence_after,
    },
    entities::quant_feedback_scheduler_state::{ActiveModel, Column, Entity, Model},
    enums::quant::FeedbackSchedulerFailureKind,
    types::{ResearchProfileId, WorkerId},
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{LockBehavior, LockType},
};

use crate::{postgres::primitives, traits::FeedbackSchedulerRepository};

const MAX_LEASE_SECS: u64 = 3_600;
const MAX_RETRY_SECS: i64 = 86_400;

/// Durable `PostgreSQL` feedback scheduler implementation.
pub struct PgFeedbackSchedulerRepository {
    db: DatabaseConnection,
}

impl PgFeedbackSchedulerRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn validated_info(row: Model) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        let state: FeedbackSchedulerStateInfo = row.into();
        state.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                format!("stored scheduler state failed integrity validation: {error}"),
            )
        })?;
        Ok(state)
    }

    fn next_revision(revision: i64) -> Result<i64, StorageError> {
        revision
            .checked_add(1)
            .ok_or_else(|| invariant("feedback scheduler revision exhausted PostgreSQL bigint"))
    }

    fn ensure_lease(
        state: &FeedbackSchedulerStateInfo,
        lease: &FeedbackSchedulerLease,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let revision_matches = state.revision == lease.expected_revision;
        if state.research_profile_id == lease.research_profile_id
            && revision_matches
            && state.lease_owner == Some(lease.worker_id)
            && state
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > now)
        {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&lease.research_profile_id),
                "scheduler lease is stale, expired, or owned by another worker",
            ))
        }
    }
}

#[async_trait::async_trait]
impl FeedbackSchedulerRepository for PgFeedbackSchedulerRepository {
    async fn sync_state(
        &self,
        candidate: NewFeedbackSchedulerState,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        candidate
            .validate()
            .map_err(|error| invariant(error.to_string()))?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let stored = Entity::find_by_id(candidate.research_profile_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?;
        let row = if let Some(stored) = stored {
            let state = Self::validated_info(stored.clone())?;
            if state.matches_profile(&candidate) {
                stored
            } else {
                if state.pending_cutoff.is_some() {
                    return Err(StorageError::state_conflict(
                        QUANT_FEEDBACK_SCHEDULER_STATE,
                        Some(&candidate.research_profile_id),
                        "cannot replace a governed profile while a scheduled cutoff is pending",
                    ));
                }
                let next_due_at = next_cadence_after(now, candidate.cadence_secs)
                    .map_err(|error| invariant(error.to_string()))?;
                let mut active = stored.into_active_model();
                active.research_profile_artifact_id = Set(candidate.research_profile_artifact_id);
                active.profile_hash = Set(candidate.profile_hash);
                active.feedback_policy_hash = Set(candidate.feedback_policy_hash);
                active.cadence_secs = Set(candidate.cadence_secs);
                active.cooldown_secs = Set(candidate.cooldown_secs);
                active.next_due_at = Set(next_due_at);
                active.pending_cutoff = Set(None);
                active.pending_started_at = Set(None);
                active.lease_owner = Set(None);
                active.lease_expires_at = Set(None);
                active.retry_at = Set(None);
                active.last_failure_kind = Set(None);
                active.last_error = Set(None);
                active.revision = Set(Self::next_revision(state.revision)?);
                active.updated_at = Set(now);
                active
                    .update(&transaction)
                    .await
                    .map_err(StorageError::from)?
            }
        } else {
            ActiveModel {
                research_profile_id: Set(candidate.research_profile_id),
                research_profile_artifact_id: Set(candidate.research_profile_artifact_id),
                profile_hash: Set(candidate.profile_hash),
                feedback_policy_hash: Set(candidate.feedback_policy_hash),
                cadence_secs: Set(candidate.cadence_secs),
                cooldown_secs: Set(candidate.cooldown_secs),
                next_due_at: Set(candidate.next_due_at),
                pending_cutoff: Set(None),
                pending_started_at: Set(None),
                last_cycle_id: Set(None),
                last_cutoff: Set(None),
                cooldown_until: Set(None),
                coalesced_gap_count: NotSet,
                last_coalesced_from: Set(None),
                last_coalesced_to: Set(None),
                lease_owner: Set(None),
                lease_expires_at: Set(None),
                attempt: NotSet,
                retry_at: Set(None),
                last_failure_kind: Set(None),
                last_error: Set(None),
                settlement_failure_count: NotSet,
                last_settlement_failed_at: Set(None),
                last_settlement_error: Set(None),
                paused: NotSet,
                pause_revision: NotSet,
                pause_reason_code: Set(None),
                pause_note: Set(None),
                revision: NotSet,
                created_at: NotSet,
                updated_at: NotSet,
            }
            .insert(&transaction)
            .await
            .map_err(StorageError::from)?
        };
        let info = Self::validated_info(row)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn find_state(
        &self,
        research_profile_id: &ResearchProfileId,
    ) -> Result<Option<FeedbackSchedulerStateInfo>, StorageError> {
        Entity::find_by_id(research_profile_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::validated_info)
            .transpose()
    }

    async fn list_states(&self) -> Result<Vec<FeedbackSchedulerStateInfo>, StorageError> {
        Entity::find()
            .order_by_asc(Column::ResearchProfileId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::validated_info)
            .collect()
    }

    async fn claim_due(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<FeedbackSchedulerClaim>, StorageError> {
        let lease_duration = lease_duration(lease_secs)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let row = Entity::find()
            .filter(Column::Paused.eq(false))
            .filter(
                Condition::any()
                    .add(Column::PendingCutoff.is_not_null())
                    .add(
                        Condition::all()
                            .add(Column::PendingCutoff.is_null())
                            .add(Column::NextDueAt.lte(now))
                            .add(
                                Condition::any()
                                    .add(Column::CooldownUntil.is_null())
                                    .add(Column::CooldownUntil.lte(now)),
                            ),
                    ),
            )
            .filter(
                Condition::any()
                    .add(Column::RetryAt.is_null())
                    .add(Column::RetryAt.lte(now)),
            )
            .filter(
                Condition::any()
                    .add(Column::LeaseExpiresAt.is_null())
                    .add(Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(Column::NextDueAt)
            .order_by_asc(Column::ResearchProfileId)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?;
        let Some(row) = row else {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let attempt = row
            .attempt
            .checked_add(1)
            .ok_or_else(|| invariant("feedback scheduler attempt counter overflowed"))?;
        let revision = Self::next_revision(row.revision)?;
        let starting_pending = row.pending_cutoff.is_none();
        let target_cutoff = if let Some(pending_cutoff) = row.pending_cutoff {
            pending_cutoff
        } else {
            cadence_cutoff(now, row.cadence_secs).map_err(|error| invariant(error.to_string()))?
        };
        let (coalesced_gap_count, last_coalesced_from, last_coalesced_to) = if starting_pending {
            let gap_seconds = target_cutoff
                .signed_duration_since(row.next_due_at)
                .num_seconds();
            if gap_seconds < 0 || gap_seconds.rem_euclid(row.cadence_secs) != 0 {
                return Err(invariant(
                    "scheduler due cursor and pending cutoff are not cadence-aligned",
                ));
            }
            let skipped = gap_seconds.div_euclid(row.cadence_secs);
            let total = row
                .coalesced_gap_count
                .checked_add(skipped)
                .ok_or_else(|| invariant("scheduler coalesced gap counter overflowed"))?;
            let range = if skipped == 0 {
                (row.last_coalesced_from, row.last_coalesced_to)
            } else {
                (
                    Some(row.next_due_at),
                    Some(
                        target_cutoff
                            .checked_sub_signed(Duration::seconds(row.cadence_secs))
                            .ok_or_else(|| {
                                invariant("scheduler coalesced gap range underflowed")
                            })?,
                    ),
                )
            };
            (total, range.0, range.1)
        } else {
            (
                row.coalesced_gap_count,
                row.last_coalesced_from,
                row.last_coalesced_to,
            )
        };
        let mut active = row.into_active_model();
        if starting_pending {
            active.pending_cutoff = Set(Some(target_cutoff));
            active.pending_started_at = Set(Some(now));
            active.coalesced_gap_count = Set(coalesced_gap_count);
            active.last_coalesced_from = Set(last_coalesced_from);
            active.last_coalesced_to = Set(last_coalesced_to);
        }
        active.lease_owner = Set(Some(worker_id));
        active.lease_expires_at = Set(Some(now + lease_duration));
        active.attempt = Set(attempt);
        active.retry_at = Set(None);
        active.last_failure_kind = Set(None);
        active.last_error = Set(None);
        active.revision = Set(revision);
        active.updated_at = Set(now);
        let state = Self::validated_info(
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        )?;
        let claim = FeedbackSchedulerClaim {
            lease: FeedbackSchedulerLease {
                research_profile_id: state.research_profile_id.clone(),
                expected_revision: revision,
                worker_id,
            },
            state,
            claimed_at: now,
        };
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(Some(claim))
    }

    async fn settle_success(
        &self,
        lease: FeedbackSchedulerLease,
        success: FeedbackSchedulerSuccess,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let row = Entity::find_by_id(lease.research_profile_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_SCHEDULER_STATE, &lease.research_profile_id)
            })?;
        let state = Self::validated_info(row.clone())?;
        Self::ensure_lease(&state, &lease, now)?;
        success
            .validate(&state, now)
            .map_err(|error| invariant(error.to_string()))?;
        let next_due_at = next_cadence_after(now, state.cadence_secs)
            .map_err(|error| invariant(error.to_string()))?;
        let cooldown_until = now
            .checked_add_signed(Duration::seconds(state.cooldown_secs))
            .ok_or_else(|| invariant("feedback scheduler cooldown time overflowed"))?;
        let mut active = row.into_active_model();
        active.last_cycle_id = Set(Some(success.feedback_cycle_id));
        active.last_cutoff = Set(Some(success.label_cutoff));
        active.next_due_at = Set(next_due_at);
        active.pending_cutoff = Set(None);
        active.pending_started_at = Set(None);
        active.cooldown_until = Set(Some(cooldown_until));
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.attempt = Set(0);
        active.retry_at = Set(None);
        active.last_failure_kind = Set(None);
        active.last_error = Set(None);
        active.revision = Set(Self::next_revision(state.revision)?);
        active.updated_at = Set(now);
        let result = Self::validated_info(
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        )?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(result)
    }

    async fn renew_lease(
        &self,
        lease: FeedbackSchedulerLease,
        lease_secs: u64,
    ) -> Result<FeedbackSchedulerLease, StorageError> {
        let lease_duration = lease_duration(lease_secs)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let row = Entity::find_by_id(lease.research_profile_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_SCHEDULER_STATE, &lease.research_profile_id)
            })?;
        let state = Self::validated_info(row.clone())?;
        Self::ensure_lease(&state, &lease, now)?;
        let revision = Self::next_revision(state.revision)?;
        let mut active = row.into_active_model();
        active.lease_expires_at = Set(Some(now + lease_duration));
        active.revision = Set(revision);
        active.updated_at = Set(now);
        Self::validated_info(
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        )?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackSchedulerLease {
            research_profile_id: lease.research_profile_id,
            expected_revision: revision,
            worker_id: lease.worker_id,
        })
    }

    async fn settle_retry(
        &self,
        lease: FeedbackSchedulerLease,
        retry: FeedbackSchedulerRetry,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        retry
            .validate()
            .map_err(|error| invariant(error.to_string()))?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let retry_delay = i64::try_from(retry.retry_delay_secs).map_err(|conversion| {
            invariant(format!("scheduler retry delay overflowed: {conversion}"))
        })?;
        if retry_delay > MAX_RETRY_SECS {
            return Err(invariant("scheduler retry exceeds repository bound"));
        }
        let retry_at = now + Duration::seconds(retry_delay);
        let row = Entity::find_by_id(lease.research_profile_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_SCHEDULER_STATE, &lease.research_profile_id)
            })?;
        let state = Self::validated_info(row.clone())?;
        Self::ensure_lease(&state, &lease, now)?;
        let mut active = row.into_active_model();
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.retry_at = Set(Some(retry_at));
        active.last_failure_kind = Set(Some(retry.failure_kind));
        active.last_error = Set(Some(retry.error.clone()));
        if retry.failure_kind == FeedbackSchedulerFailureKind::Settlement {
            active.settlement_failure_count = Set(state
                .settlement_failure_count
                .checked_add(1)
                .ok_or_else(|| invariant("scheduler settlement failure counter overflowed"))?);
            active.last_settlement_failed_at = Set(Some(now));
            active.last_settlement_error = Set(Some(retry.error));
        }
        active.revision = Set(Self::next_revision(state.revision)?);
        active.updated_at = Set(now);
        let result = Self::validated_info(
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        )?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(result)
    }

    async fn apply_control(
        &self,
        control: FeedbackSchedulerControl,
    ) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        control
            .validate()
            .map_err(|error| invariant(error.to_string()))?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let row = Entity::find_by_id(control.research_profile_id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_FEEDBACK_SCHEDULER_STATE,
                    &control.research_profile_id,
                )
            })?;
        let state = Self::validated_info(row.clone())?;
        if state.pause_revision != control.expected_pause_revision {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&control.research_profile_id),
                "scheduler pause revision changed",
            ));
        }
        if state
            .lease_expires_at
            .is_some_and(|expires_at| expires_at > now)
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&control.research_profile_id),
                "scheduler pause/resume cannot race a live materialization lease",
            ));
        }
        if state.paused == control.pause {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&control.research_profile_id),
                "scheduler is already in the requested pause state",
            ));
        }
        let pause_revision = state
            .pause_revision
            .checked_add(1)
            .ok_or_else(|| invariant("scheduler pause revision overflowed"))?;
        let mut active = row.into_active_model();
        active.paused = Set(control.pause);
        active.pause_revision = Set(pause_revision);
        if control.pause {
            active.pause_reason_code = Set(Some(control.reason_code));
            active.pause_note = Set(Some(control.note));
        } else {
            active.pause_reason_code = Set(None);
            active.pause_note = Set(None);
        }
        active.revision = Set(Self::next_revision(state.revision)?);
        active.updated_at = Set(now);
        let result = Self::validated_info(
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        )?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(result)
    }
}

fn lease_duration(lease_secs: u64) -> Result<Duration, StorageError> {
    if lease_secs == 0 || lease_secs > MAX_LEASE_SECS {
        return Err(invariant(format!(
            "scheduler lease_secs must be within 1..={MAX_LEASE_SECS}"
        )));
    }
    let seconds = i64::try_from(lease_secs)
        .map_err(|error| invariant(format!("scheduler lease duration overflowed: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn invariant(detail: impl Display) -> StorageError {
    StorageError::invariant_violation(Some(QUANT_FEEDBACK_SCHEDULER_STATE), detail)
}
