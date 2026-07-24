//! Transactional owner of the atomic operational runtime-control singleton.

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{NewRuntimeControlTransition, RuntimeControlInfo, RuntimeControlUpdate},
    entities::{
        system_runtime_control::{ActiveModel, Entity, Model},
        system_runtime_control_transition::Entity as TransitionEntity,
    },
    types::RuntimeControlTransitionId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QuerySelect, TransactionTrait, sea_query::LockType,
};

use crate::{postgres::primitives, traits::RuntimeControlRepository};

pub const SYSTEM_RUNTIME_CONTROL_ID: i32 = 1;
pub const RUNTIME_CONTROL_NOTIFY_CHANNEL: &str = "quant_runtime_control_changed";

pub struct PgRuntimeControlRepository {
    db: DatabaseConnection,
}

impl PgRuntimeControlRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

impl PgRuntimeControlRepository {
    async fn locked_control(tx: &DatabaseTransaction) -> Result<Model, StorageError> {
        Entity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .lock(LockType::Update)
            .one(tx)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("system_runtime_control", SYSTEM_RUNTIME_CONTROL_ID)
            })
    }

    pub(crate) async fn entry_allowed(db: &impl ConnectionTrait) -> Result<bool, StorageError> {
        Ok(Entity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .is_some_and(|row| row.kill_switch_state.allows_new_entry()))
    }

    pub(crate) async fn require_entry(db: &impl ConnectionTrait) -> Result<(), StorageError> {
        let row = Entity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    "system_runtime_control",
                    Some(&SYSTEM_RUNTIME_CONTROL_ID),
                    "runtime-control singleton is missing; new entry fails closed",
                )
            })?;
        if !row.kill_switch_state.allows_new_entry() {
            return Err(StorageError::state_conflict(
                "system_runtime_control",
                Some(&SYSTEM_RUNTIME_CONTROL_ID),
                format!(
                    "kill switch is {}; new entry is blocked",
                    row.kill_switch_state.as_str()
                ),
            ));
        }
        Ok(())
    }
}

fn validate_update(update: &RuntimeControlUpdate) -> Result<(), StorageError> {
    let changed_domains = usize::from(update.quant_runtime_mode.is_some())
        + usize::from(update.settlement_write_policy.is_some())
        + usize::from(update.kill_switch_state.is_some());
    if changed_domains != 1 {
        return Err(StorageError::invariant_violation(
            Some("system_runtime_control"),
            "exactly one runtime-control domain must be updated",
        ));
    }
    if update.kill_switch_requires_ack.is_some() != update.kill_switch_state.is_some() {
        return Err(StorageError::invariant_violation(
            Some("system_runtime_control"),
            "kill-switch state and acknowledgement latch must be updated together",
        ));
    }
    if update.actor.trim().is_empty() || update.reason.trim().is_empty() {
        return Err(StorageError::invariant_violation(
            Some("system_runtime_control"),
            "runtime-control actor and reason must be non-empty",
        ));
    }
    Ok(())
}

#[async_trait]
impl RuntimeControlRepository for PgRuntimeControlRepository {
    async fn load(&self) -> Result<RuntimeControlInfo, StorageError> {
        Entity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::not_found("system_runtime_control", SYSTEM_RUNTIME_CONTROL_ID)
            })
    }

    async fn compare_and_set(
        &self,
        update: RuntimeControlUpdate,
    ) -> Result<RuntimeControlInfo, StorageError> {
        validate_update(&update)?;
        let tx = self.db.begin().await.map_err(StorageError::from)?;
        let current = Self::locked_control(&tx).await?;
        if current.revision != update.expected_revision {
            return Err(StorageError::state_conflict(
                "system_runtime_control",
                Some(SYSTEM_RUNTIME_CONTROL_ID),
                format!(
                    "expected revision {}, current revision {}",
                    update.expected_revision, current.revision
                ),
            ));
        }

        let next_mode = update
            .quant_runtime_mode
            .unwrap_or(current.quant_runtime_mode);
        let next_policy = update
            .settlement_write_policy
            .unwrap_or(current.settlement_write_policy);
        let next_kill_switch = update
            .kill_switch_state
            .unwrap_or(current.kill_switch_state);
        let next_requires_ack = update
            .kill_switch_requires_ack
            .unwrap_or(current.kill_switch_requires_ack);
        if next_mode == current.quant_runtime_mode
            && next_policy == current.settlement_write_policy
            && next_kill_switch == current.kill_switch_state
            && next_requires_ack == current.kill_switch_requires_ack
        {
            tx.commit().await.map_err(StorageError::from)?;
            return Ok(current.into());
        }

        let next_revision = current.revision.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some("system_runtime_control"),
                "runtime-control revision overflow",
            )
        })?;
        let now = primitives::statement_timestamp(&tx).await?;
        TransitionEntity::insert(
            NewRuntimeControlTransition {
                runtime_control_transition_id: RuntimeControlTransitionId::from_v7(),
                from_revision: current.revision,
                to_revision: next_revision,
                from_quant_runtime_mode: current.quant_runtime_mode,
                to_quant_runtime_mode: next_mode,
                from_settlement_write_policy: current.settlement_write_policy,
                to_settlement_write_policy: next_policy,
                from_kill_switch_state: current.kill_switch_state,
                to_kill_switch_state: next_kill_switch,
                from_kill_switch_requires_ack: current.kill_switch_requires_ack,
                to_kill_switch_requires_ack: next_requires_ack,
                actor: update.actor.clone(),
                reason: update.reason.clone(),
                occurred_at: now,
            }
            .into_active_model(),
        )
        .exec(&tx)
        .await
        .map_err(StorageError::from)?;

        let mut active: ActiveModel = current.into_active_model();
        active.quant_runtime_mode = Set(next_mode);
        active.settlement_write_policy = Set(next_policy);
        active.kill_switch_state = Set(next_kill_switch);
        active.kill_switch_requires_ack = Set(next_requires_ack);
        active.revision = Set(next_revision);
        active.changed_by = Set(update.actor);
        active.reason = Set(update.reason);
        active.changed_at = Set(now);
        let updated: RuntimeControlInfo =
            active.update(&tx).await.map_err(StorageError::from)?.into();
        primitives::notify(
            &tx,
            RUNTIME_CONTROL_NOTIFY_CHANNEL,
            &next_revision.to_string(),
        )
        .await?;
        tx.commit().await.map_err(StorageError::from)?;
        Ok(updated)
    }
}
