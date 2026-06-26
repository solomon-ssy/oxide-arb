//! Postgres-backed operational kill-switch repository.

use crate::traits::KillSwitchStateRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{KillSwitchStateInfo, UpsertKillSwitchState},
    entities::system_kill_switch,
};
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};

/// Singleton row id for `system_kill_switch`.
pub const SYSTEM_KILL_SWITCH_ID: i32 = 1;

/// Postgres-backed kill-switch singleton repository.
pub struct PgKillSwitchStateRepository {
    db: DatabaseConnection,
}

impl PgKillSwitchStateRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl KillSwitchStateRepository for PgKillSwitchStateRepository {
    async fn load(&self) -> Result<Option<KillSwitchStateInfo>, StorageError> {
        system_kill_switch::Entity::find_by_id(SYSTEM_KILL_SWITCH_ID)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert(
        &self,
        state: UpsertKillSwitchState,
    ) -> Result<KillSwitchStateInfo, StorageError> {
        if state.id != SYSTEM_KILL_SWITCH_ID {
            return Err(StorageError::Conflict(format!(
                "system_kill_switch id must be {SYSTEM_KILL_SWITCH_ID}, got {}",
                state.id
            )));
        }

        let existing = system_kill_switch::Entity::find_by_id(SYSTEM_KILL_SWITCH_ID)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;

        let Some(row) = existing else {
            return system_kill_switch::Entity::insert(state.into_active_model())
                .exec_with_returning(&self.db)
                .await
                .map_err(StorageError::from)
                .map(Into::into);
        };

        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(state.state);
        active.changed_by = ActiveValue::Set(state.changed_by);
        active.reason = ActiveValue::Set(state.reason);
        active.requires_operator_ack = ActiveValue::Set(state.requires_operator_ack);
        active.changed_at = ActiveValue::Set(state.changed_at);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}
