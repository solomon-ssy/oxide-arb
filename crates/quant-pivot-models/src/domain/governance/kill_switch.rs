//! Execution kill-switch operational-state persistence DTOs.

use crate::enums::execution::KillSwitchState;
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// DB row projection for the `system_kill_switch` singleton.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::system_kill_switch::Entity")]
pub struct KillSwitchStateInfo {
    pub id: i32,
    pub state: KillSwitchState,
    pub changed_by: String,
    pub reason: String,
    pub requires_operator_ack: bool,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    KillSwitchStateInfo,
    crate::entities::system_kill_switch::Model,
    {
        id,
        state,
        changed_by,
        reason,
        requires_operator_ack,
        changed_at,
        updated_at,
    }
);

/// Upsert payload for the kill-switch singleton (`id` is always the singleton key).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::system_kill_switch::ActiveModel")]
pub struct UpsertKillSwitchState {
    pub id: i32,
    pub state: KillSwitchState,
    pub changed_by: String,
    pub reason: String,
    pub requires_operator_ack: bool,
    pub changed_at: DateTime<Utc>,
}

/// Explicit patch used by governance APIs in later execution phases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchStatePatch {
    pub state: KillSwitchState,
    pub changed_by: String,
    pub reason: String,
    pub requires_operator_ack: bool,
}
