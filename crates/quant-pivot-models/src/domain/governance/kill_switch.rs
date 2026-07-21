//! Execution kill-switch operational-state persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{entities::system_kill_switch, enums::execution::KillSwitchState};

/// DB row projection for the `system_kill_switch` singleton.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
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
    system_kill_switch::Model,
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

/// Operator-facing projection of the operational kill-switch singleton.
///
/// Surfaced by `GET /api/system/kill-switch`, embedded in `SystemStatus`, and
/// published on the `system.status` WebSocket channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KillSwitchView {
    pub state: KillSwitchState,
    pub requires_operator_ack: bool,
    pub last_reason: String,
    pub changed_by: String,
    pub changed_at: DateTime<Utc>,
}

impl From<KillSwitchStateInfo> for KillSwitchView {
    fn from(info: KillSwitchStateInfo) -> Self {
        Self {
            state: info.state,
            requires_operator_ack: info.requires_operator_ack,
            last_reason: info.reason,
            changed_by: info.changed_by,
            changed_at: info.changed_at,
        }
    }
}
