//! Atomic runtime-control state and transition contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::system_runtime_control,
    enums::{
        execution::KillSwitchState, quant::QuantRuntimeMode, settlement::SettlementWritePolicy,
    },
    types::RuntimeControlTransitionId,
};

/// Durable singleton row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::system_runtime_control::Entity")]
pub struct RuntimeControlInfo {
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub settlement_write_policy: SettlementWritePolicy,
    pub kill_switch_state: KillSwitchState,
    pub kill_switch_requires_ack: bool,
    pub revision: i64,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    RuntimeControlInfo,
    system_runtime_control::Model,
    {
        id,
        quant_runtime_mode,
        settlement_write_policy,
        kill_switch_state,
        kill_switch_requires_ack,
        revision,
        changed_by,
        reason,
        changed_at,
        updated_at,
    }
);

/// One coherent hot-path snapshot. Consumers must never read mode, settlement
/// policy, and kill-switch from separate atomics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeControlSnapshot {
    pub quant_runtime_mode: QuantRuntimeMode,
    pub settlement_write_policy: SettlementWritePolicy,
    pub kill_switch_state: KillSwitchState,
    pub kill_switch_requires_ack: bool,
    pub revision: i64,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
}

impl From<RuntimeControlInfo> for RuntimeControlSnapshot {
    fn from(info: RuntimeControlInfo) -> Self {
        Self {
            quant_runtime_mode: info.quant_runtime_mode,
            settlement_write_policy: info.settlement_write_policy,
            kill_switch_state: info.kill_switch_state,
            kill_switch_requires_ack: info.kill_switch_requires_ack,
            revision: info.revision,
            changed_by: info.changed_by,
            reason: info.reason,
            changed_at: info.changed_at,
        }
    }
}

/// Expected-revision update. Exactly one target field must be present.
#[derive(Debug, Clone)]
pub struct RuntimeControlUpdate {
    pub expected_revision: i64,
    pub quant_runtime_mode: Option<QuantRuntimeMode>,
    pub settlement_write_policy: Option<SettlementWritePolicy>,
    pub kill_switch_state: Option<KillSwitchState>,
    pub kill_switch_requires_ack: Option<bool>,
    pub actor: String,
    pub reason: String,
}

/// Append-only audit row written in the same transaction as the singleton CAS.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::system_runtime_control_transition::ActiveModel",
    exhaustive
)]
pub struct NewRuntimeControlTransition {
    pub runtime_control_transition_id: RuntimeControlTransitionId,
    pub from_revision: i64,
    pub to_revision: i64,
    pub from_quant_runtime_mode: QuantRuntimeMode,
    pub to_quant_runtime_mode: QuantRuntimeMode,
    pub from_settlement_write_policy: SettlementWritePolicy,
    pub to_settlement_write_policy: SettlementWritePolicy,
    pub from_kill_switch_state: KillSwitchState,
    pub to_kill_switch_state: KillSwitchState,
    pub from_kill_switch_requires_ack: bool,
    pub to_kill_switch_requires_ack: bool,
    pub actor: String,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
}
