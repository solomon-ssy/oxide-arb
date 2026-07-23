//! Append-only runtime-control transition audit.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::{
        execution::KillSwitchState, quant::QuantRuntimeMode, settlement::SettlementWritePolicy,
    },
    types::RuntimeControlTransitionId,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "system_runtime_control_transition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "Text")]
    pub actor: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
