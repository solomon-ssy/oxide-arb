//! Atomic operational runtime-control singleton.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::enums::{
    execution::KillSwitchState, quant::QuantRuntimeMode, settlement::SettlementWritePolicy,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "system_runtime_control")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub settlement_write_policy: SettlementWritePolicy,
    pub kill_switch_state: KillSwitchState,
    pub kill_switch_requires_ack: bool,
    pub revision: i64,
    #[sea_orm(column_type = "Text")]
    pub changed_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
