//! `system_runtime_state` table entity (singleton row).

use crate::enums::{quant::QuantRuntimeMode, system::BootstrapPhase};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "system_runtime_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub bootstrap_phase: BootstrapPhase,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
    #[sea_orm(column_type = "Text")]
    pub changed_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
