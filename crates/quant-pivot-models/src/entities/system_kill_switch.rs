//! `system_kill_switch` table entity (singleton row).

use crate::enums::execution::KillSwitchState;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "system_kill_switch")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub state: KillSwitchState,
    #[sea_orm(column_type = "Text")]
    pub changed_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub requires_operator_ack: bool,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
