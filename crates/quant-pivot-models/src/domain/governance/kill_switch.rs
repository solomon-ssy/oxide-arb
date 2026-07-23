//! Operator projection of the kill-switch fields in the atomic runtime control.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::execution::KillSwitchState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KillSwitchView {
    pub state: KillSwitchState,
    pub requires_operator_ack: bool,
    pub revision: i64,
    pub last_reason: String,
    pub changed_by: String,
    pub changed_at: DateTime<Utc>,
}
