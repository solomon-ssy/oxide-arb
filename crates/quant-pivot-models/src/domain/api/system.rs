//! System control-plane API contract.

use crate::enums::{execution::KillSwitchState, quant::QuantRuntimeMode};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Governed quant runtime mode transition request.
#[derive(Debug, Deserialize, Validate)]
pub struct SwitchQuantModeRequest {
    pub mode: QuantRuntimeMode,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Quant runtime mode read model.
#[derive(Debug, Serialize)]
pub struct QuantModeView {
    pub mode: QuantRuntimeMode,
}

/// Governed operational kill-switch transition request.
#[derive(Debug, Deserialize, Validate)]
pub struct SetKillSwitchRequest {
    pub state: KillSwitchState,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    /// Operator acknowledgement, required to clear `emergency_halted`.
    #[serde(default)]
    pub ack: bool,
}
