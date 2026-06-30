//! Execution recovery playbook views for operators.

use crate::{
    domain::{KillSwitchView, api::ReconciliationView},
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
};
use serde::{Deserialize, Serialize};

/// Ordered recovery step for unresolvable reconciliation / latched kill-switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRecoveryStep {
    ResolveUnresolvableReconciliations,
    AcknowledgeKillSwitch,
    VerifyModePreflight,
}

/// Lightweight recovery summary embedded in [`SystemStatus`](crate::domain::SystemStatus).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecoverySummary {
    pub has_unresolvable_reconciliation: bool,
    pub unresolvable_count: u64,
    pub kill_switch_requires_ack: bool,
    pub kill_switch_state: KillSwitchState,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub auto_execution_blocked: bool,
    pub next_steps: Vec<ExecutionRecoveryStep>,
}

/// Detailed recovery view for the operator dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionRecoveryView {
    pub summary: ExecutionRecoverySummary,
    pub blocking_reconciliations: Vec<ReconciliationView>,
    pub kill_switch: KillSwitchView,
}
