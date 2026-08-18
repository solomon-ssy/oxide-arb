//! Execution recovery playbook views for operators.

use serde::{Deserialize, Serialize};

use crate::{
    domain::{api::ReconciliationView, governance::KillSwitchView},
    enums::{execution::KillSwitchState, quant::EntryAuthorizationPolicy},
};

/// Ordered recovery step for unresolvable reconciliation / latched kill-switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRecoveryStep {
    ResolveUnresolvableReconciliations,
    AcknowledgeKillSwitch,
    VerifyAuthorizationPreflight,
}

/// Lightweight recovery summary embedded in [`SystemStatus`](crate::domain::governance::SystemStatus).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecoverySummary {
    pub has_unresolvable_reconciliation: bool,
    pub unresolvable_count: u64,
    pub kill_switch_requires_ack: bool,
    pub kill_switch_state: KillSwitchState,
    pub entry_authorization_policy: EntryAuthorizationPolicy,
    pub policy_automatic_blocked: bool,
    pub next_steps: Vec<ExecutionRecoveryStep>,
}

/// Detailed recovery view for the operator dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionRecoveryView {
    pub summary: ExecutionRecoverySummary,
    pub blocking_reconciliations: Vec<ReconciliationView>,
    pub kill_switch: KillSwitchView,
}
