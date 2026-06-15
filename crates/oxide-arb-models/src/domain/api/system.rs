//! System + risk control-plane API contract.
//!
//! Control endpoints are mutating and money-critical. Each carries a mandatory
//! `reason` (recorded on the operation log) and the execution-mode switch is
//! additionally governed by the `X-Acting-Role` header (authorized by the authz
//! middleware) since entering `Live` is the highest-risk operator action.

use crate::enums::{common::ExecutionMode, control_factor::MaterializationRunStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Governed runtime execution-mode hot-swap request.
#[derive(Debug, Deserialize, Validate)]
pub struct SwitchModeRequest {
    /// Target execution mode (`dry_run` / `paper` / `live`).
    pub mode: ExecutionMode,
    /// Operator justification, recorded on the operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Halt trading (risk halt + execution kill switch).
#[derive(Debug, Deserialize, Validate)]
pub struct HaltRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Resume trading after operator acknowledgement.
#[derive(Debug, Deserialize, Validate)]
pub struct ResumeRequest {
    /// Operator acknowledgement string recorded on the risk audit.
    #[validate(length(min = 1, max = 256))]
    pub operator_ack: String,
}

/// Force the circuit breaker back to `Closed`.
#[derive(Debug, Deserialize, Validate)]
pub struct CircuitBreakerResetRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Runtime activation state for a scheduled materialization cadence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MaterializationScheduleActivationView {
    Runnable,
    Inactive {
        reason: MaterializationScheduleInactiveReasonView,
    },
}

/// Why a materialization cadence is inactive in the current execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationScheduleInactiveReasonView {
    UnsupportedExecutionMode,
    LiveOnlyEvidence,
    EvidenceWarmup,
}

/// Operator-facing execution-mode contract for a scheduled cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationScheduleModeContractView {
    AllModes,
    LiveOnly,
    LiveAfterEvidenceWarmup,
}

/// Read-only health/status projection for one materialization schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaterializationScheduleStatusView {
    pub schedule_id: String,
    pub activation: MaterializationScheduleActivationView,
    pub mode_contract: MaterializationScheduleModeContractView,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_terminal_status: Option<MaterializationRunStatus>,
    pub next_due_at: Option<DateTime<Utc>>,
}
