//! System control-plane API contract.

use crate::{
    domain::SystemStatus,
    enums::{
        execution::KillSwitchState,
        quant::QuantRuntimeMode,
        system::{BootstrapPhase, CapabilityId, CapabilityReason},
    },
};
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

/// Explicit cold-start activation. The acknowledgement is intentionally named
/// and cannot be confused with a generic confirmation checkbox.
#[derive(Debug, Deserialize, Validate)]
pub struct ActivateBootstrapRequest {
    #[validate(range(min = 1))]
    pub bootstrap_contract_version: i32,
    #[validate(range(min = 0))]
    pub expected_state_revision: i64,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    #[serde(default)]
    pub report_only_forced_ack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapView {
    pub phase: BootstrapPhase,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityView {
    pub enabled: bool,
    pub reasons: Vec<CapabilityReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemCapabilities {
    pub revision: u64,
    pub control_plane_ready: CapabilityView,
    pub catalog_baseline_ready: CapabilityView,
    pub research_capture_enabled: CapabilityView,
    pub report_generation_eligible: CapabilityView,
    pub entry_admission_eligible: CapabilityView,
    pub order_submission_eligible: CapabilityView,
    pub automatic_parity_eligible: CapabilityView,
}

impl SystemCapabilities {
    #[must_use]
    pub fn fail_closed(reason: CapabilityReason) -> Self {
        let disabled = CapabilityView {
            enabled: false,
            reasons: vec![reason],
        };
        Self {
            revision: 0,
            control_plane_ready: disabled.clone(),
            catalog_baseline_ready: disabled.clone(),
            research_capture_enabled: disabled.clone(),
            report_generation_eligible: disabled.clone(),
            entry_admission_eligible: disabled.clone(),
            order_submission_eligible: disabled.clone(),
            automatic_parity_eligible: disabled,
        }
    }

    #[must_use]
    pub const fn get(&self, capability: CapabilityId) -> &CapabilityView {
        match capability {
            CapabilityId::ControlPlaneReady => &self.control_plane_ready,
            CapabilityId::CatalogBaselineReady => &self.catalog_baseline_ready,
            CapabilityId::ResearchCaptureEnabled => &self.research_capture_enabled,
            CapabilityId::ReportGenerationEligible => &self.report_generation_eligible,
            CapabilityId::EntryAdmissionEligible => &self.entry_admission_eligible,
            CapabilityId::OrderSubmissionEligible => &self.order_submission_eligible,
            CapabilityId::AutomaticParityEligible => &self.automatic_parity_eligible,
        }
    }

    #[must_use]
    pub fn decisions_equal(&self, other: &Self) -> bool {
        self.control_plane_ready == other.control_plane_ready
            && self.catalog_baseline_ready == other.catalog_baseline_ready
            && self.research_capture_enabled == other.research_capture_enabled
            && self.report_generation_eligible == other.report_generation_eligible
            && self.entry_admission_eligible == other.entry_admission_eligible
            && self.order_submission_eligible == other.order_submission_eligible
            && self.automatic_parity_eligible == other.automatic_parity_eligible
    }
}

/// Authenticated control-plane status with durable bootstrap and derived
/// capabilities layered over the live operational projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusView {
    #[serde(flatten)]
    pub runtime: SystemStatus,
    pub bootstrap: BootstrapView,
    pub capabilities: SystemCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionEligibilityDecision {
    pub enabled: bool,
    pub permission_granted: bool,
    pub capability: CapabilityView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionEligibilityView {
    pub capability_revision: u64,
    pub report_generation: ActionEligibilityDecision,
    pub entry_admission: ActionEligibilityDecision,
    pub order_submission: ActionEligibilityDecision,
}
