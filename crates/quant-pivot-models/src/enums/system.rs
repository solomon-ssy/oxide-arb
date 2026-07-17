//! System health enums.

use serde::{Deserialize, Serialize};

pg_enum! {
    type_name = "qp_bootstrap_phase",
    /// Durable cold-start lifecycle. Runtime degradation is derived separately.
    @derive(Default)
    pub enum BootstrapPhase {
        #[default]
        Initializing => "initializing",
        CollectingBaseline => "collecting_baseline",
        AwaitingActivation => "awaiting_activation",
        Active => "active",
    }
}

wire_enum! {
    /// Stable identifier used to gate capability-sensitive runtime tasks.
    pub enum CapabilityId {
        ControlPlaneReady => "control_plane_ready",
        CatalogBaselineReady => "catalog_baseline_ready",
        ResearchCaptureEnabled => "research_capture_enabled",
        ReportGenerationEligible => "report_generation_eligible",
        EntryAdmissionEligible => "entry_admission_eligible",
        OrderSubmissionEligible => "order_submission_eligible",
        AutomaticParityEligible => "automatic_parity_eligible",
    }
}

wire_enum! {
    /// Stable machine-readable reason why a derived runtime capability is disabled.
    pub enum CapabilityReason {
        BootstrapInitializing => "bootstrap_initializing",
        ControlPlaneNotReady => "control_plane_not_ready",
        CatalogBaselineMissing => "catalog_baseline_missing",
        BootstrapNotCollecting => "bootstrap_not_collecting",
        BootstrapNotActive => "bootstrap_not_active",
        OperationalPhaseBlocksReports => "operational_phase_blocks_reports",
        RuntimeModeReportOnly => "runtime_mode_report_only",
        KillSwitchBlocksEntries => "kill_switch_blocks_entries",
        OperationalPhaseBlocksSubmission => "operational_phase_blocks_submission",
        NoServingEvidence => "no_serving_evidence",
    }
}

/// Oracle/data source health state for sliding-window evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealth {
    Healthy,
    Degraded,
    Down,
}

/// Graceful shutdown lifecycle stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownStage {
    NotStarted,
    SignalReceived,
    Draining,
    Stopped,
}

/// WebSocket shard connection lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardConnectionStatus {
    Connected,
    Reconnecting { attempt: u32 },
    Disconnected,
}
