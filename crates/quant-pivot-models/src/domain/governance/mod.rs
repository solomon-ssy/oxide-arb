//! Governance & platform context for quant-pivot control-plane state.

pub mod kill_switch;
pub mod lifecycle;
pub mod mode;
pub mod operation_log;
pub mod system;

pub use kill_switch::{
    KillSwitchStateInfo, KillSwitchStatePatch, KillSwitchView, UpsertKillSwitchState,
};
pub use lifecycle::{
    MarketDataConnectivity, OperationalDegradeReason, OperationalPhase,
    WS_MARKET_DATA_STALE_THRESHOLD_MS, WsShardConnectivity,
};
pub use mode::{PreflightCheck, PreflightReport};
pub use operation_log::{NewOperationLog, OperationLogInfo};
pub use system::{
    ActivateBootstrapState, ActivePolicyResourceInfo, BootstrapActivationInfo, ConfigActivityInfo,
    ConfigResourceInventoryInfo, ConfigResourceInventoryRow, DecisionPolicySnapshotInfo,
    DecisionPolicySnapshotOptionInfo, HealthReport, NewDecisionPolicySnapshot, NewPolicyActivation,
    NewPolicyApproval, NewPolicyProfileArtifact, NewPolicyRevision, NewProductionBaseline,
    NewProductionEvidence, NewSystemBootstrapTransition, PolicyActivationCommit,
    PolicyActivationInfo, PolicyActivationOutcome, PolicyApprovalInfo, PolicyProfileArtifactInfo,
    PolicyRevisionInfo, ProductionBaselineInfo, ProductionEvidenceInfo, RecordPolicyApproval,
    ShutdownProgress, SubsystemCheckStatus, SubsystemHealth, SystemRuntimeStateInfo, SystemStatus,
    UpsertSystemRuntimeState,
};
