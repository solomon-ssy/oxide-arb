//! Governance & platform context for quant-pivot control-plane state.

pub mod kill_switch;
pub mod lifecycle;
pub mod mode;
pub mod operation_log;
pub mod runtime_control;
pub mod system;

pub use kill_switch::KillSwitchView;
pub use lifecycle::{
    MarketDataConnectivity, OperationalDegradeReason, OperationalPhase,
    WS_MARKET_DATA_STALE_THRESHOLD_MS, WsShardConnectivity,
};
pub use mode::{PreflightCheck, PreflightReport};
pub use operation_log::{NewOperationLog, OperationLogInfo};
pub use runtime_control::{
    NewRuntimeControlTransition, RuntimeControlInfo, RuntimeControlSnapshot, RuntimeControlUpdate,
};
pub use system::{
    ActivePolicyResourceInfo, ConfigActivityInfo, ConfigResourceInventoryInfo,
    ConfigResourceInventoryRow, DecisionPolicySnapshotInfo, DecisionPolicySnapshotOptionInfo,
    HealthReport, NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyApproval,
    NewPolicyProfileArtifact, NewPolicyRevision, PolicyActivationCommit, PolicyActivationInfo,
    PolicyActivationOutcome, PolicyApprovalInfo, PolicyProfileArtifactInfo, PolicyRevisionInfo,
    RecordPolicyApproval, ShutdownProgress, SubsystemCheckStatus, SubsystemHealth, SystemStatus,
};
