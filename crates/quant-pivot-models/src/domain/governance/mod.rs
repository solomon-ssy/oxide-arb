//! Governance & platform context for quant-pivot control-plane state.

pub mod entry_authorization;
pub mod kill_switch;
pub mod lifecycle;
pub mod operation_log;
pub mod runtime_control;
pub mod system;

pub use entry_authorization::{AuthorizationPreflightCheck, AuthorizationPreflightReport};
pub use kill_switch::KillSwitchView;
pub use lifecycle::{
    MarketDataConnectivity, OperationalDegradeReason, OperationalPhase,
    WS_MARKET_DATA_STALE_THRESHOLD_MS, WsShardConnectivity,
};
pub use operation_log::{NewOperationLog, OperationLogInfo};
pub use runtime_control::{
    NewRuntimeControlTransition, RuntimeControlInfo, RuntimeControlSnapshot, RuntimeControlUpdate,
};
pub use system::{
    ActivePolicyResourceInfo, ConfigActivityInfo, ConfigResourceInventoryInfo,
    ConfigResourceInventoryRow, DecisionPolicySnapshotInfo, DecisionPolicySnapshotOptionInfo,
    HealthReport, NewDecisionPolicySnapshot, NewModelBootstrapActivation,
    NewModelPromotionActivation, NewPolicyActivation, NewPolicyActivationAudit,
    NewPolicyActivationEventOutbox, NewPolicyApproval, NewPolicyProfileArtifact, NewPolicyRevision,
    PolicyActivationCommit, PolicyActivationInfo, PolicyActivationOutcome, PolicyApprovalInfo,
    PolicyProfileArtifactInfo, PolicyRevisionInfo, RecordPolicyApproval, ShutdownProgress,
    SubsystemCheckStatus, SubsystemHealth, SystemStatus,
};
