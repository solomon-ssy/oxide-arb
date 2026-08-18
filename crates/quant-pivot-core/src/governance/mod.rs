//! Runtime authorization and quant governance primitives.

pub mod bias_table;
pub mod calibration_loader;
pub mod entry_authorization_preflight;
pub mod execution_recovery;
pub mod linkage;
pub mod model_governance;
pub mod model_spec;
pub mod operational_phase;
pub(crate) mod policy_snapshot;
pub mod promotion_permit;
pub mod quality_gate_load;
pub mod runtime_control;
pub mod runtime_controls;
pub mod system_capability;
pub mod system_status;

pub use bias_table::BiasTableApplicator;
pub use calibration_loader::{
    CoreCalibrationArtifactLoader, model_score_content_hash, resolve_return_model_calibration,
};
pub use entry_authorization_preflight::{
    AuthorizationPreflight, AuthorizationPreflightDeps, DefaultAuthorizationPreflight,
};
pub use linkage::{LinkageResolverDeps, LinkageResolverService};
pub use model_governance::{ModelGovernanceDeps, ModelGovernanceService};
pub use model_spec::{ModelSpecDeps, ModelSpecService};
pub use operational_phase::operational_phase_from_readiness;
pub use promotion_permit::PromotionPermitService;
pub use quality_gate_load::{active_load_ok, model_contract_ok, shadow_load_ok};
pub use runtime_control::QuantRuntimeControl;
pub use runtime_controls::RuntimeControlsHandle;
pub use system_capability::SystemCapabilityService;
pub use system_status::SystemStatusPublisher;
