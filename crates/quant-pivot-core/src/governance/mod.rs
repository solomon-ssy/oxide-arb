//! Runtime mode handle and quant governance primitives.

pub mod execution_recovery;
pub mod factor_governance;
pub mod kill_switch;
pub mod mode_preflight;
pub mod mode_transition;
pub mod model_governance;
pub mod operational_phase;
pub mod quality_gate_load;
pub mod runtime_control;
pub mod runtime_mode;
pub mod runtime_model_pointers;
pub mod system_status;
pub mod weight_overlay;

pub use factor_governance::{FactorGovernanceDeps, FactorGovernanceService};
pub use kill_switch::{KillSwitchControl, KillSwitchHandle};
pub use mode_preflight::{DefaultModePreflight, ModePreflight, ModePreflightDeps};
pub use mode_transition::{DefaultModeTransitionGate, ModeTransitionGate};
pub use model_governance::{ModelGovernanceDeps, ModelGovernanceService};
pub use operational_phase::operational_phase_from_readiness;
pub use quality_gate_load::{
    active_load_ok, active_publication_status_ok, quality_gate_passed_ok,
    quality_gate_staleness_ok, shadow_load_ok, shadow_publication_status_ok,
};
pub use runtime_control::QuantRuntimeControl;
pub use runtime_mode::RuntimeModeHandle;
pub use runtime_model_pointers::{
    RuntimeModelPointerSync, sync_production_active, sync_shadow_candidate,
};
pub use system_status::SystemStatusPublisher;
pub use weight_overlay::{WeightOverlayApplicator, WeightOverlaySnapshot};
