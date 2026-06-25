//! Runtime mode handle and quant governance primitives.

pub mod model_governance;
pub mod quality_gate_load;
pub mod runtime_control;
pub mod runtime_mode;
pub mod runtime_model_pointers;
pub mod weight_overlay;

pub use model_governance::{ModelGovernanceDeps, ModelGovernanceService};
pub use quality_gate_load::{
    active_load_ok, active_publication_status_ok, quality_gate_passed_ok,
    quality_gate_staleness_ok, shadow_load_ok, shadow_publication_status_ok,
};
pub use runtime_control::QuantRuntimeControl;
pub use runtime_mode::RuntimeModeHandle;
pub use runtime_model_pointers::{
    RuntimeModelPointerSync, sync_production_active, sync_shadow_candidate,
};
pub use weight_overlay::{WeightOverlayApplicator, WeightOverlaySnapshot};
