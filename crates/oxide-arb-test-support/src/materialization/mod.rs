//! Materialization integration fixtures and the Phase 5.3 smoke scenario.

mod fixtures;
mod smoke;

pub use fixtures::*;
pub use smoke::{
    SMOKE_HOLDER, SMOKE_MARKET_ID, SMOKE_NO_TOKEN, SMOKE_OPPORTUNITY_ID, SMOKE_YES_TOKEN,
    SmokeRepositories, smoke_manifest, smoke_runtime_config_version, smoke_simulation_config,
    smoke_window,
};
