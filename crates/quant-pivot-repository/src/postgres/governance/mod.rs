//! Governance context Postgres repositories: operation log, runtime config,
//! and system runtime state.

mod config_activity;
mod config_resources;

pub mod kill_switch;
pub mod operation_log;
pub mod policy_bootstrap;
pub mod runtime_config;
pub mod system_runtime_state;

pub use kill_switch::{PgKillSwitchStateRepository, SYSTEM_KILL_SWITCH_ID};
pub use operation_log::PgOperationLogRepository;
pub use runtime_config::PgPolicyRepository;
pub use system_runtime_state::PgSystemRuntimeStateRepository;
