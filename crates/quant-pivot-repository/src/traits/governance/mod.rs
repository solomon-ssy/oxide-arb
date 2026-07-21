//! Governance context repository traits: operation log, reports, runtime-config
//! versions, domain events, and resolution events.

pub mod event;
pub mod kill_switch;
pub mod operation_log;
pub mod runtime_config;
pub mod system_runtime_state;

pub use event::EventRepository;
pub use kill_switch::KillSwitchStateRepository;
pub use operation_log::OperationLogRepository;
pub use runtime_config::PolicyRepository;
pub use system_runtime_state::SystemRuntimeStateRepository;
