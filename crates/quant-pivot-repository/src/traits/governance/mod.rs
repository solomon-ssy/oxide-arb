//! Governance context repository traits: operation log, reports, runtime-config
//! versions, domain events, and resolution events.

pub mod event;
pub mod operation_log;
pub mod runtime_config;
pub mod runtime_control;

pub use event::EventRepository;
pub use operation_log::OperationLogRepository;
pub use runtime_config::PolicyRepository;
pub use runtime_control::RuntimeControlRepository;
