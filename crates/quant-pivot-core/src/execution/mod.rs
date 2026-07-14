//! Execution-plane contracts.
//!
//! Phase 05.0 defines the boundaries only. Implementations, worker wiring,
//! mode preflight, admission business rules, dispatch, and exit monitoring land
//! in later Phase 05 slices.

pub mod admission;
pub mod attribution;
pub mod breaker;
pub mod dispatch_wake;
pub mod dispatcher;
pub mod entry_condition;
pub mod entry_condition_worker;
pub mod exit_dispatcher;
pub mod exit_monitor;
pub mod exit_monitor_service;
pub mod intent_lifecycle;
pub mod intent_service;
pub mod mode_gate;
pub mod order_client;
pub mod reconciliation;
pub mod settlement_redeem;
pub mod trade_policy_guard;

pub use admission::*;
pub use attribution::*;
pub use breaker::*;
pub use dispatch_wake::*;
pub use dispatcher::*;
pub use entry_condition::*;
pub use entry_condition_worker::*;
pub use exit_dispatcher::*;
pub use exit_monitor::*;
pub use exit_monitor_service::*;
pub use intent_lifecycle::*;
pub use intent_service::*;
pub use mode_gate::*;
pub use order_client::*;
pub use reconciliation::*;
pub use settlement_redeem::*;
pub use trade_policy_guard::*;
