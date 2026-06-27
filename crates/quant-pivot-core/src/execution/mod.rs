//! Execution-plane contracts.
//!
//! Phase 05.0 defines the boundaries only. Implementations, worker wiring,
//! mode preflight, admission business rules, dispatch, and exit monitoring land
//! in later Phase 05 slices.

pub mod admission;
pub mod breaker;
pub mod dispatch_wake;
pub mod dispatcher;
pub mod exit_monitor;
pub mod intent_service;
pub mod mode_gate;
pub mod order_client;

pub use admission::*;
pub use breaker::*;
pub use dispatch_wake::*;
pub use dispatcher::*;
pub use exit_monitor::*;
pub use intent_service::*;
pub use mode_gate::*;
pub use order_client::*;
