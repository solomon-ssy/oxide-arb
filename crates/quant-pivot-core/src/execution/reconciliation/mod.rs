//! Reconciliation closed loop (Phase 05.5).
//!
//! Brings the internal ledgers (`quant_execution_order` / `quant_capital_allocation`
//! / `quant_position`) into agreement with Polymarket venue truth. For every
//! truth-unknown order the engine collects venue evidence in a fixed order,
//! decides a terminal verdict, and applies a single idempotent correction.
//! An `Unresolvable` verdict latches the kill-switch (fail-closed) until an
//! operator resolves it.

mod collector;
mod decide;
mod reader;
mod service;

pub use collector::*;
pub use decide::*;
pub use reader::*;
pub use service::*;
