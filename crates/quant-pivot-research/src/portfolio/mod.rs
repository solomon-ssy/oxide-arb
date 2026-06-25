//! Portfolio-plane compute inputs.
//!
//! Currently holds the [`AccountSnapshot`] capital base consumed by the
//! governed planner (04.1). The greedy allocator still lives under
//! [`crate::backtest`] and is reused by the planner.

pub mod account;

pub use account::AccountSnapshot;
