//! Control factor materialization and governance plane.
//!
//! This crate owns offline control-factor validation, governance transitions,
//! and later materialization/evidence orchestration. It intentionally does not
//! depend on `oxide-arb-core`; live hot-path snapshot publication belongs there.

pub mod evidence;
pub mod factor;
pub mod gates;
pub mod governance;
pub mod materialization;
pub mod report;
pub mod scheduler;
