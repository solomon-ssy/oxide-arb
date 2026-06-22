//! Quant research pipeline (feature/factor/model materialization).
//!
//! Phase 1+ crate scaffold — workers land in subsequent phases.

#![deny(unsafe_code)]

/// Placeholder library root for the quant research crate.
pub const fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
