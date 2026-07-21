//! Cross-crate system-test composition.
//!
//! This crate is the only owner of tests that cross package or infrastructure
//! boundaries. Each suite starts one disposable infrastructure stack and runs
//! its scenarios against that stack; production crates keep only unit and
//! deterministic contract tests.

pub mod postgres;
pub mod production_stack;
pub mod resources;
pub mod stack;
pub mod support;
