//! Shared integration-test harnesses, fixtures, and mocks for `oxide-arb`.
//!
//! This crate is `publish = false` — it exists solely to eliminate `#[path]`
//! hacks and `#[allow(dead_code)]` across `oxide-arb-core` integration tests
//! and `oxide-arb-bench` benchmarks.

pub mod book;
pub mod fixtures;
pub mod mock_event;
pub mod mocks;
pub mod persistence;
pub mod pipeline;
pub mod risk;
pub mod runtime;
