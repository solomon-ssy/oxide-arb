//! `quant-pivot-core` — Polymarket quant-pivot system hub.
//!
//! Phase 0: data ingest, governance, and admin wiring. Report and execution
//! planes arrive in later phases.

pub mod app;
pub mod governance;
pub mod infra;
pub mod observability;
pub mod pipeline;
pub mod runtime_config;
pub mod service;
