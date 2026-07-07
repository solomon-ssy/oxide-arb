//! `quant-pivot-core` — Polymarket quant-pivot system hub.
//!
//! Phase 0: data ingest, governance, and admin wiring. Report and execution
//! planes arrive in later phases.

pub mod app;
pub mod execution;
pub mod governance;
pub mod infra;
pub mod ingest;
pub mod observability;
pub mod pit;
pub mod prefetch;
pub mod projection;
pub mod report;
pub mod runtime_config;
pub mod service;
