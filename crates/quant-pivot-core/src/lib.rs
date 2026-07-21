//! `quant-pivot-core` — Polymarket quant-pivot system hub.
//!
//! It owns application composition for data ingest, research, reporting,
//! governance, execution, and administration.

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
#[cfg(test)]
mod test_fixtures;
