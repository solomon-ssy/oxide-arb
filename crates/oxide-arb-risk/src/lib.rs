//! `oxide-arb-risk` — Independent risk engine crate.
//!
//! Implements circuit breaker, position sizing, exposure limits, blacklist
//! management, accounting, and drawdown protection. Communicates with the
//! core system exclusively through the [`traits::RiskMetrics`] and
//! [`traits::RiskPersistence`] traits (dependency injection).
//!
//! **Does not depend on `oxide-arb-core`.**
//!
//! Public API uses explicit module paths. No compatibility re-exports.

pub mod accounting;
pub mod audit;
pub mod blacklist;
pub mod builder;
pub mod circuit_breaker;
pub mod clock;
pub mod context;
pub mod engine;
pub mod pipeline;
pub mod position;
pub mod reconciliation;
pub mod sizing;
pub mod state_store;
pub mod traits;
pub mod types;
