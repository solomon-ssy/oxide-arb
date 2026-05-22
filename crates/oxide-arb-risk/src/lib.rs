//! `oxide-arb-risk` — Independent risk engine crate.
//!
//! Implements circuit breaker, position sizing, exposure limits, blacklist
//! management, accounting, and drawdown protection. Communicates with the
//! core system exclusively through the [`RiskMetrics`] and [`RiskPersistence`]
//! traits (dependency injection).
//!
//! **Does not depend on `oxide-arb-core`.**
