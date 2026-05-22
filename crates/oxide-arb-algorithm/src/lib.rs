//! Endgame strategy detection, resolution calibration, and opportunity scoring.
//!
//! `oxide-arb-algorithm` is a **pure computation** crate — it holds no
//! connections, runtime state, or I/O handles. All external dependencies
//! (fee estimation, calibration persistence) are injected via traits.
//!
//! # Module layout
//!
//! - [`fee`] — `FeeEstimator` trait for dependency-injected fee calculation.
//! - [`staleness`] — `StalenessPolicy` confidence discount lookup.
//! - [`urgency`] — `UrgencyFactor` non-linear urgency multiplier.
//! - [`cooldown`] — `InMemoryEmissionCooldown` per-market duplicate suppression.
//! - [`walker`] — `OrderbookWalker` simulates order execution through book levels.
//! - [`endgame`] — `EndgameDetector`, `InMemoryConvergenceTracker`, `ConfidenceFusion`.
//! - [`calibration`] — `ResolutionCalibrator`, `MoM` priors, 4-tier fallback, updater.
//! - [`fill_probability`] — Endgame-specific fill probability estimation.
//! - [`scorer`] — `EndgameScorer` composite opportunity ranking.
//! - [`pipeline`] — `OpportunityPipeline` end-to-end detect→score→emit orchestration.

pub mod backend;
pub mod calibration;
pub mod cooldown;
pub mod endgame;
pub mod fee;
pub mod fill_probability;
pub mod pipeline;
pub mod scorer;
pub mod staleness;
pub mod urgency;
pub mod walker;
