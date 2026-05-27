//! Backend trait abstractions for stateful algorithm components.
//!
//! These traits decouple the opportunity pipeline from the concrete storage
//! mechanism. The default implementations use `moka::sync::Cache` (in-process),
//! but can be replaced with Redis-backed implementations for multi-instance
//! deployments.

use crate::endgame::convergence::ConvergenceDirection;
use chrono::{DateTime, Utc};
use oxide_arb_models::types::MarketId;

/// Backend for per-market emission cooldown state.
///
/// Controls how often the same market can emit opportunities, preventing
/// duplicate signals on consecutive scan ticks.
///
/// TODO(redis-backend): implement `RedisEmissionCooldown` with atomic Redis
/// scripts before running multiple scanner instances. The distributed backend
/// must preserve consecutive-hit increments and cooldown expiry atomically.
pub trait CooldownBackend: Send + Sync + 'static {
    /// Returns `true` if the market may emit (NOT in cooldown).
    fn may_emit(&self, market_id: &MarketId) -> bool;

    /// Mark a successful emission, starting/extending the cooldown timer.
    fn record_emission(&self, market_id: &MarketId);

    /// Reset cooldown for a market (e.g. price left convergence zone).
    fn reset(&self, market_id: &MarketId);

    /// Swap-and-reset suppressed counter for metrics reporting.
    fn take_suppressed_count(&self) -> u64;

    /// Number of markets currently tracked.
    fn tracked_count(&self) -> u64;
}

/// Backend for per-market convergence duration tracking.
///
/// Tracks how long each market has been in the convergence zone,
/// used by the endgame detector to enforce minimum convergence duration.
///
/// TODO(redis-backend): implement `RedisConvergenceTracker` with timestamped
/// Redis state before running multiple scanner instances. Direction changes
/// and first-seen timestamp updates must be atomic per market.
pub trait ConvergenceBackend: Send + Sync + 'static {
    /// Update convergence state and return duration in seconds since first entry.
    ///
    /// If direction matches existing entry, returns elapsed seconds.
    /// If direction changed or entry expired, resets timer and returns 0.
    fn update_and_get(
        &self,
        market_id: &MarketId,
        direction: ConvergenceDirection,
        now: DateTime<Utc>,
    ) -> u64;

    /// Remove tracking for a market (e.g. on resolution or zone exit).
    fn remove(&self, market_id: &MarketId);

    /// Number of markets currently being tracked.
    fn tracked_count(&self) -> u64;
}
