//! In-memory per-market emission cooldown with exponential backoff.
//!
//! Prevents the same market from flooding the opportunity pipeline with
//! duplicate signals on every scan tick. Uses `moka::sync::Cache` for
//! automatic TTL-based eviction of stale entries.
//!
//! Implements [`CooldownBackend`] for use as the default in-process backend.

use crate::backend::CooldownBackend;
use moka::sync::Cache;
use oxide_arb_models::{config::EmissionCooldownConfig, types::MarketId};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Tracks the last emission time and consecutive hit count per market.
#[derive(Debug, Clone)]
struct CooldownEntry {
    emitted_at: Instant,
    consecutive_hits: u32,
}

/// In-memory per-market emission cooldown with exponential backoff.
///
/// After emitting an opportunity for a market, further emissions are
/// suppressed for `base_cooldown × min(2^consecutive_hits, max_multiplier)`
/// seconds. The cooldown resets when explicitly cleared (e.g. the market
/// leaves the convergence zone).
pub struct InMemoryEmissionCooldown {
    entries: Cache<MarketId, CooldownEntry>,
    base_cooldown: Duration,
    max_multiplier: Decimal,
    suppressed_count: AtomicU64,
}

impl InMemoryEmissionCooldown {
    /// Create a new cooldown tracker from configuration.
    #[must_use]
    pub fn new(config: &EmissionCooldownConfig) -> Self {
        let max_mult_secs = config.max_multiplier.ceil().to_u64().unwrap_or(16);
        let max_ttl_secs = config.base_cooldown_secs.saturating_mul(max_mult_secs);

        Self {
            entries: Cache::builder()
                .max_capacity(config.max_capacity)
                .time_to_live(Duration::from_secs(max_ttl_secs))
                .build(),
            base_cooldown: Duration::from_secs(config.base_cooldown_secs),
            max_multiplier: config.max_multiplier,
            suppressed_count: AtomicU64::new(0),
        }
    }

    /// Returns `true` if the market may emit (NOT in cooldown).
    #[must_use]
    #[inline]
    pub fn may_emit(&self, market_id: &MarketId) -> bool {
        let Some(entry) = self.entries.get(market_id) else {
            return true;
        };

        let multiplier = self.effective_multiplier(entry.consecutive_hits);
        let effective_cooldown = self.base_cooldown.mul_f64(multiplier);

        if entry.emitted_at.elapsed() >= effective_cooldown {
            return true;
        }

        self.suppressed_count.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Mark a successful emission, starting/extending the cooldown timer.
    pub fn record_emission(&self, market_id: &MarketId) {
        let consecutive_hits = self
            .entries
            .get(market_id)
            .map_or(0, |e| e.consecutive_hits);

        self.entries.insert(
            market_id.clone(),
            CooldownEntry {
                emitted_at: Instant::now(),
                consecutive_hits: consecutive_hits + 1,
            },
        );
    }

    /// Reset cooldown for a market (e.g. price left convergence zone).
    pub fn reset(&self, market_id: &MarketId) {
        self.entries.invalidate(market_id);
    }

    /// Swap-and-reset suppressed counter for metrics reporting.
    pub fn take_suppressed_count(&self) -> u64 {
        self.suppressed_count.swap(0, Ordering::Relaxed)
    }

    /// Number of markets currently tracked.
    #[must_use]
    pub fn tracked_count(&self) -> u64 {
        self.entries.entry_count()
    }

    /// Compute the effective multiplier: `min(2^hits, max_multiplier)`.
    fn effective_multiplier(&self, consecutive_hits: u32) -> f64 {
        let exp = ToPrimitive::to_i32(&consecutive_hits.min(30)).unwrap_or(30);
        let power = 2.0_f64.powi(exp);
        let max: f64 = self.max_multiplier.try_into().unwrap_or(16.0);
        power.min(max)
    }
}

impl CooldownBackend for InMemoryEmissionCooldown {
    fn may_emit(&self, market_id: &MarketId) -> bool {
        self.may_emit(market_id)
    }

    fn record_emission(&self, market_id: &MarketId) {
        self.record_emission(market_id);
    }

    fn reset(&self, market_id: &MarketId) {
        self.reset(market_id);
    }

    fn take_suppressed_count(&self) -> u64 {
        self.take_suppressed_count()
    }

    fn tracked_count(&self) -> u64 {
        self.tracked_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn test_config() -> EmissionCooldownConfig {
        EmissionCooldownConfig {
            base_cooldown_secs: 1,
            max_multiplier: dec!(16.0),
            max_capacity: 100,
        }
    }

    #[test]
    fn first_emission_allowed() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        assert!(cd.may_emit(&mid));
    }

    #[test]
    fn immediate_re_emission_suppressed() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        cd.record_emission(&mid);
        assert!(!cd.may_emit(&mid));
    }

    #[test]
    fn reset_clears_cooldown() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        cd.record_emission(&mid);
        cd.reset(&mid);
        assert!(cd.may_emit(&mid));
    }

    #[test]
    fn consecutive_hits_tracked() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");

        cd.record_emission(&mid);
        let entry1 = cd.entries.get(&mid).unwrap();
        assert_eq!(entry1.consecutive_hits, 1);

        cd.record_emission(&mid);
        let entry2 = cd.entries.get(&mid).unwrap();
        assert_eq!(entry2.consecutive_hits, 2);
    }

    #[test]
    fn suppressed_count_accumulates() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        cd.record_emission(&mid);

        assert!(!cd.may_emit(&mid));
        assert!(!cd.may_emit(&mid));
        assert_eq!(cd.take_suppressed_count(), 2);
        assert_eq!(cd.take_suppressed_count(), 0);
    }
}
