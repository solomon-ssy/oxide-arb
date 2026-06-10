//! In-memory per-market emission cooldown with exponential backoff.
//!
//! Prevents the same market from flooding the opportunity pipeline with
//! duplicate signals on every scan tick. Uses `moka::sync::Cache` for
//! automatic TTL-based eviction of stale entries.
//!
//! Implements [`CooldownBackend`] for use as the default in-process backend.

use crate::backend::CooldownBackend;
use arc_swap::ArcSwap;
use moka::sync::Cache;
use oxide_arb_models::{runtime_config::EmissionCooldownConfig, types::MarketId};
use rust_decimal::prelude::ToPrimitive;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Tracks the last emission time and consecutive hit count per market.
#[derive(Debug, Clone)]
struct CooldownEntry {
    emitted_at: Instant,
    consecutive_hits: u32,
}

/// Hot-swappable cooldown parameters derived from [`EmissionCooldownConfig`].
struct CooldownParams {
    base_cooldown: Duration,
    max_multiplier: f64,
    max_capacity: u64,
    /// Cache TTL: `base_cooldown × ceil(max_multiplier)` — the longest
    /// possible effective cooldown, so live entries never outlive their use.
    entry_ttl: Duration,
}

impl CooldownParams {
    fn from_config(config: &EmissionCooldownConfig) -> Self {
        let max_mult_secs = config.max_multiplier.ceil().to_u64().unwrap_or(16);
        let max_ttl_secs = config.base_cooldown_secs.saturating_mul(max_mult_secs);
        Self {
            base_cooldown: Duration::from_secs(config.base_cooldown_secs),
            max_multiplier: config.max_multiplier.to_f64().unwrap_or(16.0),
            max_capacity: config.max_capacity,
            entry_ttl: Duration::from_secs(max_ttl_secs),
        }
    }

    fn build_cache(&self) -> Cache<MarketId, CooldownEntry> {
        Cache::builder()
            .max_capacity(self.max_capacity)
            .time_to_live(self.entry_ttl)
            .build()
    }
}

/// In-memory per-market emission cooldown with exponential backoff.
///
/// After emitting an opportunity for a market, further emissions are
/// suppressed for `base_cooldown × min(2^consecutive_hits, max_multiplier)`
/// seconds. The cooldown resets when explicitly cleared (e.g. the market
/// leaves the convergence zone).
pub struct InMemoryEmissionCooldown {
    entries: ArcSwap<Cache<MarketId, CooldownEntry>>,
    params: ArcSwap<CooldownParams>,
    suppressed_count: AtomicU64,
}

impl InMemoryEmissionCooldown {
    /// Create a new cooldown tracker from configuration.
    #[must_use]
    pub fn new(config: &EmissionCooldownConfig) -> Self {
        let params = CooldownParams::from_config(config);
        Self {
            entries: ArcSwap::from_pointee(params.build_cache()),
            params: ArcSwap::from_pointee(params),
            suppressed_count: AtomicU64::new(0),
        }
    }

    /// Hot-reload cooldown parameters (runtime-config activation).
    ///
    /// `base_cooldown_secs` / `max_multiplier` apply immediately to existing
    /// entries. When the capacity or the derived entry TTL changes the backing
    /// cache must be rebuilt, which **clears all in-flight cooldown state**
    /// (logged; recently emitted markets may re-emit once).
    pub fn reload(&self, config: &EmissionCooldownConfig) {
        let new_params = CooldownParams::from_config(config);
        let old_params = self.params.load();
        let needs_rebuild = new_params.max_capacity != old_params.max_capacity
            || new_params.entry_ttl != old_params.entry_ttl;
        if needs_rebuild {
            tracing::warn!(
                tracked = self.tracked_count(),
                "emission cooldown cache rebuilt on reload — in-flight cooldown state cleared"
            );
            self.entries.store(Arc::new(new_params.build_cache()));
        }
        self.params.store(Arc::new(new_params));
    }

    /// Returns `true` if the market may emit (NOT in cooldown).
    #[must_use]
    #[inline]
    pub fn may_emit(&self, market_id: &MarketId) -> bool {
        let Some(entry) = self.entries.load().get(market_id) else {
            return true;
        };

        let params = self.params.load();
        let multiplier = effective_multiplier(entry.consecutive_hits, params.max_multiplier);
        let effective_cooldown = params.base_cooldown.mul_f64(multiplier);

        if entry.emitted_at.elapsed() >= effective_cooldown {
            return true;
        }

        self.suppressed_count.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Mark a successful emission, starting/extending the cooldown timer.
    pub fn record_emission(&self, market_id: &MarketId) {
        let entries = self.entries.load();
        let consecutive_hits = entries.get(market_id).map_or(0, |e| e.consecutive_hits);

        entries.insert(
            market_id.clone(),
            CooldownEntry {
                emitted_at: Instant::now(),
                consecutive_hits: consecutive_hits + 1,
            },
        );
    }

    /// Reset cooldown for a market (e.g. price left convergence zone).
    pub fn reset(&self, market_id: &MarketId) {
        self.entries.load().invalidate(market_id);
    }

    /// Swap-and-reset suppressed counter for metrics reporting.
    pub fn take_suppressed_count(&self) -> u64 {
        self.suppressed_count.swap(0, Ordering::Relaxed)
    }

    /// Number of markets currently tracked.
    #[must_use]
    pub fn tracked_count(&self) -> u64 {
        self.entries.load().entry_count()
    }
}

/// Compute the effective multiplier: `min(2^hits, max_multiplier)`.
fn effective_multiplier(consecutive_hits: u32, max_multiplier: f64) -> f64 {
    let exp = ToPrimitive::to_i32(&consecutive_hits.min(30)).unwrap_or(30);
    let power = 2.0_f64.powi(exp);
    power.min(max_multiplier)
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
        let entry1 = cd.entries.load().get(&mid).unwrap();
        assert_eq!(entry1.consecutive_hits, 1);

        cd.record_emission(&mid);
        let entry2 = cd.entries.load().get(&mid).unwrap();
        assert_eq!(entry2.consecutive_hits, 2);
    }

    #[test]
    fn reload_preserves_state_for_param_only_changes() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        cd.record_emission(&mid);
        assert!(!cd.may_emit(&mid));

        // Same capacity + same derived TTL → state preserved.
        cd.reload(&test_config());
        assert!(!cd.may_emit(&mid), "cooldown state must survive reload");
    }

    #[test]
    fn reload_capacity_change_rebuilds_cache() {
        let cd = InMemoryEmissionCooldown::new(&test_config());
        let mid = MarketId::new("m1");
        cd.record_emission(&mid);

        let config = EmissionCooldownConfig {
            max_capacity: 200,
            ..test_config()
        };
        cd.reload(&config);
        assert!(cd.may_emit(&mid), "capacity change clears cooldown state");
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
