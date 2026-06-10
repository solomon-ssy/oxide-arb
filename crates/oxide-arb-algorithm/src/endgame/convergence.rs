//! In-memory convergence duration tracker using `moka::sync::Cache`.
//!
//! Tracks how long each market has been in the convergence zone (price above
//! the high threshold). Entries are keyed by `MarketId` and automatically
//! evicted after `max_idle_secs` of inactivity.
//!
//! Implements [`ConvergenceBackend`] for use as the default in-process backend.

use crate::backend::ConvergenceBackend;
use chrono::{DateTime, Utc};
use moka::{Expiry, sync::Cache};
use num_traits::ToPrimitive;
use oxide_arb_models::{runtime_config::ConvergenceTrackerConfig, types::MarketId};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Direction of price convergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceDirection {
    /// YES token ask >= threshold → outcome likely YES.
    YesLikely,
    /// NO token ask >= threshold → outcome likely NO.
    NoLikely,
}

/// Internal tracking entry for a single market's convergence state.
#[derive(Debug, Clone)]
struct ConvergenceEntry {
    direction: ConvergenceDirection,
    first_seen: DateTime<Utc>,
}

/// Time-to-idle policy backed by an atomic so the idle bound is
/// hot-reloadable without rebuilding the cache (and without losing entries).
///
/// Reproduces `Cache::time_to_idle` semantics: the expiry clock restarts on
/// every create / read / update, each time re-reading the current bound.
struct ReloadableIdleExpiry {
    max_idle_secs: Arc<AtomicU64>,
}

impl ReloadableIdleExpiry {
    fn idle(&self) -> Duration {
        Duration::from_secs(self.max_idle_secs.load(Ordering::Relaxed))
    }
}

impl Expiry<MarketId, ConvergenceEntry> for ReloadableIdleExpiry {
    fn expire_after_create(
        &self,
        _key: &MarketId,
        _value: &ConvergenceEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(self.idle())
    }

    fn expire_after_read(
        &self,
        _key: &MarketId,
        _value: &ConvergenceEntry,
        _read_at: Instant,
        _duration_until_expiry: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        Some(self.idle())
    }

    fn expire_after_update(
        &self,
        _key: &MarketId,
        _value: &ConvergenceEntry,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(self.idle())
    }
}

/// Tracks per-market convergence duration with automatic idle eviction.
pub struct InMemoryConvergenceTracker {
    entries: Cache<MarketId, ConvergenceEntry>,
    max_idle_secs: Arc<AtomicU64>,
}

impl InMemoryConvergenceTracker {
    /// Create a new tracker from configuration.
    #[must_use]
    pub fn new(config: &ConvergenceTrackerConfig) -> Self {
        let max_idle_secs = Arc::new(AtomicU64::new(config.max_idle_secs));
        Self {
            entries: Cache::builder()
                .max_capacity(config.max_capacity)
                .expire_after(ReloadableIdleExpiry {
                    max_idle_secs: Arc::clone(&max_idle_secs),
                })
                .build(),
            max_idle_secs,
        }
    }

    /// Hot-reload the idle eviction bound (runtime-config activation).
    ///
    /// Takes effect on each entry's next touch; tracked state is preserved.
    /// Capacity stays fixed for the tracker's lifetime (restart-bound) so
    /// accumulated convergence durations are never dropped by a resize.
    pub fn set_max_idle_secs(&self, max_idle_secs: u64) {
        self.max_idle_secs.store(max_idle_secs, Ordering::Relaxed);
    }

    /// Update convergence state and return duration in seconds.
    ///
    /// If the direction matches the existing entry, returns the elapsed time
    /// since convergence first began. If the direction changed or the entry
    /// expired, the timer resets to 0.
    #[inline]
    pub fn update_and_get(
        &self,
        market_id: &MarketId,
        direction: ConvergenceDirection,
        now: DateTime<Utc>,
    ) -> u64 {
        match self.entries.get(market_id) {
            Some(existing) if existing.direction == direction => {
                let delta: chrono::TimeDelta = now - existing.first_seen;
                ToPrimitive::to_u64(&delta.num_seconds().max(0)).unwrap_or(0)
            }
            _ => {
                self.entries.insert(
                    market_id.clone(),
                    ConvergenceEntry {
                        direction,
                        first_seen: now,
                    },
                );
                0
            }
        }
    }

    /// Remove tracking for a market (e.g. on resolution or zone exit).
    pub fn remove(&self, market_id: &MarketId) {
        self.entries.invalidate(market_id);
    }

    /// Number of markets currently being tracked.
    #[must_use]
    pub fn tracked_count(&self) -> u64 {
        self.entries.entry_count()
    }
}

impl ConvergenceBackend for InMemoryConvergenceTracker {
    fn update_and_get(
        &self,
        market_id: &MarketId,
        direction: ConvergenceDirection,
        now: DateTime<Utc>,
    ) -> u64 {
        self.update_and_get(market_id, direction, now)
    }

    fn remove(&self, market_id: &MarketId) {
        self.remove(market_id);
    }

    fn tracked_count(&self) -> u64 {
        self.tracked_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration as StdTimeDuration};
    fn test_config() -> ConvergenceTrackerConfig {
        ConvergenceTrackerConfig {
            max_idle_secs: 7200,
            max_capacity: 100,
        }
    }

    #[test]
    fn first_update_returns_zero() {
        let tracker = InMemoryConvergenceTracker::new(&test_config());
        let mid = MarketId::new("m1");
        let now = Utc::now();
        let dur = tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, now);
        assert_eq!(dur, 0);
    }

    #[test]
    fn same_direction_accumulates_duration() {
        let tracker = InMemoryConvergenceTracker::new(&test_config());
        let mid = MarketId::new("m1");
        let t0 = Utc::now();
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);

        let t1 = t0 + chrono::Duration::seconds(600);
        let dur = tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t1);
        assert_eq!(dur, 600);
    }

    #[test]
    fn direction_change_resets_timer() {
        let tracker = InMemoryConvergenceTracker::new(&test_config());
        let mid = MarketId::new("m1");
        let t0 = Utc::now();
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);

        let t1 = t0 + chrono::Duration::seconds(600);
        let dur = tracker.update_and_get(&mid, ConvergenceDirection::NoLikely, t1);
        assert_eq!(dur, 0);
    }

    #[test]
    fn remove_clears_entry() {
        let tracker = InMemoryConvergenceTracker::new(&test_config());
        let mid = MarketId::new("m1");
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, Utc::now());
        tracker.entries.run_pending_tasks();
        assert_eq!(tracker.tracked_count(), 1);

        tracker.remove(&mid);
        tracker.entries.run_pending_tasks();
        assert_eq!(tracker.tracked_count(), 0);
    }

    #[test]
    fn idle_eviction() {
        let config = ConvergenceTrackerConfig {
            max_idle_secs: 0,
            max_capacity: 100,
        };
        let tracker = InMemoryConvergenceTracker::new(&config);
        let mid = MarketId::new("m1");
        let t0 = Utc::now();
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);

        // Expiry is enforced on the read path (the timer-wheel eviction that
        // reclaims memory is coarser-grained): an expired entry is never
        // readable, so the convergence timer resets instead of accumulating.
        sleep(StdTimeDuration::from_millis(50));
        assert!(tracker.entries.get(&mid).is_none());
        let dur = tracker.update_and_get(
            &mid,
            ConvergenceDirection::YesLikely,
            t0 + chrono::Duration::seconds(600),
        );
        assert_eq!(dur, 0, "expired entry must reset the convergence timer");
    }

    #[test]
    fn idle_bound_is_hot_reloadable_without_losing_state() {
        let tracker = InMemoryConvergenceTracker::new(&test_config());
        let mid = MarketId::new("m1");
        let t0 = Utc::now();
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);

        // Tighten the idle bound to zero: the next touch re-arms the entry's
        // expiry with the new bound, so it becomes unreadable immediately.
        tracker.set_max_idle_secs(0);
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);
        sleep(StdTimeDuration::from_millis(50));
        assert!(tracker.entries.get(&mid).is_none());

        // Loosen it again: new entries persist and accumulate duration.
        tracker.set_max_idle_secs(7200);
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, t0);
        let dur = tracker.update_and_get(
            &mid,
            ConvergenceDirection::YesLikely,
            t0 + chrono::Duration::seconds(60),
        );
        assert_eq!(dur, 60);
    }
}
