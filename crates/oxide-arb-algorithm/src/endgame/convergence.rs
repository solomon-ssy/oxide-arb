//! In-memory convergence duration tracker using `moka::sync::Cache`.
//!
//! Tracks how long each market has been in the convergence zone (price above
//! the high threshold). Entries are keyed by `MarketId` and automatically
//! evicted after `max_idle_secs` of inactivity.
//!
//! Implements [`ConvergenceBackend`] for use as the default in-process backend.

use crate::backend::ConvergenceBackend;
use chrono::{DateTime, Utc};
use moka::sync::Cache;
use oxide_arb_models::{config::ConvergenceTrackerConfig, types::MarketId};
use std::time::Duration;

/// Direction of price convergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceDirection {
    /// YES token ask >= threshold → outcome likely YES.
    YesLikely,
    /// NO token ask >= threshold (or YES ask <= `low_threshold`) → outcome likely NO.
    NoLikely,
}

/// Internal tracking entry for a single market's convergence state.
#[derive(Debug, Clone)]
struct ConvergenceEntry {
    direction: ConvergenceDirection,
    first_seen: DateTime<Utc>,
}

/// Tracks per-market convergence duration with automatic idle eviction.
pub struct InMemoryConvergenceTracker {
    entries: Cache<MarketId, ConvergenceEntry>,
}

impl InMemoryConvergenceTracker {
    /// Create a new tracker from configuration.
    #[must_use]
    pub fn new(config: &ConvergenceTrackerConfig) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(config.max_capacity)
                .time_to_idle(Duration::from_secs(config.max_idle_secs))
                .build(),
        }
    }

    /// Update convergence state and return duration in seconds.
    ///
    /// If the direction matches the existing entry, returns the elapsed time
    /// since convergence first began. If the direction changed or the entry
    /// expired, the timer resets to 0.
    pub fn update_and_get(
        &self,
        market_id: &MarketId,
        direction: ConvergenceDirection,
        now: DateTime<Utc>,
    ) -> u64 {
        match self.entries.get(market_id) {
            Some(existing) if existing.direction == direction => {
                let delta: chrono::TimeDelta = now - existing.first_seen;
                let duration = u64::try_from(delta.num_seconds().max(0)).unwrap_or(0);
                self.entries.insert(
                    market_id.clone(),
                    ConvergenceEntry {
                        direction,
                        first_seen: existing.first_seen,
                    },
                );
                duration
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
        tracker.update_and_get(&mid, ConvergenceDirection::YesLikely, Utc::now());

        std::thread::sleep(std::time::Duration::from_millis(50));
        tracker.entries.run_pending_tasks();
        assert_eq!(tracker.tracked_count(), 0);
    }
}
