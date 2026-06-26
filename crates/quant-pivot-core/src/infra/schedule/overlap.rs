//! Per-`schedule_id` skip-if-running overlap guard.
//!
//! Quant reports must never queue a stale `as_of`: if a schedule's report
//! pipeline is still in flight when the next fire arrives, that fire is
//! skipped (not coalesced, not back-filled). This guard provides a
//! non-blocking, per-`schedule_id` mutual exclusion slot for exactly that
//! policy (parent doc §23.6).

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Per-`schedule_id` in-flight guard implementing skip-if-running.
///
/// Cloning shares the same underlying lock map (cheap `Arc` bump), so the
/// scheduler and its job closures all observe one another's in-flight state.
#[derive(Clone, Default)]
pub struct ScheduleOverlapGuard {
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl ScheduleOverlapGuard {
    /// Build an empty overlap guard.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to claim the in-flight slot for `schedule_id` without blocking.
    ///
    /// Returns `Some(guard)` when no report for this schedule is running; the
    /// caller holds the slot until the returned guard is dropped. Returns
    /// `None` when a report is already in flight, signalling skip-if-running.
    ///
    /// The per-schedule lock is retained across remove/re-add so the in-flight
    /// identity survives a runtime-config cadence change.
    #[must_use]
    pub fn try_acquire(&self, schedule_id: &str) -> Option<OwnedMutexGuard<()>> {
        let lock = {
            let entry = self
                .locks
                .entry(schedule_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(entry.value())
        };
        lock.try_lock_owned().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::ScheduleOverlapGuard;

    #[tokio::test]
    async fn second_acquire_for_same_schedule_is_skipped_while_in_flight() {
        let guard = ScheduleOverlapGuard::new();

        let first = guard.try_acquire("daily").expect("first acquire");
        assert!(
            guard.try_acquire("daily").is_none(),
            "an in-flight schedule must be skipped"
        );

        drop(first);
        assert!(
            guard.try_acquire("daily").is_some(),
            "slot must be reclaimable once the prior run finishes"
        );
    }

    #[tokio::test]
    async fn distinct_schedules_do_not_block_each_other() {
        let guard = ScheduleOverlapGuard::new();

        let _a = guard.try_acquire("intraday").expect("acquire intraday");
        assert!(
            guard.try_acquire("daily").is_some(),
            "distinct schedule ids must fire independently"
        );
    }
}
