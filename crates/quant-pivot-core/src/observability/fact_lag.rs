//! Ingest-side fact lag tracking (`ingestion_time - event_time`).
//!
//! Updated on the book-fact hot path; consumed by data-quality snapshots and
//! Prometheus histograms. The worst-lag counter resets each observation window
//! (periodic metrics refresh) so operators see per-interval peaks.

use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks peak fact lag observed between observation windows.
pub struct FactLagTracker {
    worst_lag_ms: AtomicU64,
}

impl FactLagTracker {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            worst_lag_ms: AtomicU64::new(0),
        }
    }

    /// Record one observation; retains the maximum lag in the current window.
    pub fn record_ms(&self, lag_ms: u64) {
        self.worst_lag_ms.fetch_max(lag_ms, Ordering::Relaxed);
    }

    /// Current peak lag without resetting the window.
    #[must_use]
    pub fn peek_worst_ms(&self) -> u64 {
        self.worst_lag_ms.load(Ordering::Relaxed)
    }

    /// Return the peak lag for the elapsed window and reset for the next one.
    #[must_use]
    pub fn take_worst_ms(&self) -> u64 {
        self.worst_lag_ms.swap(0, Ordering::Relaxed)
    }
}

impl Default for FactLagTracker {
    fn default() -> Self {
        Self::new()
    }
}
