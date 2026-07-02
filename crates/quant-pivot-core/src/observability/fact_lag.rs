//! Ingest **pipeline** lag tracking (enqueue→ClickHouse flush-ack).
//!
//! Measures how long a fact waits from being handed to the `AsyncWriter` until
//! its batch is durably persisted — i.e. `ClickHouse` write backpressure. It is
//! deliberately independent of venue event age and of the lazy 1s-bucket flush
//! / reconnect snapshot re-writes, which previously inflated the old
//! `ingestion_time - event_time` metric to minutes for merely-quiet tokens.
//!
//! Fed by each `AsyncWriter`'s flush-lag reporter; consumed by data-quality
//! snapshots and Prometheus. The worst-lag counter resets each observation
//! window (periodic metrics refresh) so operators see per-interval peaks.

use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks peak ingest pipeline lag observed between observation windows.
pub struct IngestPipelineLagTracker {
    worst_lag_ms: AtomicU64,
}

impl IngestPipelineLagTracker {
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

impl Default for IngestPipelineLagTracker {
    fn default() -> Self {
        Self::new()
    }
}
