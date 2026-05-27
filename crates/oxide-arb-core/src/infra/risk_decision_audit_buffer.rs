//! Non-blocking audit buffer for pre-trade decision events.
//!
//! Pre-trade decisions are enqueued without blocking. A background worker
//! drains the channel in batches and persists them via `RiskAuditRepository`.
//! Channel full → drop + increment counter. **Never halts the engine.**

use oxide_arb_risk::audit::RiskAuditEvent;
use std::sync::atomic::{AtomicU64, Ordering};

pub enum EnqueueResult {
    Queued,
    DroppedChannelFull,
}

pub struct RiskDecisionAuditBuffer {
    tx: flume::Sender<RiskAuditEvent>,
    dropped: AtomicU64,
}

impl RiskDecisionAuditBuffer {
    pub fn new(capacity: usize) -> (Self, flume::Receiver<RiskAuditEvent>) {
        let (tx, rx) = flume::bounded(capacity);
        let buffer = Self {
            tx,
            dropped: AtomicU64::new(0),
        };
        (buffer, rx)
    }

    #[inline]
    pub fn try_enqueue(&self, event: RiskAuditEvent) -> EnqueueResult {
        if self.tx.try_send(event).is_ok() {
            EnqueueResult::Queued
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            EnqueueResult::DroppedChannelFull
        }
    }

    #[inline]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
