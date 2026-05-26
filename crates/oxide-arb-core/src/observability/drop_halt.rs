//! Engage execution halt when pipeline drops are observed (fail-closed).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::execution::fsm::ExecutionFSM;
use crate::observability::metrics_hub::MetricsHub;

static DROP_HALT_ENGAGED: AtomicBool = AtomicBool::new(false);

/// Record a book-apply channel drop and engage the kill switch once.
pub fn on_book_apply_drop(metrics: &MetricsHub, fsm: &ExecutionFSM) {
    metrics.book_apply_dropped.inc();
    engage_drop_halt(fsm, "book_apply_dropped");
}

/// Record a coalescer drop and engage the kill switch once.
pub fn on_coalescer_drop(metrics: &MetricsHub, fsm: &ExecutionFSM) {
    metrics.coalescer_dropped.inc();
    engage_drop_halt(fsm, "coalescer_dropped");
}

/// Record a post-trade drop and engage the kill switch once.
pub fn on_post_trade_drop(metrics: &MetricsHub, fsm: &ExecutionFSM) {
    metrics.post_trade_dropped.inc();
    engage_drop_halt(fsm, "post_trade_dropped");
}

fn engage_drop_halt(fsm: &ExecutionFSM, reason: &'static str) {
    if DROP_HALT_ENGAGED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        fsm.enter_emergency(reason);
    }
}

/// Shared handles for drop-driven halts.
#[derive(Clone)]
pub struct DropHaltGuard {
    pub metrics: Arc<MetricsHub>,
    pub fsm: Arc<ExecutionFSM>,
}

impl DropHaltGuard {
    pub const fn new(metrics: Arc<MetricsHub>, fsm: Arc<ExecutionFSM>) -> Self {
        Self { metrics, fsm }
    }

    #[inline]
    pub fn on_book_apply_drop(&self) {
        on_book_apply_drop(&self.metrics, &self.fsm);
    }

    #[inline]
    pub fn on_coalescer_drop(&self) {
        on_coalescer_drop(&self.metrics, &self.fsm);
    }

    #[inline]
    pub fn on_post_trade_drop(&self) {
        on_post_trade_drop(&self.metrics, &self.fsm);
    }
}
