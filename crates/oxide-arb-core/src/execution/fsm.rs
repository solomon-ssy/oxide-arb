use crate::observability::metrics_hub::MetricsHub;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Global execution kill switch — replaces the old Idle→Validate→Exec FSM.
///
/// Per-market concurrency is handled by [`super::market_inflight::MarketInFlightRegistry`].
pub struct ExecutionFSM {
    emergency: AtomicBool,
    metrics: Arc<MetricsHub>,
}

impl ExecutionFSM {
    pub const fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            emergency: AtomicBool::new(false),
            metrics,
        }
    }

    /// Halt all new executions (e.g. L4 circuit breaker, manual kill).
    pub fn enter_emergency(&self, reason: &str) {
        self.emergency.store(true, Ordering::Release);
        tracing::error!(reason = reason, "execution emergency halt engaged");
        self.metrics.fsm_emergency_entries.inc();
    }

    pub fn clear_emergency(&self) {
        self.emergency.store(false, Ordering::Release);
        tracing::info!("execution emergency halt cleared");
    }

    #[inline]
    pub fn is_emergency(&self) -> bool {
        self.emergency.load(Ordering::Acquire)
    }
}
