use crate::observability::{
    alert_dispatcher::{Alert, AlertDispatcher, AlertSeverity},
    metrics_hub::MetricsHub,
};
use chrono::Utc;
use oxide_arb_risk::engine::RiskEngine;
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
    alerts: Arc<AlertDispatcher>,
}

impl ExecutionFSM {
    pub const fn new(metrics: Arc<MetricsHub>, alerts: Arc<AlertDispatcher>) -> Self {
        Self {
            emergency: AtomicBool::new(false),
            metrics,
            alerts,
        }
    }

    /// Halt all new executions (e.g. L4 circuit breaker, manual kill).
    pub fn enter_emergency(&self, reason: &str) {
        self.emergency.store(true, Ordering::Release);
        tracing::error!(reason = reason, "execution emergency halt engaged");
        self.metrics.fsm_emergency_entries.inc();
        self.alerts.dispatch_background(Alert {
            severity: AlertSeverity::Critical,
            title: "Execution emergency halt".to_owned(),
            body: reason.to_owned(),
            timestamp: Utc::now(),
        });
    }

    pub fn clear_emergency(&self) {
        self.emergency.store(false, Ordering::Release);
        tracing::info!("execution emergency halt cleared");
    }

    /// Clear emergency halt when the risk engine permits trading (e.g. after venue recovery).
    #[must_use]
    pub fn try_auto_recover(&self, risk_engine: &RiskEngine) -> bool {
        if self.is_emergency() && risk_engine.allows_trading() {
            self.clear_emergency();
            tracing::info!("execution emergency auto-cleared: venue healthy + risk allows trading");
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_emergency(&self) -> bool {
        self.emergency.load(Ordering::Acquire)
    }
}
