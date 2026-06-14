use crate::observability::{
    alert_dispatcher::{Alert, AlertDispatcher},
    metrics_hub::MetricsHub,
};
use chrono::Utc;
use oxide_arb_models::enums::common::{AlertCategory, AlertLevel, AlertSource};
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

    /// Halt all new executions (e.g. L4 circuit breaker, pipeline fault).
    pub fn enter_emergency(&self, reason: &str) {
        self.emergency.store(true, Ordering::Release);
        tracing::error!(reason = reason, "execution emergency halt engaged");
        self.metrics.fsm_emergency_entries.inc();
        self.alerts.dispatch_background(
            Alert::new(
                "execution.emergency_halt",
                AlertLevel::Critical,
                AlertCategory::TradingSafety,
                AlertSource::Execution,
                "Execution emergency halt",
                reason,
                Utc::now(),
            )
            .with_affects_trading(true),
        );
    }

    /// Engage the kill switch for operator-controlled quiesce (mode transition,
    /// manual halt). Blocks execution identically to [`Self::enter_emergency`],
    /// but emits an informational alert so the UI status light follows
    /// `system.status.breaker_state` instead of latching a critical fault.
    pub fn enter_planned_halt(&self, reason: &str) {
        self.emergency.store(true, Ordering::Release);
        tracing::warn!(reason = reason, "execution planned halt engaged");
        self.metrics.fsm_emergency_entries.inc();
        self.alerts.dispatch_background(
            Alert::new(
                "execution.planned_halt",
                AlertLevel::Info,
                AlertCategory::TradingSafety,
                AlertSource::Execution,
                "Execution paused",
                reason,
                Utc::now(),
            )
            .with_affects_trading(false)
            .with_visible_toast(false),
        );
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
