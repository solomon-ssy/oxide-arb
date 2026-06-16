//! Global execution kill switch with emergency classification.

use crate::{
    execution::trade_safety_gate::TradeSafetyGate,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
};
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::enums::common::{AlertCategory, AlertLevel, AlertSource};
use oxide_arb_risk::engine::RiskEngine;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use thiserror::Error;

/// Reason class governing whether automatic recovery is permitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum EmergencyClass {
    /// Transient venue/connectivity fault — may auto-recover when safe.
    VenueFault = 0,
    /// Reservation accounting fault — requires operator ack after fix.
    ReservationFault = 1,
    /// Durable persistence fault — never auto-recover.
    PersistenceFault = 2,
}

impl EmergencyClass {
    /// Whether heartbeat may clear the emergency without operator ack.
    pub const fn allows_auto_recover(self) -> bool {
        matches!(self, Self::VenueFault)
    }

    /// Whether only an explicit operator ack may clear the emergency halt.
    pub const fn requires_operator_ack(self) -> bool {
        !self.allows_auto_recover()
    }
}

/// Failure to acknowledge a non-auto-recoverable execution emergency.
#[derive(Debug, Error)]
pub enum EmergencyAckError {
    #[error("execution is not in emergency halt")]
    NotInEmergency,
    #[error("emergency class does not require operator ack — use resume or wait for auto-recover")]
    AutoRecoverable,
    #[error("blocking trades remain — resolve reconciliation first")]
    BlockingTrades,
    #[error("risk engine still blocking new trades")]
    RiskBlocking,
    #[error("trade safety gate check failed: {0}")]
    Gate(#[from] StorageError),
}

/// Thread-safe storage for [`EmergencyClass`].
#[derive(Debug)]
struct AtomicEmergencyClass(AtomicU8);

impl AtomicEmergencyClass {
    const fn new(class: EmergencyClass) -> Self {
        Self(AtomicU8::new(class as u8))
    }

    fn store(&self, class: EmergencyClass) {
        self.0.store(class as u8, Ordering::Release);
    }

    fn load(&self) -> EmergencyClass {
        Self::decode(self.0.load(Ordering::Acquire))
    }

    const fn decode(value: u8) -> EmergencyClass {
        match value {
            1 => EmergencyClass::ReservationFault,
            2 => EmergencyClass::PersistenceFault,
            _ => EmergencyClass::VenueFault,
        }
    }
}

/// Global execution kill switch — replaces the old Idle→Validate→Exec FSM.
///
/// Per-market concurrency is handled by [`super::market_inflight::MarketInFlightRegistry`].
pub struct ExecutionFSM {
    emergency: AtomicBool,
    class: AtomicEmergencyClass,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
}

impl ExecutionFSM {
    pub const fn new(metrics: Arc<MetricsHub>, alerts: Arc<AlertDispatcher>) -> Self {
        Self {
            emergency: AtomicBool::new(false),
            class: AtomicEmergencyClass::new(EmergencyClass::VenueFault),
            metrics,
            alerts,
        }
    }

    /// Halt all new executions (e.g. L4 circuit breaker, pipeline fault).
    pub fn enter_emergency(&self, class: EmergencyClass, reason: &str) {
        self.emergency.store(true, Ordering::Release);
        self.class.store(class);
        tracing::error!(?class, reason = reason, "execution emergency halt engaged");
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
        self.class.store(EmergencyClass::VenueFault);
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
        self.class.store(EmergencyClass::VenueFault);
        tracing::info!("execution emergency halt cleared");
    }

    #[must_use]
    pub fn emergency_class(&self) -> EmergencyClass {
        self.class.load()
    }

    /// Clear emergency halt when the risk engine permits trading and no
    /// blocking trades remain (guarded auto-recover).
    pub async fn try_auto_recover(&self, gate: &TradeSafetyGate, risk_engine: &RiskEngine) -> bool {
        if !self.is_emergency() {
            return false;
        }
        if !self.emergency_class().allows_auto_recover() {
            return false;
        }
        let blocking = match gate.has_blocking_trades().await {
            Ok(blocking) => blocking,
            Err(error) => {
                tracing::warn!(%error, "trade safety gate check failed — skip auto-recover");
                return false;
            }
        };
        if blocking {
            return false;
        }
        if risk_engine.allows_trading() {
            self.clear_emergency();
            tracing::info!("execution emergency auto-cleared: venue healthy + no blocking trades");
            true
        } else {
            false
        }
    }

    /// Clear a reservation or persistence emergency after operator confirmation.
    pub async fn ack_operator_emergency(
        &self,
        gate: &TradeSafetyGate,
        risk_engine: &RiskEngine,
    ) -> Result<EmergencyClass, EmergencyAckError> {
        if !self.is_emergency() {
            return Err(EmergencyAckError::NotInEmergency);
        }
        let class = self.emergency_class();
        if class.allows_auto_recover() {
            return Err(EmergencyAckError::AutoRecoverable);
        }
        if gate.has_blocking_trades().await? {
            return Err(EmergencyAckError::BlockingTrades);
        }
        if !risk_engine.allows_trading() {
            return Err(EmergencyAckError::RiskBlocking);
        }
        self.clear_emergency();
        tracing::info!(?class, "execution emergency cleared by operator ack");
        Ok(class)
    }

    #[inline]
    pub fn is_emergency(&self) -> bool {
        self.emergency.load(Ordering::Acquire)
    }

    /// Record an emergency `cancel_all` invocation (success or failure).
    pub fn record_venue_cancel_all(&self) {
        self.metrics.venue_cancel_all_total.inc();
    }
}
