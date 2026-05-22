//! Builder pattern for constructing a `RiskEngine` with all sub-systems.

use crate::accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting};
use crate::audit::RiskAuditEvent;
use crate::blacklist::BlacklistManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::engine::RiskEngine;
use crate::pipeline;
use crate::position::{PositionTracker, PotentialLossLedger};
use crate::reconciliation::LedgerReconciler;
use crate::sizing::{DrawdownGuard, MultiConstraintSizer};
use crate::state_store;
use crate::traits::{RiskMetrics, RiskPersistence};
use crate::types::{AtomicStateVersion, BreakerState, ReconciliationReport};
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::EmergencySnapshot;
use oxide_arb_models::domain::blacklist::BlacklistEntry;
use oxide_arb_models::domain::potential_loss::PotentialLossEntry;
use oxide_arb_models::domain::risk::RiskEngineSnapshot;
use oxide_arb_models::types::{MarketId, Usd};
use parking_lot::RwLock;
use rust_decimal_macros::dec;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub struct RiskEngineBuilder {
    config: Option<RiskConfig>,
    snapshot: Option<RiskEngineSnapshot>,
    persistence: Option<Arc<dyn RiskPersistence>>,
    blacklist_entries: Vec<BlacklistEntry>,
    potential_loss_entries: Vec<PotentialLossEntry>,
    initial_equity: Option<Usd>,
}

impl RiskEngineBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            config: None,
            snapshot: None,
            persistence: None,
            blacklist_entries: Vec::new(),
            potential_loss_entries: Vec::new(),
            initial_equity: None,
        }
    }

    #[must_use]
    pub fn config(mut self, config: RiskConfig) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    pub fn snapshot(mut self, snapshot: RiskEngineSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    #[must_use]
    pub fn persistence(mut self, persistence: Arc<dyn RiskPersistence>) -> Self {
        self.persistence = Some(persistence);
        self
    }

    #[must_use]
    pub fn blacklist_entries(mut self, entries: Vec<BlacklistEntry>) -> Self {
        self.blacklist_entries = entries;
        self
    }

    #[must_use]
    pub fn potential_loss_entries(mut self, entries: Vec<PotentialLossEntry>) -> Self {
        self.potential_loss_entries = entries;
        self
    }

    #[must_use]
    pub const fn initial_equity(mut self, equity: Usd) -> Self {
        self.initial_equity = Some(equity);
        self
    }

    /// Build the `RiskEngine`, optionally restoring from a snapshot.
    ///
    /// If a snapshot is provided, uses `state_store::recover_state` to
    /// validate and reconstruct all subsystems. Missing or corrupt state
    /// causes a fail-closed error.
    ///
    /// **Conditional safe-start**: the engine boots halted if any of:
    /// - `recover_state()` returned an error (already fail-closed)
    /// - Persisted breaker is `Open` with unexpired cooldown
    /// - Active escalated potential-loss entries exist
    /// # Async
    /// Marked async for forward compatibility with `RiskPersistence::load_snapshot`.
    #[allow(clippy::unused_async)]
    pub async fn build(self, metrics: &dyn RiskMetrics) -> OxideResult<RiskEngine> {
        let config = self.config.unwrap_or_default();
        let equity = self.initial_equity.unwrap_or_else(|| Usd::new(dec!(1000)));

        let persistence = self
            .persistence
            .unwrap_or_else(|| Arc::new(NoopPersistence));

        let (breaker, daily, weekly, hourly, position_tracker, potential_loss, drawdown_hwm) =
            if let Some(ref snap) = self.snapshot {
                let recovered = state_store::recover_state(
                    &config,
                    snap,
                    self.blacklist_entries.clone(),
                    self.potential_loss_entries,
                    metrics,
                )?;
                (
                    recovered.breaker,
                    recovered.daily,
                    recovered.weekly,
                    recovered.hourly,
                    recovered.position_tracker,
                    recovered.potential_loss,
                    recovered.drawdown_hwm,
                )
            } else {
                let breaker = CircuitBreaker::new(config.circuit_breaker.clone());
                let daily = DailyAccounting::new(Usd::new(config.daily_budget_usd));
                let weekly = WeeklyAccounting::new();
                let hourly = HourlyAccounting::new();
                let mut pt = PositionTracker::new();
                pt.refresh(metrics);
                let pl = PotentialLossLedger::from_entries(self.potential_loss_entries);
                (breaker, daily, weekly, hourly, pt, pl, equity)
            };

        let blacklist = BlacklistManager::new(&config);
        if self.snapshot.is_some() {
            blacklist.load_entries(self.blacklist_entries);
        }

        let sizer = MultiConstraintSizer::new(&config);
        let drawdown = DrawdownGuard::new(
            drawdown_hwm,
            config.drawdown.max_drawdown_pct,
            config.drawdown.drawdown_reduction_factor,
        );
        let reconciler = LedgerReconciler::new(config.reconciliation_tolerance_usd);
        let risk_pipeline = pipeline::build_default_pipeline(&config);

        // Conditional safe-start: halt if breaker is Open with unexpired cooldown
        let safe_start_halt = should_safe_start_halt(&breaker, &potential_loss);

        let engine = RiskEngine {
            circuit_breaker: RwLock::new(breaker),
            daily: RwLock::new(daily),
            weekly: RwLock::new(weekly),
            hourly: RwLock::new(hourly),
            position_tracker: RwLock::new(position_tracker),
            potential_loss: RwLock::new(potential_loss),
            pipeline: risk_pipeline,
            blacklist,
            sizer,
            drawdown: RwLock::new(drawdown),
            reconciler,
            config,
            persistence,
            is_halted: AtomicBool::new(safe_start_halt.is_some()),
            halt_reason: RwLock::new(safe_start_halt.clone()),
            state_version: AtomicStateVersion::new(0),
        };

        if let Some(ref reason) = safe_start_halt {
            tracing::warn!(%reason, "safe-start: engine booted in halted state");
        }

        Ok(engine)
    }
}

impl Default for RiskEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if the engine should boot halted (conditional safe-start).
fn should_safe_start_halt(
    breaker: &CircuitBreaker,
    potential_loss: &PotentialLossLedger,
) -> Option<String> {
    // Breaker is Open with unexpired cooldown
    if let BreakerState::Open {
        level,
        cooldown_until,
        reason,
        ..
    } = breaker.state()
    {
        let now = chrono::Utc::now();
        if *cooldown_until > now {
            return Some(format!(
                "safe-start: breaker is Open (level={level}, reason={reason})"
            ));
        }
    }

    // Escalated potential loss entries (active entries at boot)
    let active = potential_loss.active_count();
    if active > 0 {
        return Some(format!(
            "safe-start: {active} active potential-loss entries at boot"
        ));
    }

    None
}

/// No-op persistence for testing or when persistence is not configured.
struct NoopPersistence;

#[async_trait::async_trait]
impl RiskPersistence for NoopPersistence {
    async fn save_snapshot(&self, _snapshot: &RiskEngineSnapshot) -> OxideResult<()> {
        Ok(())
    }

    async fn load_snapshot(&self) -> OxideResult<Option<RiskEngineSnapshot>> {
        Ok(None)
    }

    async fn save_blacklist_entry(&self, _entry: &BlacklistEntry) -> OxideResult<()> {
        Ok(())
    }

    async fn remove_blacklist_entry(&self, _market_id: &MarketId) -> OxideResult<()> {
        Ok(())
    }

    async fn load_blacklist_entries(&self) -> OxideResult<Vec<BlacklistEntry>> {
        Ok(Vec::new())
    }

    async fn save_emergency_snapshot(&self, _snapshot: &EmergencySnapshot) -> OxideResult<()> {
        Ok(())
    }

    async fn save_reconciliation_report(&self, _report: &ReconciliationReport) -> OxideResult<()> {
        Ok(())
    }

    async fn append_audit_event(&self, _event: &RiskAuditEvent) -> OxideResult<()> {
        Ok(())
    }
}
