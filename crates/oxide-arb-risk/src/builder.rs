//! Builder pattern for constructing a `RiskEngine` with all sub-systems.

use crate::{
    accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting},
    audit_sink::AuditSink,
    blacklist::BlacklistManager,
    circuit_breaker::CircuitBreaker,
    clock::{self, Clock},
    engine::{DynRiskEngine, RiskEngine},
    pipeline,
    position::{PositionTracker, PotentialLossLedger},
    reconciliation::LedgerReconciler,
    sizing::{DrawdownGuard, MultiConstraintSizer},
    snapshot::RiskSnapshot,
    state_store,
    traits::{RiskMetrics, RiskPersistence},
    types::{AtomicStateVersion, BreakerState},
};
use arc_swap::ArcSwap;
use oxide_arb_error::OxideResult;
use oxide_arb_models::{
    config::RiskConfig,
    domain::{
        blacklist::{BlacklistInfo, UpsertBlacklistEntry},
        potential_loss::PotentialLossInfo,
        risk::{
            NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskEngineState,
            RiskStateInfo, UpsertRiskEngineState,
        },
    },
    enums::risk::{BreakerStateName, CircuitBreakerLevel},
    types::{MarketId, Usd},
};
use parking_lot::RwLock;
use rust_decimal_macros::dec;
use std::sync::Arc;

pub struct RiskEngineBuilder {
    config: Option<RiskConfig>,
    snapshot: Option<RiskEngineState>,
    persistence: Option<Arc<dyn RiskPersistence>>,
    blacklist_entries: Vec<BlacklistInfo>,
    potential_loss_entries: Vec<PotentialLossInfo>,
    initial_equity: Option<Usd>,
    clock: Option<Arc<dyn Clock>>,
    audit_sink: Option<Arc<dyn AuditSink>>,
}

impl RiskEngineBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: None,
            snapshot: None,
            persistence: None,
            blacklist_entries: Vec::new(),
            potential_loss_entries: Vec::new(),
            initial_equity: None,
            clock: None,
            audit_sink: None,
        }
    }

    #[must_use]
    pub fn config(mut self, config: RiskConfig) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    pub fn snapshot(mut self, snapshot: RiskEngineState) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    #[must_use]
    pub fn persistence(mut self, persistence: Arc<dyn RiskPersistence>) -> Self {
        self.persistence = Some(persistence);
        self
    }

    #[must_use]
    pub fn blacklist_entries(mut self, entries: Vec<BlacklistInfo>) -> Self {
        self.blacklist_entries = entries;
        self
    }

    #[must_use]
    pub fn potential_loss_entries(mut self, entries: Vec<PotentialLossInfo>) -> Self {
        self.potential_loss_entries = entries;
        self
    }

    #[must_use]
    pub const fn initial_equity(mut self, equity: Usd) -> Self {
        self.initial_equity = Some(equity);
        self
    }

    #[must_use]
    pub fn audit_sink(mut self, audit_sink: Arc<dyn AuditSink>) -> Self {
        self.audit_sink = Some(audit_sink);
        self
    }

    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
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
    pub fn build(self, metrics: &dyn RiskMetrics) -> OxideResult<DynRiskEngine> {
        let config = self.config.unwrap_or_default();
        let equity = self.initial_equity.unwrap_or_else(|| Usd::new(dec!(1000)));
        let clock = self.clock.unwrap_or_else(clock::utc_clock);

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
                    &clock,
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
                let breaker =
                    CircuitBreaker::new(config.circuit_breaker.clone(), Arc::clone(&clock));
                let daily =
                    DailyAccounting::new(Usd::new(config.daily_budget_usd), Arc::clone(&clock));
                let weekly = WeeklyAccounting::new(Arc::clone(&clock));
                let hourly = HourlyAccounting::new(Arc::clone(&clock));
                let mut pt = PositionTracker::new();
                pt.refresh(metrics);
                let pl = PotentialLossLedger::from_entries(self.potential_loss_entries);
                (breaker, daily, weekly, hourly, pt, pl, equity)
            };

        let mut breaker = breaker;
        let safe_start_halt = should_safe_start_halt(&breaker, &potential_loss, &*clock);
        if let Some(ref reason) = safe_start_halt {
            if !breaker.state().is_halted() {
                breaker.halt(CircuitBreakerLevel::System, reason.clone());
            }
            tracing::warn!(%reason, "safe-start: engine booted in halted state");
        }

        let blacklist = BlacklistManager::new(&config, Arc::clone(&clock));
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

        let last_emergency = self.snapshot.as_ref().and_then(|snap| {
            match (snap.last_emergency_at, snap.last_emergency_reason.clone()) {
                (Some(at), Some(reason)) => Some((at, reason)),
                _ => None,
            }
        });

        let engine = RiskEngine {
            circuit_breaker: RwLock::new(breaker),
            daily: RwLock::new(daily),
            weekly: RwLock::new(weekly),
            hourly: RwLock::new(hourly),
            position_tracker: RwLock::new(position_tracker),
            potential_loss: RwLock::new(potential_loss),
            pipeline: risk_pipeline,
            risk_snapshot: ArcSwap::from_pointee(RiskSnapshot::zeroed()),
            blacklist,
            sizer,
            drawdown: RwLock::new(drawdown),
            reconciler,
            config,
            persistence,
            audit_sink: self.audit_sink,
            state_version: AtomicStateVersion::new(0),
            clock,
            last_emergency: RwLock::new(last_emergency),
        };

        engine.publish_risk_snapshot();

        Ok(engine)
    }
}

impl Default for RiskEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if the engine should boot halted (conditional safe-start).
///
/// Returns `Some(reason)` if the engine must start halted:
/// - Breaker was persisted as `Halted` (L3/L4 requiring ack)
/// - Breaker was `Open` with unexpired cooldown
/// - Active potential-loss entries exist at boot
fn should_safe_start_halt(
    breaker: &CircuitBreaker,
    potential_loss: &PotentialLossLedger,
    clock: &dyn Clock,
) -> Option<String> {
    match breaker.state() {
        BreakerState::Halted { level, reason, .. } => {
            return Some(format!(
                "safe-start: breaker is Halted (level={level}, reason={reason})"
            ));
        }
        BreakerState::Open {
            level,
            cooldown_until,
            reason,
            ..
        } if *cooldown_until > clock.now() => {
            return Some(format!(
                "safe-start: breaker is Open (level={level}, reason={reason})"
            ));
        }
        _ => {}
    }

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
    async fn upsert_state(&self, _state: UpsertRiskEngineState) -> OxideResult<()> {
        Ok(())
    }

    async fn load_state(&self) -> OxideResult<RiskStateInfo> {
        let now = chrono::Utc::now();
        Ok(RiskStateInfo {
            id: 1,
            breaker_state: BreakerStateName::Closed,
            breaker_level: None,
            is_halted: false,
            halt_reason: None,
            consecutive_misses: 0,
            cooldown_until: None,
            cooldown_multiplier: 0,
            total_exposure: Usd::ZERO,
            hourly_loss_usd: Usd::ZERO,
            hourly_fee_usd: Usd::ZERO,
            hourly_trade_count: 0,
            hourly_success_count: 0,
            hourly_miss_count: 0,
            hourly_window_start: now,
            daily_loss_usd: Usd::ZERO,
            daily_fee_usd: Usd::ZERO,
            daily_pnl: Usd::ZERO,
            daily_budget_spent: Usd::ZERO,
            daily_trade_count: 0,
            daily_success_count: 0,
            daily_miss_count: 0,
            daily_window_start: now.date_naive(),
            weekly_loss_usd: Usd::ZERO,
            weekly_trade_count: 0,
            weekly_window_start: now.date_naive(),
            hwm_equity: Usd::ZERO,
            last_emergency_at: None,
            last_emergency_reason: None,
            updated_at: now,
        })
    }

    async fn upsert_blacklist(&self, _entry: UpsertBlacklistEntry) -> OxideResult<()> {
        Ok(())
    }

    async fn remove_blacklist(&self, _market_id: &MarketId) -> OxideResult<()> {
        Ok(())
    }

    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>> {
        Ok(Vec::new())
    }

    async fn create_emergency(&self, _emergency: NewEmergencySnapshot) -> OxideResult<()> {
        Ok(())
    }

    async fn create_reconciliation(&self, _report: NewReconciliationReport) -> OxideResult<()> {
        Ok(())
    }

    async fn create_audit(&self, _audit: NewRiskAuditEvent) -> OxideResult<()> {
        Ok(())
    }
}
