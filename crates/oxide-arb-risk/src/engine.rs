//! `RiskEngine` — single entry point for all risk operations.
//!
//! Owns all risk subsystems and orchestrates them through a unified API.
//! Thread-safe: internal state is protected by `parking_lot::RwLock`.
//!
//! Mutation methods are `async` — they perform in-memory updates under sync
//! locks, then release locks and persist + audit via `RiskPersistence`.
//! If persistence fails, the engine halts (fail-closed).

use crate::accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting};
use crate::audit::{RiskAuditEvent, RiskDecisionTrace};
use crate::blacklist::BlacklistManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::context::{BlacklistGate, CircuitBreakerGate, ManualHaltGate, RiskContext};
use crate::pipeline::RiskPipeline;
use crate::position::{PositionTracker, PotentialLossLedger};
use crate::reconciliation::LedgerReconciler;
use crate::sizing::{DrawdownGuard, MultiConstraintSizer};
use crate::traits::{RiskMetrics, RiskPersistence};
use crate::types::{
    AtomicStateVersion, BreakerState, PostTradeReport, ReportMode, RiskDecision, StateVersion,
};
use chrono::Utc;
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::blacklist::BlacklistCheckResult;
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::potential_loss::PotentialLossEntry;
use oxide_arb_models::domain::risk::{ProbabilityInput, RiskEngineSnapshot};
use oxide_arb_models::domain::trade::TradeRecord;
use oxide_arb_models::enums::common::LedgerStatus;
use oxide_arb_models::enums::risk::{
    BlacklistReason, BlacklistScope, CircuitBreakerLevel, TradeAccountingPhase,
};
use oxide_arb_models::types::{MarketId, Usd};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

pub struct RiskEngine {
    pub(crate) circuit_breaker: RwLock<CircuitBreaker>,
    pub(crate) daily: RwLock<DailyAccounting>,
    pub(crate) weekly: RwLock<WeeklyAccounting>,
    pub(crate) hourly: RwLock<HourlyAccounting>,
    pub(crate) position_tracker: RwLock<PositionTracker>,
    pub(crate) potential_loss: RwLock<PotentialLossLedger>,
    pub(crate) pipeline: RiskPipeline,
    pub(crate) blacklist: BlacklistManager,
    pub(crate) sizer: MultiConstraintSizer,
    pub(crate) drawdown: RwLock<DrawdownGuard>,
    pub(crate) reconciler: LedgerReconciler,
    pub(crate) config: RiskConfig,
    pub(crate) persistence: Arc<dyn RiskPersistence>,
    pub(crate) is_halted: AtomicBool,
    pub(crate) halt_reason: RwLock<Option<String>>,
    pub(crate) state_version: AtomicStateVersion,
}

impl RiskEngine {
    // ── Pre-trade (hot path, sync) ─────────────────────────────────────

    #[must_use]
    pub fn pre_trade_check(
        &self,
        opp: &Opportunity,
        probability: &ProbabilityInput,
        metrics: &dyn RiskMetrics,
        mode: ReportMode,
    ) -> RiskDecision {
        let eval_start = Instant::now();
        let now = Utc::now();
        let version = self.state_version.load();

        let ctx = self.build_context(opp, probability, metrics, version);
        let gate_report = self.pipeline.evaluate(&ctx, mode);

        if gate_report.has_failed_hard_gate {
            let denial_reason = gate_report.results.iter().find(|c| !c.passed).map(|c| {
                format!(
                    "{}: {}",
                    c.check_id,
                    c.detail.as_deref().unwrap_or("failed")
                )
            });

            return RiskDecision {
                allowed: false,
                checks: gate_report.results.clone(),
                denial_reason,
                recommended_size: None,
                drawdown_factor: ctx.drawdown_factor,
                evaluated_at: now,
                state_version: version,
                trace: RiskDecisionTrace {
                    check_results: gate_report.results,
                    sizing_breakdown: None,
                    state_version: version,
                    total_elapsed_us: u64::try_from(eval_start.elapsed().as_micros())
                        .unwrap_or(u64::MAX),
                    evaluated_at: now,
                },
            };
        }

        let bankroll = available_bankroll(
            ctx.cached_balance,
            Usd::new(self.config.reserve_balance_usd),
            ctx.total_exposure_before,
            ctx.total_potential_loss,
            Usd::new(self.config.bankroll_usd),
        );

        let size_result = self.sizer.size(&ctx, bankroll, ctx.drawdown_factor);

        let allowed = size_result.bet_usd > Usd::ZERO
            && size_result.bet_usd >= Usd::new(self.config.min_trade_usd);

        let denial_reason = if allowed {
            None
        } else {
            Some(format!(
                "sizing returned {} (binding: {}, min_trade: {})",
                size_result.bet_usd, size_result.binding_constraint, self.config.min_trade_usd,
            ))
        };

        RiskDecision {
            allowed,
            checks: gate_report.results.clone(),
            denial_reason,
            recommended_size: Some(size_result.clone()),
            drawdown_factor: ctx.drawdown_factor,
            evaluated_at: now,
            state_version: version,
            trace: RiskDecisionTrace {
                check_results: gate_report.results,
                sizing_breakdown: Some(size_result.breakdown),
                state_version: version,
                total_elapsed_us: u64::try_from(eval_start.elapsed().as_micros())
                    .unwrap_or(u64::MAX),
                evaluated_at: now,
            },
        }
    }

    // ── Post-trade (async mutation path) ────────────────────────────────

    /// Process a trade result with Fill/Settlement phase separation.
    ///
    /// - **Fill**: records cost, counts, and potential loss; no realized
    ///   profit flows into daily/weekly/hourly loss caps.
    /// - **Settlement**: records realized profit, resolves potential loss,
    ///   checks caps and may trip the circuit breaker.
    ///
    /// After in-memory mutation, persists the snapshot and appends an audit
    /// event. If persistence fails, the engine halts (fail-closed).
    pub async fn on_trade_result(
        &self,
        phase: TradeAccountingPhase,
        trade: &TradeRecord,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<PostTradeReport> {
        let (report, audit_event) = self.apply_trade_result(phase, trade, metrics);
        self.persist_and_audit(&report.snapshot, audit_event)
            .await?;
        Ok(report)
    }

    fn apply_trade_result(
        &self,
        phase: TradeAccountingPhase,
        trade: &TradeRecord,
        metrics: &dyn RiskMetrics,
    ) -> (PostTradeReport, RiskAuditEvent) {
        let mut breaker_tripped: Option<CircuitBreakerLevel> = None;
        let mut auto_blacklisted = None;

        let (daily_rolled, weekly_rolled, hourly_rolled) = match phase {
            TradeAccountingPhase::Fill => self.apply_fill(trade),
            TradeAccountingPhase::Settlement => {
                let (dr, wr, hr) = self.apply_settlement(trade);
                breaker_tripped = self.check_loss_caps();
                (dr, wr, hr)
            }
        };

        if let Some(bt) = self.apply_common_checks(trade, metrics) {
            if breaker_tripped.is_none() {
                breaker_tripped = Some(bt);
            }
        }

        self.state_version.increment();
        let snapshot = self.snapshot(metrics);

        let audit_event = RiskAuditEvent::PostTradeUpdate {
            trade_id: trade.trade_id.clone(),
            outcome: trade.status,
            daily_loss_after: snapshot.daily_loss,
            weekly_loss_after: snapshot.weekly_loss,
        };

        if trade.is_miss() {
            let miss_count = metrics.consecutive_market_misses(&trade.market_id);
            if self
                .blacklist
                .maybe_auto_blacklist(&trade.market_id, miss_count)
                .is_some()
            {
                auto_blacklisted = Some(trade.market_id.clone());
            }
        }

        (
            PostTradeReport {
                snapshot,
                daily_rolled,
                weekly_rolled,
                hourly_rolled,
                breaker_tripped,
                auto_blacklisted,
            },
            audit_event,
        )
    }

    /// Fill phase: record cost/counts/potential loss, no realized profit.
    fn apply_fill(&self, trade: &TradeRecord) -> (bool, bool, bool) {
        let daily_rolled = {
            let mut daily = self.daily.write();
            daily.record_trade(
                Usd::ZERO,
                trade.total_fees_usd,
                trade.total_cost_usd,
                trade.status,
            )
        };

        if trade.is_success() {
            self.potential_loss
                .write()
                .record_entry(PotentialLossEntry {
                    entry_id: trade.trade_id.to_string(),
                    market_id: trade.market_id.clone(),
                    token_id: trade.token_id.clone(),
                    cost_basis: trade.total_cost_usd,
                    max_loss: trade.total_cost_usd + trade.total_fees_usd,
                    status: LedgerStatus::Active,
                    created_at: Utc::now(),
                    resolved_at: None,
                });
        }

        (daily_rolled, false, false)
    }

    /// Settlement phase: realized profit flows into all accounting windows.
    fn apply_settlement(&self, trade: &TradeRecord) -> (bool, bool, bool) {
        let daily_rolled = self.daily.write().record_trade(
            trade.net_profit_usd,
            Usd::ZERO,
            Usd::ZERO,
            trade.status,
        );
        let weekly_rolled =
            self.weekly
                .write()
                .record_trade(trade.net_profit_usd, Usd::ZERO, trade.status);
        let hourly_rolled =
            self.hourly
                .write()
                .record_trade(trade.net_profit_usd, Usd::ZERO, trade.status);

        self.potential_loss.write().resolve(trade.trade_id.as_ref());

        (daily_rolled, weekly_rolled, hourly_rolled)
    }

    /// Check all loss caps and trip the breaker if any are breached.
    fn check_loss_caps(&self) -> Option<CircuitBreakerLevel> {
        let mut tripped = None;

        let daily_loss = self.daily.read().daily_loss();
        if daily_loss.inner() >= self.config.max_daily_loss_usd {
            let reason = format!("daily loss cap breached: {daily_loss}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Daily, reason);
            tripped = Some(CircuitBreakerLevel::Daily);
        }

        let weekly_loss = self.weekly.read().weekly_loss();
        if weekly_loss.inner() >= self.config.max_weekly_loss_usd {
            let reason = format!("weekly loss cap breached: {weekly_loss}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Daily, reason);
            if tripped.is_none() {
                tripped = Some(CircuitBreakerLevel::Daily);
            }
        }

        let hourly_loss = self.hourly.read().hourly_loss();
        if hourly_loss.inner() >= self.config.max_hourly_loss_usd {
            let reason = format!("hourly loss cap breached: {hourly_loss}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            if tripped.is_none() {
                tripped = Some(CircuitBreakerLevel::Session);
            }
        }

        tripped
    }

    /// Common checks run on both Fill and Settlement phases.
    fn apply_common_checks(
        &self,
        trade: &TradeRecord,
        metrics: &dyn RiskMetrics,
    ) -> Option<CircuitBreakerLevel> {
        let mut tripped = None;

        {
            let mut cb = self.circuit_breaker.write();
            if cb.is_probe_mode() {
                cb.on_trade_result(trade.is_success());
            }
        }

        self.drawdown
            .write()
            .update_equity(metrics.cached_balance());

        let miss_count = metrics.consecutive_market_misses(&trade.market_id);
        if miss_count >= self.config.max_consecutive_misses {
            let reason = format!(
                "consecutive misses: {miss_count} >= {}",
                self.config.max_consecutive_misses
            );
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            tripped = Some(CircuitBreakerLevel::Session);
        }

        let req_count = metrics.api_request_count();
        if req_count > 0 {
            let error_rate = rust_decimal::Decimal::from(metrics.api_error_count())
                / rust_decimal::Decimal::from(req_count);
            if error_rate >= self.config.api_error_rate_threshold {
                let reason = format!(
                    "API error rate {error_rate:.2} >= threshold {}",
                    self.config.api_error_rate_threshold
                );
                self.circuit_breaker
                    .write()
                    .trip(CircuitBreakerLevel::Session, reason);
                if tripped.is_none() {
                    tripped = Some(CircuitBreakerLevel::Session);
                }
            }
        }

        tripped
    }

    // ── Tick (async) ───────────────────────────────────────────────────

    /// Periodic tick — drives CB FSM transitions, accounting rollover,
    /// and blacklist GC.
    pub async fn tick(&self, metrics: &dyn RiskMetrics) -> OxideResult<bool> {
        let cb_transitioned = self.circuit_breaker.write().tick();
        let daily_rolled = self.daily.write().maybe_rollover();
        let weekly_rolled = self.weekly.write().maybe_rollover();
        let hourly_rolled = self.hourly.write().maybe_rollover();
        self.blacklist.gc();

        let any_change = cb_transitioned || daily_rolled || weekly_rolled || hourly_rolled;
        if any_change {
            let snapshot = self.snapshot(metrics);
            if let Err(e) = self.persistence.save_snapshot(&snapshot).await {
                self.halt_internal(format!("tick persist failed: {e}"));
                return Err(e);
            }
        }

        Ok(any_change)
    }

    /// Refresh the cached position tracker from the live metrics source.
    pub fn refresh_positions(&self, metrics: &dyn RiskMetrics) {
        self.position_tracker.write().refresh(metrics);
    }

    // ── Operations (async with persistence) ────────────────────────────

    #[must_use]
    pub fn is_halted(&self) -> bool {
        self.is_halted.load(Ordering::Acquire)
    }

    /// Halt the engine and persist the state change.
    pub async fn halt(&self, reason: String) {
        self.halt_internal(reason.clone());

        let audit = RiskAuditEvent::EngineHalted { reason };
        // Best-effort persist — if it fails, engine is already halted
        let _ = self.persistence.append_audit_event(&audit).await;
    }

    /// Resume from manual halt with persistence and audit.
    pub async fn resume(&self) -> OxideResult<()> {
        tracing::info!("risk engine resumed from manual halt");
        self.is_halted.store(false, Ordering::Release);
        *self.halt_reason.write() = None;
        self.state_version.increment();

        let audit = RiskAuditEvent::EngineResumed;
        if let Err(e) = self.persistence.append_audit_event(&audit).await {
            self.halt_internal(format!("resume audit failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    /// Reset the circuit breaker with persistence and audit.
    pub async fn reset_circuit_breaker(
        &self,
        reason: &str,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        let previous_state = self.circuit_breaker.read().state().to_name();
        self.circuit_breaker.write().reset(reason);
        self.state_version.increment();

        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BreakerReset {
            operator_reason: reason.to_owned(),
        };
        self.persist_and_audit(&snapshot, audit).await?;

        tracing::info!(
            previous = %previous_state,
            reason,
            "circuit breaker reset with persistence"
        );
        Ok(())
    }

    /// Add a blacklist entry through the engine (persisted + audited).
    pub async fn add_blacklist(
        &self,
        market_id: MarketId,
        reason: BlacklistReason,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        let entry = self.blacklist.add_permanent(market_id, reason);
        self.state_version.increment();

        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BlacklistAdded {
            entry: entry.clone(),
        };
        self.persist_and_audit(&snapshot, audit).await?;

        if let Err(e) = self.persistence.save_blacklist_entry(&entry).await {
            self.halt_internal(format!("blacklist persist failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    /// Remove a blacklist entry through the engine (persisted + audited).
    pub async fn remove_blacklist(
        &self,
        market_id: &MarketId,
        reason: &str,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        let _ = self.blacklist.remove(market_id);
        self.state_version.increment();

        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BlacklistRemoved {
            market_id: market_id.clone(),
            operator_reason: reason.to_owned(),
        };
        self.persist_and_audit(&snapshot, audit).await?;

        if let Err(e) = self.persistence.remove_blacklist_entry(market_id).await {
            self.halt_internal(format!("blacklist remove persist failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    /// Resolve a potential loss entry (market settled / position closed).
    pub async fn resolve_potential_loss(
        &self,
        entry_id: &str,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        self.potential_loss.write().resolve(entry_id);
        self.state_version.increment();

        let snapshot = self.snapshot(metrics);
        if let Err(e) = self.persistence.save_snapshot(&snapshot).await {
            self.halt_internal(format!("potential loss resolve persist failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    #[must_use]
    pub const fn blacklist(&self) -> &BlacklistManager {
        &self.blacklist
    }

    #[must_use]
    pub const fn reconciler(&self) -> &LedgerReconciler {
        &self.reconciler
    }

    #[must_use]
    pub fn snapshot(&self, metrics: &dyn RiskMetrics) -> RiskEngineSnapshot {
        let cb = self.circuit_breaker.read();
        let daily = self.daily.read();
        let weekly = self.weekly.read();
        let hourly = self.hourly.read();
        let dg = self.drawdown.read();

        RiskEngineSnapshot {
            breaker_state: cb.state().to_name(),
            breaker_level: match cb.state() {
                BreakerState::Open { level, .. } | BreakerState::HalfOpen { level, .. } => {
                    Some(*level)
                }
                _ => None,
            },
            breaker_reason: match cb.state() {
                BreakerState::Open { reason, .. } => Some(reason.clone()),
                _ => None,
            },
            cooling_until: match cb.state() {
                BreakerState::Open { cooldown_until, .. } => Some(*cooldown_until),
                _ => None,
            },
            total_exposure: metrics.total_exposure(),
            daily_pnl: daily.daily_pnl(),
            daily_loss: daily.daily_loss(),
            weekly_loss: weekly.weekly_loss(),
            hourly_loss: hourly.hourly_loss(),
            hourly_trade_count: hourly.stats().trade_count,
            hourly_success_count: hourly.stats().success_count,
            hourly_miss_count: hourly.stats().miss_count,
            consecutive_misses: 0,
            l2_trip_count: cb.l2_trip_count(),
            daily_budget_spent: daily.budget_spent(),
            daily_trade_count: daily.stats().trade_count,
            daily_success_count: daily.stats().success_count,
            daily_miss_count: daily.stats().miss_count,
            weekly_trade_count: weekly.stats().trade_count,
            hwm_equity: dg.hwm(),
            snapshot_at: Utc::now(),
        }
    }

    // ── Private ─────────────────────────────────────────────────────────

    /// Sync halt — sets the halted flag without persistence.
    /// Used when persistence itself has failed (avoid recursion).
    fn halt_internal(&self, reason: String) {
        tracing::error!(%reason, "risk engine halted (fail-closed)");
        self.is_halted.store(true, Ordering::Release);
        *self.halt_reason.write() = Some(reason);
    }

    /// Persist snapshot + append audit event. Halts on failure.
    async fn persist_and_audit(
        &self,
        snapshot: &RiskEngineSnapshot,
        event: RiskAuditEvent,
    ) -> OxideResult<()> {
        if let Err(e) = self.persistence.save_snapshot(snapshot).await {
            self.halt_internal(format!("persist failed: {e}"));
            return Err(e);
        }
        if let Err(e) = self.persistence.append_audit_event(&event).await {
            self.halt_internal(format!("audit failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    fn build_context(
        &self,
        opp: &Opportunity,
        probability: &ProbabilityInput,
        metrics: &dyn RiskMetrics,
        version: StateVersion,
    ) -> RiskContext {
        let cb = self.circuit_breaker.read();
        let daily = self.daily.read();
        let weekly = self.weekly.read();
        let hourly = self.hourly.read();
        let dg = self.drawdown.read();
        let pl = self.potential_loss.read();

        let bl_result = self
            .blacklist
            .check(&opp.market_id, BlacklistScope::TradingPath);
        let blacklist_gate = match &bl_result {
            BlacklistCheckResult::Clear => BlacklistGate::Clear,
            BlacklistCheckResult::Blocked { reason, scope, .. } => BlacklistGate::Blocked {
                detail: format!("{reason} (scope: {scope})"),
            },
        };

        let token_blacklisted = self.blacklist.is_token_blacklisted(&opp.token_id);

        let manual_halt_gate = if self.is_halted.load(Ordering::Acquire) {
            ManualHaltGate::Halted {
                reason: self
                    .halt_reason
                    .read()
                    .clone()
                    .unwrap_or_else(|| "halted".into()),
            }
        } else {
            ManualHaltGate::Clear
        };

        let equity = metrics.cached_balance();
        let drawdown_factor = dg.sizing_factor(equity);
        let (_, drawdown_action) = dg.evaluate(equity);
        drop(dg);

        RiskContext {
            state_version: version,
            opportunity: opp.clone(),
            probability: probability.clone(),
            market_exposure_before: metrics.market_exposure(&opp.market_id),
            total_exposure_before: metrics.total_exposure(),
            total_potential_loss: pl.total_potential_loss(),
            active_reservation_count: metrics.active_reservation_count(),
            reserved_usd: metrics.reserved_usd(),
            open_position_count: metrics.open_position_count(),
            cached_balance: equity,
            ws_disconnect_secs: metrics.ws_disconnect_secs(),
            open_directional_count_same_side: metrics.open_directional_count(opp.side),
            daily_directional_trades_same_side: metrics.daily_directional_trades(opp.side),
            consecutive_market_misses: metrics.consecutive_market_misses(&opp.market_id),
            hourly_loss: hourly.hourly_loss(),
            daily_loss: daily.daily_loss(),
            daily_budget_remaining: daily.budget_remaining(),
            weekly_loss: weekly.weekly_loss(),
            daily_pnl: daily.daily_pnl(),
            circuit_breaker: CircuitBreakerGate {
                allows_trading: cb.allows_trading(),
                is_probe: cb.is_probe_mode(),
            },
            manual_halt: manual_halt_gate,
            blacklist: blacklist_gate,
            token_blacklisted,
            api_error_count: metrics.api_error_count(),
            api_request_count: metrics.api_request_count(),
            drawdown_factor,
            drawdown_action,
            snapshot_at: Utc::now(),
        }
    }
}

/// Effective bankroll for Kelly sizing: `min(dynamic, config_bankroll)`.
///
/// `dynamic = balance - reserve - exposure - potential_loss`, floored at zero.
#[inline]
fn available_bankroll(
    cached_balance: Usd,
    reserve: Usd,
    total_exposure: Usd,
    total_potential_loss: Usd,
    config_bankroll: Usd,
) -> Usd {
    let dynamic = (cached_balance - reserve - total_exposure - total_potential_loss).max(Usd::ZERO);
    let capped = Usd::new(config_bankroll.inner().min(dynamic.inner()));
    capped.max(Usd::ZERO)
}
