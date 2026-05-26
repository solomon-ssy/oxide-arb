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
use crate::audit_sink::{AuditEnqueueResult, AuditSink};
use crate::blacklist::BlacklistManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::clock::Clock;
use crate::context::{BlacklistGate, CircuitBreakerGate, ManualHaltGate, RiskContext};
use crate::pipeline::StaticRiskPipeline;
use crate::position::{PositionTracker, PotentialLossLedger};
use crate::reconciliation::LedgerReconciler;
use crate::sizing::{DrawdownGuard, MultiConstraintSizer};
use crate::snapshot::{
    CircuitBreakerSnapshot, DailyAccountingSnapshot, DrawdownSnapshot, HourlyAccountingSnapshot,
    RiskSnapshot, WeeklyAccountingSnapshot,
};
use crate::traits::{RiskMetrics, RiskMetricsSnapshot, RiskPersistence};
use crate::types::{
    AtomicStateVersion, BreakerState, ExecutionRiskEvent, PostTradeReport, ReconciliationReport,
    ReportMode, RiskDecision, StateVersion,
};
use arc_swap::ArcSwap;
use num_traits::ToPrimitive;
use oxide_arb_error::OxideResult;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::BlacklistInfo;
use oxide_arb_models::domain::blacklist::UpsertBlacklistEntry;
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::potential_loss::PotentialLossInfo;
use oxide_arb_models::domain::risk::{
    NewEmergencySnapshot, NewRiskAuditEvent, ProbabilityInput, RiskEngineState,
    UpsertRiskEngineState,
};
use oxide_arb_models::domain::trade::PostTradeInput;
use oxide_arb_models::enums::ReconciliationStatus;
use oxide_arb_models::enums::blacklist::BlacklistCheckResult;
use oxide_arb_models::enums::common::LedgerStatus;
use oxide_arb_models::enums::risk::{
    BlacklistReason, BlacklistScope, CircuitBreakerLevel, TradeAccountingPhase, WindowType,
};
use oxide_arb_models::types::{LedgerId, MarketId, OpportunityId, Price, Shares, Usd};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Instant;

/// Gates evaluated before live metrics are loaded (halt / CB / blacklist).
const PHASE1_GATE_COUNT: usize = 4;

pub struct RiskEngine {
    pub(crate) circuit_breaker: RwLock<CircuitBreaker>,
    pub(crate) daily: RwLock<DailyAccounting>,
    pub(crate) weekly: RwLock<WeeklyAccounting>,
    pub(crate) hourly: RwLock<HourlyAccounting>,
    pub(crate) position_tracker: RwLock<PositionTracker>,
    pub(crate) potential_loss: RwLock<PotentialLossLedger>,
    pub(crate) pipeline: StaticRiskPipeline,
    pub(crate) risk_snapshot: ArcSwap<RiskSnapshot>,
    pub(crate) blacklist: BlacklistManager,
    pub(crate) sizer: MultiConstraintSizer,
    pub(crate) drawdown: RwLock<DrawdownGuard>,
    pub(crate) reconciler: LedgerReconciler,
    pub(crate) config: RiskConfig,
    pub(crate) persistence: Arc<dyn RiskPersistence>,
    pub(crate) audit_sink: Option<Arc<dyn AuditSink>>,
    pub(crate) state_version: AtomicStateVersion,
    pub(crate) clock: Arc<dyn Clock>,
}

impl RiskEngine {
    // ── Pre-trade (sync hot path + non-blocking audit) ───────────────

    /// Evaluate all pre-trade risk checks and compute sizing.
    ///
    /// Produces an immutable `RiskDecision` and enqueues an audit event
    /// (`TradeAllowed` or `TradeDenied`) when an [`AuditSink`] is configured.
    /// Audit enqueue failures are best-effort and never halt the engine.
    #[must_use]
    pub fn pre_trade_check_core<M: RiskMetrics>(
        &self,
        opp: &Arc<Opportunity>,
        probability: &ProbabilityInput,
        metrics: &M,
        mode: ReportMode,
    ) -> RiskDecision {
        let eval_start = Instant::now();
        let now = self.clock.now();
        let version = self.state_version.load();
        let snap = self.risk_snapshot.load();

        let phase1_ctx = self.build_context_phase1(opp, probability, &snap, version, now);
        let mut gate_report = self
            .pipeline
            .evaluate_range(&phase1_ctx, mode, 0, PHASE1_GATE_COUNT);

        let ctx = if gate_report.has_failed_hard_gate && mode == ReportMode::ShortCircuit {
            phase1_ctx
        } else {
            let metrics_snap = metrics.snapshot_for(&opp.market_id);
            let ctx = self.build_context_full(opp, probability, &snap, &metrics_snap, version, now);
            let phase2 =
                self.pipeline
                    .evaluate_range(&ctx, mode, PHASE1_GATE_COUNT, self.pipeline.len());
            gate_report.merge(phase2);
            ctx
        };

        let (allowed, denial_reason, recommended_size, sizing_breakdown) = if gate_report
            .has_failed_hard_gate
        {
            let denial_reason = gate_report.results.iter().find(|c| !c.passed).map(|c| {
                format!(
                    "{}: {}",
                    c.check_id,
                    c.detail.as_deref().unwrap_or("failed")
                )
            });
            (false, denial_reason, None, None)
        } else {
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

            if allowed {
                (true, denial_reason, Some(size_result), None)
            } else {
                let breakdown = size_result.breakdown;
                (false, denial_reason, None, Some(breakdown))
            }
        };

        let check_results = gate_report.results;
        let trace = RiskDecisionTrace {
            check_results,
            sizing_breakdown,
            state_version: version,
            total_elapsed_us: ToPrimitive::to_u64(&eval_start.elapsed().as_micros())
                .unwrap_or(u64::MAX),
            evaluated_at: now,
        };

        let decision = RiskDecision {
            allowed,
            denial_reason,
            recommended_size,
            drawdown_factor: ctx.drawdown_factor,
            evaluated_at: now,
            state_version: version,
            trace,
        };

        self.enqueue_pre_trade_audit(allowed, &decision.trace, opp.opportunity_id.clone());
        decision
    }

    // ── Post-trade (async mutation path) ────────────────────────────────

    /// Process a trade result with Fill/Settlement phase separation.
    ///
    /// - **Fill**: records cost, fees, counts, and potential loss across all
    ///   accounting windows. No realized profit flows into loss caps.
    /// - **Settlement**: records realized profit, resolves potential loss,
    ///   checks caps and may trip the circuit breaker.
    ///
    /// After in-memory mutation, persists the snapshot, appends an audit
    /// event, and persists any auto-blacklist entry. If persistence fails,
    /// the engine halts (fail-closed).
    #[must_use = "post-trade report contains breaker/blacklist state changes"]
    pub async fn on_trade_result(
        &self,
        phase: TradeAccountingPhase,
        trade: &PostTradeInput,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<PostTradeReport> {
        let previous_breaker_state = self.circuit_breaker.read().state().to_name();
        let (report, audit_event, auto_bl_entry) = self.apply_trade_result(phase, trade, metrics);

        self.persist_and_audit(&report.snapshot, audit_event)
            .await?;

        if let Some(tripped_level) = report.breaker_tripped {
            let reason = match self.circuit_breaker.read().state() {
                BreakerState::Open { reason, .. } | BreakerState::Halted { reason, .. } => {
                    reason.clone()
                }
                _ => "loss cap breached".into(),
            };
            let tripped_audit = RiskAuditEvent::BreakerTripped {
                level: tripped_level,
                reason: reason.clone(),
                previous_state: previous_breaker_state,
            };
            let _ = self.persistence.create_audit(tripped_audit.into()).await;
            let _ = self.record_emergency(tripped_level, &reason, metrics).await;
        }

        if let Some(ref entry) = auto_bl_entry {
            let upsert = UpsertBlacklistEntry {
                market_id: entry.market_id.clone(),
                token_id: entry.token_id.clone(),
                scope: entry.scope,
                reason: entry.reason,
                expires_at: entry.expires_at,
                miss_count: entry.miss_count,
            };
            if let Err(e) = self.persistence.upsert_blacklist(upsert).await {
                self.halt_internal(format!("auto-blacklist persist failed: {e}"));
                return Err(e);
            }
        }

        Ok(report)
    }

    fn apply_trade_result(
        &self,
        phase: TradeAccountingPhase,
        trade: &PostTradeInput,
        metrics: &dyn RiskMetrics,
    ) -> (PostTradeReport, NewRiskAuditEvent, Option<BlacklistInfo>) {
        let mut breaker_tripped: Option<CircuitBreakerLevel> = None;
        let mut auto_blacklisted = None;
        let mut auto_bl_entry = None;

        let (daily_rolled, weekly_rolled, hourly_rolled) = match phase {
            TradeAccountingPhase::Fill => self.apply_fill(trade),
            TradeAccountingPhase::Settlement => {
                let (dr, wr, hr) = self.apply_settlement(trade);
                breaker_tripped = self.check_loss_caps();
                (dr, wr, hr)
            }
        };

        if trade.is_miss() {
            let miss_count = metrics.consecutive_market_misses(&trade.market_id);
            if let Some(entry) = self
                .blacklist
                .maybe_auto_blacklist(&trade.market_id, miss_count)
            {
                auto_blacklisted = Some(trade.market_id.clone());
                auto_bl_entry = Some(entry);
            }
        }

        if let Some(bt) = self.apply_common_checks(trade, metrics) {
            breaker_tripped = Some(breaker_tripped.map_or(bt, |existing| existing.max(bt)));
        }

        self.state_version.increment();
        self.publish_risk_snapshot();
        let snapshot = self.snapshot(metrics);

        let audit_event = RiskAuditEvent::PostTradeUpdate {
            trade_id: trade.trade_id.clone(),
            outcome: trade.outcome,
            phase,
            daily_loss_after: snapshot.daily_loss_usd,
            weekly_loss_after: snapshot.weekly_loss_usd,
            hourly_loss_after: snapshot.hourly_loss_usd,
            breaker_tripped,
            auto_blacklisted: auto_blacklisted.clone(),
            daily_rolled,
            weekly_rolled,
            hourly_rolled,
        }
        .into();

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
            auto_bl_entry,
        )
    }

    /// Fill phase: record cost/fees/counts across all windows + potential loss.
    fn apply_fill(&self, trade: &PostTradeInput) -> (bool, bool, bool) {
        let daily_rolled = {
            let mut daily = self.daily.write();
            daily.record_trade(Usd::ZERO, trade.fee_usd, trade.cost_usd, trade.outcome)
        };

        let weekly_rolled =
            self.weekly
                .write()
                .record_trade(Usd::ZERO, trade.fee_usd, trade.outcome);

        let hourly_rolled =
            self.hourly
                .write()
                .record_trade(Usd::ZERO, trade.fee_usd, trade.outcome);

        if trade.is_success() {
            self.potential_loss.write().record_entry(PotentialLossInfo {
                ledger_id: LedgerId::new(trade.trade_id.as_str()),
                market_id: trade.market_id.clone(),
                token_id: trade.token_id.clone(),
                shares: Shares::new(rust_decimal::Decimal::ZERO),
                entry_price: Price::new(rust_decimal::Decimal::ZERO),
                max_loss_usd: trade.cost_usd + trade.fee_usd,
                status: LedgerStatus::Active,
                created_at: self.clock.now(),
                resolved_at: None,
            });
        }

        (daily_rolled, weekly_rolled, hourly_rolled)
    }

    /// Settlement phase: realized profit flows into all accounting windows.
    fn apply_settlement(&self, trade: &PostTradeInput) -> (bool, bool, bool) {
        let net_profit = trade.net_profit_usd.unwrap_or(Usd::ZERO);
        let daily_rolled =
            self.daily
                .write()
                .record_trade(net_profit, Usd::ZERO, Usd::ZERO, trade.outcome);
        let weekly_rolled = self
            .weekly
            .write()
            .record_trade(net_profit, Usd::ZERO, trade.outcome);
        let hourly_rolled = self
            .hourly
            .write()
            .record_trade(net_profit, Usd::ZERO, trade.outcome);

        let ledger_id = LedgerId::new(trade.trade_id.as_str());
        self.potential_loss.write().resolve(&ledger_id);

        (daily_rolled, weekly_rolled, hourly_rolled)
    }

    /// Check all loss caps and halt/trip the breaker at the highest applicable level.
    ///
    /// - Daily / Weekly / Single-loss caps → `halt()` (L3 — requires operator ack)
    /// - Hourly cap → `trip()` (L2 — auto-recovery via `HalfOpen`)
    fn check_loss_caps(&self) -> Option<CircuitBreakerLevel> {
        let mut highest: Option<CircuitBreakerLevel> = None;

        let daily_loss = self.daily.read().daily_loss();
        if daily_loss.inner() >= self.config.max_daily_loss_usd {
            let reason = format!("daily loss cap breached: {daily_loss}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            highest = Some(CircuitBreakerLevel::Daily);
        }

        let weekly_loss = self.weekly.read().weekly_loss();
        if weekly_loss.inner() >= self.config.max_weekly_loss_usd {
            let reason = format!("weekly loss cap breached: {weekly_loss}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            highest = Some(highest.map_or(CircuitBreakerLevel::Daily, |h| {
                h.max(CircuitBreakerLevel::Daily)
            }));
        }

        let max_single = self.daily.read().stats().max_single_loss;
        if max_single.inner() >= self.config.max_single_loss_usd {
            let reason = format!("single loss cap breached: {max_single}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            highest = Some(highest.map_or(CircuitBreakerLevel::Daily, |h| {
                h.max(CircuitBreakerLevel::Daily)
            }));
        }

        let hourly_loss = self.hourly.read().hourly_loss();
        if hourly_loss.inner() >= self.config.max_hourly_loss_usd {
            let reason = format!("hourly loss cap breached: {hourly_loss}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            highest = Some(highest.map_or(CircuitBreakerLevel::Session, |h| {
                h.max(CircuitBreakerLevel::Session)
            }));
        }

        highest
    }

    /// Common checks run on both Fill and Settlement phases.
    fn apply_common_checks(
        &self,
        trade: &PostTradeInput,
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
                tripped = Some(
                    tripped.map_or(CircuitBreakerLevel::Session, |h: CircuitBreakerLevel| {
                        h.max(CircuitBreakerLevel::Session)
                    }),
                );
            }
        }

        tripped
    }

    // ── Tick (async) ───────────────────────────────────────────────────

    /// Periodic tick — drives CB FSM transitions, accounting rollover,
    /// and blacklist GC.
    pub async fn tick(&self, metrics: &dyn RiskMetrics) -> OxideResult<bool> {
        let cb_transitioned = self.circuit_breaker.write().tick();

        let (pre_daily_start, pre_daily_stats) = {
            let d = self.daily.read();
            (d.window_start(), d.stats().clone())
        };
        let (pre_weekly_start, pre_weekly_stats) = {
            let w = self.weekly.read();
            (w.week_start(), w.stats().clone())
        };
        let (pre_hourly_start, pre_hourly_stats) = {
            let h = self.hourly.read();
            (h.window_start().date_naive(), h.stats().clone())
        };

        let daily_rolled = self.daily.write().maybe_rollover();
        let weekly_rolled = self.weekly.write().maybe_rollover();
        let hourly_rolled = self.hourly.write().maybe_rollover();
        self.blacklist.gc();

        let any_change = cb_transitioned || daily_rolled || weekly_rolled || hourly_rolled;
        if any_change {
            self.publish_risk_snapshot();
            let snapshot = self.snapshot(metrics);
            let upsert = UpsertRiskEngineState::from(&snapshot);
            if let Err(e) = self.persistence.upsert_state(upsert).await {
                self.halt_internal(format!("tick persist failed: {e}"));
                return Err(e);
            }
        }

        if daily_rolled {
            let audit = RiskAuditEvent::AccountingRollover {
                window_type: WindowType::Daily,
                old_start: pre_daily_start,
                new_start: self.daily.read().window_start(),
                final_stats: pre_daily_stats,
            };
            let _ = self.persistence.create_audit(audit.into()).await;
        }
        if weekly_rolled {
            let audit = RiskAuditEvent::AccountingRollover {
                window_type: WindowType::Weekly,
                old_start: pre_weekly_start,
                new_start: self.weekly.read().week_start(),
                final_stats: pre_weekly_stats,
            };
            let _ = self.persistence.create_audit(audit.into()).await;
        }
        if hourly_rolled {
            let audit = RiskAuditEvent::AccountingRollover {
                window_type: WindowType::Hourly,
                old_start: pre_hourly_start,
                new_start: self.hourly.read().window_start().date_naive(),
                final_stats: pre_hourly_stats,
            };
            let _ = self.persistence.create_audit(audit.into()).await;
        }

        Ok(any_change)
    }

    /// Refresh the cached position tracker from the live metrics source.
    pub fn refresh_positions(&self, metrics: &dyn RiskMetrics) {
        self.position_tracker.write().refresh(metrics);
    }

    // ── Reconciliation ──────────────────────────────────────────────────

    /// Process a reconciliation report: persist, audit, and trip L4
    /// breaker if drift is critical.
    pub async fn on_reconciliation_result(
        &self,
        report: &ReconciliationReport,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        let mismatches_json =
            serde_json::to_value(&report.mismatches).unwrap_or(serde_json::Value::Null);
        let new_report = oxide_arb_models::domain::risk::NewReconciliationReport {
            status: report.status,
            mismatches: mismatches_json,
            internal_balance: report.internal_balance,
            external_balance: report.external_balance,
            internal_exposure: report.internal_exposure,
            external_exposure: report.external_exposure,
            reserved: report.reserved,
            tolerance: report.tolerance,
            checked_at: report.checked_at,
            duration_ms: ToPrimitive::to_i64(&report.duration_ms).unwrap_or(i64::MAX),
        };

        if let Err(e) = self.persistence.create_reconciliation(new_report).await {
            self.halt_internal(format!("reconciliation report persist failed: {e}"));
            return Err(e);
        }

        let audit = RiskAuditEvent::ReconciliationCompleted {
            status: report.status,
            mismatch_count: report.mismatches.len(),
        };
        self.persist_audit_event(audit.into()).await?;

        if report.status == ReconciliationStatus::Critical {
            let reason = format!(
                "reconciliation critical drift: {} mismatches",
                report.mismatches.len()
            );
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::System, reason.clone());
            self.state_version.increment();
            self.publish_risk_snapshot();

            let snapshot = self.snapshot(metrics);
            let upsert = UpsertRiskEngineState::from(&snapshot);
            if let Err(e) = self.persistence.upsert_state(upsert).await {
                self.halt_internal(format!("reconciliation breaker persist failed: {e}"));
                return Err(e);
            }

            let _ = self
                .record_emergency(CircuitBreakerLevel::System, &reason, metrics)
                .await;
        }

        Ok(())
    }

    // ── Operations (async with persistence) ────────────────────────────

    #[must_use]
    pub fn is_halted(&self) -> bool {
        self.circuit_breaker.read().state().is_halted()
    }

    /// Whether new trades may pass circuit-breaker gates (Closed / `HalfOpen` / Recovered).
    #[inline]
    pub fn allows_trading(&self) -> bool {
        self.circuit_breaker.read().allows_trading()
    }

    /// Halt the engine and persist the state change.
    ///
    /// Transitions the circuit breaker to `Halted(System)` — the single authority
    /// for all halt semantics.
    pub async fn halt(&self, reason: String) {
        self.halt_internal(reason.clone());

        let audit = RiskAuditEvent::EngineHalted { reason };
        let _ = self.persistence.create_audit(audit.into()).await;
    }

    /// Resume from halt with operator acknowledgement.
    ///
    /// Clears the CB FSM `Halted` state. Persists an audit event; if persistence
    /// fails, re-halts (fail-closed).
    pub async fn acknowledge_and_resume(&self, operator_ack: &str) -> OxideResult<()> {
        let was_halted = self
            .circuit_breaker
            .write()
            .acknowledge_and_resume(operator_ack);
        self.state_version.increment();
        self.publish_risk_snapshot();

        let audit = RiskAuditEvent::EngineResumed;
        if let Err(e) = self.persistence.create_audit(audit.into()).await {
            self.halt_internal(format!("resume audit failed: {e}"));
            return Err(e);
        }
        tracing::info!(
            ack = operator_ack,
            was_cb_halted = was_halted,
            "risk engine resumed"
        );
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
        self.publish_risk_snapshot();

        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BreakerReset {
            operator_reason: reason.to_owned(),
        };
        self.persist_and_audit(&snapshot, audit.into()).await?;

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

        let upsert = UpsertBlacklistEntry {
            market_id: entry.market_id.clone(),
            token_id: entry.token_id.clone(),
            scope: entry.scope,
            reason: entry.reason,
            expires_at: entry.expires_at,
            miss_count: entry.miss_count,
        };
        if let Err(e) = self.persistence.upsert_blacklist(upsert).await {
            self.halt_internal(format!("blacklist persist failed: {e}"));
            return Err(e);
        }

        self.state_version.increment();
        self.publish_risk_snapshot();
        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BlacklistAdded {
            entry: entry.clone(),
        };
        self.persist_and_audit(&snapshot, audit.into()).await?;

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

        if let Err(e) = self.persistence.remove_blacklist(market_id).await {
            self.halt_internal(format!("blacklist remove persist failed: {e}"));
            return Err(e);
        }

        self.state_version.increment();
        self.publish_risk_snapshot();
        let snapshot = self.snapshot(metrics);
        let audit = RiskAuditEvent::BlacklistRemoved {
            market_id: market_id.clone(),
            operator_reason: reason.to_owned(),
        };
        self.persist_and_audit(&snapshot, audit.into()).await?;

        Ok(())
    }

    /// Resolve a potential loss entry (market settled / position closed).
    pub async fn resolve_potential_loss(
        &self,
        ledger_id: &LedgerId,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        self.potential_loss.write().resolve(ledger_id);
        self.state_version.increment();
        self.publish_risk_snapshot();

        let snapshot = self.snapshot(metrics);
        let upsert = UpsertRiskEngineState::from(&snapshot);
        if let Err(e) = self.persistence.upsert_state(upsert).await {
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
    pub fn snapshot(&self, metrics: &dyn RiskMetrics) -> RiskEngineState {
        let cb = self.circuit_breaker.read();
        let daily = self.daily.read();
        let weekly = self.weekly.read();
        let hourly = self.hourly.read();
        let dg = self.drawdown.read();

        let is_halted = cb.state().is_halted();

        RiskEngineState {
            breaker_state: cb.state().to_name(),
            breaker_level: match cb.state() {
                BreakerState::Open { level, .. }
                | BreakerState::HalfOpen { level, .. }
                | BreakerState::Halted { level, .. } => Some(*level),
                _ => None,
            },
            is_halted,
            halt_reason: match cb.state() {
                BreakerState::Open { reason, .. } | BreakerState::Halted { reason, .. } => {
                    Some(reason.clone())
                }
                _ => None,
            },
            cooldown_until: match cb.state() {
                BreakerState::Open { cooldown_until, .. } => Some(*cooldown_until),
                _ => None,
            },
            total_exposure: metrics.total_exposure(),
            hourly_loss_usd: hourly.hourly_loss(),
            hourly_fee_usd: hourly.stats().fees,
            hourly_trade_count: ToPrimitive::to_i32(&hourly.stats().trade_count)
                .unwrap_or(i32::MAX),
            hourly_success_count: ToPrimitive::to_i32(&hourly.stats().success_count)
                .unwrap_or(i32::MAX),
            hourly_miss_count: ToPrimitive::to_i32(&hourly.stats().miss_count).unwrap_or(i32::MAX),
            hourly_window_start: hourly.window_start(),
            daily_pnl: daily.daily_pnl(),
            daily_loss_usd: daily.daily_loss(),
            daily_fee_usd: daily.stats().fees,
            daily_budget_spent: daily.budget_spent(),
            daily_trade_count: ToPrimitive::to_i32(&daily.stats().trade_count).unwrap_or(i32::MAX),
            daily_success_count: ToPrimitive::to_i32(&daily.stats().success_count)
                .unwrap_or(i32::MAX),
            daily_miss_count: ToPrimitive::to_i32(&daily.stats().miss_count).unwrap_or(i32::MAX),
            daily_window_start: daily.window_start(),
            weekly_loss_usd: weekly.weekly_loss(),
            weekly_trade_count: ToPrimitive::to_i32(&weekly.stats().trade_count)
                .unwrap_or(i32::MAX),
            weekly_window_start: weekly.week_start(),
            consecutive_misses: ToPrimitive::to_i32(
                &metrics
                    .consecutive_market_misses(&oxide_arb_models::types::MarketId::new("_global_")),
            )
            .unwrap_or(0),
            cooldown_multiplier: ToPrimitive::to_i32(&cb.l2_trip_count()).unwrap_or(i32::MAX),
            hwm_equity: dg.hwm(),
            last_emergency_at: None,
            last_emergency_reason: None,
            snapshot_at: self.clock.now(),
        }
    }

    // ── Private ─────────────────────────────────────────────────────────

    async fn record_emergency(
        &self,
        level: CircuitBreakerLevel,
        reason: &str,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        let snapshot = self.snapshot(metrics);
        let emergency = NewEmergencySnapshot {
            trigger_level: level,
            reason: reason.to_owned(),
            risk_state: serde_json::to_value(&snapshot).unwrap_or(serde_json::Value::Null),
            open_positions_count: i32::try_from(metrics.open_position_count()).unwrap_or(i32::MAX),
            open_reservations_count: i32::try_from(metrics.active_reservation_count())
                .unwrap_or(i32::MAX),
            triggered_at: self.clock.now(),
        };
        self.persistence.create_emergency(emergency).await
    }

    fn halt_internal(&self, reason: String) {
        tracing::error!(%reason, "risk engine halted (fail-closed)");
        self.circuit_breaker
            .write()
            .halt(CircuitBreakerLevel::System, reason);
        self.publish_risk_snapshot();
    }

    /// Process execution-layer health and blacklist signals.
    pub fn on_execution_event(&self, event: ExecutionRiskEvent) {
        match event {
            ExecutionRiskEvent::HeartbeatFailure => {
                let max_failures = self.config.heartbeat_max_failures;
                let tripped = self
                    .circuit_breaker
                    .write()
                    .on_heartbeat_failure(max_failures);
                if tripped {
                    self.state_version.increment();
                    self.publish_risk_snapshot();
                }
            }
            ExecutionRiskEvent::HeartbeatSuccess => {
                self.circuit_breaker.write().on_heartbeat_success();
            }
            ExecutionRiskEvent::FokFailure {
                market_id,
                consecutive,
                ..
            } if consecutive >= self.config.max_consecutive_misses => {
                let reason = format!(
                    "consecutive FOK failures on {market_id}: {consecutive} >= {}",
                    self.config.max_consecutive_misses
                );
                self.circuit_breaker
                    .write()
                    .trip(CircuitBreakerLevel::Session, reason);
                self.state_version.increment();
                self.publish_risk_snapshot();
            }
            ExecutionRiskEvent::TradeFailed { market_id, .. } => {
                tracing::warn!(%market_id, "execution trade failed event received");
            }
            ExecutionRiskEvent::DepthDrop {
                market_id,
                pct_drop,
            } => {
                tracing::warn!(
                    %market_id,
                    %pct_drop,
                    "execution depth drop event received"
                );
            }
            ExecutionRiskEvent::FokFailure { .. } => {}
        }
    }

    async fn persist_and_audit(
        &self,
        snapshot: &RiskEngineState,
        audit: NewRiskAuditEvent,
    ) -> OxideResult<()> {
        let upsert = UpsertRiskEngineState::from(snapshot);
        if let Err(e) = self.persistence.upsert_state(upsert).await {
            self.halt_internal(format!("persist failed: {e}"));
            return Err(e);
        }
        if let Err(e) = self.persistence.create_audit(audit).await {
            self.halt_internal(format!("audit failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    async fn persist_audit_event(&self, audit: NewRiskAuditEvent) -> OxideResult<()> {
        if let Err(e) = self.persistence.create_audit(audit).await {
            self.halt_internal(format!("audit persist failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    fn compile_risk_snapshot(&self) -> RiskSnapshot {
        let cb = self.circuit_breaker.read();
        let daily = self.daily.read();
        let weekly = self.weekly.read();
        let hourly = self.hourly.read();
        let dg = self.drawdown.read();
        let pl = self.potential_loss.read();

        let manual_halt = if cb.state().is_halted() {
            ManualHaltGate::Halted {
                reason: match cb.state() {
                    BreakerState::Halted { reason, .. } => reason.clone(),
                    _ => "halted".into(),
                },
            }
        } else {
            ManualHaltGate::Clear
        };

        RiskSnapshot {
            circuit_breaker: CircuitBreakerSnapshot {
                circuit_breaker: CircuitBreakerGate {
                    allows_trading: cb.allows_trading(),
                    is_probe: cb.is_probe_mode(),
                },
                manual_halt,
            },
            daily: DailyAccountingSnapshot {
                daily_loss: daily.daily_loss(),
                daily_pnl: daily.daily_pnl(),
                daily_budget_remaining: daily.budget_remaining(),
            },
            weekly: WeeklyAccountingSnapshot {
                weekly_loss: weekly.weekly_loss(),
            },
            hourly: HourlyAccountingSnapshot {
                hourly_loss: hourly.hourly_loss(),
            },
            drawdown: DrawdownSnapshot {
                hwm: dg.hwm(),
                max_drawdown_pct: self.config.drawdown.max_drawdown_pct,
                reduction_factor: self.config.drawdown.drawdown_reduction_factor,
            },
            total_potential_loss: pl.total_potential_loss(),
        }
    }

    pub(crate) fn publish_risk_snapshot(&self) {
        self.risk_snapshot
            .store(Arc::new(self.compile_risk_snapshot()));
    }

    fn enqueue_pre_trade_audit(
        &self,
        allowed: bool,
        trace: &RiskDecisionTrace,
        opportunity_id: OpportunityId,
    ) {
        let Some(sink) = &self.audit_sink else {
            return;
        };

        let audit = if allowed {
            RiskAuditEvent::TradeAllowed {
                trace: trace.clone(),
                opportunity_id,
            }
        } else {
            RiskAuditEvent::TradeDenied {
                trace: trace.clone(),
                opportunity_id,
            }
        };

        if sink.try_enqueue(audit) == AuditEnqueueResult::Dropped {
            tracing::warn!("pre-trade audit channel full — event dropped");
        }
    }

    fn build_context_phase1(
        &self,
        opp: &Arc<Opportunity>,
        probability: &ProbabilityInput,
        snap: &RiskSnapshot,
        version: StateVersion,
        now: chrono::DateTime<chrono::Utc>,
    ) -> RiskContext {
        let blacklist_gate = self.blacklist_gate(opp);
        let token_blacklisted = self.blacklist.is_token_blacklisted(&opp.token_id);

        RiskContext {
            state_version: version,
            opportunity: Arc::clone(opp),
            probability: *probability,
            market_exposure_before: Usd::ZERO,
            total_exposure_before: Usd::ZERO,
            total_potential_loss: snap.total_potential_loss,
            active_reservation_count: 0,
            reserved_usd: Usd::ZERO,
            open_position_count: 0,
            cached_balance: Usd::ZERO,
            ws_disconnect_secs: 0,
            open_directional_count_same_side: 0,
            daily_directional_trades_same_side: 0,
            consecutive_market_misses: 0,
            hourly_loss: snap.hourly.hourly_loss,
            daily_loss: snap.daily.daily_loss,
            daily_budget_remaining: snap.daily.daily_budget_remaining,
            weekly_loss: snap.weekly.weekly_loss,
            daily_pnl: snap.daily.daily_pnl,
            circuit_breaker: snap.circuit_breaker.circuit_breaker,
            manual_halt: snap.circuit_breaker.manual_halt.clone(),
            blacklist: blacklist_gate,
            token_blacklisted,
            api_error_count: 0,
            api_request_count: 0,
            drawdown_factor: Decimal::ONE,
            drawdown_action: crate::types::DrawdownAction::Normal,
            snapshot_at: now,
        }
    }

    fn build_context_full(
        &self,
        opp: &Arc<Opportunity>,
        probability: &ProbabilityInput,
        snap: &RiskSnapshot,
        metrics: &RiskMetricsSnapshot,
        version: StateVersion,
        now: chrono::DateTime<chrono::Utc>,
    ) -> RiskContext {
        let equity = metrics.cached_balance;
        let drawdown_factor = snap.drawdown.sizing_factor(equity);
        let (_, drawdown_action) = snap.drawdown.evaluate(equity);
        let blacklist_gate = self.blacklist_gate(opp);
        let token_blacklisted = self.blacklist.is_token_blacklisted(&opp.token_id);

        RiskContext {
            state_version: version,
            opportunity: Arc::clone(opp),
            probability: *probability,
            market_exposure_before: metrics.market_exposure,
            total_exposure_before: metrics.total_exposure,
            total_potential_loss: snap.total_potential_loss,
            active_reservation_count: metrics.active_reservation_count,
            reserved_usd: metrics.reserved_usd,
            open_position_count: metrics.open_position_count,
            cached_balance: equity,
            ws_disconnect_secs: metrics.ws_disconnect_secs,
            open_directional_count_same_side: metrics.open_directional_count(opp.side),
            daily_directional_trades_same_side: metrics.daily_directional_trades(opp.side),
            consecutive_market_misses: metrics.consecutive_market_misses,
            hourly_loss: snap.hourly.hourly_loss,
            daily_loss: snap.daily.daily_loss,
            daily_budget_remaining: snap.daily.daily_budget_remaining,
            weekly_loss: snap.weekly.weekly_loss,
            daily_pnl: snap.daily.daily_pnl,
            circuit_breaker: snap.circuit_breaker.circuit_breaker,
            manual_halt: snap.circuit_breaker.manual_halt.clone(),
            blacklist: blacklist_gate,
            token_blacklisted,
            api_error_count: metrics.api_error_count,
            api_request_count: metrics.api_request_count,
            drawdown_factor,
            drawdown_action,
            snapshot_at: now,
        }
    }

    fn blacklist_gate(&self, opp: &Opportunity) -> BlacklistGate {
        let bl_result = self
            .blacklist
            .check(&opp.market_id, BlacklistScope::TradingPath);
        match &bl_result {
            BlacklistCheckResult::Clear => BlacklistGate::Clear,
            BlacklistCheckResult::Blocked { reason, scope, .. } => BlacklistGate::Blocked {
                detail: format!("{reason} (scope: {scope})"),
            },
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
