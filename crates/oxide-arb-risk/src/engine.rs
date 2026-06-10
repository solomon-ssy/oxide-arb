//! `RiskEngine` — single entry point for all risk operations.
//!
//! Owns all risk subsystems and orchestrates them through a unified API.
//! Thread-safe: internal state is protected by `parking_lot::RwLock`.
//!
//! Mutation methods are `async` — they perform in-memory updates under sync
//! locks, then release locks and persist + audit via `RiskPersistence`.
//! If persistence fails, the engine halts (fail-closed).

use crate::{
    accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting},
    audit::{RiskAuditEvent, RiskDecisionTrace},
    audit_sink::{AuditEnqueueResult, AuditSink},
    blacklist::BlacklistManager,
    circuit_breaker::CircuitBreaker,
    clock::Clock,
    context::{CircuitBreakerGate, ManualHaltGate, PreTradeContext},
    pipeline::{StaticRiskPipeline, build_default_pipeline},
    position::PotentialLossLedger,
    reconciliation::LedgerReconciler,
    sizing::{DrawdownGuard, MultiConstraintSizer},
    snapshot::{
        CircuitBreakerSnapshot, DailyAccountingSnapshot, DrawdownSnapshot,
        HourlyAccountingSnapshot, RiskSnapshot, WeeklyAccountingSnapshot,
    },
    traits::{FillClaim, PotentialLossStore, RiskMetrics, RiskMetricsSnapshot, RiskPersistence},
    types::{
        AtomicStateVersion, BreakerState, ExecutionRiskEvent, PostTradeReport,
        ReconciliationReport, ReportMode, RiskDecision, StateVersion,
    },
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::OxideResult;
use oxide_arb_models::types::MarketId as ModelsMarketId;
use oxide_arb_models::{
    domain::{
        BlacklistInfo, CoreEvent, CoreEventPublisher,
        blacklist::UpsertBlacklistEntry,
        control_factor::FactorDecisionContext,
        opportunity::Opportunity,
        potential_loss::{NewPotentialLoss, PotentialLossInfo},
        risk,
        risk::{
            FillCommit, NewEmergencySnapshot, NewRiskAuditEvent, ProbabilityInput, RiskEngineState,
            UpsertRiskEngineState,
        },
        settlement::MarketSettlementInput,
        trade::PostTradeInput,
    },
    enums::{
        common::{LedgerStatus, TradeBusinessOutcome},
        risk::{
            BlacklistReason, BreakerStateName, CircuitBreakerLevel, ReconciliationStatus,
            TradeAccountingPhase, WindowType,
        },
    },
    runtime_config::RiskConfig,
    types::{LedgerId, MarketId, OpportunityId, Usd},
};
use parking_lot::RwLock;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Atomically published derived parameter set: the risk configuration plus
/// every subsystem deterministically rebuilt from it (static check pipeline,
/// sizer, reconciler).
///
/// Hot-path readers load this once per decision, so a concurrent
/// [`RiskEngine::reload`] can never expose a new pipeline alongside an
/// old config (or vice versa) — the snapshot swaps as one unit.
pub(crate) struct EngineParams {
    pub(crate) config: Arc<RiskConfig>,
    pub(crate) pipeline: StaticRiskPipeline,
    pub(crate) sizer: MultiConstraintSizer,
    pub(crate) reconciler: Arc<LedgerReconciler>,
}

impl EngineParams {
    pub(crate) fn from_config(config: RiskConfig) -> Self {
        Self {
            pipeline: build_default_pipeline(&config),
            sizer: MultiConstraintSizer::new(&config),
            reconciler: Arc::new(LedgerReconciler::new(config.reconciliation_tolerance_usd)),
            config: Arc::new(config),
        }
    }
}

pub struct RiskEngine<P = Arc<dyn RiskPersistence>>
where
    P: RiskPersistence + Send + Sync + 'static,
{
    pub(crate) circuit_breaker: RwLock<CircuitBreaker>,
    pub(crate) daily: RwLock<DailyAccounting>,
    pub(crate) weekly: RwLock<WeeklyAccounting>,
    pub(crate) hourly: RwLock<HourlyAccounting>,
    pub(crate) potential_loss: RwLock<PotentialLossLedger>,
    /// Lifetime cumulative realized `PnL`, on the same accounting basis as
    /// [`DailyAccounting::daily_pnl`]. Write-only telemetry — never consulted by
    /// any pre-trade gate; surfaced through the snapshot and the `pnl.update`
    /// event so the dashboard's "total" agrees with the persisted state.
    pub(crate) lifetime_realized: RwLock<Usd>,
    /// Config + config-derived subsystems, swapped as one unit on reload.
    pub(crate) params: ArcSwap<EngineParams>,
    pub(crate) risk_snapshot: ArcSwap<RiskSnapshot>,
    pub(crate) blacklist: BlacklistManager,
    pub(crate) drawdown: RwLock<DrawdownGuard>,
    pub(crate) persistence: P,
    pub(crate) potential_loss_store: Arc<dyn PotentialLossStore>,
    pub(crate) audit_sink: Option<Arc<dyn AuditSink>>,
    pub(crate) event_publisher: Option<CoreEventPublisher>,
    pub(crate) state_version: AtomicStateVersion,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) last_emergency: RwLock<Option<(DateTime<Utc>, String)>>,
}

struct MutableRiskState {
    circuit_breaker: CircuitBreaker,
    daily: DailyAccounting,
    weekly: WeeklyAccounting,
    hourly: HourlyAccounting,
    potential_loss: PotentialLossLedger,
    drawdown: DrawdownGuard,
    state_version: StateVersion,
}

/// Default engine type — dynamic persistence via [`Arc<dyn RiskPersistence>`].
pub type DynRiskEngine = RiskEngine<Arc<dyn RiskPersistence>>;

impl<P> RiskEngine<P>
where
    P: RiskPersistence + Send + Sync + 'static,
{
    // ── Pre-trade (sync hot path + non-blocking audit) ───────────────

    /// Evaluate all pre-trade risk checks and compute sizing.
    ///
    /// Produces an immutable `RiskDecision` and enqueues an audit event
    /// (`TradeAllowed` or `TradeDenied`) when an [`AuditSink`] is configured.
    /// Audit enqueue failures are best-effort and never halt the engine.
    #[must_use]
    #[inline]
    pub fn pre_trade_check_core<M: RiskMetrics>(
        &self,
        opp: &Opportunity,
        probability: &ProbabilityInput,
        metrics: &M,
        factor_context: Option<&FactorDecisionContext>,
        mode: ReportMode,
    ) -> RiskDecision {
        let eval_start = Instant::now();
        let now = self.clock.now();
        let version = self.state_version.load();
        let snap = self.risk_snapshot.load();
        // Single load: config, pipeline, and sizer come from one consistent
        // snapshot even if a reload lands mid-decision.
        let params = self.params.load();
        let config = &params.config;
        let pipeline = &params.pipeline;

        let phase1_ctx = PreTradeContext {
            opportunity: opp,
            probability: *probability,
            snap: &snap,
            metrics: RiskMetricsSnapshot::zeroed(),
            factor_context,
            now,
        };
        let metrics_split = pipeline.metrics_split_index();
        let mut gate_report = pipeline.evaluate_range(&phase1_ctx, mode, 0, metrics_split);

        let ctx = if gate_report.has_failed_hard_gate && mode == ReportMode::ShortCircuit {
            phase1_ctx
        } else {
            let metrics_snap = metrics.snapshot_for(&opp.market_id);
            let full_ctx = PreTradeContext {
                metrics: metrics_snap,
                ..phase1_ctx
            };
            let phase2 = pipeline.evaluate_range(&full_ctx, mode, metrics_split, pipeline.len());
            gate_report.merge(phase2);
            full_ctx
        };

        let (allowed, denial_reason, recommended_size, sizing_breakdown) =
            if gate_report.has_failed_hard_gate {
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
                    ctx.equity(),
                    Usd::new(config.reserve_balance_usd),
                    ctx.reserved_usd(),
                    ctx.total_potential_loss(),
                    Usd::new(config.bankroll_usd),
                );

                let drawdown_factor = ctx.drawdown_factor();
                let size_result = params.sizer.size(&ctx, bankroll, drawdown_factor);

                let allowed = size_result.bet_usd > Usd::ZERO
                    && size_result.bet_usd >= Usd::new(config.min_trade_usd);

                let denial_reason = if allowed {
                    None
                } else {
                    Some(format!(
                        "sizing returned {} (binding: {}, min_trade: {})",
                        size_result.bet_usd, size_result.binding_constraint, config.min_trade_usd,
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
            drawdown_factor: ctx.drawdown_factor(),
            evaluated_at: now,
            state_version: version,
            trace,
        };

        self.enqueue_pre_trade_audit(allowed, &decision.trace, opp.opportunity_id.clone(), mode);
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
    ) -> OxideResult<Option<PostTradeReport>> {
        match phase {
            TradeAccountingPhase::Fill => {
                let fill_claim = match self
                    .persistence
                    .begin_fill(&trade.trade_id, self.clock.now())
                    .await
                {
                    Ok(FillClaim::AlreadyApplied) => return Ok(None),
                    Ok(FillClaim::Claimed(claim)) => claim,
                    Err(e) => {
                        self.halt_internal(format!("risk fill marker claim failed: {e}"));
                        return Err(e);
                    }
                };
                let previous_breaker_state = self.circuit_breaker.read().state().to_name();
                let fill_potential_loss = Self::fill_potential_loss_entry(trade);
                let pre_commit_state = self.capture_mutable_state();
                metrics.record_trade_outcome(trade.side, &trade.market_id, trade.is_miss());
                let (report, audit_event, auto_bl_entry) =
                    self.apply_trade_result(phase, trade, metrics);
                let commit = FillCommit {
                    trade_id: trade.trade_id.clone(),
                    potential_loss: fill_potential_loss,
                    state: UpsertRiskEngineState::from(&report.snapshot),
                    audit: audit_event,
                };
                if let Err(e) = fill_claim.commit(commit).await {
                    self.restore_mutable_state(pre_commit_state);
                    self.halt_internal(format!("risk fill commit failed: {e}"));
                    return Err(e);
                }
                self.persist_post_trade_followups(
                    &report,
                    auto_bl_entry,
                    previous_breaker_state,
                    metrics,
                )
                .await?;
                Ok(Some(report))
            }
            TradeAccountingPhase::Settlement => {
                self.persist_settlement_potential_loss(trade).await?;
                let previous_breaker_state = self.circuit_breaker.read().state().to_name();
                let (report, audit_event, auto_bl_entry) =
                    self.apply_trade_result(phase, trade, metrics);
                self.persist_and_audit(&report.snapshot, audit_event)
                    .await?;
                self.persist_post_trade_followups(
                    &report,
                    auto_bl_entry,
                    previous_breaker_state,
                    metrics,
                )
                .await?;
                Ok(Some(report))
            }
        }
    }

    async fn persist_post_trade_followups(
        &self,
        report: &PostTradeReport,
        auto_bl_entry: Option<BlacklistInfo>,
        previous_breaker_state: BreakerStateName,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
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
            // Surface the trip to the real-time bus (best-effort, non-blocking).
            if let Some(publisher) = &self.event_publisher {
                publisher.publish(CoreEvent::CircuitBreakerTripped {
                    level: tripped_level.as_u8(),
                    reason,
                });
            }
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

        // Surface the post-commit realized-PnL change to the real-time bus.
        // Single emission point for both Fill and Settlement phases (both reach
        // here after their state is durably persisted), so the dashboard's
        // daily/total always reflect committed accounting. Best-effort,
        // non-blocking — never affects the trading path.
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(CoreEvent::PnlUpdate {
                daily: self.daily.read().daily_pnl(),
                total: *self.lifetime_realized.read(),
            });
        }

        Ok(())
    }

    pub async fn on_market_settled(
        &self,
        input: &MarketSettlementInput,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<Option<PostTradeReport>> {
        let trade = PostTradeInput {
            trade_id: input.trade_id.clone(),
            market_id: input.market_id.clone(),
            token_id: input.token_id.clone(),
            side: input.side,
            outcome: TradeBusinessOutcome::Success,
            cost_usd: input.cost_usd,
            fee_usd: input.fee_usd,
            net_profit_usd: Some(input.realized_pnl_usd),
            shares: input.shares,
            entry_price: input.entry_price,
        };

        self.on_trade_result(TradeAccountingPhase::Settlement, &trade, metrics)
            .await
    }

    fn fill_potential_loss_entry(trade: &PostTradeInput) -> Option<NewPotentialLoss> {
        if !trade.is_success() {
            return None;
        }

        Some(NewPotentialLoss {
            ledger_id: LedgerId::new(trade.trade_id.as_uuid()),
            market_id: trade.market_id.clone(),
            token_id: trade.token_id.clone(),
            shares: trade.shares,
            entry_price: trade.entry_price,
            max_loss_usd: trade.cost_usd + trade.fee_usd,
        })
    }

    async fn persist_settlement_potential_loss(&self, trade: &PostTradeInput) -> OxideResult<()> {
        if !trade.is_success() {
            return Ok(());
        }

        let ledger_id = LedgerId::new(trade.trade_id.as_uuid());
        if let Err(e) = self.potential_loss_store.resolve(&ledger_id).await {
            self.halt_internal(format!("potential loss resolve failed: {e}"));
            return Err(e);
        }

        Ok(())
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
                ledger_id: LedgerId::new(trade.trade_id.as_uuid()),
                market_id: trade.market_id.clone(),
                token_id: trade.token_id.clone(),
                shares: trade.shares,
                entry_price: trade.entry_price,
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
        // Lifetime realized PnL tracks the same delta booked into the daily
        // window (which resets at midnight); lifetime never resets.
        *self.lifetime_realized.write() += net_profit;
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

        let ledger_id = LedgerId::new(trade.trade_id.as_uuid());
        self.potential_loss.write().resolve(&ledger_id);

        (daily_rolled, weekly_rolled, hourly_rolled)
    }

    /// Check all loss caps and halt/trip the breaker at the highest applicable level.
    ///
    /// - Daily / Weekly / Single-loss caps → `halt()` (L3 — requires operator ack)
    /// - Hourly cap → `trip()` (L2 — auto-recovery via `HalfOpen`)
    fn check_loss_caps(&self) -> Option<CircuitBreakerLevel> {
        let mut highest: Option<CircuitBreakerLevel> = None;
        let params = self.params.load();
        let config = &params.config;

        let daily_loss = self.daily.read().daily_loss();
        if daily_loss.inner() >= config.max_daily_loss_usd {
            let reason = format!("daily loss cap breached: {daily_loss}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            escalate(&mut highest, CircuitBreakerLevel::Daily);
        }

        let weekly_loss = self.weekly.read().weekly_loss();
        if weekly_loss.inner() >= config.max_weekly_loss_usd {
            let reason = format!("weekly loss cap breached: {weekly_loss}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::System, reason);
            escalate(&mut highest, CircuitBreakerLevel::System);
        }

        let max_single = self.daily.read().stats().max_single_loss;
        if max_single.inner() >= config.max_single_loss_usd {
            let reason = format!("single loss cap breached: {max_single}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            escalate(&mut highest, CircuitBreakerLevel::Daily);
        }

        let daily_fee = self.daily.read().fees();
        if daily_fee.inner() >= config.max_daily_fee_spend_usd {
            let reason = format!("daily fee spend cap breached: {daily_fee}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            escalate(&mut highest, CircuitBreakerLevel::Daily);
        }

        let hourly_loss = self.hourly.read().hourly_loss();
        if hourly_loss.inner() >= config.max_hourly_loss_usd {
            let reason = format!("hourly loss cap breached: {hourly_loss}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            escalate(&mut highest, CircuitBreakerLevel::Session);
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
        let params = self.params.load();
        let config = &params.config;

        {
            let mut cb = self.circuit_breaker.write();
            if cb.is_probe_mode() {
                cb.on_trade_result(trade.is_success());
            }
        }

        self.drawdown.write().update_equity(metrics.equity());

        let miss_count = metrics.consecutive_market_misses(&trade.market_id);
        if miss_count >= config.max_consecutive_misses {
            let reason = format!(
                "consecutive misses: {miss_count} >= {}",
                config.max_consecutive_misses
            );
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            tripped = Some(CircuitBreakerLevel::Session);
        }

        let daily_fee = self.daily.read().fees();
        if daily_fee.inner() >= config.max_daily_fee_spend_usd {
            let reason = format!("daily fee spend cap breached: {daily_fee}");
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::Daily, reason);
            tripped = Some(CircuitBreakerLevel::Daily);
        }

        let hourly_fee = self.hourly.read().fees();
        if hourly_fee.inner() >= config.max_hourly_fee_spend_usd {
            let reason = format!("hourly fee spend cap breached: {hourly_fee}");
            self.circuit_breaker
                .write()
                .trip(CircuitBreakerLevel::Session, reason);
            escalate(&mut tripped, CircuitBreakerLevel::Session);
        }

        let req_count = metrics.api_request_count();
        if req_count > 0 {
            let error_rate = rust_decimal::Decimal::from(metrics.api_error_count())
                / rust_decimal::Decimal::from(req_count);
            if error_rate >= config.api_error_rate_threshold {
                let reason = format!(
                    "API error rate {error_rate:.2} >= threshold {}",
                    config.api_error_rate_threshold
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
        let pre_state = self.circuit_breaker.read().state().to_name();
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
        let potential_loss_escalation_secs =
            self.params.load().config.potential_loss_escalation_secs;
        let stale_potential_loss = self
            .potential_loss_store
            .find_stale(Duration::from_secs(potential_loss_escalation_secs))
            .await?;
        if !stale_potential_loss.is_empty() {
            let reason = format!(
                "{} stale potential-loss entries exceeded {potential_loss_escalation_secs}s",
                stale_potential_loss.len(),
            );
            self.circuit_breaker
                .write()
                .halt(CircuitBreakerLevel::System, reason.clone());
            self.publish_risk_snapshot();
            self.record_emergency(CircuitBreakerLevel::System, &reason, metrics)
                .await?;
            return Ok(true);
        }

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

        if cb_transitioned {
            let post_state = self.circuit_breaker.read().state().to_name();
            if pre_state == BreakerStateName::Recovered && post_state == BreakerStateName::Closed {
                let audit = RiskAuditEvent::BreakerRecovered {
                    from: BreakerStateName::Recovered,
                };
                let _ = self.persistence.create_audit(audit.into()).await;
            }
        }

        Ok(any_change)
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
        let new_report = risk::NewReconciliationReport {
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
        let snap = self.risk_snapshot.load();
        snap.circuit_breaker.circuit_breaker.allows_trading
            && snap.circuit_breaker.manual_halt.allows_trading()
    }

    #[must_use]
    pub fn load_risk_snapshot(&self) -> Arc<RiskSnapshot> {
        self.risk_snapshot.load_full()
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

    /// Snapshot of the active ledger reconciler (lock-free read).
    #[must_use]
    pub fn reconciler(&self) -> Arc<LedgerReconciler> {
        Arc::clone(&self.params.load().reconciler)
    }

    /// Snapshot of the active risk configuration (lock-free read).
    #[must_use]
    pub fn config(&self) -> Arc<RiskConfig> {
        Arc::clone(&self.params.load().config)
    }

    /// Hot-reload the full risk configuration (runtime-config activation).
    ///
    /// The config and every subsystem derived from it (static check pipeline,
    /// sizer, reconciler) are rebuilt off-path and published as **one**
    /// [`EngineParams`] swap, so the hot path can never observe a new pipeline
    /// paired with an old config. The independently locked subsystems
    /// (circuit-breaker cooldowns, drawdown policy, daily budget, blacklist
    /// thresholds) are updated next, then the lock-free risk snapshot is
    /// republished. Accounting windows, breaker FSM state, potential-loss
    /// ledger, and runtime blacklist entries are all preserved.
    ///
    /// Caller contract (enforced by the activation preflight): exposure
    /// ceilings must not fall below currently reserved capital.
    pub fn reload(&self, config: RiskConfig) {
        let params = EngineParams::from_config(config);
        self.circuit_breaker
            .write()
            .set_config(params.config.circuit_breaker.clone());
        self.drawdown.write().set_params(
            params.config.drawdown.max_drawdown_pct,
            params.config.drawdown.drawdown_reduction_factor,
        );
        self.daily
            .write()
            .set_budget(Usd::new(params.config.daily_budget_usd));
        self.blacklist.reload(&params.config);
        self.params.store(Arc::new(params));
        self.state_version.increment();
        self.publish_risk_snapshot();
        tracing::info!("risk engine configuration reloaded");
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
                &metrics.consecutive_market_misses(&ModelsMarketId::new("_global_")),
            )
            .unwrap_or(0),
            cooldown_multiplier: ToPrimitive::to_i32(&cb.l2_trip_count()).unwrap_or(i32::MAX),
            hwm_equity: dg.hwm(),
            total_realized_pnl: *self.lifetime_realized.read(),
            last_emergency_at: {
                let emergency = self.last_emergency.read();
                emergency.as_ref().map(|(at, _)| *at)
            },
            last_emergency_reason: {
                let emergency = self.last_emergency.read();
                emergency.as_ref().map(|(_, reason)| reason.clone())
            },
            snapshot_at: self.clock.now(),
        }
    }

    // ── Private ─────────────────────────────────────────────────────────

    fn capture_mutable_state(&self) -> MutableRiskState {
        MutableRiskState {
            circuit_breaker: self.circuit_breaker.read().clone(),
            daily: self.daily.read().clone(),
            weekly: self.weekly.read().clone(),
            hourly: self.hourly.read().clone(),
            potential_loss: self.potential_loss.read().clone(),
            drawdown: self.drawdown.read().clone(),
            state_version: self.state_version.load(),
        }
    }

    fn restore_mutable_state(&self, state: MutableRiskState) {
        *self.circuit_breaker.write() = state.circuit_breaker;
        *self.daily.write() = state.daily;
        *self.weekly.write() = state.weekly;
        *self.hourly.write() = state.hourly;
        *self.potential_loss.write() = state.potential_loss;
        *self.drawdown.write() = state.drawdown;
        self.state_version.store(state.state_version);
        self.publish_risk_snapshot();
    }

    async fn record_emergency(
        &self,
        level: CircuitBreakerLevel,
        reason: &str,
        metrics: &dyn RiskMetrics,
    ) -> OxideResult<()> {
        *self.last_emergency.write() = Some((self.clock.now(), reason.to_owned()));
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
        let params = self.params.load();
        let config = &params.config;
        match event {
            ExecutionRiskEvent::HeartbeatFailure => {
                let max_failures = config.heartbeat_max_failures;
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
            } if consecutive >= config.max_consecutive_misses => {
                let reason = format!(
                    "consecutive FOK failures on {market_id}: {consecutive} >= {}",
                    config.max_consecutive_misses
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
        let params = self.params.load();
        let config = &params.config;
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
                daily_fee: daily.fees(),
                daily_pnl: daily.daily_pnl(),
                daily_budget_remaining: daily.budget_remaining(),
            },
            weekly: WeeklyAccountingSnapshot {
                weekly_loss: weekly.weekly_loss(),
            },
            hourly: HourlyAccountingSnapshot {
                hourly_loss: hourly.hourly_loss(),
                hourly_fee: hourly.fees(),
            },
            drawdown: DrawdownSnapshot {
                hwm: dg.hwm(),
                max_drawdown_pct: config.drawdown.max_drawdown_pct,
                reduction_factor: config.drawdown.drawdown_reduction_factor,
            },
            total_potential_loss: pl.total_potential_loss(),
            blacklist: self.blacklist.build_bloom_snapshot(),
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
        mode: ReportMode,
    ) {
        let Some(sink) = &self.audit_sink else {
            return;
        };

        let audit = if allowed && mode == ReportMode::ShortCircuit {
            RiskAuditEvent::TradeAllowedSummary {
                opportunity_id,
                state_version: trace.state_version,
                check_count: trace.check_results.len(),
                total_elapsed_us: trace.total_elapsed_us,
                evaluated_at: trace.evaluated_at,
            }
        } else if allowed {
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
}

#[inline]
fn escalate(current: &mut Option<CircuitBreakerLevel>, new: CircuitBreakerLevel) {
    *current = Some(current.map_or(new, |existing| existing.max(new)));
}

/// Effective bankroll for Kelly sizing: `min(dynamic, config_bankroll)`.
///
/// `dynamic = equity - reserve - reserved - potential_loss`, floored at zero.
#[inline]
fn available_bankroll(
    equity: Usd,
    reserve: Usd,
    reserved_usd: Usd,
    potential_loss: Usd,
    config_bankroll: Usd,
) -> Usd {
    let dynamic = (equity - reserve - reserved_usd - potential_loss).max(Usd::ZERO);
    let capped = Usd::new(config_bankroll.inner().min(dynamic.inner()));
    capped.max(Usd::ZERO)
}
