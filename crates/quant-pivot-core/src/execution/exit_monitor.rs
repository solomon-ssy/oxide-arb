//! Exit-monitor decision engine (Phase 05.6).
//!
//! Each filled entry intent owns exactly one position lot (`A0` per-lot ledger).
//! The monitor evaluates a **deterministic priority ladder** over that lot from
//! the intent's **frozen** [`ExitPolicySpec`] plus the current book mark — it
//! never re-reads the (possibly expired/revoked) recommendation for the
//! price/time/trailing/partial ladder. The model-driven dimension
//! (thesis-invalidation now, opportunistic Sell in Phase 6) enters through the
//! [`ExitSignalEvaluator`] seam as a pre-resolved [`ExitSignalVerdict`], so
//! [`decide_exit`] stays a pure, side-effect-free, same-input-same-output
//! function.
//!
//! Priority (父文档 §16.5, fail-closed):
//! 1. kill-switch emergency → policy (`LiquidateAll` submit / `ManualOnly` manual)
//! 2. data stale → manual (never guess a price)
//! 3. market abnormal → manual
//! 4. stop-loss (+ trailing folded into the effective stop)
//! 5. thesis invalidated (model re-inference; auto-execution intents only)
//!    - a `HoldToResolution` lot short-circuits to hold here: the protective
//!      tiers above still fire, but the take-gains / timeout tiers below are skipped.
//! 6. time exit
//! 7. manual review checkpoint (frozen `manual_review_at`)
//! 8. take-profit
//! 9. cumulative scale-out target (each target settles at most once)
//! 10. opportunistic Sell (advisory; Phase 6 model)
//! 11. hold
//!
//! Every non-emergency submit is gated by [`KillSwitchState::allows_auto_exit`]:
//! when auto-exit is frozen (`execution_halted`) a would-be exit is routed to
//! manual review instead of being auto-submitted (父文档 §8).

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::{OrderIntentInfo, PositionInfo},
    enums::{
        common::{OrderType, Side},
        execution::{ExitReason, KillSwitchState},
        quant::ExitSettlementMode,
    },
    runtime_config::{EmergencyExitKind, EmergencyExitPolicy},
    types::{
        ExitPolicySpec, ExitReinferenceObservation, PendingScaleOut, Price, ScaleOutState, Shares,
        TokenId,
    },
};
use rust_decimal::Decimal;

/// Lock-free exit-monitor health published by the worker and read by admission `#20`.
///
/// A new entry is only admitted while the worker has scanned within its healthy
/// window — otherwise a position could be opened with no live exit monitoring
/// (fail-closed).
#[derive(Debug, Clone)]
pub struct ExitMonitorHealth {
    /// Last successful scan time; `None` until the worker's first pass.
    pub last_scan_at: Option<DateTime<Utc>>,
    /// Healthy window in seconds (the worker publishes `2 × monitor_secs`).
    pub healthy_window_secs: u64,
}

/// Lock-free handle for the exit-monitor health hot read.
#[derive(Debug, Clone)]
pub struct ExitMonitorHealthHandle {
    inner: Arc<ArcSwap<ExitMonitorHealth>>,
}

impl ExitMonitorHealthHandle {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(ExitMonitorHealth {
                last_scan_at: None,
                healthy_window_secs: 0,
            })),
        }
    }

    /// Publish a fresh heartbeat after a successful scan pass.
    pub fn publish(&self, scanned_at: DateTime<Utc>, healthy_window_secs: u64) {
        self.inner.store(Arc::new(ExitMonitorHealth {
            last_scan_at: Some(scanned_at),
            healthy_window_secs,
        }));
    }

    /// Whether the monitor is healthy at `now` (spawned + scanned within window).
    #[must_use]
    pub fn is_ready(&self, now: DateTime<Utc>) -> bool {
        let health = self.inner.load();
        match health.last_scan_at {
            Some(last) => {
                let window = Duration::seconds(
                    i64::try_from(health.healthy_window_secs).unwrap_or(i64::MAX),
                );
                now <= last + window
            }
            None => false,
        }
    }
}

impl Default for ExitMonitorHealthHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Verdict produced by the model-driven [`ExitSignalEvaluator`] seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitSignalVerdict {
    /// The entry thesis no longer holds — force a full exit (high priority).
    ThesisInvalidated { detail: String },
    /// The thesis still holds, but the model ranks selling now as advantageous
    /// (advisory, lowest non-hold tier). `target_cumulative_exit_pct` is the
    /// desired cumulative fraction of the shared entry-filled denominator; the
    /// ladder sells only the incremental delta beyond all settled scale-outs.
    OpportunisticSell {
        target_cumulative_exit_pct: Decimal,
        detail: String,
    },
    /// The thesis holds and there is no opportunistic edge.
    Holds,
    /// The signal could not be evaluated (missing features / model / stale data).
    /// Fail-safe: never forces an exit — stop-loss / time / trailing still guard
    /// the downside, and stale data is handled by its own higher-priority rule.
    Indeterminate { detail: String },
}

/// Signal verdict plus the latest governed reinference observation, if one ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitSignalEvaluation {
    pub verdict: ExitSignalVerdict,
    pub reinference: Option<ExitReinferenceObservation>,
}

impl ExitSignalEvaluation {
    #[must_use]
    pub const fn verdict(verdict: ExitSignalVerdict) -> Self {
        Self {
            verdict,
            reinference: None,
        }
    }
}

/// Context handed to the model-driven exit-signal evaluator.
#[derive(Clone, Copy)]
pub struct ExitSignalContext<'a> {
    /// The governed intent (carries the frozen entry thesis baselines).
    pub intent: &'a OrderIntentInfo,
    /// The open position lot being evaluated.
    pub lot: &'a PositionInfo,
    /// Current sell-side mark (best bid), when the book is readable.
    pub mark_price: Option<Price>,
    /// Evaluation time.
    pub now: DateTime<Utc>,
}

/// Model-driven exit-signal seam.
///
/// Unifies **thesis-invalidation** (implemented this phase via side-effect-free
/// re-inference) and **opportunistic Sell** (Phase 6 Sell ranking model) behind
/// one contract so the priority ladder is wired for both today.
#[async_trait]
pub trait ExitSignalEvaluator: Send + Sync {
    /// Evaluate the model-driven exit signal for one lot. Must be side-effect
    /// free and fail-safe (return [`ExitSignalVerdict::Indeterminate`] rather
    /// than erroring when it cannot evaluate).
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalEvaluation;
}

/// Composes thesis-invalidation re-inference (Phase 06.0) with opportunistic
/// Sell scoring (Phase 06.1) behind the single [`ExitSignalEvaluator`] seam.
///
/// Invalidation is strictly prior: re-inference runs first, and only when it
/// **holds** does opportunistic scoring run. A `ThesisInvalidated` short-circuits
/// (the ladder forces a full exit), and an `Indeterminate` also short-circuits —
/// this includes the case where re-inference is disabled or the intent is not
/// auto-execution — so opportunistic advisory selling is gated on thesis
/// validity being checkable and holding. Fail-safe throughout: neither path can
/// error, and any inability to evaluate resolves to Hold.
pub struct CompositeExitSignalEvaluator {
    reinference: Arc<dyn ExitSignalEvaluator>,
    opportunistic: Arc<dyn ExitSignalEvaluator>,
}

impl CompositeExitSignalEvaluator {
    /// Compose the invalidation-first re-inference evaluator with the
    /// opportunistic Sell evaluator.
    #[must_use]
    pub fn new(
        reinference: Arc<dyn ExitSignalEvaluator>,
        opportunistic: Arc<dyn ExitSignalEvaluator>,
    ) -> Self {
        Self {
            reinference,
            opportunistic,
        }
    }
}

#[async_trait]
impl ExitSignalEvaluator for CompositeExitSignalEvaluator {
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalEvaluation {
        // Only when the thesis still holds does opportunistic scoring get a say;
        // `ThesisInvalidated` (forced exit) and `Indeterminate` (thesis validity
        // unknown / re-inference disabled) short-circuit unchanged — as does a
        // stray `OpportunisticSell`, which re-inference never emits.
        let mut reinference = self.reinference.evaluate(ctx).await;
        if reinference.verdict == ExitSignalVerdict::Holds {
            let opportunistic = self.opportunistic.evaluate(ctx).await;
            reinference.verdict = opportunistic.verdict;
        }
        reinference
    }
}

/// The concrete sell order an exit decision will submit to the venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitOrderSpec {
    /// Outcome token to sell.
    pub token_id: TokenId,
    /// Always [`Side::Sell`] for an exit.
    pub side: Side,
    /// Time-in-force (FOK for emergency liquidation, GTC otherwise).
    pub order_type: OrderType,
    /// Sell limit price (crosses the bid for prompt exits; the target for TP).
    pub limit_price: Price,
    /// Share quantity to sell (full remaining, or a partial node fraction).
    pub shares: Shares,
}

/// The deterministic exit decision for one lot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitDecision {
    /// Keep monitoring; the service advances `next_check_at` and persists peak.
    Hold,
    /// Submit a venue sell order and advance the exit FSM.
    SubmitExitOrder {
        reason: ExitReason,
        order: ExitOrderSpec,
        /// Stable deterministic target id; opportunistic cumulative targets use
        /// no id and advance through the same cumulative filled-share state.
        pending_scale_out: Option<PendingScaleOut>,
    },
    /// Route to manual operator handling (fail-closed: no auto submission).
    RequireManualReview { reason: ExitReason },
}

/// Pre-resolved, deterministic inputs for one exit evaluation.
///
/// All non-deterministic work (book read, freshness/abnormality classification,
/// trailing-peak update, kill-switch snapshot, model re-inference) is resolved
/// by the caller so [`decide_exit`] is a pure function.
pub struct ExitMonitorInput {
    /// The open position lot (price truth: `avg_price`, `shares`, `opened_at`).
    pub lot: PositionInfo,
    /// The intent's frozen exit contract.
    pub exit_policy: ExitPolicySpec,
    /// Current sell-side mark (best bid); `None` when the book is unreadable.
    pub mark_price: Option<Price>,
    /// Whether the book is fresh enough to act on (else → manual).
    pub book_fresh: bool,
    /// Whether the market looks abnormal (crossed / empty / non-tradable).
    pub market_abnormal: bool,
    /// Operational kill-switch state snapshot.
    pub kill_switch: KillSwitchState,
    /// Emergency-exit policy applied when the kill-switch is in emergency halt.
    pub emergency_policy: EmergencyExitPolicy,
    /// Up-to-date trailing peak mark (caller folds in the current mark).
    pub peak_mark_price: Option<Price>,
    /// Pre-resolved model-driven exit signal (`Holds` when not due/evaluated).
    pub signal: ExitSignalVerdict,
    /// Unified cumulative state for deterministic and opportunistic scale-outs.
    pub scale_out_state: ScaleOutState,
    /// Minimum incremental fraction (of the shared entry-filled denominator) worth
    /// submitting; smaller deltas hold to avoid dust exits / fee churn.
    pub min_opportunistic_clip_pct: Decimal,
    /// Evaluation time.
    pub now: DateTime<Utc>,
}

/// Basis-point denominator for percentage conversions.
fn bps_fraction(bps: Decimal) -> Decimal {
    bps / Decimal::from(10_000)
}

/// The take-profit target: the earliest (lowest) of the configured absolute and
/// percentage targets. `None` when no take-profit is configured.
fn take_profit_target(policy: &ExitPolicySpec, avg_price: Price) -> Option<Price> {
    let mut target = policy.take_profit_price;
    if let Some(pct) = policy.take_profit_pct {
        let from_pct = avg_price * (Decimal::ONE + pct);
        target = Some(target.map_or(from_pct, |t| t.min(from_pct)));
    }
    target
}

/// Whether the lot's time-based exit is due (absolute deadline or max hold).
fn time_exit_due(policy: &ExitPolicySpec, opened_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    if policy.time_exit_at.is_some_and(|at| now >= at) {
        return true;
    }
    policy.max_hold_secs.is_some_and(|secs| {
        i64::try_from(secs)
            .ok()
            .and_then(|secs| opened_at.checked_add_signed(Duration::seconds(secs)))
            .is_some_and(|deadline| now >= deadline)
    })
}

/// Build a sell order spec for `shares` at `limit`, with `order_type` urgency.
fn sell_order(
    token_id: &TokenId,
    shares: Shares,
    limit: Price,
    order_type: OrderType,
) -> ExitOrderSpec {
    ExitOrderSpec {
        token_id: token_id.clone(),
        side: Side::Sell,
        order_type,
        limit_price: limit,
        shares,
    }
}

/// Resolve a submit-or-manual decision for a triggered non-emergency exit:
/// gated by `allows_auto_exit` (manual when frozen) and by a readable mark
/// (manual when the sell cannot be priced — never guess).
fn submit_or_manual(
    input: &ExitMonitorInput,
    reason: ExitReason,
    shares: Shares,
    limit: Option<Price>,
) -> ExitDecision {
    if !input.kill_switch.allows_auto_exit() {
        return ExitDecision::RequireManualReview { reason };
    }
    match limit {
        Some(limit) if shares.is_positive() => ExitDecision::SubmitExitOrder {
            reason,
            order: sell_order(&input.lot.token_id, shares, limit, OrderType::Gtc),
            pending_scale_out: None,
        },
        _ => ExitDecision::RequireManualReview { reason },
    }
}

/// The deterministic exit priority ladder (父文档 §16.5). Pure: same input ⇒
/// same decision (modulo `input.now` / `mark_price`).
#[must_use]
pub fn decide_exit(input: &ExitMonitorInput) -> ExitDecision {
    let lot = &input.lot;
    let policy = &input.exit_policy;
    let shares = lot.shares;

    // 1. Kill-switch emergency — overrides the auto-exit gate, applies policy.
    if input.kill_switch.requires_emergency_exit() {
        return match input.emergency_policy.kind {
            EmergencyExitKind::ManualOnly => ExitDecision::RequireManualReview {
                reason: ExitReason::KillSwitchEmergency,
            },
            EmergencyExitKind::LiquidateAll => match input.mark_price {
                // Liquidate within the configured slippage as an immediate FOK.
                Some(mark) if shares.is_positive() => {
                    let slip = bps_fraction(Decimal::from(input.emergency_policy.max_slippage_bps));
                    let limit = mark * (Decimal::ONE - slip);
                    ExitDecision::SubmitExitOrder {
                        reason: ExitReason::KillSwitchEmergency,
                        order: sell_order(&lot.token_id, shares, limit, OrderType::Fok),
                        pending_scale_out: None,
                    }
                }
                // Cannot price the liquidation → fail closed to manual.
                _ => ExitDecision::RequireManualReview {
                    reason: ExitReason::KillSwitchEmergency,
                },
            },
        };
    }

    // 2. Data stale beyond threshold → manual (never guess a price).
    if !input.book_fresh {
        return ExitDecision::RequireManualReview {
            reason: ExitReason::DataStale,
        };
    }

    // 3. Market abnormal (crossed / empty / non-tradable) → manual.
    if input.market_abnormal {
        return ExitDecision::RequireManualReview {
            reason: ExitReason::MarketAbnormal,
        };
    }

    // 4. Stop-loss (with trailing folded into the effective stop).
    if let (Some(mark), Some(stop)) = (
        input.mark_price,
        policy.effective_stop(lot.avg_price, input.peak_mark_price),
    ) && mark <= stop
    {
        return submit_or_manual(input, ExitReason::StopLoss, shares, Some(mark));
    }

    // 5. Thesis invalidated (model re-inference) — forced full exit (protective:
    //    fires even under hold-to-resolution).
    if let ExitSignalVerdict::ThesisInvalidated { .. } = &input.signal {
        return submit_or_manual(
            input,
            ExitReason::SignalInvalidated,
            shares,
            input.mark_price,
        );
    }

    // A `HoldToResolution` lot is held to settlement: it skips the take-gains /
    // time-out tiers below (only the protective exits above and the emergency
    // override act). Redemption is handled at settlement when RedeemPolicy::Auto
    // is enabled for the frozen exit policy.
    if policy.settlement_mode == ExitSettlementMode::HoldToResolution {
        return ExitDecision::Hold;
    }

    // 6. Time exit.
    if time_exit_due(policy, lot.opened_at, input.now) {
        return submit_or_manual(input, ExitReason::TimeExit, shares, input.mark_price);
    }

    // 7. Manual-review checkpoint (frozen absolute deadline → operator).
    if policy.manual_review_at.is_some_and(|at| input.now >= at) {
        return ExitDecision::RequireManualReview {
            reason: ExitReason::Manual,
        };
    }

    // 8. Take-profit.
    if let (Some(mark), Some(target)) =
        (input.mark_price, take_profit_target(policy, lot.avg_price))
        && mark >= target
    {
        // Sell at the target (the resting bid is at or above it).
        return submit_or_manual(input, ExitReason::TakeProfit, shares, Some(target));
    }

    // 9. Deterministic cumulative scale-out target.
    if let Some((target_shares, pending)) = next_scale_out_shares(input) {
        return submit_or_manual_with_target(
            input,
            ExitReason::PartialExit,
            target_shares,
            input.mark_price,
            pending,
        );
    }

    // 10. Opportunistic Sell (advisory; Phase 6 Sell scorer). Idempotent
    //     scale-out: the verdict is a *target cumulative* fraction of a frozen
    //     denominator, so repeated ticks at the same target only ever sell the
    //     incremental delta — never re-firing the whole fraction each tick.
    if let ExitSignalVerdict::OpportunisticSell {
        target_cumulative_exit_pct,
        ..
    } = &input.signal
    {
        if let Some(delta) = opportunistic_delta(input, *target_cumulative_exit_pct) {
            return submit_or_manual_opportunistic(input, delta, *target_cumulative_exit_pct);
        }
        return ExitDecision::Hold;
    }

    // 11. Otherwise hold and keep monitoring.
    ExitDecision::Hold
}

/// The incremental opportunistic sell quantity and the frozen denominator, or
/// `None` when nothing is due. The denominator is frozen at entry fill
/// (`record_submission_result`) to venue-confirmed shares; without it the ladder
/// fail-closes to hold. The delta is capped at the shares still open, and held
/// when below the min clip.
fn opportunistic_delta(input: &ExitMonitorInput, target_pct: Decimal) -> Option<Shares> {
    let state = &input.scale_out_state;
    let denominator = state.denominator_shares?;
    if !denominator.is_positive() {
        return None;
    }
    let target = target_pct.clamp(Decimal::ZERO, Decimal::ONE);
    let delta = state.delta_to_target(target).inner();
    let min_clip = denominator.inner() * input.min_opportunistic_clip_pct;
    if delta <= Decimal::ZERO || delta < min_clip {
        return None;
    }
    // Never sell more than remains open on the lot.
    let sell = delta.min(input.lot.shares.inner());
    if sell <= Decimal::ZERO {
        return None;
    }
    Some(Shares::new(sell))
}

/// Submit-or-manual for an opportunistic exit, carrying the frozen denominator
/// so the dispatcher can persist the cumulative-sold advance on fill.
fn submit_or_manual_opportunistic(
    input: &ExitMonitorInput,
    shares: Shares,
    target_cumulative_exit_pct: Decimal,
) -> ExitDecision {
    if !input.kill_switch.allows_auto_exit() {
        return ExitDecision::RequireManualReview {
            reason: ExitReason::Opportunistic,
        };
    }
    match input.mark_price {
        Some(limit) if shares.is_positive() => ExitDecision::SubmitExitOrder {
            reason: ExitReason::Opportunistic,
            order: sell_order(&input.lot.token_id, shares, limit, OrderType::Gtc),
            pending_scale_out: Some(PendingScaleOut {
                target_id: None,
                target_cumulative_exit_pct,
            }),
        },
        _ => ExitDecision::RequireManualReview {
            reason: ExitReason::Opportunistic,
        },
    }
}

/// Like [`submit_or_manual`] but carries the deterministic target id.
fn submit_or_manual_with_target(
    input: &ExitMonitorInput,
    reason: ExitReason,
    shares: Shares,
    limit: Option<Price>,
    pending_scale_out: PendingScaleOut,
) -> ExitDecision {
    if !input.kill_switch.allows_auto_exit() {
        return ExitDecision::RequireManualReview { reason };
    }
    match limit {
        Some(limit) if shares.is_positive() => ExitDecision::SubmitExitOrder {
            reason,
            order: sell_order(&input.lot.token_id, shares, limit, OrderType::Gtc),
            pending_scale_out: Some(pending_scale_out),
        },
        _ => ExitDecision::RequireManualReview { reason },
    }
}

/// The next due scale-out target's share quantity and stable id, if any target is
/// currently active, not yet settled, and the mark satisfies its trigger.
fn next_scale_out_shares(input: &ExitMonitorInput) -> Option<(Shares, PendingScaleOut)> {
    let projection = input.exit_policy.next_scale_out(
        &input.scale_out_state,
        input.lot.shares,
        input.mark_price,
        input.now,
    )?;
    Some((
        projection.delta_shares,
        PendingScaleOut {
            target_id: Some(projection.target_id),
            target_cumulative_exit_pct: projection.target_cumulative_exit_pct,
        },
    ))
}
