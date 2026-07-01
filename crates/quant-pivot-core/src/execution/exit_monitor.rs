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
//! 9. partial-exit node (each node fires at most once)
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
        quant::{ExitSettlementMode, ExitTriggerKind},
    },
    runtime_config::{EmergencyExitKind, EmergencyExitPolicy},
    types::{ExitPolicySpec, PartialExitNode, Price, Shares, TokenId},
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
    /// (advisory, lowest non-hold tier). Implemented by the Phase 6 Sell scorer.
    OpportunisticSell { sell_pct: Decimal, detail: String },
    /// The thesis holds and there is no opportunistic edge.
    Holds,
    /// The signal could not be evaluated (missing features / model / stale data).
    /// Fail-safe: never forces an exit — stop-loss / time / trailing still guard
    /// the downside, and stale data is handled by its own higher-priority rule.
    Indeterminate { detail: String },
}

/// Context handed to the model-driven exit-signal evaluator.
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
    async fn evaluate(&self, ctx: ExitSignalContext<'_>) -> ExitSignalVerdict;
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
        /// Set when the trigger is a scaled partial-exit node (for one-shot tracking).
        partial_exit_node_id: Option<String>,
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
    /// Partial-exit node ids already settled on this lot (one-shot nodes).
    pub executed_partial_exit_node_ids: Vec<String>,
    /// Evaluation time.
    pub now: DateTime<Utc>,
}

/// Exit-monitor boundary: produce an [`ExitDecision`] for one open lot.
#[async_trait]
pub trait ExitMonitor: Send + Sync {
    async fn evaluate(&self, input: ExitMonitorInput) -> ExitDecision;
}

/// Production exit monitor — a thin async wrapper that delegates to the pure
/// [`decide_exit`].
///
/// The model-driven signal is resolved upstream (the worker) and carried on
/// [`ExitMonitorInput::signal`], keeping the decision deterministic.
pub struct DefaultExitMonitor;

#[async_trait]
impl ExitMonitor for DefaultExitMonitor {
    async fn evaluate(&self, input: ExitMonitorInput) -> ExitDecision {
        decide_exit(&input)
    }
}

/// Basis-point denominator for percentage conversions.
fn bps_fraction(bps: Decimal) -> Decimal {
    bps / Decimal::from(10_000)
}

/// The effective stop-loss floor: the tightest (highest) of the configured
/// absolute stop, the percentage stop (relative to the lot cost basis), and the
/// trailing stop (relative to the peak mark). `None` when no stop is configured.
fn effective_stop(policy: &ExitPolicySpec, avg_price: Price, peak: Option<Price>) -> Option<Price> {
    let mut stop = policy.stop_loss_price;
    if let Some(pct) = policy.stop_loss_pct {
        let from_pct = avg_price * (Decimal::ONE - pct);
        stop = Some(stop.map_or(from_pct, |s| s.max(from_pct)));
    }
    if let (Some(trailing), Some(peak)) = (&policy.trailing_stop, peak) {
        let armed = trailing
            .activation_price
            .is_none_or(|activation| peak >= activation);
        if armed {
            let trail = peak * (Decimal::ONE - bps_fraction(trailing.trail_bps.inner()));
            stop = Some(stop.map_or(trail, |s| s.max(trail)));
        }
    }
    stop
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
            partial_exit_node_id: None,
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
                        partial_exit_node_id: None,
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
        effective_stop(policy, lot.avg_price, input.peak_mark_price),
    ) {
        if mark <= stop {
            return submit_or_manual(input, ExitReason::StopLoss, shares, Some(mark));
        }
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
    {
        if mark >= target {
            // Sell at the target (the resting bid is at or above it).
            return submit_or_manual(input, ExitReason::TakeProfit, shares, Some(target));
        }
    }

    // 9. Partial-exit node due (each node fires at most once).
    if let Some((node_shares, node_id)) = next_partial_exit_shares(input) {
        return submit_or_manual_with_node(
            input,
            ExitReason::PartialExit,
            node_shares,
            input.mark_price,
            Some(node_id),
        );
    }

    // 10. Opportunistic Sell (advisory; Phase 6 Sell scorer).
    if let ExitSignalVerdict::OpportunisticSell { sell_pct, .. } = &input.signal {
        let node_shares = (shares * *sell_pct).min(shares);
        return submit_or_manual(
            input,
            ExitReason::Opportunistic,
            node_shares,
            input.mark_price,
        );
    }

    // 11. Otherwise hold and keep monitoring.
    ExitDecision::Hold
}

/// Like [`submit_or_manual`] but carries the partial-exit node id when submitting.
fn submit_or_manual_with_node(
    input: &ExitMonitorInput,
    reason: ExitReason,
    shares: Shares,
    limit: Option<Price>,
    partial_exit_node_id: Option<String>,
) -> ExitDecision {
    if !input.kill_switch.allows_auto_exit() {
        return ExitDecision::RequireManualReview { reason };
    }
    match limit {
        Some(limit) if shares.is_positive() => ExitDecision::SubmitExitOrder {
            reason,
            order: sell_order(&input.lot.token_id, shares, limit, OrderType::Gtc),
            partial_exit_node_id,
        },
        _ => ExitDecision::RequireManualReview { reason },
    }
}

/// The next due partial-exit node's share quantity and stable id, if any node is
/// currently active, not yet settled, and the mark satisfies its trigger.
fn next_partial_exit_shares(input: &ExitMonitorInput) -> Option<(Shares, String)> {
    let lot = &input.lot;
    let mark = input.mark_price?;
    for node in &input.exit_policy.partial_exit_nodes {
        if input.executed_partial_exit_node_ids.contains(&node.node_id) {
            continue;
        }
        let active = node.valid_after.is_none_or(|after| input.now >= after)
            && node.valid_until.is_none_or(|until| input.now <= until);
        if !active {
            continue;
        }
        let min_ok = node.min_price.is_none_or(|min| mark >= min);
        if min_ok && partial_trigger_met(node, mark, input.now) {
            let node_shares = (lot.shares * node.sell_pct).min(lot.shares);
            if node_shares.is_positive() {
                return Some((node_shares, node.node_id.clone()));
            }
        }
    }
    None
}

/// Whether a partial-exit node's trigger condition is satisfied at `mark`.
fn partial_trigger_met(node: &PartialExitNode, mark: Price, now: DateTime<Utc>) -> bool {
    let value = node.trigger_value;
    match node.trigger_kind {
        ExitTriggerKind::TakeProfit => mark.inner() >= value,
        ExitTriggerKind::StopLoss | ExitTriggerKind::TrailingStop => mark.inner() <= value,
        ExitTriggerKind::TimeExit => now.timestamp() >= value.try_into().unwrap_or(i64::MAX),
        // Signal/manual nodes are not price-evaluable here; the ladder's
        // model/manual tiers own them.
        ExitTriggerKind::SignalInvalidation | ExitTriggerKind::Manual => false,
    }
}
