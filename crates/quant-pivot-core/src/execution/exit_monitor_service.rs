//! Exit-monitor service + sweep pass (Phase 05.6).
//!
//! One sweep scans every open position lot, resolves its book mark / freshness /
//! abnormality, rate-limits the model re-inference, runs the deterministic
//! [`decide_exit`] ladder, and acts: submit the exit (via [`CoreExitDispatcher`]),
//! route to manual review, or hold (persisting `next_check_at` / trailing peak /
//! last re-inference time). After a successful pass it publishes the
//! [`ExitMonitorHealthHandle`] heartbeat that gates admission `#20`.
//!
//! Lots whose exit FSM is `OrderSubmitted` (in-flight, recon resolves),
//! `ManualRequired` (operator owns it), or terminal (`Exited`/`Failed`) are
//! skipped — only `NotStarted` / `Monitoring` / `Triggered` / `PartiallyExited`
//! lots are (re)evaluated, so a single in-flight exit is never double-submitted.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{OrderIntentInfo, PositionInfo, market::book::BookSnapshot},
    enums::execution::ExitState,
    runtime_config::EmergencyExitPolicy,
    types::{OrderIntentId, Price},
};
use quant_pivot_repository::traits::{
    ExecutionSubmissionRepository, OrderIntentRepository, PositionRepository,
    RecommendationRepository,
};

use crate::{
    execution::{
        exit_dispatcher::{CoreExitDispatcher, ExitSubmitRequest},
        exit_monitor::{
            ExitDecision, ExitMonitorHealthHandle, ExitMonitorInput, ExitSignalContext,
            ExitSignalEvaluator, ExitSignalVerdict, decide_exit,
        },
    },
    governance::KillSwitchHandle,
    observability::metrics_hub::MetricsHub,
    pipeline::book_store::BookStore,
    runtime_config::RuntimeConfigStore,
};

/// Max lots evaluated per sweep (bounds one pass's book + DB load).
const SCAN_BATCH_GUARD: usize = 4_096;

/// Collaborators for [`ExitMonitorService`].
pub struct ExitMonitorServiceDeps {
    pub positions: Arc<dyn PositionRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub book_store: Arc<BookStore>,
    pub kill_switch: KillSwitchHandle,
    pub config: Arc<RuntimeConfigStore>,
    pub signal: Arc<dyn ExitSignalEvaluator>,
    pub dispatcher: Arc<CoreExitDispatcher>,
    pub health: ExitMonitorHealthHandle,
    pub metrics: Arc<MetricsHub>,
}

/// Scans open lots and drives the exit priority ladder (Phase 05.6).
pub struct ExitMonitorService {
    deps: ExitMonitorServiceDeps,
}

impl ExitMonitorService {
    #[must_use]
    pub const fn new(deps: ExitMonitorServiceDeps) -> Self {
        Self { deps }
    }

    /// One sweep: evaluate every open lot, then publish the health heartbeat.
    ///
    /// When the monitor is disabled in config the heartbeat is **not** published,
    /// so admission `#20` fails closed (no new entries without live monitoring).
    pub async fn run_pass(&self, now: DateTime<Utc>) -> QuantResult<()> {
        let policy = self.deps.config.current().execution.exit_monitor.clone();
        if !policy.enabled {
            return Ok(());
        }
        let monitor_secs = policy.monitor_secs.max(1);
        let recheck =
            Duration::seconds(i64::try_from(policy.signal_recheck_secs).unwrap_or(i64::MAX));

        let lots = self.deps.positions.find_open_lots().await?;
        for lot in lots.into_iter().take(SCAN_BATCH_GUARD) {
            if let Err(error) = self.evaluate_lot(&lot, monitor_secs, recheck, now).await {
                tracing::warn!(
                    %error,
                    token_id = %lot.token_id,
                    order_intent_id = %lot.order_intent_id,
                    "exit-monitor evaluation failed for lot"
                );
            }
        }

        self.deps
            .health
            .publish(now, monitor_secs.saturating_mul(2));
        Ok(())
    }

    async fn evaluate_lot(
        &self,
        lot: &PositionInfo,
        monitor_secs: u64,
        recheck: Duration,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let Some(intent) = self.deps.intents.find_by_id(&lot.order_intent_id).await? else {
            return Ok(());
        };
        // Skip in-flight / manual / terminal exit states — only re-evaluate lots
        // that are still being actively monitored.
        if matches!(
            intent.exit_state,
            ExitState::OrderSubmitted
                | ExitState::ManualRequired
                | ExitState::Exited
                | ExitState::Failed
        ) {
            return Ok(());
        }

        let snapshot = self.deps.book_store.load(&lot.token_id);
        let now_ms = u64::try_from(now.timestamp_millis()).unwrap_or(0);

        // The recommendation supplies the venue neg-risk signing context (and the
        // re-inference thesis context); a missing one is non-fatal.
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?;
        // Same per-book staleness ceiling as admission `#7` (`BookFreshnessCheck`):
        // frozen `entry_plan.max_book_age_ms` when available, else live config.
        let max_book_age_ms = recommendation.as_ref().map_or_else(
            || self.deps.config.current().data_quality.max_book_age_ms,
            |rec| rec.entry_plan.max_book_age_ms,
        );
        let (mark_price, book_fresh, market_abnormal) =
            classify_book(snapshot.as_deref(), now_ms, max_book_age_ms);

        let neg_risk = recommendation
            .as_ref()
            .is_some_and(|rec| rec.market_context.neg_risk);

        // Fold the current mark into the trailing peak.
        let peak_mark_price = max_price(intent.peak_mark_price, mark_price);

        let (signal, did_recheck) = resolve_exit_signal(
            self.deps.signal.as_ref(),
            ExitSignalResolve {
                intent: &intent,
                lot,
                mark_price,
                book_fresh,
                market_abnormal,
                recheck,
                now,
            },
        )
        .await;

        let emergency_policy = self.emergency_policy();
        let min_opportunistic_clip_pct = self.opportunistic_clip_pct();
        let input = ExitMonitorInput {
            lot: lot.clone(),
            exit_policy: intent.exit_policy_json.clone(),
            mark_price,
            book_fresh,
            market_abnormal,
            kill_switch: self.deps.kill_switch.current(),
            emergency_policy,
            peak_mark_price,
            signal,
            executed_partial_exit_node_ids: intent.executed_partial_exit_node_ids.node_ids.clone(),
            opportunistic_exit_state: intent.opportunistic_exit_state.clone(),
            min_opportunistic_clip_pct,
            now,
        };

        match decide_exit(&input) {
            ExitDecision::SubmitExitOrder {
                reason,
                order,
                partial_exit_node_id,
                opportunistic_denominator,
            } => {
                self.deps
                    .dispatcher
                    .submit_exit(ExitSubmitRequest {
                        lot: lot.clone(),
                        reason,
                        order,
                        partial_exit_node_id,
                        opportunistic_denominator,
                        neg_risk,
                    })
                    .await?;
            }
            ExitDecision::RequireManualReview { reason } => {
                self.deps
                    .submission
                    .mark_exit_manual(&intent.order_intent_id, reason)
                    .await?;
            }
            ExitDecision::Hold => {
                self.touch_lot_monitor(
                    &intent.order_intent_id,
                    monitor_secs,
                    now,
                    peak_mark_price,
                    did_recheck,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// The live emergency-exit policy applied under kill-switch emergency halt.
    fn emergency_policy(&self) -> EmergencyExitPolicy {
        self.deps
            .config
            .current()
            .execution
            .kill_switch
            .emergency_exit
            .clone()
    }

    /// The live opportunistic min-clip fraction (fail-safe `0` on a malformed
    /// snapshot — config validation rejects that at load).
    fn opportunistic_clip_pct(&self) -> rust_decimal::Decimal {
        self.deps
            .config
            .current()
            .execution
            .exit_monitor
            .opportunistic_sell
            .min_opportunistic_clip_pct
            .value
            .parse()
            .unwrap_or(rust_decimal::Decimal::ZERO)
    }

    async fn touch_lot_monitor(
        &self,
        order_intent_id: &OrderIntentId,
        monitor_secs: u64,
        now: DateTime<Utc>,
        peak_mark_price: Option<Price>,
        did_recheck: bool,
    ) -> QuantResult<()> {
        let next_check_at =
            now + Duration::seconds(i64::try_from(monitor_secs).unwrap_or(i64::MAX));
        let last_recheck = did_recheck.then_some(now);
        self.deps
            .submission
            .touch_exit_monitor(
                order_intent_id,
                next_check_at,
                peak_mark_price,
                last_recheck,
            )
            .await?;
        Ok(())
    }
}

/// Inputs for rate-limited exit-signal re-inference on one lot.
struct ExitSignalResolve<'a> {
    intent: &'a OrderIntentInfo,
    lot: &'a PositionInfo,
    mark_price: Option<Price>,
    book_fresh: bool,
    market_abnormal: bool,
    recheck: Duration,
    now: DateTime<Utc>,
}

/// Rate-limited model re-inference for one lot (only when the book is actionable).
async fn resolve_exit_signal(
    signal: &dyn ExitSignalEvaluator,
    input: ExitSignalResolve<'_>,
) -> (ExitSignalVerdict, bool) {
    let ExitSignalResolve {
        intent,
        lot,
        mark_price,
        book_fresh,
        market_abnormal,
        recheck,
        now,
    } = input;
    let recheck_due = intent
        .last_signal_recheck_at
        .is_none_or(|last| now - last >= recheck);
    if recheck_due && book_fresh && !market_abnormal {
        let verdict = signal
            .evaluate(ExitSignalContext {
                intent,
                lot,
                mark_price,
                now,
            })
            .await;
        (verdict, true)
    } else {
        (ExitSignalVerdict::Holds, false)
    }
}

/// Classify a token's book for exit decisioning: the sell-side mark (best bid),
/// whether it is fresh enough to act on (same threshold as admission book
/// freshness), and whether the market looks abnormal.
fn classify_book(
    snapshot: Option<&BookSnapshot>,
    now_ms: u64,
    max_book_age_ms: u64,
) -> (Option<Price>, bool, bool) {
    let Some(snapshot) = snapshot else {
        return (None, false, false);
    };
    let fresh = now_ms.saturating_sub(snapshot.timestamp_ms) <= max_book_age_ms;
    let bid = snapshot.best_bid();
    let ask = snapshot.best_ask();
    let crossed = matches!((bid, ask), (Some(b), Some(a)) if b >= a);
    let abnormal = bid.is_none() || crossed;
    (bid, fresh, abnormal)
}

/// The larger of an existing peak and the current mark (either may be absent).
fn max_price(existing: Option<Price>, current: Option<Price>) -> Option<Price> {
    match (existing, current) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, current) => current,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quant_pivot_models::{
        domain::market::book::{BookLevel, BookSnapshot},
        types::{Price, Shares},
    };
    use rust_decimal_macros::dec;

    use super::classify_book;

    #[test]
    fn classify_book_uses_configured_max_age() {
        let now_ms = 1_000_000u64;
        let snapshot = BookSnapshot::new(
            Arc::from([
                BookLevel::from_decimal(Price::new(dec!(0.50)), Shares::new(dec!(100)))
                    .expect("bid"),
            ]),
            Arc::from([
                BookLevel::from_decimal(Price::new(dec!(0.52)), Shares::new(dec!(100)))
                    .expect("ask"),
            ]),
            now_ms - 4_000,
            1,
        );
        let (_, fresh_at_5s, _) = classify_book(Some(&snapshot), now_ms, 5_000);
        let (_, fresh_at_3s, _) = classify_book(Some(&snapshot), now_ms, 3_000);
        assert!(fresh_at_5s);
        assert!(!fresh_at_3s);
    }
}
