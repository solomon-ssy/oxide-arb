//! Reconciliation service + worker pass (Phase 05.5).
//!
//! For each reconcilable order the service resolves its recommendation context,
//! collects venue evidence, optionally cancels a stale resting order, decides a
//! verdict, and applies one idempotent ledger correction. An `Unresolvable`
//! verdict freezes the capital (`Impaired`), latches the kill-switch via the
//! execution breaker, and bumps the unresolvable metric — fail-closed until an
//! operator resolves it. Reconciliation runs in **all** modes: in-flight money
//! must be reconciled regardless of the current runtime mode.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_error::{
    QuantResult,
    storage::{StorageError, entity},
};
use quant_pivot_models::types::RecommendationId;
use quant_pivot_models::{
    domain::{
        CapitalReconcileSettlement, ExecutionOrderInfo, OrderIntentInfo, PositionExit,
        PositionFill, PositionInfo, RecommendationInfo, ReconciliationLedgerWrite,
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        execution::{
            ExecutionOrderPhase, ExitReason, ExitState, ReconciliationEvidenceKind,
            ReconciliationResult, VenueOrderStatus,
        },
        quant::{AccountSource, ExecutionOrderState, OrderIntentStatus},
    },
    types::{
        ExecutionOrderId, OrderIntentId, Price, ReconciliationEvidence,
        ReconciliationEvidenceChain, Shares, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    OrderIntentRepository, PositionRepository, RecommendationRepository, ReconciliationRepository,
};

use super::{CollectedReconciliation, EvidenceCollector, VenuePresence, decide};
use crate::{
    execution::{ExecutionBreaker, IntentLifecyclePublisher, PolymarketOrderClient},
    observability::{
        capital_allocation_fact_writer::CapitalAllocationEventWriter,
        execution_fact_writer::ExecutionEventWriter,
        ledger_fact_projection::{
            project_capital_event, project_execution_event, project_position_event,
        },
        metrics_hub::MetricsHub,
        position_fact_writer::PositionEventWriter,
    },
    runtime_config::RuntimeConfigStore,
};

/// Max orders reconciled per sweep pass (bounds one sweep's venue + DB load).
const RECONCILE_BATCH: u64 = 256;
/// Audit actor recorded for machine reconciliation corrections.
const WORKER_ACTOR: &str = "system:reconciliation_worker";

/// Collaborators for [`ReconciliationService`].
pub struct ReconciliationServiceDeps {
    pub collector: Arc<dyn EvidenceCollector>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub execution_orders: Arc<dyn ExecutionOrderRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub positions: Arc<dyn PositionRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub fees: Arc<FeeCalculator>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub config: Arc<RuntimeConfigStore>,
    pub execution_events: Arc<ExecutionEventWriter>,
    pub capital_events: Arc<CapitalAllocationEventWriter>,
    pub position_events: Arc<PositionEventWriter>,
    /// Fans out the settled `quant.intent` status after a reconciliation write.
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
}

/// An operator's manual resolution of an unresolvable reconciliation.
pub struct OperatorReconcileResolution {
    pub execution_order_id: ExecutionOrderId,
    /// Terminal verdict the operator determined from the venue.
    pub result: ReconciliationResult,
    /// Confirmed filled shares (required for `Filled` / `PartiallyFilled`).
    pub filled_shares: Option<Shares>,
    /// Confirmed average fill price (required for `Filled` / `PartiallyFilled`).
    pub avg_price: Option<Price>,
    /// Operator identity recorded as `resolved_by`.
    pub operator: String,
    /// Free-text operator note appended as `OperatorNote` evidence.
    pub note: String,
}

/// Reconciles in-flight orders against Polymarket venue truth (Phase 05.5).
pub struct ReconciliationService {
    deps: ReconciliationServiceDeps,
}

impl ReconciliationService {
    #[must_use]
    pub const fn new(deps: ReconciliationServiceDeps) -> Self {
        Self { deps }
    }

    /// One sweep: reconcile every order whose venue truth is still unknown.
    pub async fn reconcile_pass(&self, now: DateTime<Utc>) -> QuantResult<()> {
        let policy = self.deps.config.current().execution.reconciliation.clone();
        if !policy.enabled {
            return Ok(());
        }
        let stale_after =
            Duration::seconds(i64::try_from(policy.stale_open_secs).unwrap_or(i64::MAX));

        let orders = self
            .deps
            .execution_orders
            .find_reconcilable(RECONCILE_BATCH)
            .await?;
        for order in orders {
            if let Err(error) = self.reconcile_one(&order, now, stale_after).await {
                tracing::warn!(
                    %error,
                    execution_order_id = %order.execution_order_id,
                    "reconciliation pass failed for order"
                );
            }
        }
        Ok(())
    }

    /// Reconcile a single order to a terminal verdict (or leave it pending).
    async fn reconcile_one(
        &self,
        order: &ExecutionOrderInfo,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<()> {
        if self.skip_unresolvable_awaiting_operator(order).await? {
            return Ok(());
        }

        let (intent, recommendation) = self.load_reconcile_context(order).await?;

        // Collect venue evidence. A venue read failure is fail-closed: freeze
        // only once the order is past the staleness deadline, else retry later.
        let collected = match self.deps.collector.collect(order, now, stale_after).await {
            Ok(collected) => collected,
            Err(error) => {
                let submitted_at = order.submitted_at.unwrap_or(order.created_at);
                if now - submitted_at > stale_after {
                    let evidence = vec![system_note(
                        ReconciliationEvidenceKind::ClobOrderStatus,
                        format!("venue unreachable past staleness deadline: {error}"),
                        now,
                    )];
                    return self
                        .apply_unresolvable(order, intent.status, evidence)
                        .await;
                }
                return Ok(());
            }
        };

        // Actively cancel a stale (or GTD-expired) resting order, then re-collect
        // the post-cancel truth so unfilled capital is released promptly.
        let collected = recollect_after_stale_cancel(
            &self.deps.collector,
            &self.deps.order_client,
            order,
            collected,
            now,
            stale_after,
        )
        .await;

        let decision = decide(&collected.facts);
        if decision.result == ReconciliationResult::Pending {
            // No terminal decision yet — leave for the next sweep.
            return Ok(());
        }

        let evidence = collected.evidence;
        if decision.result == ReconciliationResult::Unresolvable {
            return self
                .apply_unresolvable(order, intent.status, evidence)
                .await;
        }

        // An exit order's correction needs the lot's cost basis to price the
        // realized PnL; load it for the exit phase only.
        let lot = self.exit_lot(order).await?;
        let terminal = TerminalDecision {
            result: decision.result,
            filled_shares: decision.filled_shares,
            avg_price: decision.avg_price,
            resolved_by: WORKER_ACTOR.to_owned(),
            exit_reason: intent.exit_reason,
        };
        let exit_realized_pnl = exit_reconcile_realized_pnl(
            &self.deps.fees,
            order,
            &recommendation,
            lot.as_ref(),
            &terminal,
        );
        let write = self.build_terminal_write(
            order,
            &recommendation,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(evidence),
            now,
        );
        self.deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
        self.mirror_ledger_events(
            order,
            recommendation.recommendation_id.clone(),
            ChQuantLedgerEventKind::Reconciled,
            now,
        )
        .await?;
        self.publish_intent_transition(&order.order_intent_id, intent.status, now)
            .await?;
        if let Some(pnl) = exit_realized_pnl {
            self.deps.breaker.observe_realized_pnl(pnl, now).await;
        }
        Ok(())
    }

    /// Operator override of an unresolvable reconciliation (Phase 05.5 §5).
    ///
    /// Appends an `OperatorNote`, drives the order/capital/position to the
    /// operator-determined terminal outcome, and clears the `has_unresolvable`
    /// block. The kill-switch latch is **not** auto-cleared — the operator must
    /// ack it separately (05.1).
    pub async fn resolve(
        &self,
        resolution: OperatorReconcileResolution,
        now: DateTime<Utc>,
    ) -> QuantResult<ExecutionOrderInfo> {
        let order = self
            .deps
            .execution_orders
            .find_by_id(&resolution.execution_order_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    entity::QUANT_EXECUTION_ORDER,
                    &resolution.execution_order_id,
                )
            })?;
        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_ORDER_INTENT, &order.order_intent_id)
            })?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_RECOMMENDATION, &intent.recommendation_id)
            })?;

        let note = system_note(
            ReconciliationEvidenceKind::OperatorNote,
            format!(
                "operator {} resolved as {}: {}",
                resolution.operator,
                resolution.result.as_str(),
                resolution.note
            ),
            now,
        );
        let lot = self.exit_lot(&order).await?;
        let terminal = TerminalDecision {
            result: resolution.result,
            filled_shares: resolution.filled_shares.unwrap_or(Shares::ZERO),
            avg_price: resolution.avg_price,
            resolved_by: resolution.operator.clone(),
            exit_reason: intent.exit_reason,
        };
        let exit_realized_pnl = exit_reconcile_realized_pnl(
            &self.deps.fees,
            &order,
            &recommendation,
            lot.as_ref(),
            &terminal,
        );
        let write = self.build_terminal_write(
            &order,
            &recommendation,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(vec![note]),
            now,
        );
        let recorded = self
            .deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
        self.mirror_ledger_events(
            &recorded,
            recommendation.recommendation_id.clone(),
            ChQuantLedgerEventKind::OperatorResolved,
            now,
        )
        .await?;
        self.publish_intent_transition(&order.order_intent_id, intent.status, now)
            .await?;
        if let Some(pnl) = exit_realized_pnl {
            self.deps.breaker.observe_realized_pnl(pnl, now).await;
        }
        Ok(recorded)
    }

    async fn skip_unresolvable_awaiting_operator(
        &self,
        order: &ExecutionOrderInfo,
    ) -> QuantResult<bool> {
        let Some(existing) = self
            .deps
            .reconciliation
            .find_by_execution_order(&order.execution_order_id)
            .await?
        else {
            return Ok(false);
        };
        Ok(existing.result == ReconciliationResult::Unresolvable && existing.resolved_at.is_none())
    }

    async fn load_reconcile_context(
        &self,
        order: &ExecutionOrderInfo,
    ) -> QuantResult<(OrderIntentInfo, RecommendationInfo)> {
        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_ORDER_INTENT, &order.order_intent_id)
            })?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_RECOMMENDATION, &intent.recommendation_id)
            })?;
        Ok((intent, recommendation))
    }

    /// Fan out the settled `quant.intent` status after a reconciliation write.
    /// A no-op for exit-order reconciliations (the entry intent stays terminal)
    /// and unresolvable verdicts (status unchanged), gated by `prior_status`.
    async fn publish_intent_transition(
        &self,
        order_intent_id: &OrderIntentId,
        prior_status: OrderIntentStatus,
        at: DateTime<Utc>,
    ) -> QuantResult<()> {
        if let Some(settled) = self.deps.intents.find_by_id(order_intent_id).await? {
            self.deps
                .intent_lifecycle
                .publish_transition(prior_status, &settled, at);
        }
        Ok(())
    }

    async fn mirror_ledger_events(
        &self,
        order: &ExecutionOrderInfo,
        recommendation_id: RecommendationId,
        event_kind: ChQuantLedgerEventKind,
        event_time: DateTime<Utc>,
    ) -> QuantResult<()> {
        self.deps.execution_events.write(project_execution_event(
            order,
            recommendation_id,
            event_kind,
            event_time,
        ));
        if let Some(capital) = self
            .deps
            .capital
            .find_by_intent(&order.order_intent_id)
            .await?
        {
            self.deps
                .capital_events
                .write(project_capital_event(&capital, event_kind, event_time));
        }
        if let Some(position) = self
            .deps
            .positions
            .find_by_intent(&order.order_intent_id)
            .await?
        {
            self.deps
                .position_events
                .write(project_position_event(&position, event_kind, event_time));
        }
        Ok(())
    }

    /// Persist an `Unresolvable` verdict, then latch the kill-switch + bump the
    /// metric. The order/intent are left in place (non-terminal); capital is
    /// impaired (frozen).
    async fn apply_unresolvable(
        &self,
        order: &ExecutionOrderInfo,
        intent_status: OrderIntentStatus,
        evidence: Vec<ReconciliationEvidence>,
    ) -> QuantResult<()> {
        let (_, recommendation) = self.load_reconcile_context(order).await?;
        let detail = format!(
            "unresolvable reconciliation for execution order {}",
            order.execution_order_id
        );
        let write = ReconciliationLedgerWrite {
            order_state: order.state,
            intent_status,
            venue_status: order.venue_status,
            venue_order_id: order.venue_order_id.clone(),
            filled_at: None,
            cancelled_at: None,
            error_message: Some(detail.clone()),
            capital: CapitalReconcileSettlement::Impair,
            fill: None,
            exit: None,
            exit_fully: false,
            exit_state: None,
            revert_lot: false,
            result: ReconciliationResult::Unresolvable,
            evidence: ReconciliationEvidenceChain(evidence),
            venue_filled_shares: None,
            venue_avg_price: None,
            discrepancy_usd: None,
            resolved_by: None,
            resolved_at: None,
        };
        self.deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;

        self.mirror_ledger_events(
            order,
            recommendation.recommendation_id.clone(),
            ChQuantLedgerEventKind::Unresolvable,
            Utc::now(),
        )
        .await?;

        self.deps.breaker.trip_kill_switch("recon", &detail).await;
        self.deps.metrics.inc_reconciliation_unresolvable();
        Ok(())
    }

    /// The open position lot backing an exit order (its cost basis prices the
    /// realized `PnL`); `None` for entry orders or an absent lot.
    async fn exit_lot(&self, order: &ExecutionOrderInfo) -> QuantResult<Option<PositionInfo>> {
        if order.order_phase == ExecutionOrderPhase::Exit {
            Ok(self
                .deps
                .positions
                .find_by_intent(&order.order_intent_id)
                .await?)
        } else {
            Ok(None)
        }
    }

    /// Build the ledger correction for a terminal verdict (machine or operator).
    ///
    /// Starts from a neutral base (order/intent unchanged, capital held) and
    /// applies only the fields the verdict changes; `Unresolvable`/`Pending`
    /// (defensive) leave the base untouched.
    fn build_terminal_write(
        &self,
        order: &ExecutionOrderInfo,
        recommendation: &RecommendationInfo,
        lot: Option<&PositionInfo>,
        decision: TerminalDecision,
        evidence: ReconciliationEvidenceChain,
        now: DateTime<Utc>,
    ) -> ReconciliationLedgerWrite {
        let write = neutral_terminal_write(order, decision.result, evidence);

        // Exit-order reconciliation has a distinct money effect (reduce the lot +
        // release capital, not open a position); handle it on its own path.
        if order.order_phase == ExecutionOrderPhase::Exit {
            let TerminalDecision {
                filled_shares,
                avg_price,
                resolved_by,
                ..
            } = decision;
            return exit_reconcile_write(ExitReconcileWriteInput {
                write,
                order,
                lot,
                recommendation,
                fees: &self.deps.fees,
                filled_shares,
                avg_price,
                resolved_by,
                exit_reason: decision.exit_reason.unwrap_or(ExitReason::Manual),
                now,
            });
        }

        entry_reconcile_write(
            write,
            EntryReconcileInput {
                fees: &self.deps.fees,
                order,
                recommendation,
                decision,
                now,
            },
        )
    }
}

/// A terminal verdict ready to be turned into a ledger correction.
struct TerminalDecision {
    result: ReconciliationResult,
    filled_shares: Shares,
    avg_price: Option<Price>,
    resolved_by: String,
    /// Frozen exit trigger on the intent (exit-order reconciliation only).
    exit_reason: Option<ExitReason>,
}

/// Inputs for applying an entry-order terminal verdict to a ledger write.
struct EntryReconcileInput<'a> {
    fees: &'a FeeCalculator,
    order: &'a ExecutionOrderInfo,
    recommendation: &'a RecommendationInfo,
    decision: TerminalDecision,
    now: DateTime<Utc>,
}

/// Neutral ledger correction before a terminal verdict is applied.
fn neutral_terminal_write(
    order: &ExecutionOrderInfo,
    result: ReconciliationResult,
    evidence: ReconciliationEvidenceChain,
) -> ReconciliationLedgerWrite {
    ReconciliationLedgerWrite {
        order_state: order.state,
        intent_status: OrderIntentStatus::Submitted,
        venue_status: order.venue_status,
        venue_order_id: order.venue_order_id.clone(),
        filled_at: None,
        cancelled_at: None,
        error_message: None,
        capital: CapitalReconcileSettlement::Hold,
        fill: None,
        exit: None,
        exit_fully: false,
        exit_state: None,
        revert_lot: false,
        result,
        evidence,
        venue_filled_shares: None,
        venue_avg_price: None,
        discrepancy_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

/// Entry-order terminal verdict: open/extend a position lot or release capital.
fn entry_reconcile_write(
    mut write: ReconciliationLedgerWrite,
    input: EntryReconcileInput<'_>,
) -> ReconciliationLedgerWrite {
    let EntryReconcileInput {
        fees,
        order,
        recommendation,
        decision:
            TerminalDecision {
                result,
                filled_shares,
                avg_price,
                resolved_by,
                exit_reason: _,
            },
        now,
    } = input;
    match result {
        ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
            let price = avg_price.unwrap_or(order.price);
            let spent = filled_shares * price
                + fees.calculate(
                    filled_shares,
                    price,
                    recommendation.identity.category,
                    &order.market_id,
                    &order.token_id,
                );
            let full = result == ReconciliationResult::Filled;
            write.order_state = if full {
                ExecutionOrderState::Filled
            } else {
                ExecutionOrderState::PartiallyFilled
            };
            write.intent_status = if full {
                OrderIntentStatus::Filled
            } else {
                OrderIntentStatus::PartiallyFilled
            };
            write.venue_status = Some(if full {
                VenueOrderStatus::Filled
            } else {
                VenueOrderStatus::PartiallyFilled
            });
            write.filled_at = Some(now);
            write.capital = CapitalReconcileSettlement::Settle { spent_usd: spent };
            write.fill = Some(position_fill(
                order,
                recommendation,
                filled_shares,
                price,
                spent,
                now,
            ));
            write.venue_filled_shares = Some(filled_shares);
            write.venue_avg_price = avg_price;
            write.discrepancy_usd = Some(spent - filled_shares * order.price);
            write.resolved_by = Some(resolved_by);
            write.resolved_at = Some(now);
        }
        // `NotFilled` (GTD lapse) and `Cancelled` both release capital; only
        // the recorded terminal order/intent state differs.
        ReconciliationResult::NotFilled | ReconciliationResult::Cancelled => {
            let not_filled = result == ReconciliationResult::NotFilled;
            write.order_state = if not_filled {
                ExecutionOrderState::Failed
            } else {
                ExecutionOrderState::Cancelled
            };
            write.intent_status = if not_filled {
                OrderIntentStatus::Failed
            } else {
                OrderIntentStatus::Cancelled
            };
            write.venue_status = Some(if not_filled {
                VenueOrderStatus::Expired
            } else {
                VenueOrderStatus::Cancelled
            });
            write.cancelled_at = Some(now);
            write.capital = CapitalReconcileSettlement::Release;
            write.venue_filled_shares = Some(Shares::ZERO);
            write.resolved_by = Some(resolved_by);
            write.resolved_at = Some(now);
        }
        // Defensive: never reached (the caller handles these out of band).
        ReconciliationResult::Unresolvable | ReconciliationResult::Pending => {}
    }
    write
}

/// Realized `PnL` for a reconciled exit fill (net venue fee), when the lot is
/// present and the verdict is a (partial) fill. Used for ledger writes and the
/// breaker's daily-loss dimension (same path as [`CoreExitDispatcher`]).
fn exit_reconcile_realized_pnl(
    fees: &FeeCalculator,
    order: &ExecutionOrderInfo,
    recommendation: &RecommendationInfo,
    lot: Option<&PositionInfo>,
    decision: &TerminalDecision,
) -> Option<Usd> {
    if order.order_phase != ExecutionOrderPhase::Exit {
        return None;
    }
    let lot = lot?;
    match decision.result {
        ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
            Some(compute_exit_realized_pnl(
                fees,
                order,
                recommendation,
                lot,
                decision.filled_shares,
                decision.avg_price,
            ))
        }
        _ => None,
    }
}

/// Exact per-lot average-cost realized `PnL` for an exit fill, net the venue fee
/// (mirrors [`CoreExitDispatcher::build_exit_ledger_write`]).
fn compute_exit_realized_pnl(
    fees: &FeeCalculator,
    order: &ExecutionOrderInfo,
    recommendation: &RecommendationInfo,
    lot: &PositionInfo,
    filled_shares: Shares,
    avg_price: Option<Price>,
) -> Usd {
    let exit_price = avg_price.unwrap_or(order.price);
    let exit_fee = fees.calculate(
        filled_shares,
        exit_price,
        recommendation.identity.category,
        &order.market_id,
        &order.token_id,
    );
    let proceeds_usd = filled_shares * exit_price - exit_fee;
    let cost_basis = lot.avg_price * filled_shares;
    proceeds_usd - cost_basis
}

struct ExitReconcileWriteInput<'a> {
    write: ReconciliationLedgerWrite,
    order: &'a ExecutionOrderInfo,
    lot: Option<&'a PositionInfo>,
    recommendation: &'a RecommendationInfo,
    fees: &'a FeeCalculator,
    filled_shares: Shares,
    avg_price: Option<Price>,
    resolved_by: String,
    exit_reason: ExitReason,
    now: DateTime<Utc>,
}

fn exit_reconcile_write(input: ExitReconcileWriteInput<'_>) -> ReconciliationLedgerWrite {
    let ExitReconcileWriteInput {
        mut write,
        order,
        lot,
        recommendation,
        fees,
        filled_shares,
        avg_price,
        resolved_by,
        exit_reason,
        now,
    } = input;
    match write.result {
        ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
            let full = write.result == ReconciliationResult::Filled;
            write.order_state = if full {
                ExecutionOrderState::Filled
            } else {
                ExecutionOrderState::PartiallyFilled
            };
            write.venue_status = Some(if full {
                VenueOrderStatus::Filled
            } else {
                VenueOrderStatus::PartiallyFilled
            });
            write.filled_at = Some(now);
            write.venue_filled_shares = Some(filled_shares);
            write.venue_avg_price = avg_price;
            write.resolved_by = Some(resolved_by);
            write.resolved_at = Some(now);

            // Without the lot we cannot price the realized PnL — fail closed:
            // leave the order terminal but route the lot to manual review.
            let Some(lot) = lot else {
                write.exit_state = Some(ExitState::ManualRequired);
                return write;
            };
            let exit_price = avg_price.unwrap_or(order.price);
            let exit_fee = fees.calculate(
                filled_shares,
                exit_price,
                recommendation.identity.category,
                &order.market_id,
                &order.token_id,
            );
            let proceeds_usd = filled_shares * exit_price - exit_fee;
            let cost_basis = lot.avg_price * filled_shares;
            let realized_pnl_usd = proceeds_usd - cost_basis;
            let fully_exited = filled_shares >= lot.shares;
            write.discrepancy_usd = Some(proceeds_usd - cost_basis);
            write.exit = Some(PositionExit {
                shares: filled_shares,
                avg_price: exit_price,
                proceeds_usd,
                realized_pnl_usd,
                exited_at: now,
                reason: exit_reason,
            });
            write.exit_fully = fully_exited;
            write.exit_state = Some(if fully_exited {
                ExitState::Exited
            } else {
                ExitState::PartiallyExited
            });
        }
        // Confirmed non-fill / cancel: the lot never left — re-monitor it.
        ReconciliationResult::NotFilled | ReconciliationResult::Cancelled => {
            let not_filled = write.result == ReconciliationResult::NotFilled;
            write.order_state = if not_filled {
                ExecutionOrderState::Failed
            } else {
                ExecutionOrderState::Cancelled
            };
            write.venue_status = Some(if not_filled {
                VenueOrderStatus::Expired
            } else {
                VenueOrderStatus::Cancelled
            });
            write.cancelled_at = Some(now);
            write.venue_filled_shares = Some(Shares::ZERO);
            write.revert_lot = true;
            write.exit_state = Some(ExitState::Monitoring);
            write.resolved_by = Some(resolved_by);
            write.resolved_at = Some(now);
        }
        ReconciliationResult::Unresolvable | ReconciliationResult::Pending => {}
    }
    write
}

async fn recollect_after_stale_cancel(
    collector: &Arc<dyn EvidenceCollector>,
    order_client: &Arc<dyn PolymarketOrderClient>,
    order: &ExecutionOrderInfo,
    collected: CollectedReconciliation,
    now: DateTime<Utc>,
    stale_after: Duration,
) -> CollectedReconciliation {
    if collected.facts.presence == VenuePresence::Resting
        && (collected.facts.past_stale_deadline || collected.facts.gtd_expired)
        && let Some(venue_order_id) = order.venue_order_id.as_ref()
    {
        let _ = order_client.cancel(venue_order_id).await;
        return collector
            .collect(order, now, stale_after)
            .await
            .unwrap_or(collected);
    }
    collected
}

/// Build the position upsert for a confirmed fill.
fn position_fill(
    order: &ExecutionOrderInfo,
    recommendation: &RecommendationInfo,
    shares: Shares,
    price: Price,
    cost_usd: Usd,
    now: DateTime<Utc>,
) -> PositionFill {
    PositionFill {
        order_intent_id: order.order_intent_id.clone(),
        token_id: order.token_id.clone(),
        market_id: order.market_id.clone(),
        event_id: Some(recommendation.event_id.clone()),
        category: recommendation.identity.category,
        side: recommendation.outcome_side,
        shares,
        price,
        cost_usd,
        filled_at: now,
        source: AccountSource::Polymarket,
    }
}

/// A machine/system reconciliation note recorded as one evidence entry.
const fn system_note(
    kind: ReconciliationEvidenceKind,
    detail: String,
    now: DateTime<Utc>,
) -> ReconciliationEvidence {
    ReconciliationEvidence {
        kind,
        observed_at: now,
        detail,
        venue_ref: None,
        shares: None,
        price: None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_api::fees::FeeCalculator;
    use quant_pivot_models::{
        domain::{ExecutionOrderInfo, PositionInfo},
        enums::{
            common::{MarketCategory, Side},
            execution::{
                ExecutionOrderPhase, ExitReason, OrderTypeKind, PositionLedgerState,
                ReconciliationResult,
            },
            quant::{AccountSource, ExecutionOrderState, OutcomeSide},
        },
        types::{
            ExecutionOrderId, MarketId, OrderIntentId, PositionId, Price, RecommendationId,
            RecommendationReportId, ReconciliationEvidenceChain, Shares, TokenId, Usd,
        },
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ExitReconcileWriteInput, TerminalDecision, compute_exit_realized_pnl,
        exit_reconcile_realized_pnl, exit_reconcile_write, neutral_terminal_write,
    };

    fn lot(avg: Decimal) -> PositionInfo {
        PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            token_id: TokenId::new("token-1"),
            market_id: MarketId::new("0xmkt"),
            event_id: None,
            category: MarketCategory::Politics,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Closing,
            shares: Shares::new(dec!(100)),
            avg_price: Price::new(avg),
            cost_usd: Shares::new(dec!(100)) * Price::new(avg),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        }
    }

    fn exit_order() -> ExecutionOrderInfo {
        ExecutionOrderInfo {
            execution_order_id: ExecutionOrderId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            order_phase: ExecutionOrderPhase::Exit,
            market_id: MarketId::new("0xmkt"),
            token_id: TokenId::new("token-1"),
            side: Side::Sell,
            order_type: OrderTypeKind::Gtc,
            price: Price::new(dec!(0.55)),
            shares: Shares::new(dec!(100)),
            cost_usd: Usd::new(dec!(55)),
            venue_order_id: None,
            venue_status: None,
            state: ExecutionOrderState::Ambiguous,
            submitted_at: Some(Utc::now()),
            filled_at: None,
            cancelled_at: None,
            gtd_expiration_at: None,
            error_message: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn exit_realized_pnl_is_net_fee() {
        let fees = FeeCalculator::default();
        let recommendation = report_fixtures::recommendation(
            RecommendationReportId::from_v7(),
            RecommendationId::from_v7(),
            1,
            "0xmkt",
            OutcomeSide::Yes,
            Usd::new(dec!(100)),
        );
        let order = exit_order();
        let position = lot(dec!(0.60));
        // gross (0.55 - 0.60) * 100 = -5; fee reduces proceeds further
        let pnl = compute_exit_realized_pnl(
            &fees,
            &order,
            &recommendation,
            &position,
            Shares::new(dec!(100)),
            Some(Price::new(dec!(0.55))),
        );
        assert!(pnl < Usd::new(dec!(-5)));
    }

    #[test]
    fn exit_reconcile_realized_pnl_only_on_exit_fill() {
        let fees = FeeCalculator::default();
        let recommendation = report_fixtures::recommendation(
            RecommendationReportId::from_v7(),
            RecommendationId::from_v7(),
            1,
            "0xmkt",
            OutcomeSide::Yes,
            Usd::new(dec!(100)),
        );
        let order = exit_order();
        let position = lot(dec!(0.60));
        let filled = TerminalDecision {
            result: ReconciliationResult::Filled,
            filled_shares: Shares::new(dec!(100)),
            avg_price: Some(Price::new(dec!(0.55))),
            resolved_by: "test".to_owned(),
            exit_reason: Some(ExitReason::StopLoss),
        };
        assert!(
            exit_reconcile_realized_pnl(&fees, &order, &recommendation, Some(&position), &filled,)
                .is_some()
        );
        let cancelled = TerminalDecision {
            result: ReconciliationResult::Cancelled,
            filled_shares: Shares::ZERO,
            avg_price: None,
            resolved_by: "test".to_owned(),
            exit_reason: None,
        };
        assert!(
            exit_reconcile_realized_pnl(
                &fees,
                &order,
                &recommendation,
                Some(&position),
                &cancelled,
            )
            .is_none()
        );
    }

    #[test]
    fn exit_reconcile_write_preserves_trigger_exit_reason() {
        let fees = FeeCalculator::default();
        let recommendation = report_fixtures::recommendation(
            RecommendationReportId::from_v7(),
            RecommendationId::from_v7(),
            1,
            "0xmkt",
            OutcomeSide::Yes,
            Usd::new(dec!(100)),
        );
        let order = exit_order();
        let position = lot(dec!(0.60));
        let write = exit_reconcile_write(ExitReconcileWriteInput {
            write: neutral_terminal_write(
                &order,
                ReconciliationResult::Filled,
                ReconciliationEvidenceChain(Vec::new()),
            ),
            order: &order,
            lot: Some(&position),
            recommendation: &recommendation,
            fees: &fees,
            filled_shares: Shares::new(dec!(100)),
            avg_price: Some(Price::new(dec!(0.55))),
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::StopLoss,
            now: Utc::now(),
        });
        assert_eq!(
            write.exit.as_ref().expect("exit fill").reason,
            ExitReason::StopLoss
        );
    }
}
