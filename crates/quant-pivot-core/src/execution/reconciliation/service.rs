//! Reconciliation service + worker pass.
//!
//! For each reconcilable order the service resolves its recommendation context,
//! collects venue evidence, optionally cancels a stale resting order, decides a
//! verdict, and applies one idempotent ledger correction. An `Unresolvable`
//! verdict freezes the capital (`Impaired`), latches the kill-switch via the
//! execution breaker, and bumps the unresolvable metric — fail-closed until an
//! operator resolves it. Reconciliation runs in **all** modes: in-flight money
//! must be reconciled regardless of the current runtime mode.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    QuantResult,
    execution::ExecutionError,
    storage::{
        StorageError,
        entity::{QUANT_EXECUTION_ORDER, QUANT_ORDER_INTENT, QUANT_RECOMMENDATION},
    },
};
use quant_pivot_models::{
    domain::{
        quant::{
            CapitalReconcileSettlement, ExecutionOrderInfo, OrderIntentInfo, PositionExit,
            PositionFill, PositionInfo, RecommendationInfo, ReconciliationLedgerWrite,
        },
        runtime::{CoreEvent, CoreEventPublisher, ReconciliationLifecycleEvent},
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
        ExecutionOrderId, FeeEvidence, FeeEvidencePriority, OrderIntentId, Price, RecommendationId,
        ReconciliationEvidence, ReconciliationEvidenceChain, Shares, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    OrderIntentRepository, PositionRepository, RecommendationRepository, ReconciliationRepository,
};
use quant_pivot_research::execution_semantics::{LiquidityRole, PitFeeSchedule};

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
    runtime_config::DecisionPolicyStore,
};

/// Max orders reconciled per sweep pass (bounds one sweep's venue + DB load).
const RECONCILE_BATCH: u64 = 256;
/// Audit actor recorded for machine reconciliation corrections.
const WORKER_ACTOR: &str = "system:reconciliation_worker";

/// Preloaded intent / recommendation maps for one reconcile sweep.
struct ReconcileContextMaps {
    intents_by_id: HashMap<OrderIntentId, OrderIntentInfo>,
    recommendations_by_id: HashMap<RecommendationId, RecommendationInfo>,
}

fn load_reconcile_context(
    order: &ExecutionOrderInfo,
    context: &ReconcileContextMaps,
) -> QuantResult<(OrderIntentInfo, RecommendationInfo)> {
    let intent = context
        .intents_by_id
        .get(&order.order_intent_id)
        .cloned()
        .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, &order.order_intent_id))?;
    let recommendation = context
        .recommendations_by_id
        .get(&intent.recommendation_id)
        .cloned()
        .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, &intent.recommendation_id))?;
    Ok((intent, recommendation))
}

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
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub config: Arc<DecisionPolicyStore>,
    pub execution_events: Arc<ExecutionEventWriter>,
    pub capital_events: Arc<CapitalAllocationEventWriter>,
    pub position_events: Arc<PositionEventWriter>,
    /// Fans out the settled `quant.intent` status after a reconciliation write.
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Fans out `quant.reconciliation` revision hints after a reconciliation write.
    pub events: CoreEventPublisher,
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

/// Reconciles in-flight orders against Polymarket venue truth.
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
        let policy = self
            .deps
            .config
            .current()
            .execution_risk
            .reconciliation
            .clone();
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
        let context = self.preload_reconcile_context(&orders).await?;
        for order in orders {
            if let Err(error) = self.reconcile_one(&order, now, stale_after, &context).await {
                tracing::warn!(
                    %error,
                    execution_order_id = %order.execution_order_id,
                    "reconciliation pass failed for order"
                );
            }
        }
        Ok(())
    }

    /// Batch-load intents + recommendations for one reconcile sweep.
    async fn preload_reconcile_context(
        &self,
        orders: &[ExecutionOrderInfo],
    ) -> QuantResult<ReconcileContextMaps> {
        let intent_ids: Vec<OrderIntentId> = orders
            .iter()
            .map(|order| order.order_intent_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let intents = self.deps.intents.find_by_ids(&intent_ids).await?;
        let intents_by_id: HashMap<OrderIntentId, OrderIntentInfo> = intents
            .into_iter()
            .map(|intent| (intent.order_intent_id.clone(), intent))
            .collect();
        let recommendation_ids: Vec<RecommendationId> = intents_by_id
            .values()
            .map(|intent| intent.recommendation_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let recommendations = self
            .deps
            .recommendations
            .find_by_ids(&recommendation_ids)
            .await?;
        let recommendations_by_id = recommendations
            .into_iter()
            .map(|rec| (rec.recommendation_id.clone(), rec))
            .collect();
        Ok(ReconcileContextMaps {
            intents_by_id,
            recommendations_by_id,
        })
    }

    /// Reconcile a single order to a terminal verdict (or leave it pending).
    async fn reconcile_one(
        &self,
        order: &ExecutionOrderInfo,
        now: DateTime<Utc>,
        stale_after: Duration,
        context: &ReconcileContextMaps,
    ) -> QuantResult<()> {
        if self.skip_unresolvable_awaiting_operator(order).await? {
            return Ok(());
        }

        let (intent, recommendation) = load_reconcile_context(order, context)?;

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
                        .apply_unresolvable(order, intent.status, evidence, &recommendation)
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
                .apply_unresolvable(order, intent.status, evidence, &recommendation)
                .await;
        }

        // An exit order's correction needs the lot's cost basis to price the
        // realized PnL; load it for exit orders only.
        let lot = self.exit_lot(order).await?;
        let terminal = TerminalDecision {
            result: decision.result,
            filled_shares: decision.filled_shares,
            avg_price: decision.avg_price,
            resolved_by: WORKER_ACTOR.to_owned(),
            exit_reason: intent.exit_reason,
        };
        let write = Self::build_terminal_write(
            order,
            &recommendation,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(evidence),
            now,
        )?;
        let exit_realized_pnl = write.exit.as_ref().map(|exit| exit.realized_pnl_usd);
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
        self.publish_reconciliation(order, decision.result, false);
        if let Some(pnl) = exit_realized_pnl {
            self.deps.breaker.observe_realized_pnl(pnl, now).await;
        }
        Ok(())
    }

    /// Operator override of an unresolvable reconciliation.
    ///
    /// Appends an `OperatorNote`, drives the order/capital/position to the
    /// operator-determined terminal outcome, and clears the `has_unresolvable`
    /// block. The kill-switch latch is **not** auto-cleared — the operator must
    /// ack it separately.
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
                StorageError::not_found(QUANT_EXECUTION_ORDER, &resolution.execution_order_id)
            })?;
        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, &order.order_intent_id))?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, &intent.recommendation_id)
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
        let write = Self::build_terminal_write(
            &order,
            &recommendation,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(vec![note]),
            now,
        )?;
        let exit_realized_pnl = write.exit.as_ref().map(|exit| exit.realized_pnl_usd);
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
        self.publish_reconciliation(&order, resolution.result, true);
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
        recommendation: &RecommendationInfo,
    ) -> QuantResult<()> {
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
            expected_cash_delta_usd: None,
            venue_cash_delta_usd: None,
            realized_pnl_usd: None,
            expected_fee_usd: None,
            observed_fee_usd: None,
            fee_delta_usd: None,
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
        self.publish_reconciliation(order, ReconciliationResult::Unresolvable, false);
        Ok(())
    }

    /// Fan out a `quant.reconciliation` revision hint after a reconciliation
    /// write commits. Consumers re-fetch the queue + recovery panel over REST.
    fn publish_reconciliation(
        &self,
        order: &ExecutionOrderInfo,
        result: ReconciliationResult,
        operator_resolved: bool,
    ) {
        self.deps
            .events
            .publish(CoreEvent::Reconciliation(ReconciliationLifecycleEvent {
                execution_order_id: order.execution_order_id.to_string(),
                order_intent_id: order.order_intent_id.to_string(),
                result,
                operator_resolved,
            }));
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
        order: &ExecutionOrderInfo,
        recommendation: &RecommendationInfo,
        lot: Option<&PositionInfo>,
        decision: TerminalDecision,
        evidence: ReconciliationEvidenceChain,
        now: DateTime<Utc>,
    ) -> QuantResult<ReconciliationLedgerWrite> {
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
    order: &'a ExecutionOrderInfo,
    recommendation: &'a RecommendationInfo,
    decision: TerminalDecision,
    now: DateTime<Utc>,
}

struct ReconciliationFee {
    expected: Usd,
    observed: Option<Usd>,
    applied: Usd,
}

fn reconciliation_fee(
    order: &ExecutionOrderInfo,
    evidence: &ReconciliationEvidenceChain,
    filled_shares: Shares,
    price: Price,
    matched_at: DateTime<Utc>,
) -> QuantResult<ReconciliationFee> {
    let prepared = &order.prepared_order_json.fee_schedule;
    let role = if order.prepared_order_json.post_only {
        LiquidityRole::Maker
    } else {
        LiquidityRole::Taker
    };
    let expected = PitFeeSchedule {
        schedule_hash: prepared.schedule_hash.clone(),
        effective_at: prepared.effective_at,
        available_at: prepared.available_at,
        platform_rate: prepared.platform_rate,
        exponent: prepared.exponent,
        taker_only: prepared.taker_only,
        builder_maker_fee_bps: prepared.builder_maker_fee_bps,
        builder_taker_fee_bps: prepared.builder_taker_fee_bps,
        builder_attribution: prepared.builder_attribution,
    }
    .fee(role, price, filled_shares, matched_at)
    .map_err(|error| ExecutionError::ReconciliationUnresolvable {
        reason: format!("frozen fee schedule cannot price reconciled fill: {error:?}"),
    })?;
    let strongest = evidence
        .0
        .iter()
        .filter_map(|item| item.fee_evidence.as_ref())
        .map(FeeEvidence::priority)
        .filter(|priority| *priority != FeeEvidencePriority::PreparedScheduleExpected)
        .max();
    let observed = strongest.map(|priority| {
        evidence
            .0
            .iter()
            .filter_map(|item| item.fee_evidence.as_ref())
            .filter(|item| item.priority() == priority)
            .fold(Usd::ZERO, |total, item| total + item.fee())
    });
    Ok(ReconciliationFee {
        expected,
        observed,
        applied: observed.unwrap_or(expected),
    })
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
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

/// Entry-order terminal verdict: open/extend a position lot or release capital.
fn entry_reconcile_write(
    mut write: ReconciliationLedgerWrite,
    input: EntryReconcileInput<'_>,
) -> QuantResult<ReconciliationLedgerWrite> {
    let EntryReconcileInput {
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
            let fee = reconciliation_fee(order, &write.evidence, filled_shares, price, now)?;
            let spent = filled_shares * price + fee.applied;
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
            write.expected_cash_delta_usd =
                Some(Usd::new(order.prepared_order_json.total_cash_delta));
            write.expected_fee_usd = Some(fee.expected);
            write.observed_fee_usd = fee.observed;
            if let Some(observed) = fee.observed {
                write.venue_cash_delta_usd = Some(Usd::new(
                    -((filled_shares * price).inner() + observed.inner()),
                ));
                write.fee_delta_usd = Some(observed - fee.expected);
            }
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
    Ok(write)
}

/// Exact per-lot average-cost realized `PnL` for an exit fill, net the venue fee
/// (mirrors [`CoreExitDispatcher::build_exit_ledger_write`]).
#[cfg(test)]
fn compute_exit_realized_pnl(
    order: &ExecutionOrderInfo,
    lot: &PositionInfo,
    filled_shares: Shares,
    avg_price: Option<Price>,
    evidence: &ReconciliationEvidenceChain,
    matched_at: DateTime<Utc>,
) -> QuantResult<Usd> {
    let exit_price = avg_price.unwrap_or(order.price);
    let exit_fee =
        reconciliation_fee(order, evidence, filled_shares, exit_price, matched_at)?.applied;
    let proceeds_usd = filled_shares * exit_price - exit_fee;
    let cost_basis = lot.avg_price * filled_shares;
    Ok(proceeds_usd - cost_basis)
}

struct ExitReconcileWriteInput<'a> {
    write: ReconciliationLedgerWrite,
    order: &'a ExecutionOrderInfo,
    lot: Option<&'a PositionInfo>,
    filled_shares: Shares,
    avg_price: Option<Price>,
    resolved_by: String,
    exit_reason: ExitReason,
    now: DateTime<Utc>,
}

fn exit_reconcile_write(
    input: ExitReconcileWriteInput<'_>,
) -> QuantResult<ReconciliationLedgerWrite> {
    let ExitReconcileWriteInput {
        mut write,
        order,
        lot,
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
                return Ok(write);
            };
            let exit_price = avg_price.unwrap_or(order.price);
            let fee = reconciliation_fee(order, &write.evidence, filled_shares, exit_price, now)?;
            let exit_fee = fee.applied;
            let proceeds_usd = filled_shares * exit_price - exit_fee;
            let cost_basis = lot.avg_price * filled_shares;
            let realized_pnl_usd = proceeds_usd - cost_basis;
            let fully_exited = filled_shares >= lot.shares;
            write.expected_cash_delta_usd =
                Some(Usd::new(order.prepared_order_json.total_cash_delta));
            write.venue_cash_delta_usd = fee
                .observed
                .map(|observed| filled_shares * exit_price - observed);
            write.realized_pnl_usd = Some(realized_pnl_usd);
            write.expected_fee_usd = Some(fee.expected);
            write.observed_fee_usd = fee.observed;
            write.fee_delta_usd = fee.observed.map(|observed| observed - fee.expected);
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
    Ok(write)
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
        fee_evidence: None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::quant::{ExecutionOrderInfo, PositionInfo},
        enums::{
            common::{MarketCategory, OrderType, Side},
            execution::{
                ExecutionOrderPhase, ExitReason, OrderTypeKind, PositionLedgerState,
                ReconciliationResult,
            },
            quant::{AccountSource, ExecutionOrderState, OutcomeSide},
        },
        types::{
            ExecutionOrderId, MarketId, OrderIntentId, PositionId, Price,
            ReconciliationEvidenceChain, Shares, TokenId, Usd, VenueOrderAmount,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ExitReconcileWriteInput, compute_exit_realized_pnl, exit_reconcile_write,
        neutral_terminal_write,
    };
    use crate::test_fixtures::execution_pg_seed::prepared_order;

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
            prepared_order_json: prepared_order(
                Side::Sell,
                OrderType::Gtc,
                VenueOrderAmount::Shares(Shares::new(dec!(100))),
                Usd::ZERO,
                Shares::new(dec!(100)),
                Price::new(dec!(0.55)),
            ),
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
        let order = exit_order();
        let position = lot(dec!(0.60));
        let matched_at = Utc::now();
        // gross (0.55 - 0.60) * 100 = -5; fee reduces proceeds further
        let pnl = compute_exit_realized_pnl(
            &order,
            &position,
            Shares::new(dec!(100)),
            Some(Price::new(dec!(0.55))),
            &ReconciliationEvidenceChain(Vec::new()),
            matched_at,
        )
        .expect("frozen fee fixture");
        assert!(pnl < Usd::new(dec!(-5)));
    }

    #[test]
    fn exit_reconcile_write_only_realizes_pnl_on_fill() {
        let order = exit_order();
        let position = lot(dec!(0.60));
        let filled = exit_reconcile_write(ExitReconcileWriteInput {
            write: neutral_terminal_write(
                &order,
                ReconciliationResult::Filled,
                ReconciliationEvidenceChain(Vec::new()),
            ),
            order: &order,
            lot: Some(&position),
            filled_shares: Shares::new(dec!(100)),
            avg_price: Some(Price::new(dec!(0.55))),
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::StopLoss,
            now: Utc::now(),
        })
        .expect("frozen fee fixture");
        assert!(filled.exit.is_some());
        let cancelled = exit_reconcile_write(ExitReconcileWriteInput {
            write: neutral_terminal_write(
                &order,
                ReconciliationResult::Cancelled,
                ReconciliationEvidenceChain(Vec::new()),
            ),
            order: &order,
            lot: Some(&position),
            filled_shares: Shares::ZERO,
            avg_price: None,
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::Manual,
            now: Utc::now(),
        })
        .expect("non-fill does not quote fees");
        assert!(cancelled.exit.is_none());
    }

    #[test]
    fn exit_reconcile_write_preserves_trigger_exit_reason() {
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
            filled_shares: Shares::new(dec!(100)),
            avg_price: Some(Price::new(dec!(0.55))),
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::StopLoss,
            now: Utc::now(),
        })
        .expect("CLOB fee fixture");
        assert_eq!(
            write.exit.as_ref().expect("exit fill").reason,
            ExitReason::StopLoss
        );
    }
}
