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
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        CapitalReconcileSettlement, ExecutionOrderInfo, PositionFill, RecommendationInfo,
        ReconciliationLedgerWrite,
    },
    enums::{
        execution::{ReconciliationEvidenceKind, ReconciliationResult, VenueOrderStatus},
        quant::{AccountSource, ExecutionOrderState, OrderIntentStatus},
    },
    types::{
        ExecutionOrderId, Price, ReconciliationEvidence, ReconciliationEvidenceChain, Shares, Usd,
    },
};
use quant_pivot_repository::traits::{
    ExecutionOrderRepository, ExecutionSubmissionRepository, OrderIntentRepository,
    RecommendationRepository, ReconciliationRepository,
};

use super::{EvidenceCollector, VenuePresence, decide};
use crate::{
    execution::{ExecutionBreaker, PolymarketOrderClient},
    observability::metrics_hub::MetricsHub,
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
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub fees: Arc<FeeCalculator>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub config: Arc<RuntimeConfigStore>,
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
        // Skip orders already escalated to `Unresolvable` and awaiting an
        // operator — re-processing would re-impair and re-trip the breaker.
        if let Some(existing) = self
            .deps
            .reconciliation
            .find_by_execution_order(&order.execution_order_id)
            .await?
        {
            if existing.result == ReconciliationResult::Unresolvable
                && existing.resolved_at.is_none()
            {
                return Ok(());
            }
        }

        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| {
                StorageError::Conflict(format!(
                    "intent {} not found for reconcilable order {}",
                    order.order_intent_id, order.execution_order_id
                ))
            })?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::Conflict(format!(
                    "recommendation {} not found for reconcilable order {}",
                    intent.recommendation_id, order.execution_order_id
                ))
            })?;

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
        let collected = if collected.facts.presence == VenuePresence::Resting
            && (collected.facts.past_stale_deadline || collected.facts.gtd_expired)
        {
            if let Some(venue_order_id) = order.venue_order_id.as_ref() {
                let _ = self.deps.order_client.cancel(venue_order_id).await;
                self.deps
                    .collector
                    .collect(order, now, stale_after)
                    .await
                    .unwrap_or(collected)
            } else {
                collected
            }
        } else {
            collected
        };

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

        let write = self.build_terminal_write(
            order,
            &recommendation,
            TerminalDecision {
                result: decision.result,
                filled_shares: decision.filled_shares,
                avg_price: decision.avg_price,
                resolved_by: WORKER_ACTOR.to_owned(),
            },
            ReconciliationEvidenceChain(evidence),
            now,
        );
        self.deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
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
                StorageError::Conflict(format!(
                    "execution order {} not found for resolve",
                    resolution.execution_order_id
                ))
            })?;
        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| {
                StorageError::Conflict(format!("intent {} not found", order.order_intent_id))
            })?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::Conflict(format!(
                    "recommendation {} not found",
                    intent.recommendation_id
                ))
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
        let write = self.build_terminal_write(
            &order,
            &recommendation,
            TerminalDecision {
                result: resolution.result,
                filled_shares: resolution.filled_shares.unwrap_or(Shares::ZERO),
                avg_price: resolution.avg_price,
                resolved_by: resolution.operator,
            },
            ReconciliationEvidenceChain(vec![note]),
            now,
        );
        self.deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await
            .map_err(Into::into)
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

        self.deps.breaker.trip_kill_switch("recon", &detail).await;
        self.deps.metrics.inc_reconciliation_unresolvable();
        Ok(())
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
        decision: TerminalDecision,
        evidence: ReconciliationEvidenceChain,
        now: DateTime<Utc>,
    ) -> ReconciliationLedgerWrite {
        let TerminalDecision {
            result,
            filled_shares,
            avg_price,
            resolved_by,
        } = decision;
        let mut write = ReconciliationLedgerWrite {
            order_state: order.state,
            intent_status: OrderIntentStatus::Submitted,
            venue_status: order.venue_status,
            venue_order_id: order.venue_order_id.clone(),
            filled_at: None,
            cancelled_at: None,
            error_message: None,
            capital: CapitalReconcileSettlement::Hold,
            fill: None,
            result,
            evidence,
            venue_filled_shares: None,
            venue_avg_price: None,
            discrepancy_usd: None,
            resolved_by: None,
            resolved_at: None,
        };

        match result {
            ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
                let price = avg_price.unwrap_or(order.price);
                let spent = filled_shares * price
                    + self.deps.fees.calculate(
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
}

/// A terminal verdict ready to be turned into a ledger correction.
struct TerminalDecision {
    result: ReconciliationResult,
    filled_shares: Shares,
    avg_price: Option<Price>,
    resolved_by: String,
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
