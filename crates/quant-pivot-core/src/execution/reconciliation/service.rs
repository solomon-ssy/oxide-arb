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
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use futures_util::future::join_all;
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
        data_plane::DecisionClock,
        quant::{
            CapitalReconcileSettlement, CumulativePositionExit, CumulativePositionFill,
            ExecutionOrderIdentityRefs, ExecutionOrderInfo, OrderIntentInfo, RecommendationInfo,
            ReconciliationLedgerWrite, StrategyPositionLot,
        },
        runtime::{CoreEvent, CoreEventPublisher, ReconciliationLifecycleEvent},
    },
    enums::{
        clickhouse::ChQuantLedgerEventKind,
        execution::{
            ExecutionOrderPhase, ExitReason, ExitState, ReconciliationEvidenceKind,
            ReconciliationResult, VenueOrderStatus,
        },
        fee::FeeLiquidityRole,
        quant::{AccountSource, ExecutionOrderState, OrderIntentStatus},
    },
    types::{
        EntryMakerRebateTerms, ExecutionAccountId, ExecutionOrderId, FeeMeasurement, MarketId,
        OrderIntentId, Price, RecommendationId, ReconciliationEvidence,
        ReconciliationEvidenceChain, Shares, Usd, VenueTradeId,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    ExecutionOrderRepository, ExecutionSubmissionRepository, OrderIntentRepository,
    RecommendationRepository, ReconciliationRepository, StrategyPositionLotRepository,
};
use quant_pivot_research::execution_semantics::{
    LiquidityRole, PitFeeSchedule, PitMakerRebateEvidence, PitMarketExecutionEconomics,
};

use super::{CollectedReconciliation, EvidenceCollector, VenuePresence};
use crate::{
    execution::{
        ExecutionBreaker, ExecutionOrderLifecyclePublisher, IntentLifecyclePublisher,
        PolymarketOrderClient,
    },
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
    terms_by_market: HashMap<MarketId, CurrentExecutionTerms>,
}

enum CurrentExecutionTerms {
    Available(Box<PitMarketExecutionEconomics>),
    Unavailable(String),
}

struct TermsDriftRequest<'a> {
    original_order: &'a ExecutionOrderInfo,
    effective_order: &'a ExecutionOrderInfo,
    identity_refs: &'a ExecutionOrderIdentityRefs,
    recommendation: &'a RecommendationInfo,
    intent_status: OrderIntentStatus,
    collected: CollectedReconciliation,
    context: &'a ReconcileContextMaps,
    now: DateTime<Utc>,
    stale_after: Duration,
}

fn load_reconcile_context(
    order: &ExecutionOrderInfo,
    context: &ReconcileContextMaps,
) -> QuantResult<(OrderIntentInfo, RecommendationInfo)> {
    let intent = context
        .intents_by_id
        .get(&order.order_intent_id)
        .cloned()
        .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, order.order_intent_id))?;
    let recommendation = context
        .recommendations_by_id
        .get(&intent.recommendation_id)
        .cloned()
        .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, intent.recommendation_id))?;
    Ok((intent, recommendation))
}

/// Collaborators for [`ReconciliationService`].
pub struct ReconciliationServiceDeps {
    pub collector: Arc<dyn EvidenceCollector>,
    pub order_client: Arc<dyn PolymarketOrderClient>,
    pub execution_orders: Arc<dyn ExecutionOrderRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub positions: Arc<dyn StrategyPositionLotRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub submission: Arc<dyn ExecutionSubmissionRepository>,
    pub catalog_ledger: Arc<dyn CatalogLedgerRepository>,
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub breaker: Arc<ExecutionBreaker>,
    pub metrics: Arc<MetricsHub>,
    pub config: Arc<DecisionPolicyStore>,
    pub execution_events: Arc<ExecutionEventWriter>,
    pub capital_events: Arc<CapitalAllocationEventWriter>,
    pub position_events: Arc<PositionEventWriter>,
    /// Fans out the settled `quant.intent` status after a reconciliation write.
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    /// Fans out the exact committed execution-order row after reconciliation.
    pub order_lifecycle: Arc<ExecutionOrderLifecyclePublisher>,
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

    fn passive_terms_drifted(
        order: &ExecutionOrderInfo,
        context: &ReconcileContextMaps,
    ) -> QuantResult<bool> {
        if !order.prepared_order_json.post_only {
            return Ok(false);
        }
        let current = match context.terms_by_market.get(&order.market_id) {
            Some(CurrentExecutionTerms::Available(current)) => current,
            Some(CurrentExecutionTerms::Unavailable(reason)) => {
                return Err(ExecutionError::ReconciliationUnresolvable {
                    reason: reason.clone(),
                }
                .into());
            }
            None => return Ok(true),
        };
        if current.fee_schedule.schedule_hash
            != order.prepared_order_json.fee_schedule.schedule_hash
        {
            return Ok(true);
        }
        Ok(
            match (
                order.prepared_order_json.maker_rebate_terms,
                &current.maker_rebate_evidence,
            ) {
                (
                    EntryMakerRebateTerms::PassiveNoProgram { terms_hash, .. },
                    PitMakerRebateEvidence::NoProgram {
                        terms_hash: current,
                        ..
                    },
                ) => terms_hash != *current,
                (
                    EntryMakerRebateTerms::PassiveProgram { schedule },
                    PitMakerRebateEvidence::Available { schedule: current },
                ) => {
                    schedule.terms_hash != current.terms_hash
                        || schedule.platform_rate != current.platform_rate
                        || schedule.exponent != current.exponent
                        || schedule.taker_only != current.taker_only
                        || schedule.rebate_rate != current.rebate_rate
                }
                _ => true,
            },
        )
    }

    async fn current_execution_terms(
        &self,
        market_id: &MarketId,
        now: DateTime<Utc>,
    ) -> QuantResult<PitMarketExecutionEconomics> {
        let boundary = DecisionClock::new(0).boundary(now)?;
        let (catalog, clob) = tokio::join!(
            self.deps.catalog_ledger.market_at(market_id, &boundary),
            self.deps.clob_market_info.at(market_id, now, now),
        );
        let catalog = catalog?.ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
            reason: format!("current Gamma catalog evidence is missing for market {market_id}"),
        })?;
        let clob = clob?.ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
            reason: format!("current CLOB market-info evidence is missing for market {market_id}"),
        })?;
        let market = catalog.verified_payload().map_err(|error| {
            ExecutionError::ReconciliationUnresolvable {
                reason: format!("current Gamma catalog object is invalid: {error}"),
            }
        })?;
        Ok(PitMarketExecutionEconomics::resolve(
            &clob.fee_schedule(),
            &market.maker_rebate_evidence,
            catalog.available_at,
            now,
        )
        .map_err(|error| ExecutionError::ReconciliationUnresolvable {
            reason: format!("current resting-order terms are invalid: {error:?}"),
        })?)
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
        self.reconcile_orders(orders, now, stale_after).await
    }

    /// Immediately guard resting orders in markets whose committed execution
    /// terms changed. The periodic pass remains the durable recovery backstop.
    pub async fn reconcile_terms_changes(
        &self,
        now: DateTime<Utc>,
        market_ids: &[MarketId],
    ) -> QuantResult<()> {
        if market_ids.is_empty() {
            return Ok(());
        }
        let policy = self
            .deps
            .config
            .current()
            .execution_risk
            .reconciliation
            .clone();
        let stale_after =
            Duration::seconds(i64::try_from(policy.stale_open_secs).unwrap_or(i64::MAX));
        let orders = self
            .deps
            .execution_orders
            .find_reconcilable_for_markets(market_ids, RECONCILE_BATCH)
            .await?;
        self.reconcile_orders(orders, now, stale_after).await
    }

    async fn reconcile_orders(
        &self,
        orders: Vec<ExecutionOrderInfo>,
        now: DateTime<Utc>,
        stale_after: Duration,
    ) -> QuantResult<()> {
        let context = self.preload_reconcile_context(&orders, now).await?;
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
        now: DateTime<Utc>,
    ) -> QuantResult<ReconcileContextMaps> {
        let intent_ids: Vec<OrderIntentId> = orders
            .iter()
            .map(|order| order.order_intent_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let intents = self.deps.intents.find_by_ids(&intent_ids).await?;
        let intents_by_id: HashMap<OrderIntentId, OrderIntentInfo> = intents
            .into_iter()
            .map(|intent| (intent.order_intent_id, intent))
            .collect();
        let recommendation_ids: Vec<RecommendationId> = intents_by_id
            .values()
            .map(|intent| intent.recommendation_id)
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
            .map(|rec| (rec.recommendation_id, rec))
            .collect();
        let market_ids = orders
            .iter()
            .filter(|order| order.prepared_order_json.post_only)
            .map(|order| order.market_id.clone())
            .collect::<HashSet<_>>();
        let terms_by_market = join_all(market_ids.into_iter().map(|market_id| async move {
            let terms = self
                .current_execution_terms(&market_id, now)
                .await
                .map_or_else(
                    |error| CurrentExecutionTerms::Unavailable(error.to_string()),
                    |terms| CurrentExecutionTerms::Available(Box::new(terms)),
                );
            (market_id, terms)
        }))
        .await
        .into_iter()
        .collect();
        Ok(ReconcileContextMaps {
            intents_by_id,
            recommendations_by_id,
            terms_by_market,
        })
    }

    async fn cancel_terms_drift(
        &self,
        request: TermsDriftRequest<'_>,
    ) -> QuantResult<Option<CollectedReconciliation>> {
        let TermsDriftRequest {
            original_order,
            effective_order,
            identity_refs,
            recommendation,
            intent_status,
            mut collected,
            context,
            now,
            stale_after,
        } = request;
        let drifted = match Self::passive_terms_drifted(effective_order, context) {
            Ok(drifted) => drifted,
            Err(error) => {
                tracing::warn!(
                    execution_order_id = %effective_order.execution_order_id,
                    error = %error,
                    "resting-order terms evidence is unavailable; cancelling fail-closed"
                );
                true
            }
        };
        if !drifted {
            return Ok(Some(collected));
        }
        self.deps
            .metrics
            .record_maker_rebate_diagnostic("terms_drift", "cancel_requested");
        let Some(venue_order_id) = effective_order.venue_order_id.as_ref() else {
            collected.evidence.push(system_note(
                ReconciliationEvidenceKind::ClobOrderStatus,
                "terms drift detected but no exact venue order id is available".to_owned(),
                now,
            ));
            self.apply_unresolvable(
                original_order,
                intent_status,
                collected.evidence,
                recommendation,
            )
            .await?;
            return Ok(None);
        };
        let cancel = self.deps.order_client.cancel(venue_order_id).await;
        self.deps.metrics.record_maker_rebate_diagnostic(
            "terms_drift_cancel",
            if cancel.cancelled {
                "cancelled"
            } else {
                "race_or_rejected"
            },
        );
        match self
            .deps
            .collector
            .collect(effective_order, identity_refs, now, stale_after)
            .await
        {
            Ok(mut recollected) => {
                recollected.evidence.push(system_note(
                    ReconciliationEvidenceKind::ClobOrderStatus,
                    format!(
                        "terms drift cancellation requested; cancelled={}",
                        cancel.cancelled
                    ),
                    cancel.responded_at,
                ));
                Ok(Some(recollected))
            }
            Err(error) => {
                collected.evidence.push(system_note(
                    ReconciliationEvidenceKind::ClobOrderStatus,
                    format!("terms drift cancellation recollection failed: {error}"),
                    cancel.responded_at,
                ));
                self.apply_unresolvable(
                    original_order,
                    intent_status,
                    collected.evidence,
                    recommendation,
                )
                .await?;
                Ok(None)
            }
        }
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

        let identity_refs = self
            .deps
            .submission
            .load_identity_refs(&order.execution_order_id)
            .await?;

        // Collect venue evidence. A venue read failure is fail-closed: freeze
        // only once the order is past the staleness deadline, else retry later.
        let collected = match self
            .deps
            .collector
            .collect(order, &identity_refs, now, stale_after)
            .await
        {
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

        // Exact order/trade lookup is identity enrichment, never a placement
        // verdict rewrite. Persist it before any cancellation or terminal money
        // transition so restart recovery resumes from the same identities.
        let discovered_order_id = collected.identity_enrichment.discovered_order_id.clone();
        let identity_refs = self
            .deps
            .submission
            .enrich_identity_refs(
                &order.execution_order_id,
                collected.identity_enrichment.clone(),
            )
            .await?;
        let mut effective_order = order.clone();
        if effective_order.venue_order_id.is_none() {
            effective_order.venue_order_id = discovered_order_id;
        }

        let Some(collected) = self
            .cancel_terms_drift(TermsDriftRequest {
                original_order: order,
                effective_order: &effective_order,
                identity_refs: &identity_refs,
                recommendation: &recommendation,
                intent_status: intent.status,
                collected,
                context,
                now,
                stale_after,
            })
            .await?
        else {
            return Ok(());
        };

        // Actively cancel a stale (or GTD-expired) resting order, then re-collect
        // the post-cancel truth so unfilled capital is released promptly.
        let (collected, recollected) = recollect_after_stale_cancel(
            &self.deps.collector,
            &self.deps.order_client,
            &effective_order,
            &identity_refs,
            collected,
            now,
            stale_after,
        )
        .await;
        if recollected {
            self.deps
                .submission
                .enrich_identity_refs(
                    &order.execution_order_id,
                    collected.identity_enrichment.clone(),
                )
                .await?;
        }

        let decision = collected.facts.decide();
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
            venue_terminal: decision.venue_terminal,
            expired: decision.expired,
            resolved_by: WORKER_ACTOR.to_owned(),
            exit_reason: intent.exit_reason,
        };
        let write = Self::build_terminal_write(
            order,
            &recommendation,
            intent.execution_account_id,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(evidence),
            now,
        )?;
        let exit_realized_pnl = write
            .cumulative_exit
            .as_ref()
            .map(|exit| exit.cumulative_realized_pnl_usd);
        let recorded = self
            .deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
        self.deps.order_lifecycle.transition(order, &recorded, now);
        self.mirror_ledger_events(
            &recorded,
            recommendation.recommendation_id,
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
                StorageError::not_found(QUANT_EXECUTION_ORDER, resolution.execution_order_id)
            })?;
        let intent = self
            .deps
            .intents
            .find_by_id(&order.order_intent_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_ORDER_INTENT, order.order_intent_id))?;
        let recommendation = self
            .deps
            .recommendations
            .find_by_id(&intent.recommendation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECOMMENDATION, intent.recommendation_id)
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
            venue_terminal: true,
            expired: false,
            resolved_by: resolution.operator.clone(),
            exit_reason: intent.exit_reason,
        };
        let write = Self::build_terminal_write(
            &order,
            &recommendation,
            intent.execution_account_id,
            lot.as_ref(),
            terminal,
            ReconciliationEvidenceChain(vec![note]),
            now,
        )?;
        let exit_realized_pnl = write
            .cumulative_exit
            .as_ref()
            .map(|exit| exit.cumulative_realized_pnl_usd);
        let recorded = self
            .deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
        self.deps.order_lifecycle.transition(&order, &recorded, now);
        self.mirror_ledger_events(
            &recorded,
            recommendation.recommendation_id,
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
            cumulative_fill: None,
            cumulative_exit: None,
            exit_state: None,
            revert_lot: false,
            result: ReconciliationResult::Unresolvable,
            evidence: ReconciliationEvidenceChain(evidence),
            venue_filled_shares: None,
            venue_avg_price: None,
            expected_cash_delta_usd: None,
            venue_cash_delta_usd: None,
            realized_pnl_usd: None,
            resolved_by: None,
            resolved_at: None,
        };
        let recorded = self
            .deps
            .submission
            .apply_reconciliation(&order.execution_order_id, write)
            .await?;
        self.deps
            .order_lifecycle
            .transition(order, &recorded, Utc::now());

        self.mirror_ledger_events(
            &recorded,
            recommendation.recommendation_id,
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
    async fn exit_lot(
        &self,
        order: &ExecutionOrderInfo,
    ) -> QuantResult<Option<StrategyPositionLot>> {
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
        execution_account_id: ExecutionAccountId,
        lot: Option<&StrategyPositionLot>,
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
                venue_terminal,
                expired,
                resolved_by,
                exit_reason,
                ..
            } = decision;
            return exit_reconcile_write(ExitReconcileWriteInput {
                write,
                order,
                lot,
                filled_shares,
                avg_price,
                venue_terminal,
                expired,
                resolved_by,
                exit_reason: exit_reason.unwrap_or(ExitReason::Manual),
                now,
            });
        }

        entry_reconcile_write(
            write,
            EntryReconcileInput {
                order,
                recommendation,
                execution_account_id,
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
    venue_terminal: bool,
    expired: bool,
    resolved_by: String,
    /// Frozen exit trigger on the intent (exit-order reconciliation only).
    exit_reason: Option<ExitReason>,
}

/// Inputs for applying an entry-order terminal verdict to a ledger write.
struct EntryReconcileInput<'a> {
    order: &'a ExecutionOrderInfo,
    recommendation: &'a RecommendationInfo,
    execution_account_id: ExecutionAccountId,
    decision: TerminalDecision,
    now: DateTime<Utc>,
}

struct ReconciliationFee {
    applied: Usd,
}

#[derive(Clone, Copy)]
struct TradeFeeAggregate {
    shares: Shares,
    expected: Usd,
    derived: Option<Usd>,
}

fn reconciliation_fee(
    order: &ExecutionOrderInfo,
    evidence: &ReconciliationEvidenceChain,
    filled_shares: Shares,
    price: Price,
    matched_at: DateTime<Utc>,
) -> QuantResult<ReconciliationFee> {
    let prepared = &order.prepared_order_json.fee_schedule;
    let schedule = PitFeeSchedule {
        schedule_hash: prepared.schedule_hash,
        effective_at: prepared.effective_at,
        available_at: prepared.available_at,
        platform_rate: prepared.platform_rate,
        exponent: prepared.exponent,
        taker_only: prepared.taker_only,
        builder_maker_fee_bps: prepared.builder_maker_fee_bps,
        builder_taker_fee_bps: prepared.builder_taker_fee_bps,
        builder_attribution: prepared.builder_attribution,
    };
    let mut trades = BTreeMap::<VenueTradeId, TradeFeeAggregate>::new();
    for item in &evidence.0 {
        let Some(measurement) = item.fee_evidence.as_ref() else {
            continue;
        };
        let (trade_id, role, observed_at, derived) = match measurement {
            FeeMeasurement::PreparedExpected { .. } => continue,
            FeeMeasurement::AuthenticatedTradeDerived {
                trade_id,
                liquidity_role,
                derived_fee,
                matched_at,
                ..
            } => (
                trade_id.clone(),
                *liquidity_role,
                *matched_at,
                Some(*derived_fee),
            ),
            FeeMeasurement::OnChainSettled { .. } => continue,
        };
        let shares = item
            .shares
            .ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
                reason: format!("fee evidence for trade {trade_id} has no fill shares"),
            })?;
        let fill_price = item
            .price
            .ok_or_else(|| ExecutionError::ReconciliationUnresolvable {
                reason: format!("fee evidence for trade {trade_id} has no fill price"),
            })?;
        let liquidity_role = match role {
            FeeLiquidityRole::Maker => LiquidityRole::Maker,
            FeeLiquidityRole::Taker => LiquidityRole::Taker,
        };
        let expected = schedule
            .fee(liquidity_role, fill_price, shares, observed_at)
            .map_err(|error| ExecutionError::ReconciliationUnresolvable {
                reason: format!(
                    "frozen fee schedule cannot price authenticated trade {trade_id}: {error:?}"
                ),
            })?;
        trades
            .entry(trade_id.clone())
            .and_modify(|current| {
                if let Some(value) = derived {
                    current.derived = Some(value);
                }
            })
            .or_insert(TradeFeeAggregate {
                shares,
                expected,
                derived,
            });
    }
    if trades.is_empty() {
        let role = if order.prepared_order_json.post_only {
            LiquidityRole::Maker
        } else {
            LiquidityRole::Taker
        };
        let expected = schedule
            .fee(role, price, filled_shares, matched_at)
            .map_err(|error| ExecutionError::ReconciliationUnresolvable {
                reason: format!("frozen fee schedule cannot price operator fill: {error:?}"),
            })?;
        return Ok(ReconciliationFee { applied: expected });
    }
    let authenticated_shares = trades
        .values()
        .fold(Shares::ZERO, |total, trade| total + trade.shares);
    if authenticated_shares != filled_shares {
        return Err(ExecutionError::ReconciliationUnresolvable {
            reason: format!(
                "authenticated trade shares {authenticated_shares} differ from cumulative fill {filled_shares}"
            ),
        }
        .into());
    }
    let applied = trades.values().fold(Usd::ZERO, |total, trade| {
        total + trade.derived.unwrap_or(trade.expected)
    });
    Ok(ReconciliationFee { applied })
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
        cumulative_fill: None,
        cumulative_exit: None,
        exit_state: None,
        revert_lot: false,
        result,
        evidence,
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
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
        execution_account_id,
        decision:
            TerminalDecision {
                result,
                filled_shares,
                avg_price,
                venue_terminal,
                expired,
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
            } else if venue_terminal {
                if expired {
                    ExecutionOrderState::Failed
                } else {
                    ExecutionOrderState::Cancelled
                }
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
            } else if venue_terminal && expired {
                VenueOrderStatus::Expired
            } else if venue_terminal {
                VenueOrderStatus::Cancelled
            } else {
                VenueOrderStatus::PartiallyFilled
            });
            write.filled_at = order.filled_at.or(Some(now));
            write.capital = if full || venue_terminal {
                CapitalReconcileSettlement::Settle { spent_usd: spent }
            } else {
                CapitalReconcileSettlement::SettlePartial { spent_usd: spent }
            };
            write.cumulative_fill = Some(cumulative_position_fill(
                order,
                recommendation,
                execution_account_id,
                filled_shares,
                spent,
                now,
            ));
            write.venue_filled_shares = Some(filled_shares);
            write.venue_avg_price = avg_price;
            write.expected_cash_delta_usd =
                Some(Usd::new(order.prepared_order_json.total_cash_delta));
            write.venue_cash_delta_usd = Some(Usd::new(
                -((filled_shares * price).inner() + fee.applied.inner()),
            ));
            if full || venue_terminal {
                write.resolved_by = Some(resolved_by);
                write.resolved_at = Some(now);
            }
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
    lot: &StrategyPositionLot,
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
    lot: Option<&'a StrategyPositionLot>,
    filled_shares: Shares,
    avg_price: Option<Price>,
    venue_terminal: bool,
    expired: bool,
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
        venue_terminal,
        expired,
        resolved_by,
        exit_reason,
        now,
    } = input;
    match write.result {
        ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
            let full = write.result == ReconciliationResult::Filled;
            write.order_state = if full {
                ExecutionOrderState::Filled
            } else if venue_terminal && expired {
                ExecutionOrderState::Failed
            } else if venue_terminal {
                ExecutionOrderState::Cancelled
            } else {
                ExecutionOrderState::PartiallyFilled
            };
            write.venue_status = Some(if full {
                VenueOrderStatus::Filled
            } else if venue_terminal && expired {
                VenueOrderStatus::Expired
            } else if venue_terminal {
                VenueOrderStatus::Cancelled
            } else {
                VenueOrderStatus::PartiallyFilled
            });
            write.filled_at = order.filled_at.or(Some(now));
            if venue_terminal && !full {
                write.cancelled_at = Some(now);
            }
            write.venue_filled_shares = Some(filled_shares);
            write.venue_avg_price = avg_price;
            if full || venue_terminal {
                write.resolved_by = Some(resolved_by);
                write.resolved_at = Some(now);
            }

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
            write.expected_cash_delta_usd =
                Some(Usd::new(order.prepared_order_json.total_cash_delta));
            write.venue_cash_delta_usd = Some(proceeds_usd);
            write.realized_pnl_usd = Some(realized_pnl_usd);
            write.cumulative_exit = Some(CumulativePositionExit {
                cumulative_shares: filled_shares,
                avg_price: exit_price,
                cumulative_proceeds_usd: proceeds_usd,
                cumulative_realized_pnl_usd: realized_pnl_usd,
                observed_at: now,
                reason: exit_reason,
            });
            write.exit_state = Some(ExitState::PartiallyExited);
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
    identity_refs: &ExecutionOrderIdentityRefs,
    collected: CollectedReconciliation,
    now: DateTime<Utc>,
    stale_after: Duration,
) -> (CollectedReconciliation, bool) {
    if collected.facts.presence == VenuePresence::Resting
        && (collected.facts.past_stale_deadline || collected.facts.gtd_expired)
        && let Some(venue_order_id) = order.venue_order_id.as_ref()
    {
        let _ = order_client.cancel(venue_order_id).await;
        return collector
            .collect(order, identity_refs, now, stale_after)
            .await
            .map_or((collected, false), |recollected| (recollected, true));
    }
    (collected, false)
}

/// Build the position upsert for a confirmed fill.
fn cumulative_position_fill(
    order: &ExecutionOrderInfo,
    recommendation: &RecommendationInfo,
    execution_account_id: ExecutionAccountId,
    shares: Shares,
    cost_usd: Usd,
    now: DateTime<Utc>,
) -> CumulativePositionFill {
    CumulativePositionFill {
        order_intent_id: order.order_intent_id,
        execution_account_id,
        token_id: order.token_id.clone(),
        market_id: order.market_id.clone(),
        event_id: Some(recommendation.event_id.clone()),
        category: recommendation.identity.category,
        side: recommendation.outcome_side,
        cumulative_shares: shares,
        cumulative_cost_usd: cost_usd,
        observed_at: now,
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
        domain::quant::{ExecutionOrderInfo, StrategyPositionLot},
        enums::{
            common::{MarketCategory, OrderType, Side},
            execution::{
                ExecutionOrderPhase, ExitReason, OrderTypeKind, PositionLedgerState,
                ReconciliationResult, StrategyPositionOriginKind,
            },
            quant::{AccountSource, ExecutionOrderState, OutcomeSide},
        },
        types::{
            ExecutionAccountId, ExecutionOrderId, MarketId, OrderIntentId, Price,
            ReconciliationEvidenceChain, Shares, StrategyPositionLotId, TokenId, Usd,
            VenueOrderAmount,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ExitReconcileWriteInput, compute_exit_realized_pnl, exit_reconcile_write,
        neutral_terminal_write,
    };
    use crate::test_fixtures::execution_pg_seed::prepared_order;

    fn lot(avg: Decimal) -> StrategyPositionLot {
        StrategyPositionLot {
            strategy_position_lot_id: StrategyPositionLotId::from_v7(),
            origin_kind: StrategyPositionOriginKind::SystemIntent,
            order_intent_id: Some(OrderIntentId::from_v7()),
            recovery_incident_id: None,
            execution_account_id: ExecutionAccountId::from_v7(),
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
    fn exit_realized_pnl_fee() {
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
    fn exit_reconcile_write_fill() {
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
            venue_terminal: true,
            expired: false,
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::StopLoss,
            now: Utc::now(),
        })
        .expect("frozen fee fixture");
        assert!(filled.cumulative_exit.is_some());
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
            venue_terminal: true,
            expired: false,
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::Manual,
            now: Utc::now(),
        })
        .expect("non-fill does not quote fees");
        assert!(cancelled.cumulative_exit.is_none());
    }

    #[test]
    fn exit_reconcile_preserves_reason() {
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
            venue_terminal: true,
            expired: false,
            resolved_by: "test".to_owned(),
            exit_reason: ExitReason::StopLoss,
            now: Utc::now(),
        })
        .expect("CLOB fee fixture");
        assert_eq!(
            write.cumulative_exit.as_ref().expect("exit fill").reason,
            ExitReason::StopLoss
        );
    }
}
