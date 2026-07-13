//! Final recommendation-attribution builder and sweep service.
//!
//! Attribution is a WORM terminal fact: the builder only emits rows when the
//! execution ledger is unambiguous. Open positions, resting / ambiguous orders,
//! and unresolvable reconciliation are skipped so a later sweep can write the
//! single final row once truth is available.

use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    clickhouse::{ChDecimal64, ChUsd, QuantRecommendationAttributionEventRow},
    domain::{
        ExecutionOrderInfo, InsertFinalOutcome, NewRecommendationAttribution, OrderIntentInfo,
        PositionInfo, RecommendationAttributionInfo, RecommendationInfo, ReconciliationInfo,
    },
    enums::{
        execution::{ExecutionOrderPhase, ExitReason, PositionLedgerState},
        quant::{
            ExecutionOrderState, OrderIntentStatus, RecommendationAttributionOutcome,
            RecommendationOutcome,
        },
    },
    types::{AttributionDetail, Bps, EntryOutcome, ExitOutcome, Price, RecommendationId, Usd},
};
use quant_pivot_repository::traits::{
    AttributionRepository, ExecutionOrderRepository, OrderIntentRepository, PositionRepository,
    RecommendationRepository, ReconciliationRepository,
};

use crate::observability::attribution_fact_writer::AttributionEventWriter;
use rust_decimal::Decimal;

/// Dependencies for [`AttributionService`].
pub struct AttributionServiceDeps {
    pub attribution: Arc<dyn AttributionRepository>,
    pub intents: Arc<dyn OrderIntentRepository>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub execution_orders: Arc<dyn ExecutionOrderRepository>,
    pub positions: Arc<dyn PositionRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub attribution_events: Arc<AttributionEventWriter>,
}

/// One attribution sweep result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AttributionPassSummary {
    pub considered: u64,
    pub written: u64,
    pub skipped: u64,
}

/// Builds and persists final attribution rows for recommendations whose ledger
/// state has reached an unambiguous terminal outcome.
pub struct AttributionService {
    attribution: Arc<dyn AttributionRepository>,
    intents: Arc<dyn OrderIntentRepository>,
    recommendations: Arc<dyn RecommendationRepository>,
    execution_orders: Arc<dyn ExecutionOrderRepository>,
    positions: Arc<dyn PositionRepository>,
    reconciliation: Arc<dyn ReconciliationRepository>,
    attribution_events: Arc<AttributionEventWriter>,
}

impl AttributionService {
    #[must_use]
    pub fn new(deps: AttributionServiceDeps) -> Self {
        Self {
            attribution: deps.attribution,
            intents: deps.intents,
            recommendations: deps.recommendations,
            execution_orders: deps.execution_orders,
            positions: deps.positions,
            reconciliation: deps.reconciliation,
            attribution_events: deps.attribution_events,
        }
    }

    /// Run one bounded attribution sweep.
    ///
    /// Duplicate/racing rows are not updated: an existing attribution row wins
    /// and this pass simply skips that recommendation.
    pub async fn run_pass(
        &self,
        now: DateTime<Utc>,
        batch_size: u64,
    ) -> QuantResult<AttributionPassSummary> {
        if batch_size == 0 {
            return Ok(AttributionPassSummary::default());
        }

        let mut summary = AttributionPassSummary::default();
        let mut seen = HashSet::<RecommendationId>::new();
        let intent_candidates = self
            .intents
            .find_attribution_candidates(
                OrderIntentStatus::ATTRIBUTION_ELIGIBLE.to_vec(),
                batch_size,
            )
            .await?;
        let recommendations_by_id = self
            .preload_recommendations_for_intents(&intent_candidates)
            .await?;

        for intent in intent_candidates {
            if summary.considered >= batch_size {
                break;
            }
            if !seen.insert(intent.recommendation_id.clone()) {
                continue;
            }
            summary.considered += 1;
            let recommendation = recommendations_by_id.get(&intent.recommendation_id);
            match self.build_from_intent(now, &intent, recommendation).await? {
                Some(row) => {
                    self.insert_final(row, &mut summary).await?;
                }
                None => summary.skipped += 1,
            }
        }

        let remaining = batch_size.saturating_sub(summary.considered);
        if remaining == 0 {
            return Ok(summary);
        }

        let expired = self
            .recommendations
            .find_expired_attribution_candidates(remaining)
            .await?;
        for recommendation in expired {
            if summary.considered >= batch_size {
                break;
            }
            if !seen.insert(recommendation.recommendation_id.clone()) {
                continue;
            }
            summary.considered += 1;
            if recommendation.status.excluded_from_attribution() {
                summary.skipped += 1;
                continue;
            }
            if self
                .recommendations
                .recommendation_blocks_final_attribution(&recommendation.recommendation_id)
                .await?
            {
                summary.skipped += 1;
                continue;
            }
            let row = Self::build_expired_unfilled(now, &recommendation);
            self.insert_final(row, &mut summary).await?;
        }

        Ok(summary)
    }

    async fn insert_final(
        &self,
        row: NewRecommendationAttribution,
        summary: &mut AttributionPassSummary,
    ) -> QuantResult<()> {
        match self
            .attribution
            .insert_final_and_mark_attributed(row)
            .await?
        {
            InsertFinalOutcome::Written(info) => {
                self.mirror_attribution_event(info.as_ref());
                summary.written += 1;
            }
            InsertFinalOutcome::AlreadyExists => summary.skipped += 1,
        }
        Ok(())
    }

    fn mirror_attribution_event(&self, info: &RecommendationAttributionInfo) {
        let ingestion_time = Utc::now().timestamp_millis();
        let row = QuantRecommendationAttributionEventRow {
            event_time: info.created_at.timestamp_millis(),
            recommendation_id: info.recommendation_id.clone(),
            outcome: info.outcome.into(),
            realized_pnl_usd: ChUsd::from(info.realized_pnl_usd.unwrap_or(Usd::ZERO)),
            max_adverse_excursion_bps: info.max_adverse_excursion_bps.map(ChDecimal64::from),
            max_favorable_excursion_bps: ChDecimal64::from(
                info.max_favorable_excursion_bps.unwrap_or(Decimal::ZERO),
            ),
            label_available_at: info
                .label_available_at
                .map_or(0, |timestamp| timestamp.timestamp_millis()),
            ingestion_time,
        };
        self.attribution_events.write(row);
    }

    async fn preload_recommendations_for_intents(
        &self,
        intents: &[OrderIntentInfo],
    ) -> QuantResult<HashMap<RecommendationId, RecommendationInfo>> {
        let recommendation_ids: Vec<RecommendationId> = intents
            .iter()
            .map(|intent| intent.recommendation_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let rows = self
            .recommendations
            .find_by_ids(&recommendation_ids)
            .await?;
        Ok(rows
            .into_iter()
            .map(|rec| (rec.recommendation_id.clone(), rec))
            .collect())
    }

    async fn build_from_intent(
        &self,
        now: DateTime<Utc>,
        intent: &OrderIntentInfo,
        recommendation: Option<&RecommendationInfo>,
    ) -> QuantResult<Option<NewRecommendationAttribution>> {
        // Candidate rows are already terminal-status filtered; skip when the
        // preloaded recommendation map missed the id (race / orphan).
        let Some(recommendation) = recommendation.cloned() else {
            tracing::warn!(
                recommendation_id = %intent.recommendation_id,
                intent_id = %intent.order_intent_id,
                "attribution candidate references a missing recommendation",
            );
            return Ok(None);
        };
        if recommendation.status.excluded_from_attribution() {
            return Ok(None);
        }
        let orders = self
            .execution_orders
            .find_by_intent(&intent.order_intent_id)
            .await?;
        if orders.iter().any(|order| order.state.blocks_attribution()) {
            return Ok(None);
        }
        let position = self
            .positions
            .find_by_intent(&intent.order_intent_id)
            .await?;

        if let Some(position) = position.as_ref() {
            return self
                .build_filled_terminal(now, intent, &recommendation, &orders, position)
                .await;
        }

        if orders.iter().any(entry_order_filled) {
            return Ok(None);
        }

        let outcome = match intent.status {
            OrderIntentStatus::Expired => RecommendationAttributionOutcome::ExpiredUnfilled,
            OrderIntentStatus::Cancelled => RecommendationAttributionOutcome::CancelledUnfilled,
            OrderIntentStatus::Failed
            | OrderIntentStatus::Rejected
            | OrderIntentStatus::AdmissionRejected
            | OrderIntentStatus::Invalidated => RecommendationAttributionOutcome::FailedUnfilled,
            _ => return Ok(None),
        };
        Ok(Some(unfilled_row(
            now,
            &recommendation,
            outcome,
            intent.status,
        )))
    }

    fn build_expired_unfilled(
        now: DateTime<Utc>,
        recommendation: &RecommendationInfo,
    ) -> NewRecommendationAttribution {
        NewRecommendationAttribution {
            recommendation_id: recommendation.recommendation_id.clone(),
            outcome: RecommendationAttributionOutcome::ExpiredUnfilled,
            entry_outcome_json: EntryOutcome::default(),
            exit_outcome_json: ExitOutcome {
                settlement_outcome: Some(RecommendationOutcome::ExpiredUnfilled),
                ..ExitOutcome::default()
            },
            realized_pnl_usd: Some(Usd::ZERO),
            max_adverse_excursion_bps: Some(Decimal::ZERO),
            max_favorable_excursion_bps: Some(Decimal::ZERO),
            label_available_at: Some(now),
            attribution_json: AttributionDetail {
                notes: vec!["expired_unfilled_without_intent".to_owned()],
                ..AttributionDetail::default()
            },
        }
    }

    async fn build_filled_terminal(
        &self,
        now: DateTime<Utc>,
        intent: &OrderIntentInfo,
        recommendation: &RecommendationInfo,
        orders: &[ExecutionOrderInfo],
        position: &PositionInfo,
    ) -> QuantResult<Option<NewRecommendationAttribution>> {
        let outcome = match position.state {
            PositionLedgerState::Closed => RecommendationAttributionOutcome::FilledExited,
            PositionLedgerState::Settled => RecommendationAttributionOutcome::FilledSettled,
            PositionLedgerState::Open | PositionLedgerState::Closing => return Ok(None),
        };
        let Some(entry_order) = orders.iter().find(|order| {
            order.order_phase == ExecutionOrderPhase::Entry && entry_order_filled(order)
        }) else {
            return Ok(None);
        };
        let entry_reconciliation = self
            .reconciliation
            .find_by_execution_order(&entry_order.execution_order_id)
            .await?;
        let exit_order = latest_filled_exit_order(orders);
        let exit_reconciliation = match exit_order {
            Some(order) => {
                self.reconciliation
                    .find_by_execution_order(&order.execution_order_id)
                    .await?
            }
            None => None,
        };

        if entry_reconciliation
            .as_ref()
            .is_some_and(|row| row.result.blocks_final_attribution())
        {
            return Ok(None);
        }
        if exit_reconciliation
            .as_ref()
            .is_some_and(|row| row.result.blocks_final_attribution())
        {
            return Ok(None);
        }

        Ok(Some(NewRecommendationAttribution {
            recommendation_id: recommendation.recommendation_id.clone(),
            outcome,
            entry_outcome_json: entry_outcome(entry_order, entry_reconciliation.as_ref(), intent),
            exit_outcome_json: exit_outcome(
                exit_order,
                exit_reconciliation.as_ref(),
                intent,
                position,
                outcome,
            ),
            realized_pnl_usd: Some(position.realized_pnl_usd),
            // MAE: deferred to 06.6 book replay; see phase-06/06.6 §3.
            max_adverse_excursion_bps: None,
            max_favorable_excursion_bps: favorable_excursion_bps(
                entry_anchor_price(position, entry_order),
                intent.peak_mark_price,
            ),
            label_available_at: Some(now),
            attribution_json: AttributionDetail {
                hit_stop_loss: intent.exit_reason == Some(ExitReason::StopLoss),
                hit_take_profit: intent.exit_reason == Some(ExitReason::TakeProfit),
                liquidity_exit_possible: exit_order.is_some(),
                notes: vec!["filled_terminal_from_execution_ledger".to_owned()],
            },
        }))
    }
}

fn favorable_excursion_bps(entry: Price, peak: Option<Price>) -> Option<Decimal> {
    let peak = peak?;
    Bps::spread(peak, entry).and_then(|spread| {
        if spread.inner() > Decimal::ZERO {
            Some(spread.inner())
        } else {
            None
        }
    })
}

/// Lot entry anchor for MFE: closed lots zero out `avg_price`, so fall back to the
/// filled entry order's price (05.7).
const fn entry_anchor_price(position: &PositionInfo, entry_order: &ExecutionOrderInfo) -> Price {
    if position.avg_price.is_zero() {
        entry_order.price
    } else {
        position.avg_price
    }
}

fn entry_order_filled(order: &ExecutionOrderInfo) -> bool {
    order.order_phase == ExecutionOrderPhase::Entry
        && matches!(
            order.state,
            ExecutionOrderState::Filled | ExecutionOrderState::PartiallyFilled
        )
}

fn latest_filled_exit_order(orders: &[ExecutionOrderInfo]) -> Option<&ExecutionOrderInfo> {
    orders
        .iter()
        .filter(|order| {
            order.order_phase == ExecutionOrderPhase::Exit
                && matches!(
                    order.state,
                    ExecutionOrderState::Filled | ExecutionOrderState::PartiallyFilled
                )
        })
        .max_by_key(|order| order.filled_at.unwrap_or(order.updated_at))
}

fn entry_outcome(
    order: &ExecutionOrderInfo,
    reconciliation: Option<&ReconciliationInfo>,
    intent: &OrderIntentInfo,
) -> EntryOutcome {
    let fill_price = reconciliation
        .and_then(|row| row.venue_avg_price)
        .or(Some(order.price));
    let fill_shares = reconciliation
        .and_then(|row| row.venue_filled_shares)
        .or(Some(order.shares));
    EntryOutcome {
        entry_filled: true,
        fill_price,
        fill_shares,
        entry_slippage_bps: fill_price
            .and_then(|price| Bps::spread(price, intent.entry_order_json.limit_price)),
        filled_at: order.filled_at,
    }
}

fn exit_outcome(
    order: Option<&ExecutionOrderInfo>,
    reconciliation: Option<&ReconciliationInfo>,
    intent: &OrderIntentInfo,
    position: &PositionInfo,
    outcome: RecommendationAttributionOutcome,
) -> ExitOutcome {
    let exit_price = order
        .map(|row| row.price)
        .or_else(|| reconciliation.and_then(|row| row.venue_avg_price));
    let exit_shares = order
        .map(|row| row.shares)
        .or_else(|| reconciliation.and_then(|row| row.venue_filled_shares));
    ExitOutcome {
        exit_price,
        exit_shares,
        exit_trigger: intent.exit_reason,
        exit_compliance: order.is_some()
            || outcome == RecommendationAttributionOutcome::FilledSettled,
        settlement_outcome: Some(realized_settlement_outcome(position.realized_pnl_usd)),
        exited_at: position
            .closed_at
            .or_else(|| order.and_then(|row| row.filled_at)),
    }
}

fn realized_settlement_outcome(realized_pnl_usd: Usd) -> RecommendationOutcome {
    match realized_pnl_usd.cmp(&Usd::ZERO) {
        Greater => RecommendationOutcome::Won,
        Less => RecommendationOutcome::Lost,
        Equal => RecommendationOutcome::Unknown,
    }
}

fn unfilled_row(
    now: DateTime<Utc>,
    recommendation: &RecommendationInfo,
    outcome: RecommendationAttributionOutcome,
    intent_status: OrderIntentStatus,
) -> NewRecommendationAttribution {
    NewRecommendationAttribution {
        recommendation_id: recommendation.recommendation_id.clone(),
        outcome,
        entry_outcome_json: EntryOutcome::default(),
        exit_outcome_json: ExitOutcome {
            settlement_outcome: Some(match outcome {
                RecommendationAttributionOutcome::ExpiredUnfilled => {
                    RecommendationOutcome::ExpiredUnfilled
                }
                RecommendationAttributionOutcome::CancelledUnfilled => {
                    RecommendationOutcome::Cancelled
                }
                RecommendationAttributionOutcome::FailedUnfilled
                | RecommendationAttributionOutcome::FilledExited
                | RecommendationAttributionOutcome::FilledSettled => RecommendationOutcome::Unknown,
            }),
            ..ExitOutcome::default()
        },
        realized_pnl_usd: Some(Usd::ZERO),
        max_adverse_excursion_bps: Some(Decimal::ZERO),
        max_favorable_excursion_bps: Some(Decimal::ZERO),
        label_available_at: Some(now),
        attribution_json: AttributionDetail {
            notes: vec![format!(
                "unfilled_terminal_intent_status={}",
                intent_status.as_str()
            )],
            ..AttributionDetail::default()
        },
    }
}
