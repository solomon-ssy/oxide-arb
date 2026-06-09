//! Idempotent post-trade consumer: applies side-effects for one observed trade.

use crate::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::fsm::ExecutionFSM,
    observability::{execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub},
    service::risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{
        CoreEvent, CoreEventPublisher,
        calibration::NewCalibrationOutcome,
        position::{NewPosition, PositionInfo},
        scored_snapshot::ScoredOpportunitySnapshot,
        trade::{PostTradeInput, TradeInfo},
    },
    enums::{
        common::{RedeemStatus, Side},
        risk::TradeAccountingPhase,
    },
    types::{PositionId, Probability},
};
use oxide_arb_repository::traits::{CalibrationRepository, PositionRepository, TradeRepository};
use oxide_arb_risk::engine::RiskEngine;
use std::sync::Arc;

/// Dependencies for processing observed trades into terminal state.
///
/// Constructed via a struct literal at the wiring site (see `app::build`).
pub struct PostTradeConsumer {
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub fsm: Arc<ExecutionFSM>,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub position_repo: Arc<dyn PositionRepository>,
    pub calibration_repo: Arc<dyn CalibrationRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    pub metrics: Arc<MetricsHub>,
    /// Non-blocking real-time bus handle: emits `TradeFilled` + `PositionChanged`
    /// once a fill is durably advanced to its terminal state.
    pub events: CoreEventPublisher,
}

impl PostTradeConsumer {
    /// Process one claimed (`*_observed`) trade through to its terminal state.
    ///
    /// Every step is idempotent so at-least-once replay (after crash or a lost
    /// notify) is safe: risk Fill accounting is deduped by `trade_id`, position
    /// creation is guarded by the unique `trade_id`, and the terminal transition
    /// is a conditional `WHERE state = <observed>` gate.
    pub async fn process(&self, trade: &TradeInfo) {
        let Some(terminal) = trade.state.processed_terminal() else {
            tracing::warn!(trade_id = %trade.trade_id, state = %trade.state, "relay claimed non-observed trade");
            return;
        };
        let Some(risk_input) = PostTradeInput::from_trade_info(trade) else {
            tracing::warn!(trade_id = %trade.trade_id, "observed trade has no business outcome");
            return;
        };

        if let Err(error) = self
            .risk_engine
            .on_trade_result(
                TradeAccountingPhase::Fill,
                &risk_input,
                self.risk_metrics.as_ref(),
            )
            .await
        {
            tracing::error!(%error, trade_id = %trade.trade_id, "post-trade fill accounting failed");
            self.fsm
                .enter_emergency("post-trade fill accounting failed");
            self.metrics.post_trade_relay_failed.inc();
            return;
        }

        let snapshot = match trade.scored_opportunity_snapshot() {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "scored snapshot deserialize failed");
                None
            }
        };

        // The position created by *this* worker (if any); `None` on idempotent
        // replay where the position already existed.
        let mut opened_position = None;
        if trade.state.is_success() {
            match self.ensure_position(trade).await {
                Ok(position) => opened_position = position,
                Err(error) => {
                    tracing::error!(%error, trade_id = %trade.trade_id, "position creation failed after fill");
                    self.fsm.enter_emergency("position create failed");
                    self.metrics.post_trade_relay_failed.inc();
                    return;
                }
            }
            let Some(snapshot) = &snapshot else {
                self.fsm
                    .enter_emergency("calibration outcome create failed");
                self.metrics.post_trade_relay_failed.inc();
                return;
            };
            if let Err(error) = self.ensure_calibration_outcome(trade, snapshot).await {
                tracing::error!(%error, trade_id = %trade.trade_id, "calibration outcome creation failed after fill");
                self.fsm
                    .enter_emergency("calibration outcome create failed");
                self.metrics.post_trade_relay_failed.inc();
                return;
            }
        }

        self.write_audit(trade, snapshot.as_ref());

        match self
            .trade_repo
            .advance_state(&trade.trade_id, trade.state, terminal)
            .await
        {
            Ok(true) => {
                self.metrics.post_trade_relay_processed.inc();
                // Real-time push: only the worker that actually advanced the row
                // emits, so at-least-once replay never double-publishes. A fill
                // surfaces the terminal trade and, when this worker opened it,
                // the new position.
                if trade.state.is_success() {
                    self.events.publish(CoreEvent::TradeFilled(trade.clone()));
                    if let Some(position) = opened_position {
                        self.events.publish(CoreEvent::PositionChanged(position));
                    }
                }
            }
            Ok(false) => {
                tracing::debug!(trade_id = %trade.trade_id, "trade already advanced by another worker");
            }
            Err(error) => {
                tracing::error!(%error, trade_id = %trade.trade_id, "trade state advance failed");
                self.fsm.enter_emergency("trade state advance failed");
                self.metrics.post_trade_relay_failed.inc();
                return;
            }
        }

        self.metrics_state.mark_stale();
        if let Some(refresher) = &self.metrics_refresh {
            if let Err(error) = refresher.refresh().await {
                tracing::warn!(%error, "post-trade metrics refresh failed");
            }
        }
    }

    /// Create the position for a filled trade, idempotently (one position per
    /// trade). Returns `Some(position)` only when *this* call created it, so the
    /// caller can emit `PositionChanged` exactly once; `None` on replay.
    async fn ensure_position(&self, trade: &TradeInfo) -> Result<Option<PositionInfo>, OxideError> {
        if self
            .position_repo
            .find_by_trade_id(&trade.trade_id)
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let position = NewPosition {
            position_id: PositionId::from_v7(),
            trade_id: trade.trade_id.clone(),
            market_id: trade.market_id.clone(),
            token_id: trade.token_id.clone(),
            side: trade.side,
            shares: trade.shares,
            avg_entry_price: trade.price,
            total_cost_usd: trade.cost_usd,
            total_fees_usd: trade.fee_usd,
            redeem_status: RedeemStatus::initial_for_mode(trade.execution_mode),
        };
        let created = self.position_repo.create(position).await?;
        Ok(Some(created))
    }

    async fn ensure_calibration_outcome(
        &self,
        trade: &TradeInfo,
        snapshot: &ScoredOpportunitySnapshot,
    ) -> Result<(), OxideError> {
        let bucket = snapshot.calibration_bucket_key();
        self.calibration_repo
            .create_outcome(NewCalibrationOutcome {
                trade_id: trade.trade_id.clone(),
                opportunity_id: trade.opportunity_id.clone(),
                market_id: trade.market_id.clone(),
                category: bucket.category,
                price_zone: bucket.price_zone,
                duration_bucket: bucket.duration_bucket,
                predicted_yes: snapshot
                    .token_yes
                    .as_ref()
                    .map_or(snapshot.side == Side::Buy, |token_yes| {
                        token_yes == &trade.token_id
                    }),
                actual_yes: None,
                entry_price: snapshot.entry_price,
                confidence_at_entry: Probability::new(snapshot.confidence_decimal),
                convergence_secs: i32::try_from(snapshot.convergence_secs).unwrap_or(i32::MAX),
                resolved_at: None,
            })
            .await?;
        Ok(())
    }

    /// Best-effort terminal audit to `ClickHouse` (analytics; loss-tolerant).
    fn write_audit(&self, trade: &TradeInfo, snapshot: Option<&ScoredOpportunitySnapshot>) {
        if let Some(snapshot) = snapshot {
            self.audit_writer.write_terminal(trade, snapshot);
        } else {
            self.audit_writer
                .write_terminal_missing_snapshot(trade, "scored snapshot deserialize failed");
        }
    }
}
