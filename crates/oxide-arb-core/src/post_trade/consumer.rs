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
        position::NewPosition,
        scored_snapshot::ScoredOpportunitySnapshot,
        trade::{PostTradeInput, TradeInfo},
    },
    enums::{
        common::{ExecutionMode, RedeemStatus},
        risk::TradeAccountingPhase,
    },
    types::PositionId,
};
use oxide_arb_repository::traits::{PositionRepository, TradeRepository};
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
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    pub metrics: Arc<MetricsHub>,
    pub execution_mode: ExecutionMode,
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

        if trade.state.is_success() {
            if let Err(error) = self.ensure_position(trade).await {
                tracing::error!(%error, trade_id = %trade.trade_id, "position creation failed after fill");
                self.fsm.enter_emergency("position create failed");
                self.metrics.post_trade_relay_failed.inc();
                return;
            }
        }

        self.write_audit(trade);

        match self
            .trade_repo
            .advance_state(&trade.trade_id, trade.state, terminal)
            .await
        {
            Ok(true) => self.metrics.post_trade_relay_processed.inc(),
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

    /// Create the position for a filled trade, idempotently (one position per trade).
    async fn ensure_position(&self, trade: &TradeInfo) -> Result<(), OxideError> {
        if self
            .position_repo
            .find_by_trade_id(&trade.trade_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let position = NewPosition {
            position_id: PositionId::generate(),
            trade_id: trade.trade_id.clone(),
            market_id: trade.market_id.clone(),
            token_id: trade.token_id.clone(),
            side: trade.side,
            shares: trade.shares,
            avg_entry_price: trade.price,
            total_cost_usd: trade.cost_usd,
            total_fees_usd: trade.fee_usd,
            redeem_status: RedeemStatus::initial_for_mode(self.execution_mode),
        };
        self.position_repo.create(position).await?;
        Ok(())
    }

    /// Best-effort terminal audit to `ClickHouse` (analytics; loss-tolerant).
    fn write_audit(&self, trade: &TradeInfo) {
        match serde_json::from_value::<ScoredOpportunitySnapshot>(trade.scored_snapshot.clone()) {
            Ok(snapshot) => self.audit_writer.write_terminal(trade, &snapshot),
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "scored snapshot deserialize failed; skipping audit");
            }
        }
    }
}
