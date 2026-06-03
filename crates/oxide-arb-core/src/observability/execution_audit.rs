//! Fire-and-forget CH audit writer for execution outcomes and rejections.

use crate::infra::async_writer::AsyncWriter;
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        opportunity::Opportunity,
        position::PositionInfo,
        scored_snapshot::ScoredOpportunitySnapshot,
        settlement::{MarketSettlementRequest, SettlementEconomics},
        trade::TradeInfo,
    },
    types::ExecutionId,
};
use std::sync::Arc;

/// Non-blocking writer for `ClickHouse` `opportunity_audit` rows.
///
/// Handles both terminal outcomes (filled/miss/failed) and pre-dispatch
/// rejections. Backed by [`AsyncWriter`] with batched CH inserts.
pub struct ExecutionAuditWriter {
    writer: Arc<AsyncWriter<OpportunityAuditRow>>,
}

impl ExecutionAuditWriter {
    pub const fn new(writer: Arc<AsyncWriter<OpportunityAuditRow>>) -> Self {
        Self { writer }
    }

    pub fn write_rejection(
        &self,
        execution_id: &ExecutionId,
        opp: &Opportunity,
        stage: &str,
        reason: &str,
        snapshot: &ScoredOpportunitySnapshot,
    ) {
        let row = (execution_id, opp, stage, reason, snapshot).into();
        self.writer.write(row);
    }

    pub fn write_terminal(&self, trade: &TradeInfo, snapshot: &ScoredOpportunitySnapshot) {
        let row = OpportunityAuditRow::from_terminal_trade(trade, snapshot);
        self.writer.write(row);
    }

    pub fn write_terminal_missing_snapshot(&self, trade: &TradeInfo, reason: &str) {
        let row = OpportunityAuditRow::from_terminal_trade_missing_snapshot(trade, reason);
        self.writer.write(row);
    }

    pub fn write_settlement(
        &self,
        trade: &TradeInfo,
        position: &PositionInfo,
        request: &MarketSettlementRequest,
        economics: &SettlementEconomics,
    ) {
        let row = match serde_json::from_value::<ScoredOpportunitySnapshot>(
            trade.scored_snapshot.clone(),
        ) {
            Ok(snapshot) => OpportunityAuditRow::from_settlement_trade(
                trade, position, request, economics, &snapshot,
            ),
            Err(error) => {
                tracing::warn!(
                    %error,
                    trade_id = %trade.trade_id,
                    "settlement audit scored snapshot deserialize failed"
                );
                (trade, position, request, economics).into()
            }
        };
        self.writer.write(row);
    }
}
