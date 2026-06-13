//! Opportunity API contract: outbound projections.
//!
//! Three outbound shapes, all projected from `ClickHouse` evidence rows so the
//! wire never leaks storage details (scaled integers, `Enum8` discriminants):
//!
//! - [`OpportunityView`] — slim feed item shared by the WebSocket
//!   `opportunity.detected` push and the `sync` snapshot.
//! - [`OpportunityListView`] — detection row projection for the paginated
//!   `recent` / `history` list endpoints.
//! - [`OpportunityAuditView`] — audit-trail row projection for the
//!   per-opportunity detail timeline.
//! - [`OpportunityFunnelView`] — aggregated stage funnel for the `stats`
//!   endpoint (detected baseline + per-stage counts and rates).

use crate::{
    clickhouse::{
        AuditStageCountRow, ChBps, ChPrice, ChProbability, ChShares, ChUsd, OpportunityAuditRow,
        OpportunityDetectionRow,
    },
    domain::{Opportunity, TimeWindow},
    enums::{
        audit::{AuditOutcome, OpportunityAuditStage, RejectionStage, SettlementOutcome},
        calibration::{DurationBucket, PriceZone},
        common::{
            MarketCategory, SettlementAccountingStatus, SettlementTrigger, Side, StalenessLevel,
        },
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, MicroScore, OpportunityId, Price, Probability, Shares,
        TokenId, TradeId, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;

/// Outbound projection of a detected opportunity for the dashboard feed.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityView {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub edge_bps: Bps,
    /// Calibration-adjusted expected net profit at detection time.
    pub expected_net_profit_usd: Usd,
    pub detected_at: DateTime<Utc>,
}

impl From<&Opportunity> for OpportunityView {
    fn from(opportunity: &Opportunity) -> Self {
        Self {
            opportunity_id: opportunity.opportunity_id.clone(),
            market_id: opportunity.market_id.clone(),
            edge_bps: opportunity.edge_bps,
            expected_net_profit_usd: opportunity.expected_net_profit,
            detected_at: opportunity.detected_at,
        }
    }
}

impl From<&OpportunityDetectionRow> for OpportunityView {
    fn from(row: &OpportunityDetectionRow) -> Self {
        Self {
            opportunity_id: row.opportunity_id.clone(),
            market_id: row.market_id.clone(),
            edge_bps: row.edge_bps.to_bps(),
            expected_net_profit_usd: row.expected_net_profit_usd.to_usd(),
            detected_at: datetime_from_millis(row.detected_at),
        }
    }
}

/// Outbound projection of a detection row for the paginated list endpoints.
///
/// Money / price / probability fields surface as `Decimal` (string on the
/// wire); enums surface in their domain `snake_case` form. Detection
/// internals the list never renders (calibration snapshot, score components,
/// book context JSON blobs) are stripped.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityListView {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub entry_price: Price,
    pub shares: Shares,
    pub edge_bps: Bps,
    /// Calibration-adjusted expected net profit at detection time.
    pub expected_net_profit_usd: Usd,
    /// Net profit if the prediction settles correctly (gross of probability).
    pub net_profit_if_correct_usd: Usd,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub resolution_prob: Probability,
    pub confidence: Probability,
    pub fill_probability: Option<Probability>,
    /// Composite ranking score (profit × probability products).
    pub score: Option<Decimal>,
    /// Share of available book depth consumed by the sizing (0–100).
    pub depth_used_pct: Decimal,
    pub convergence_secs: u32,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub detected_at: DateTime<Utc>,
}

impl From<&OpportunityDetectionRow> for OpportunityListView {
    fn from(row: &OpportunityDetectionRow) -> Self {
        Self {
            opportunity_id: row.opportunity_id.clone(),
            market_id: row.market_id.clone(),
            event_id: row.event_id.clone(),
            token_id: row.token_id.clone(),
            side: Side::from(row.side),
            entry_price: row.entry_price.to_price(),
            shares: row.shares.to_shares(),
            edge_bps: row.edge_bps.to_bps(),
            expected_net_profit_usd: row.expected_net_profit_usd.to_usd(),
            net_profit_if_correct_usd: row.net_profit_if_correct_usd.to_usd(),
            total_cost_usd: row.total_cost_usd.to_usd(),
            total_fees_usd: row.total_fees_usd.to_usd(),
            resolution_prob: row.resolution_prob.to_probability(),
            confidence: row.confidence.to_probability(),
            fill_probability: row.fill_probability.map(ChProbability::to_probability),
            score: row
                .score
                .map(|micro| MicroScore::from_micro(micro).to_decimal()),
            depth_used_pct: row.depth_used_pct.to_decimal(),
            convergence_secs: row.convergence_secs,
            category: MarketCategory::from(row.category),
            price_zone: PriceZone::from(row.price_zone),
            duration_bucket: DurationBucket::from(row.duration_bucket),
            detected_at: datetime_from_millis(row.detected_at),
        }
    }
}

/// Outbound projection of one audit-trail stage for the detail timeline.
///
/// Each row is one lifecycle stage of an opportunity (rejection, terminal
/// execution outcome, settlement). The frozen scored snapshot is surfaced as
/// parsed JSON so the timeline can fold it without a second request.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityAuditView {
    pub opportunity_id: OpportunityId,
    pub execution_id: ExecutionId,
    pub trade_id: Option<TradeId>,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub stage: OpportunityAuditStage,
    pub stage_order: u8,
    pub stage_at: DateTime<Utc>,
    pub outcome: Option<AuditOutcome>,
    pub rejection_stage: Option<RejectionStage>,
    pub rejection_reason: Option<String>,
    pub entry_price: Option<Price>,
    pub fill_price: Option<Price>,
    pub requested_shares: Option<Shares>,
    pub filled_shares: Option<Shares>,
    pub total_cost_usd: Option<Usd>,
    pub fees_usd: Option<Usd>,
    pub net_profit_usd: Option<Usd>,
    pub expected_profit_usd: Option<Usd>,
    pub edge_bps: Option<Bps>,
    pub resolution_prob: Option<Probability>,
    pub confidence: Option<Probability>,
    pub fill_probability: Option<Probability>,
    pub convergence_secs: Option<u32>,
    pub price_zone: Option<PriceZone>,
    pub duration_bucket: Option<DurationBucket>,
    pub staleness: Option<StalenessLevel>,
    pub category: Option<MarketCategory>,
    pub payout_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub settlement_status: Option<SettlementOutcome>,
    pub settlement_trigger: Option<SettlementTrigger>,
    pub winning_token_id: Option<TokenId>,
    pub accounting_status: Option<SettlementAccountingStatus>,
    /// Frozen scored-opportunity snapshot captured at this stage, if recorded.
    pub scored_snapshot: Option<Value>,
    pub detected_at: DateTime<Utc>,
}

impl From<&OpportunityAuditRow> for OpportunityAuditView {
    fn from(row: &OpportunityAuditRow) -> Self {
        Self {
            opportunity_id: row.opportunity_id.clone(),
            execution_id: row.execution_id.clone(),
            trade_id: row.trade_id.clone(),
            market_id: row.market_id.clone(),
            event_id: row.event_id.clone(),
            token_id: row.token_id.clone(),
            side: Side::from(row.side),
            stage: OpportunityAuditStage::from(row.stage),
            stage_order: row.stage_order,
            stage_at: datetime_from_millis(row.stage_at),
            outcome: row.outcome.map(AuditOutcome::from),
            rejection_stage: row.rejection_stage.map(RejectionStage::from),
            rejection_reason: row.rejection_reason.clone(),
            entry_price: row.entry_price.map(ChPrice::to_price),
            fill_price: row.fill_price.map(ChPrice::to_price),
            requested_shares: row.requested_shares.map(ChShares::to_shares),
            filled_shares: row.filled_shares.map(ChShares::to_shares),
            total_cost_usd: row.total_cost_usd.map(ChUsd::to_usd),
            fees_usd: row.fees_usd.map(ChUsd::to_usd),
            net_profit_usd: row.net_profit_usd.map(ChUsd::to_usd),
            expected_profit_usd: row.expected_profit_usd.map(ChUsd::to_usd),
            edge_bps: row.edge_bps.map(ChBps::to_bps),
            resolution_prob: row.resolution_prob.map(ChProbability::to_probability),
            confidence: row.confidence.map(ChProbability::to_probability),
            fill_probability: row.fill_probability.map(ChProbability::to_probability),
            convergence_secs: row.convergence_secs,
            price_zone: row.price_zone.map(PriceZone::from),
            duration_bucket: row.duration_bucket.map(DurationBucket::from),
            staleness: row.staleness.map(StalenessLevel::from),
            category: row.category.map(MarketCategory::from),
            payout_usd: row.payout_usd.map(ChUsd::to_usd),
            realized_pnl_usd: row.realized_pnl_usd.map(ChUsd::to_usd),
            settlement_status: row.settlement_status.map(SettlementOutcome::from),
            settlement_trigger: row.settlement_trigger.map(SettlementTrigger::from),
            winning_token_id: row.winning_token_id.clone(),
            accounting_status: row.accounting_status.map(SettlementAccountingStatus::from),
            scored_snapshot: row
                .scored_snapshot_json
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok()),
            detected_at: datetime_from_millis(row.detected_at),
        }
    }
}

/// One funnel stage: count of distinct opportunities that reached it.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityFunnelStageView {
    pub stage: OpportunityAuditStage,
    /// Distinct opportunities recorded at this stage inside the window.
    pub count: u64,
    /// Share of detected opportunities reaching this stage (0..1); `None`
    /// when the window has no detection baseline to divide by.
    pub rate: Option<Decimal>,
}

/// Aggregated detection→execution→settlement funnel for a time window.
///
/// `total_detected` comes from the `opportunity_detection` table (the scanner
/// baseline); per-stage counts come from `opportunity_audit`, which only
/// records rejection / terminal / settlement stages. Stages are ordered by
/// their lifecycle position ([`OpportunityAuditStage::order`]).
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityFunnelView {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// Distinct opportunities detected inside the window.
    pub total_detected: u64,
    pub stages: Vec<OpportunityFunnelStageView>,
}

impl OpportunityFunnelView {
    /// Assemble the funnel from the detection baseline and raw stage counts,
    /// sorting stages into lifecycle order and deriving per-stage rates.
    #[must_use]
    pub fn from_counts(
        window: TimeWindow,
        total_detected: u64,
        counts: &[AuditStageCountRow],
    ) -> Self {
        let mut stages: Vec<OpportunityFunnelStageView> = counts
            .iter()
            .map(|row| {
                let stage = OpportunityAuditStage::from(row.stage);
                let rate = (total_detected > 0)
                    .then(|| Decimal::from(row.count) / Decimal::from(total_detected));
                OpportunityFunnelStageView {
                    stage,
                    count: row.count,
                    rate,
                }
            })
            .collect();
        stages.sort_by_key(|view| view.stage.order());
        Self {
            from: window.from,
            to: window.to,
            total_detected,
            stages,
        }
    }
}

/// Convert persisted epoch milliseconds into a UTC timestamp.
///
/// Persisted rows always carry in-range epoch millis; epoch zero is an
/// unreachable fallback that keeps this panic-free.
fn datetime_from_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{OpportunityFunnelView, TimeWindow};
    use crate::{clickhouse::AuditStageCountRow, enums::clickhouse::ChOpportunityAuditStage};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn window() -> TimeWindow {
        TimeWindow::new(Utc::now() - chrono::Duration::days(1), Utc::now())
    }

    #[test]
    fn funnel_orders_stages_by_lifecycle_and_derives_rates() {
        let counts = [
            AuditStageCountRow {
                stage: ChOpportunityAuditStage::Settled,
                count: 10,
            },
            AuditStageCountRow {
                stage: ChOpportunityAuditStage::RiskRejected,
                count: 25,
            },
            AuditStageCountRow {
                stage: ChOpportunityAuditStage::Filled,
                count: 50,
            },
        ];
        let funnel = OpportunityFunnelView::from_counts(window(), 100, &counts);

        assert_eq!(funnel.total_detected, 100);
        let stages: Vec<_> = funnel.stages.iter().map(|s| s.stage.as_str()).collect();
        assert_eq!(
            stages,
            vec!["risk_rejected", "filled", "settled"],
            "stages sorted by lifecycle order"
        );
        assert_eq!(funnel.stages[1].rate, Some(dec!(0.5)));
        assert_eq!(funnel.stages[2].rate, Some(dec!(0.1)));
    }

    #[test]
    fn funnel_without_detection_baseline_yields_no_rates() {
        let counts = [AuditStageCountRow {
            stage: ChOpportunityAuditStage::Filled,
            count: 3,
        }];
        let funnel = OpportunityFunnelView::from_counts(window(), 0, &counts);
        assert_eq!(funnel.stages[0].count, 3);
        assert!(
            funnel.stages[0].rate.is_none(),
            "no baseline → rate must be absent, never divide-by-zero"
        );
    }
}
