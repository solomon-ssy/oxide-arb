use crate::{
    domain::{
        opportunity::Opportunity,
        position::PositionInfo,
        scored_snapshot::ScoredOpportunitySnapshot,
        settlement::{MarketSettlementRequest, SettlementEconomics},
        trade::TradeInfo,
    },
    enums::audit::OpportunityAuditStage,
    types::ExecutionId,
};
use chrono::Utc;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `opportunity_audit` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct OpportunityAuditRow {
    pub opportunity_id: String,
    pub execution_id: String,
    pub trade_id: String,
    pub market_id: String,
    pub event_id: String,
    pub side: String,
    pub entry_price: f64,
    pub shares: f64,
    pub total_cost_usd: f64,
    pub total_fees_usd: f64,
    pub net_profit_usd: f64,
    pub expected_profit: f64,
    pub edge_bps: u32,
    pub resolution_prob: f64,
    pub confidence: f64,
    pub convergence_secs: u32,
    pub price_zone: String,
    pub duration_bucket: String,
    pub depth_used_pct: f64,
    pub staleness: String,
    pub category: String,
    pub stage: String,
    pub stage_order: u8,
    pub stage_at: i64,
    pub payout_usd: f64,
    pub realized_pnl_usd: f64,
    pub settlement_status: Option<String>,
    pub settlement_trigger: Option<String>,
    pub winning_token_id: Option<String>,
    pub accounting_status: Option<String>,
    pub fee_source: Option<String>,
    pub outcome: Option<String>,
    pub rejection_stage: Option<String>,
    pub rejection_reason: Option<String>,
    pub detected_at: i64,
    pub updated_at: i64,
}

impl OpportunityAuditRow {
    /// Build a terminal (`filled`/`miss`/`failed`) audit row from a persisted
    /// trade row and its frozen scored snapshot. The relay calls this for
    /// `*_observed` rows, where `business_outcome` is always `Some`.
    #[must_use]
    pub fn from_terminal_trade(trade: &TradeInfo, snapshot: &ScoredOpportunitySnapshot) -> Self {
        let outcome = trade.business_outcome;
        let stage = outcome.map_or(OpportunityAuditStage::Failed, |o| {
            OpportunityAuditStage::from_business_outcome(o)
        });
        Self {
            opportunity_id: trade.opportunity_id.to_string(),
            execution_id: trade.execution_id.to_string(),
            trade_id: trade.trade_id.to_string(),
            market_id: trade.market_id.to_string(),
            event_id: trade.event_id.to_string(),
            side: trade.side.to_string(),
            entry_price: trade.price.inner().to_f64().unwrap_or(0.0),
            shares: trade.shares.inner().to_f64().unwrap_or(0.0),
            total_cost_usd: trade.cost_usd.inner().to_f64().unwrap_or(0.0),
            total_fees_usd: trade.fee_usd.inner().to_f64().unwrap_or(0.0),
            net_profit_usd: trade
                .net_profit_usd
                .map_or(0.0, |v| v.inner().to_f64().unwrap_or(0.0)),
            expected_profit: trade
                .detected_profit_usd
                .map_or(0.0, |v| v.inner().to_f64().unwrap_or(0.0)),
            edge_bps: trade
                .detected_edge_bps
                .map_or(0, |v| v.inner().to_u32().unwrap_or(0)),
            resolution_prob: snapshot.resolution_prob,
            confidence: snapshot.confidence,
            convergence_secs: snapshot.convergence_secs,
            price_zone: snapshot.price_zone.to_string(),
            duration_bucket: snapshot.duration_bucket.to_string(),
            depth_used_pct: snapshot.depth_used_pct,
            staleness: snapshot.staleness.to_string(),
            category: trade.category.to_string(),
            stage: stage.to_string(),
            stage_order: stage.order(),
            stage_at: Utc::now().timestamp_millis(),
            payout_usd: 0.0,
            realized_pnl_usd: 0.0,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            fee_source: None,
            outcome: outcome.map(|o| o.to_string()),
            rejection_stage: None,
            rejection_reason: None,
            detected_at: trade.created_at.timestamp_millis(),
            updated_at: Utc::now().timestamp_millis(),
        }
    }
}

impl
    From<(
        &ExecutionId,
        &Opportunity,
        &str,
        &str,
        &ScoredOpportunitySnapshot,
    )> for OpportunityAuditRow
{
    fn from(
        (execution_id, opp, stage, reason, snapshot): (
            &ExecutionId,
            &Opportunity,
            &str,
            &str,
            &ScoredOpportunitySnapshot,
        ),
    ) -> Self {
        let audit_stage = OpportunityAuditStage::from_rejection_stage(stage);
        Self {
            opportunity_id: opp.opportunity_id.to_string(),
            execution_id: execution_id.to_string(),
            trade_id: String::new(),
            market_id: opp.market_id.to_string(),
            event_id: opp.event_id.to_string(),
            side: opp.side.to_string(),
            entry_price: opp.entry_price.inner().to_f64().unwrap_or(0.0),
            shares: opp.shares.inner().to_f64().unwrap_or(0.0),
            total_cost_usd: 0.0,
            total_fees_usd: 0.0,
            net_profit_usd: 0.0,
            expected_profit: opp.expected_net_profit.inner().to_f64().unwrap_or(0.0),
            edge_bps: opp.edge_bps.inner().to_u32().unwrap_or(0),
            resolution_prob: snapshot.resolution_prob,
            confidence: snapshot.confidence,
            convergence_secs: snapshot.convergence_secs,
            price_zone: snapshot.price_zone.to_string(),
            duration_bucket: snapshot.duration_bucket.to_string(),
            depth_used_pct: snapshot.depth_used_pct,
            staleness: snapshot.staleness.to_string(),
            category: opp.category.to_string(),
            stage: audit_stage.to_string(),
            stage_order: audit_stage.order(),
            stage_at: Utc::now().timestamp_millis(),
            payout_usd: 0.0,
            realized_pnl_usd: 0.0,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            fee_source: None,
            outcome: Some("rejected".to_owned()),
            rejection_stage: Some(stage.to_owned()),
            rejection_reason: Some(reason.to_owned()),
            detected_at: opp.detected_at.timestamp_millis(),
            updated_at: Utc::now().timestamp_millis(),
        }
    }
}

impl
    From<(
        &TradeInfo,
        &PositionInfo,
        &MarketSettlementRequest,
        &SettlementEconomics,
    )> for OpportunityAuditRow
{
    fn from(
        (trade, position, request, economics): (
            &TradeInfo,
            &PositionInfo,
            &MarketSettlementRequest,
            &SettlementEconomics,
        ),
    ) -> Self {
        let stage = OpportunityAuditStage::Settled;
        Self {
            opportunity_id: trade.opportunity_id.to_string(),
            execution_id: trade.execution_id.to_string(),
            trade_id: trade.trade_id.to_string(),
            market_id: trade.market_id.to_string(),
            event_id: trade.event_id.to_string(),
            side: trade.side.to_string(),
            entry_price: position.avg_entry_price.inner().to_f64().unwrap_or(0.0),
            shares: position.shares.inner().to_f64().unwrap_or(0.0),
            total_cost_usd: position.total_cost_usd.inner().to_f64().unwrap_or(0.0),
            total_fees_usd: position.total_fees_usd.inner().to_f64().unwrap_or(0.0),
            net_profit_usd: trade
                .net_profit_usd
                .map_or(0.0, |value| value.inner().to_f64().unwrap_or(0.0)),
            expected_profit: trade
                .detected_profit_usd
                .map_or(0.0, |value| value.inner().to_f64().unwrap_or(0.0)),
            edge_bps: trade
                .detected_edge_bps
                .map_or(0, |value| value.inner().to_u32().unwrap_or(0)),
            resolution_prob: 0.0,
            confidence: 0.0,
            convergence_secs: 0,
            price_zone: String::new(),
            duration_bucket: String::new(),
            depth_used_pct: 0.0,
            staleness: String::new(),
            category: String::new(),
            stage: stage.to_string(),
            stage_order: stage.order(),
            stage_at: request.observed_at.timestamp_millis(),
            payout_usd: economics.payout_usd.inner().to_f64().unwrap_or(0.0),
            realized_pnl_usd: economics.realized_pnl_usd.inner().to_f64().unwrap_or(0.0),
            settlement_status: Some(if economics.won { "won" } else { "lost" }.to_owned()),
            settlement_trigger: Some(request.source.to_string()),
            winning_token_id: Some(request.winning_token_id.to_string()),
            accounting_status: Some(position.settlement_accounting_status.to_string()),
            fee_source: None,
            outcome: Some("settled".to_owned()),
            rejection_stage: None,
            rejection_reason: None,
            detected_at: trade.created_at.timestamp_millis(),
            updated_at: Utc::now().timestamp_millis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            execution::ResolvedOutcome, scored_snapshot::ScoredOpportunitySnapshot,
            trade::TradeInfo,
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{ExecutionMode, MarketCategory, Side, StalenessLevel},
            execution::ExecutionOutcome,
        },
        types::{
            Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId,
            Shares, TokenId, TradeId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    fn test_snapshot() -> ScoredOpportunitySnapshot {
        ScoredOpportunitySnapshot {
            resolution_prob: 0.95,
            confidence: 0.95,
            convergence_secs: 600,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
            depth_used_pct: 10.0,
            staleness: StalenessLevel::Fresh,
        }
    }

    fn test_trade(resolved: &ResolvedOutcome) -> TradeInfo {
        let now = chrono::Utc::now();
        TradeInfo {
            trade_id: TradeId::new("t1"),
            execution_id: ExecutionId::generate(),
            reservation_id: ReservationId::new("r1"),
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("tok1"),
            side: Side::Buy,
            shares: resolved.filled_shares,
            price: resolved.avg_fill_price,
            cost_usd: resolved.cost_usd,
            fee_usd: resolved.fee_usd,
            detected_edge_bps: Some(Bps::new(dec!(300))),
            detected_profit_usd: Some(Usd::new(dec!(4.5))),
            net_profit_usd: resolved.net_profit_usd,
            order_id: resolved.order_id.clone(),
            tx_hash: resolved.tx_hash.clone(),
            state: resolved.observed_state(),
            business_outcome: Some(resolved.business_outcome),
            scored_snapshot: serde_json::to_value(test_snapshot()).expect("snapshot json"),
            category: MarketCategory::Politics,
            needs_reconcile: false,
            post_trade_claim_owner: None,
            post_trade_claimed_at: None,
            post_trade_attempts: 0,
            execution_mode: ExecutionMode::Paper,
            latency_ms: resolved
                .latency_ms
                .map(|ms| i32::try_from(ms).unwrap_or(i32::MAX)),
            error_message: resolved.error_message.clone(),
            submitted_at: Some(now),
            confirmed_at: Some(now),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn from_execution_filled_maps_all_fields() {
        let outcome = ExecutionOutcome::Filled {
            order_id: OrderId::new("ord1"),
            filled_shares: Shares::new(dec!(80)),
            avg_fill_price: Some(Price::new(dec!(0.93))),
            fee_paid: Usd::new(dec!(0.50)),
            tx_hash: Some("0xabc".into()),
            execution_mode: ExecutionMode::Paper,
            latency_ms: 42,
        };
        let resolved = ResolvedOutcome::resolve(&outcome, Price::new(dec!(0.92)), 0.95);
        let trade = test_trade(&resolved);
        let snapshot = test_snapshot();
        let row = OpportunityAuditRow::from_terminal_trade(&trade, &snapshot);

        assert!((row.shares - 80.0).abs() < f64::EPSILON);
        assert!((row.total_cost_usd - 74.4).abs() < 0.01);
        assert!((row.total_fees_usd - 0.5).abs() < f64::EPSILON);
        assert!(
            row.net_profit_usd.abs() > f64::EPSILON,
            "fill EV should be nonzero"
        );
        assert!((row.expected_profit - 4.5).abs() < f64::EPSILON);
        assert_eq!(row.outcome.as_deref(), Some("success"));
        assert!(row.rejection_stage.is_none());
    }

    #[test]
    fn from_execution_miss_zeros_financial() {
        let outcome = ExecutionOutcome::Miss {
            reason: "no fill".into(),
            execution_mode: ExecutionMode::Paper,
        };
        let resolved = ResolvedOutcome::resolve(&outcome, Price::new(dec!(0.92)), 0.95);
        let trade = test_trade(&resolved);
        let snapshot = test_snapshot();
        let row = OpportunityAuditRow::from_terminal_trade(&trade, &snapshot);

        assert!((row.total_cost_usd).abs() < f64::EPSILON);
        assert!((row.total_fees_usd).abs() < f64::EPSILON);
        assert!((row.net_profit_usd).abs() < f64::EPSILON);
        assert!((row.shares).abs() < f64::EPSILON);
        assert_eq!(row.outcome.as_deref(), Some("miss"));
    }

    #[test]
    fn from_rejection_sets_stage_and_reason() {
        use crate::domain::calibration::{BucketKey, CalibrationSnapshot};
        use crate::domain::opportunity::{EndgameMeta, Opportunity};
        use crate::enums::opportunity::PayoutModel;

        let opp = Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("tok1"),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::new(dec!(100)),
                expected_payout: Usd::new(dec!(95)),
                predicted_side: Side::Buy,
            },
            shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.92)),
            total_cost: Usd::new(dec!(92)),
            total_fees: Usd::new(dec!(0.40)),
            net_profit: Usd::new(dec!(7.60)),
            expected_net_profit: Usd::new(dec!(2.60)),
            edge_bps: Bps::new(dec!(300)),
            resolution_adjust: dec!(0.95),
            depth_used_pct: dec!(10),
            staleness: StalenessLevel::Fresh,
            category: MarketCategory::Politics,
            meta: EndgameMeta {
                predicted_yes: true,
                confidence: dec!(0.95),
                convergence_duration_secs: 600,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
                settlement_deadline: None,
            },
            calibration: CalibrationSnapshot {
                bucket_key: BucketKey {
                    category: MarketCategory::Politics,
                    price_zone: PriceZone::Z97,
                    duration_bucket: DurationBucket::Medium,
                },
                posterior_mean: dec!(0.93),
                sample_size: 50,
                alpha_prior: dec!(2.0),
                beta_prior: dec!(1.0),
                fallback_tier: 1,
                fused_probability: dec!(0.99),
            },
            detected_at: chrono::Utc::now(),
        };
        let snapshot = ScoredOpportunitySnapshot::from_opportunity(&opp);
        let exec_id = ExecutionId::generate();
        let row = OpportunityAuditRow::from((&exec_id, &opp, "risk", "max exposure", &snapshot));

        assert_eq!(row.rejection_stage.as_deref(), Some("risk"));
        assert_eq!(row.rejection_reason.as_deref(), Some("max exposure"));
        assert!((row.total_cost_usd).abs() < f64::EPSILON);
        assert!((row.total_fees_usd).abs() < f64::EPSILON);
        assert!((row.net_profit_usd).abs() < f64::EPSILON);
    }
}
