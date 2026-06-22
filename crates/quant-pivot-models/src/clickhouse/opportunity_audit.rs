use crate::{
    clickhouse::{ChBps, ChFactor, ChPrice, ChProbability, ChSchemaVersion, ChShares, ChUsd},
    domain::{
        opportunity::Opportunity,
        position::PositionInfo,
        scored_snapshot::ScoredOpportunitySnapshot,
        settlement::{MarketSettlementRequest, SettlementEconomics},
        trade::TradeInfo,
    },
    enums::{
        audit::OpportunityAuditStage,
        clickhouse::{
            ChAuditOutcome, ChDurationBucket, ChMarketCategory, ChOpportunityAuditStage,
            ChPriceZone, ChRejectionStage, ChSettlementAccountingStatus, ChSettlementOutcome,
            ChSettlementTrigger, ChSide, ChStalenessLevel,
        },
        evidence::MissingEvidenceField,
    },
    types::{EventId, ExecutionId, MarketId, OpportunityId, TokenId, TradeId},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Aggregated `GROUP BY stage` projection over `opportunity_audit`.
///
/// One row per stage with the count of distinct opportunities recorded at
/// that stage; consumed by the funnel stats endpoint.
#[derive(Debug, Clone, Copy, clickhouse::Row, Serialize, Deserialize)]
pub struct AuditStageCountRow {
    pub stage: ChOpportunityAuditStage,
    pub count: u64,
}

/// `ClickHouse` row for `opportunity_audit` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct OpportunityAuditRow {
    pub opportunity_id: OpportunityId,
    pub execution_id: ExecutionId,
    pub trade_id: Option<TradeId>,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: ChSide,
    pub entry_price: Option<ChPrice>,
    pub fill_price: Option<ChPrice>,
    pub requested_shares: Option<ChShares>,
    pub filled_shares: Option<ChShares>,
    pub total_cost_usd: Option<ChUsd>,
    pub fees_usd: Option<ChUsd>,
    pub net_profit_usd: Option<ChUsd>,
    pub expected_profit_usd: Option<ChUsd>,
    pub edge_bps: Option<ChBps>,
    pub resolution_prob: Option<ChProbability>,
    pub confidence: Option<ChProbability>,
    pub fill_probability: Option<ChProbability>,
    pub convergence_secs: Option<u32>,
    pub price_zone: Option<ChPriceZone>,
    pub duration_bucket: Option<ChDurationBucket>,
    pub depth_used_pct: Option<ChFactor>,
    pub staleness: Option<ChStalenessLevel>,
    pub category: Option<ChMarketCategory>,
    pub stage: ChOpportunityAuditStage,
    pub stage_order: u8,
    pub stage_at: i64,
    pub payout_usd: Option<ChUsd>,
    pub realized_pnl_usd: Option<ChUsd>,
    pub settlement_status: Option<ChSettlementOutcome>,
    pub settlement_trigger: Option<ChSettlementTrigger>,
    pub winning_token_id: Option<TokenId>,
    pub accounting_status: Option<ChSettlementAccountingStatus>,
    pub redeem_route: Option<String>,
    pub redeem_resolution: Option<String>,
    pub fee_source: Option<String>,
    pub outcome: Option<ChAuditOutcome>,
    pub rejection_stage: Option<ChRejectionStage>,
    pub rejection_reason: Option<String>,
    pub scored_snapshot_json: Option<String>,
    pub book_context_json: Option<String>,
    pub applied_factor_ids_json: Option<String>,
    pub missing_fields_json: Option<String>,
    pub detected_at: i64,
    pub ingestion_time: i64,
    pub sequence: u64,
    pub schema_version: ChSchemaVersion,
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
            opportunity_id: trade.opportunity_id.clone(),
            execution_id: trade.execution_id.clone(),
            trade_id: Some(trade.trade_id.clone()),
            market_id: trade.market_id.clone(),
            event_id: trade.event_id.clone(),
            token_id: trade.token_id.clone(),
            side: ChSide::from(trade.side),
            entry_price: Some(ChPrice::from(trade.price)),
            fill_price: Some(ChPrice::from(trade.price)),
            requested_shares: Some(ChShares::from(trade.shares)),
            filled_shares: Some(ChShares::from(trade.shares)),
            total_cost_usd: Some(ChUsd::from(trade.cost_usd)),
            fees_usd: Some(ChUsd::from(trade.fee_usd)),
            net_profit_usd: trade.net_profit_usd.map(ChUsd::from),
            expected_profit_usd: trade.detected_profit_usd.map(ChUsd::from),
            edge_bps: trade.detected_edge_bps.map(ChBps::from),
            resolution_prob: Some(ChProbability::from(snapshot.resolution_prob_decimal)),
            confidence: Some(ChProbability::from(snapshot.confidence_decimal)),
            fill_probability: snapshot
                .fill_probability
                .map(|value| ChProbability::from(value.to_decimal())),
            convergence_secs: Some(snapshot.convergence_secs),
            price_zone: Some(ChPriceZone::from(snapshot.price_zone)),
            duration_bucket: Some(ChDurationBucket::from(snapshot.duration_bucket)),
            depth_used_pct: Some(ChFactor::from(snapshot.depth_used_pct_decimal)),
            staleness: Some(ChStalenessLevel::from(snapshot.staleness)),
            category: Some(ChMarketCategory::from(trade.category)),
            stage: ChOpportunityAuditStage::from(stage),
            stage_order: stage.order(),
            stage_at: Utc::now().timestamp_millis(),
            payout_usd: None,
            realized_pnl_usd: None,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            redeem_route: None,
            redeem_resolution: None,
            fee_source: None,
            outcome: outcome.map(ChAuditOutcome::from),
            rejection_stage: None,
            rejection_reason: None,
            scored_snapshot_json: serde_json::to_string(snapshot).ok(),
            book_context_json: snapshot
                .book
                .as_ref()
                .and_then(|book| serde_json::to_string(book).ok()),
            applied_factor_ids_json: snapshot
                .factors
                .as_ref()
                .and_then(|factors| serde_json::to_string(&factors.factor_ids).ok()),
            missing_fields_json: missing_fields_json(&snapshot.missing_fields),
            detected_at: trade.created_at.timestamp_millis(),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: 0,
            schema_version: ChSchemaVersion(3),
            updated_at: Utc::now().timestamp_millis(),
        }
    }

    #[must_use]
    pub fn from_terminal_trade_missing_snapshot(trade: &TradeInfo, reason: &str) -> Self {
        let outcome = trade.business_outcome;
        let stage = outcome.map_or(OpportunityAuditStage::Failed, |o| {
            OpportunityAuditStage::from_business_outcome(o)
        });
        Self {
            opportunity_id: trade.opportunity_id.clone(),
            execution_id: trade.execution_id.clone(),
            trade_id: Some(trade.trade_id.clone()),
            market_id: trade.market_id.clone(),
            event_id: trade.event_id.clone(),
            token_id: trade.token_id.clone(),
            side: ChSide::from(trade.side),
            entry_price: Some(ChPrice::from(trade.price)),
            fill_price: Some(ChPrice::from(trade.price)),
            requested_shares: Some(ChShares::from(trade.shares)),
            filled_shares: Some(ChShares::from(trade.shares)),
            total_cost_usd: Some(ChUsd::from(trade.cost_usd)),
            fees_usd: Some(ChUsd::from(trade.fee_usd)),
            net_profit_usd: trade.net_profit_usd.map(ChUsd::from),
            expected_profit_usd: trade.detected_profit_usd.map(ChUsd::from),
            edge_bps: trade.detected_edge_bps.map(ChBps::from),
            resolution_prob: None,
            confidence: None,
            fill_probability: None,
            convergence_secs: None,
            price_zone: None,
            duration_bucket: None,
            depth_used_pct: None,
            staleness: None,
            category: Some(ChMarketCategory::from(trade.category)),
            stage: ChOpportunityAuditStage::from(stage),
            stage_order: stage.order(),
            stage_at: Utc::now().timestamp_millis(),
            payout_usd: None,
            realized_pnl_usd: None,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            redeem_route: None,
            redeem_resolution: None,
            fee_source: None,
            outcome: outcome.map(ChAuditOutcome::from),
            rejection_stage: None,
            rejection_reason: Some(reason.to_owned()),
            scored_snapshot_json: None,
            book_context_json: None,
            applied_factor_ids_json: None,
            missing_fields_json: missing_fields_json(&[
                MissingEvidenceField::ScoredSnapshot,
                MissingEvidenceField::ResolutionProb,
                MissingEvidenceField::Confidence,
                MissingEvidenceField::FillProbability,
                MissingEvidenceField::PriceZone,
                MissingEvidenceField::DurationBucket,
                MissingEvidenceField::DepthUsedPct,
                MissingEvidenceField::Staleness,
                MissingEvidenceField::BookContext,
                MissingEvidenceField::AppliedFactors,
            ]),
            detected_at: trade.created_at.timestamp_millis(),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: 0,
            schema_version: ChSchemaVersion(3),
            updated_at: Utc::now().timestamp_millis(),
        }
    }

    #[must_use]
    pub fn from_settlement_trade(
        trade: &TradeInfo,
        position: &PositionInfo,
        request: &MarketSettlementRequest,
        economics: &SettlementEconomics,
        snapshot: &ScoredOpportunitySnapshot,
    ) -> Self {
        let mut row = Self::from((trade, position, request, economics));
        row.apply_scored_snapshot(snapshot);
        row
    }

    fn apply_scored_snapshot(&mut self, snapshot: &ScoredOpportunitySnapshot) {
        self.resolution_prob = Some(ChProbability::from(snapshot.resolution_prob_decimal));
        self.confidence = Some(ChProbability::from(snapshot.confidence_decimal));
        self.fill_probability = snapshot
            .fill_probability
            .map(|value| ChProbability::from(value.to_decimal()));
        self.convergence_secs = Some(snapshot.convergence_secs);
        self.price_zone = Some(ChPriceZone::from(snapshot.price_zone));
        self.duration_bucket = Some(ChDurationBucket::from(snapshot.duration_bucket));
        self.depth_used_pct = Some(ChFactor::from(snapshot.depth_used_pct_decimal));
        self.staleness = Some(ChStalenessLevel::from(snapshot.staleness));
        self.category = Some(ChMarketCategory::from(snapshot.category));
        self.scored_snapshot_json = serde_json::to_string(snapshot).ok();
        self.book_context_json = snapshot
            .book
            .as_ref()
            .and_then(|book| serde_json::to_string(book).ok());
        self.applied_factor_ids_json = snapshot
            .factors
            .as_ref()
            .and_then(|factors| serde_json::to_string(&factors.factor_ids).ok());
        self.missing_fields_json = missing_fields_json(&snapshot.missing_fields);
        self.detected_at = snapshot.detected_at.timestamp_millis();
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
            opportunity_id: opp.opportunity_id.clone(),
            execution_id: execution_id.clone(),
            trade_id: None,
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            side: ChSide::from(opp.side),
            entry_price: Some(ChPrice::from(opp.entry_price)),
            fill_price: None,
            requested_shares: Some(ChShares::from(opp.shares)),
            filled_shares: None,
            total_cost_usd: None,
            fees_usd: None,
            net_profit_usd: None,
            expected_profit_usd: Some(ChUsd::from(opp.expected_net_profit)),
            edge_bps: Some(ChBps::from(opp.edge_bps)),
            resolution_prob: Some(ChProbability::from(snapshot.resolution_prob_decimal)),
            confidence: Some(ChProbability::from(snapshot.confidence_decimal)),
            fill_probability: snapshot
                .fill_probability
                .map(|value| ChProbability::from(value.to_decimal())),
            convergence_secs: Some(snapshot.convergence_secs),
            price_zone: Some(ChPriceZone::from(snapshot.price_zone)),
            duration_bucket: Some(ChDurationBucket::from(snapshot.duration_bucket)),
            depth_used_pct: Some(ChFactor::from(snapshot.depth_used_pct_decimal)),
            staleness: Some(ChStalenessLevel::from(snapshot.staleness)),
            category: Some(ChMarketCategory::from(opp.category)),
            stage: ChOpportunityAuditStage::from(audit_stage),
            stage_order: audit_stage.order(),
            stage_at: Utc::now().timestamp_millis(),
            payout_usd: None,
            realized_pnl_usd: None,
            settlement_status: None,
            settlement_trigger: None,
            winning_token_id: None,
            accounting_status: None,
            redeem_route: None,
            redeem_resolution: None,
            fee_source: None,
            outcome: Some(ChAuditOutcome::Rejected),
            rejection_stage: Some(ChRejectionStage::from_stage(stage)),
            rejection_reason: Some(reason.to_owned()),
            scored_snapshot_json: serde_json::to_string(snapshot).ok(),
            book_context_json: snapshot
                .book
                .as_ref()
                .and_then(|book| serde_json::to_string(book).ok()),
            applied_factor_ids_json: snapshot
                .factors
                .as_ref()
                .and_then(|factors| serde_json::to_string(&factors.factor_ids).ok()),
            missing_fields_json: missing_fields_json(&snapshot.missing_fields),
            detected_at: opp.detected_at.timestamp_millis(),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: 0,
            schema_version: ChSchemaVersion(3),
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
            opportunity_id: trade.opportunity_id.clone(),
            execution_id: trade.execution_id.clone(),
            trade_id: Some(trade.trade_id.clone()),
            market_id: trade.market_id.clone(),
            event_id: trade.event_id.clone(),
            token_id: trade.token_id.clone(),
            side: ChSide::from(trade.side),
            entry_price: Some(ChPrice::from(position.avg_entry_price)),
            fill_price: None,
            requested_shares: Some(ChShares::from(position.shares)),
            filled_shares: Some(ChShares::from(position.shares)),
            total_cost_usd: Some(ChUsd::from(position.total_cost_usd)),
            fees_usd: Some(ChUsd::from(position.total_fees_usd)),
            net_profit_usd: trade.net_profit_usd.map(ChUsd::from),
            expected_profit_usd: trade.detected_profit_usd.map(ChUsd::from),
            edge_bps: trade.detected_edge_bps.map(ChBps::from),
            resolution_prob: None,
            confidence: None,
            fill_probability: None,
            convergence_secs: None,
            price_zone: None,
            duration_bucket: None,
            depth_used_pct: None,
            staleness: None,
            category: None,
            stage: ChOpportunityAuditStage::from(stage),
            stage_order: stage.order(),
            stage_at: request.observed_at.timestamp_millis(),
            payout_usd: Some(ChUsd::from(economics.payout_usd)),
            realized_pnl_usd: Some(ChUsd::from(economics.realized_pnl_usd)),
            settlement_status: Some(if economics.won {
                ChSettlementOutcome::Won
            } else {
                ChSettlementOutcome::Lost
            }),
            settlement_trigger: Some(ChSettlementTrigger::from(request.source)),
            winning_token_id: Some(request.winning_token_id.clone()),
            accounting_status: Some(ChSettlementAccountingStatus::from(
                position.settlement_accounting_status,
            )),
            redeem_route: Some(position.redeem_route.clone()),
            redeem_resolution: Some(position.redeem_resolution.as_str().to_owned()),
            fee_source: None,
            outcome: Some(ChAuditOutcome::Settled),
            rejection_stage: None,
            rejection_reason: None,
            scored_snapshot_json: None,
            book_context_json: None,
            applied_factor_ids_json: None,
            missing_fields_json: missing_fields_json(&[
                MissingEvidenceField::ScoredSnapshot,
                MissingEvidenceField::ResolutionProb,
                MissingEvidenceField::Confidence,
                MissingEvidenceField::FillProbability,
                MissingEvidenceField::PriceZone,
                MissingEvidenceField::DurationBucket,
                MissingEvidenceField::DepthUsedPct,
                MissingEvidenceField::Staleness,
                MissingEvidenceField::Category,
            ]),
            detected_at: trade.created_at.timestamp_millis(),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: 0,
            schema_version: ChSchemaVersion(3),
            updated_at: Utc::now().timestamp_millis(),
        }
    }
}

fn missing_fields_json(fields: &[MissingEvidenceField]) -> Option<String> {
    if fields.is_empty() {
        return None;
    }
    Some(serde_json::to_string(fields).unwrap_or_else(|_| "[]".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            AppliedFactorTrace, BookEvidenceSnapshot, CalibrationEvidenceSnapshot,
            execution::ResolvedOutcome,
            position::PositionInfo,
            scored_snapshot::ScoredOpportunitySnapshot,
            settlement::{MarketSettlementRequest, SettlementEconomics},
            trade::TradeInfo,
        },
        enums::{
            LegacyExecutionMode,
            calibration::{DurationBucket, PriceZone},
            common::{
                MarketCategory, PositionStatus, RedeemResolutionSource, RedeemStatus,
                SettlementAccountingStatus, SettlementTrigger, Side, StalenessLevel,
            },
            execution::ExecutionOutcome,
        },
        types::{
            Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, PositionId, Price,
            ReservationId, Shares, TokenId, TradeId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    fn test_snapshot() -> ScoredOpportunitySnapshot {
        ScoredOpportunitySnapshot {
            opportunity_id: OpportunityId::from_v7(),
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            token_id: TokenId::new("tok1"),
            token_yes: Some(TokenId::new("yes")),
            token_no: Some(TokenId::new("no")),
            side: Side::Buy,
            category: MarketCategory::Politics,
            entry_price: Price::new(dec!(0.92)),
            edge_bps: Bps::new(dec!(300)),
            expected_net_profit: Usd::new(dec!(4.5)),
            net_profit_if_correct: Usd::new(dec!(7.5)),
            shares: Shares::new(dec!(80)),
            total_cost: Usd::new(dec!(74.4)),
            total_fees: Usd::new(dec!(0.5)),
            resolution_prob: 0.95,
            resolution_prob_decimal: dec!(0.95),
            confidence: 0.95,
            confidence_decimal: dec!(0.95),
            fill_probability: None,
            score: None,
            urgency_factor: None,
            category_weight: None,
            staleness_discount: None,
            convergence_secs: 600,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
            depth_used_pct: 10.0,
            depth_used_pct_decimal: dec!(10.0),
            staleness: StalenessLevel::Fresh,
            calibration: CalibrationEvidenceSnapshot {
                sample_size: 50,
                alpha_prior: dec!(2.0),
                beta_prior: dec!(1.0),
                posterior_mean: dec!(0.93),
                fallback_tier: 1,
                snapshot_hash: None,
            },
            book: Some(BookEvidenceSnapshot {
                yes_book_version: Some(1),
                no_book_version: Some(2),
                book_age_ms: None,
                context_id: Some("ctx-1".to_owned()),
            }),
            factors: Some(AppliedFactorTrace::known_empty()),
            missing_fields: Vec::new(),
            detected_at: chrono::Utc::now(),
            schema_version: ScoredOpportunitySnapshot::SCHEMA_VERSION,
        }
    }

    fn test_trade(resolved: &ResolvedOutcome) -> TradeInfo {
        let now = chrono::Utc::now();
        TradeInfo {
            trade_id: TradeId::from_v7(),
            execution_id: ExecutionId::from_v7(),
            reservation_id: ReservationId::from_v7(),
            opportunity_id: OpportunityId::from_v7(),
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
            reconcile_resolution: None,
            reconciled_at: None,
            reconcile_note: None,
            pre_submit_ctf_balance: None,
            reconcile_attempts: 0,
            reconcile_defer_until: None,
            post_trade_claim_owner: None,
            post_trade_claimed_at: None,
            post_trade_attempts: 0,
            execution_mode: LegacyExecutionMode::Paper,
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
            execution_mode: LegacyExecutionMode::Paper,
            latency_ms: 42,
        };
        let resolved = ResolvedOutcome::try_resolve(&outcome, Price::new(dec!(0.92)), 0.95)
            .expect("test outcome is known");
        let trade = test_trade(&resolved);
        let snapshot = test_snapshot();
        let row = OpportunityAuditRow::from_terminal_trade(&trade, &snapshot);

        assert_eq!(
            row.filled_shares
                .expect("filled shares")
                .to_shares()
                .inner(),
            dec!(80)
        );
        assert_eq!(
            row.total_cost_usd.expect("total cost").to_usd().inner(),
            dec!(74.4)
        );
        assert_eq!(row.fees_usd.expect("fees").to_usd().inner(), dec!(0.5));
        assert!(
            row.net_profit_usd
                .expect("net profit")
                .to_usd()
                .is_positive(),
            "fill EV should be nonzero"
        );
        assert_eq!(
            row.expected_profit_usd
                .expect("expected profit")
                .to_usd()
                .inner(),
            dec!(4.5)
        );
        assert_eq!(row.outcome, Some(ChAuditOutcome::Success));
        assert!(row.rejection_stage.is_none());
    }

    #[test]
    fn from_execution_miss_zeros_financial() {
        let outcome = ExecutionOutcome::Miss {
            reason: "no fill".into(),
            execution_mode: LegacyExecutionMode::Paper,
        };
        let resolved = ResolvedOutcome::try_resolve(&outcome, Price::new(dec!(0.92)), 0.95)
            .expect("test outcome is known");
        let trade = test_trade(&resolved);
        let snapshot = test_snapshot();
        let row = OpportunityAuditRow::from_terminal_trade(&trade, &snapshot);

        assert!(row.total_cost_usd.expect("total cost").to_usd().is_zero());
        assert!(row.fees_usd.expect("fees").to_usd().is_zero());
        assert!(row.net_profit_usd.is_none());
        assert!(
            row.filled_shares
                .expect("filled shares")
                .to_shares()
                .is_zero()
        );
        assert_eq!(row.outcome, Some(ChAuditOutcome::Miss));
    }

    #[test]
    fn from_rejection_sets_stage_and_reason() {
        use crate::domain::calibration::{BucketKey, CalibrationSnapshot};
        use crate::domain::opportunity::{EndgameMeta, Opportunity};
        use crate::enums::opportunity::PayoutModel;

        let opp = Opportunity {
            opportunity_id: OpportunityId::from_v7(),
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
        let exec_id = ExecutionId::from_v7();
        let row = OpportunityAuditRow::from((&exec_id, &opp, "risk", "max exposure", &snapshot));

        assert_eq!(row.rejection_stage, Some(ChRejectionStage::Risk));
        assert_eq!(row.rejection_reason.as_deref(), Some("max exposure"));
        assert!(row.total_cost_usd.is_none());
        assert!(row.fees_usd.is_none());
        assert!(row.net_profit_usd.is_none());
    }

    #[test]
    fn settlement_row_restores_scored_snapshot_attribution() {
        let outcome = ExecutionOutcome::Filled {
            order_id: OrderId::new("ord1"),
            filled_shares: Shares::new(dec!(80)),
            avg_fill_price: Some(Price::new(dec!(0.93))),
            fee_paid: Usd::new(dec!(0.50)),
            tx_hash: Some("0xabc".into()),
            execution_mode: LegacyExecutionMode::Paper,
            latency_ms: 42,
        };
        let resolved = ResolvedOutcome::try_resolve(&outcome, Price::new(dec!(0.92)), 0.95)
            .expect("test outcome is known");
        let trade = test_trade(&resolved);
        let snapshot = test_snapshot();
        let now = chrono::Utc::now();
        let position = PositionInfo {
            position_id: PositionId::from_v7(),
            trade_id: trade.trade_id.clone(),
            market_id: trade.market_id.clone(),
            token_id: trade.token_id.clone(),
            side: Side::Buy,
            execution_mode: trade.execution_mode,
            shares: Shares::new(dec!(80)),
            avg_entry_price: Price::new(dec!(0.93)),
            total_cost_usd: Usd::new(dec!(74.4)),
            total_fees_usd: Usd::new(dec!(0.5)),
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            opened_at: now,
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            status: PositionStatus::Open,
            redeem_status: RedeemStatus::NotRequired,
            redeem_tx_hash: None,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: Some(SettlementTrigger::Ws),
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
            redeem_neg_risk: false,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
            redeem_gas_paid_usd: None,
        };
        let request = MarketSettlementRequest {
            market_id: trade.market_id.clone(),
            winning_token_id: trade.token_id.clone(),
            winning_outcome: "Yes".to_owned(),
            source: SettlementTrigger::Ws,
            observed_at: now,
        };
        let economics = SettlementEconomics {
            won: true,
            payout_usd: Usd::new(dec!(80)),
            realized_pnl_usd: Usd::new(dec!(5.1)),
        };

        let row = OpportunityAuditRow::from_settlement_trade(
            &trade, &position, &request, &economics, &snapshot,
        );

        assert_eq!(
            row.category,
            Some(ChMarketCategory::from(snapshot.category))
        );
        assert_eq!(row.price_zone, Some(ChPriceZone::from(snapshot.price_zone)));
        assert_eq!(
            row.duration_bucket,
            Some(ChDurationBucket::from(snapshot.duration_bucket))
        );
        assert_eq!(row.redeem_route.as_deref(), Some("standard_ctf"));
        assert_eq!(row.redeem_resolution.as_deref(), Some("class_standard"));
        assert!(row.scored_snapshot_json.is_some());
        assert!(row.missing_fields_json.is_none());
    }
}
