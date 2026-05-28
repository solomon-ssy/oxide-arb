//! Canonical test fixtures for opportunities, scored opportunities, and post-trade jobs.

use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::{
    domain::{
        calibration::{BucketKey, CalibrationSnapshot},
        execution::PostTradeJob,
        latency::LatencyTrace,
        opportunity::{EndgameMeta, Opportunity},
        scored_snapshot::ScoredOpportunitySnapshot,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{ExecutionMode, MarketCategory, Side, StalenessLevel},
        execution::ExecutionOutcome,
        opportunity::PayoutModel,
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, MicroProb, MicroScore, OpportunityId, Price, Shares,
        TokenId, TradeId, Usd,
    },
};
use rust_decimal_macros::dec;
use std::sync::Arc;

#[must_use]
pub fn sample_opportunity() -> Opportunity {
    Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xtest_market"),
        event_id: EventId::new("test_event"),
        token_id: TokenId::new("yes-token"),
        side: Side::Buy,
        payout_model: PayoutModel::DirectionalSettlement {
            projected_payout_if_correct: Usd::new(dec!(100)),
            expected_payout: Usd::new(dec!(95)),
            predicted_side: Side::Buy,
        },
        shares: Shares::new(dec!(100)),
        entry_price: Price::new(dec!(0.92)),
        total_cost: Usd::new(dec!(20)),
        total_fees: Usd::new(dec!(0.40)),
        net_profit: Usd::new(dec!(5)),
        expected_net_profit: Usd::new(dec!(4.5)),
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
        detected_at: Utc::now(),
    }
}

#[must_use]
pub fn sample_scored() -> Arc<ScoredOpportunity> {
    let yes = TokenId::new("yes-token");
    let no = TokenId::new("no-token");

    Arc::new(ScoredOpportunity {
        opportunity: Arc::new(sample_opportunity()),
        score: MicroScore::try_from_decimal(dec!(0.8)).unwrap(),
        token_yes: yes,
        token_no: no,
        book_yes_version: 1,
        book_no_version: 1,
        fill_probability: MicroProb::try_from_decimal(dec!(0.99)).unwrap(),
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        trace: Arc::new(LatencyTrace::default()),
    })
}

#[must_use]
pub fn minimal_post_trade_job(trade_id: &str) -> PostTradeJob {
    let opp = sample_opportunity();
    PostTradeJob {
        trade_id: TradeId::new(trade_id),
        execution_id: ExecutionId::generate(),
        opportunity_id: opp.opportunity_id.clone(),
        market_id: opp.market_id.clone(),
        event_id: opp.event_id.clone(),
        token_id: opp.token_id.clone(),
        side: opp.side,
        plan_shares: opp.shares,
        entry_price: opp.entry_price,
        execution_mode: ExecutionMode::Paper,
        edge_bps: Some(opp.edge_bps),
        detected_profit: Some(opp.expected_net_profit),
        detected_at: opp.detected_at,
        category: opp.category,
        scored_snapshot: ScoredOpportunitySnapshot::from_opportunity(&opp),
        outcome: ExecutionOutcome::Miss {
            reason: "test".into(),
            execution_mode: ExecutionMode::Paper,
        },
    }
}
