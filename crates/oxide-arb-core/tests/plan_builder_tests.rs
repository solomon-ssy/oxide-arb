//! `PlanBuilder` `neg_risk` resolution from `MarketRegistry`.

use chrono::Utc;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_core::{
    execution::plan_builder::PlanBuilder, pipeline::market_registry::MarketRegistry,
};
use oxide_arb_models::{
    domain::{
        execution::ReservationHandle,
        market::{MarketRegistryInfo, TokenInfo},
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, ExecutionId, MarketId, Price, ReservationId, TokenId, Usd},
};
use oxide_arb_test_support::fixtures::sample_opportunity;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn sample_market(id: &str, neg_risk: bool) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(id),
        event_id: EventId::new("evt-1"),
        token_yes: TokenId::new(format!("{id}-yes")),
        token_no: TokenId::new(format!("{id}-no")),
        question: "Test?".into(),
        slug: "test".into(),
        category: MarketCategory::Other,
        status: MarketStatus::Active,
        outcome: None,
        neg_risk,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(format!("{id}-yes")),
                outcome: "Yes".into(),
                neg_risk,
            },
            TokenInfo {
                token_id: TokenId::new(format!("{id}-no")),
                outcome: "No".into(),
                neg_risk,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(5),
        volume_24h: Usd::ZERO,
        fee_schedule: None,
        end_date: None,
        resolved_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn plan_neg_risk_from_market_registry() {
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(sample_market("0xneg_risk_market", true));

    let builder = PlanBuilder::new(Arc::new(FeeCalculator::default()), Arc::clone(&registry));

    let mut opp = sample_opportunity();
    opp.market_id = MarketId::new("0xneg_risk_market");

    let reservation = ReservationHandle {
        id: ReservationId::new_id(),
        amount: Usd::new(dec!(20)),
        market_id: opp.market_id.clone(),
    };

    let plan = builder.build(
        &opp,
        Usd::new(dec!(20)),
        &reservation,
        ExecutionId::generate(),
    );
    assert!(
        plan.neg_risk,
        "neg_risk market must propagate to execution plan"
    );
}

#[test]
fn plan_neg_risk_false_when_market_unknown() {
    let registry = Arc::new(MarketRegistry::new());
    let builder = PlanBuilder::new(Arc::new(FeeCalculator::default()), Arc::clone(&registry));

    let opp = sample_opportunity();
    let reservation = ReservationHandle {
        id: ReservationId::new_id(),
        amount: Usd::new(dec!(20)),
        market_id: opp.market_id.clone(),
    };

    let plan = builder.build(
        &opp,
        Usd::new(dec!(20)),
        &reservation,
        ExecutionId::generate(),
    );
    assert!(
        !plan.neg_risk,
        "unknown market must default neg_risk to false (fail-safe until Gamma sync)"
    );
}

#[test]
fn plan_shares_never_exceed_approved_notional() {
    let registry = Arc::new(MarketRegistry::new());
    let builder = PlanBuilder::new(Arc::new(FeeCalculator::default()), Arc::clone(&registry));

    let mut opp = sample_opportunity();
    opp.entry_price = Price::new(dec!(0.97));
    let approved = Usd::new(dec!(20));
    let reservation = ReservationHandle {
        id: ReservationId::new_id(),
        amount: approved,
        market_id: opp.market_id.clone(),
    };

    let plan = builder.build(&opp, approved, &reservation, ExecutionId::generate());
    let planned_notional = plan.shares * plan.limit_price;

    assert!(
        planned_notional <= approved,
        "planned notional {planned_notional} must not exceed approved size {approved}"
    );
    assert_eq!(plan.shares.inner(), dec!(20.618556));
}
