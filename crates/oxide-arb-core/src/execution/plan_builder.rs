use crate::pipeline::market_registry::MarketRegistry;
use chrono::Utc;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_models::{
    domain::{
        execution::{ExecutionPlan, ReservationHandle},
        opportunity::Opportunity,
    },
    types::{ExecutionId, Shares, Usd},
};
use std::sync::Arc;

pub struct PlanBuilder {
    fee_calculator: Arc<FeeCalculator>,
    market_registry: Arc<MarketRegistry>,
}

impl PlanBuilder {
    pub const fn new(
        fee_calculator: Arc<FeeCalculator>,
        market_registry: Arc<MarketRegistry>,
    ) -> Self {
        Self {
            fee_calculator,
            market_registry,
        }
    }

    pub fn build(
        &self,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
        execution_id: ExecutionId,
    ) -> ExecutionPlan {
        let shares = if opp.entry_price.inner() > rust_decimal::Decimal::ZERO {
            Shares::new((approved_size.inner() / opp.entry_price.inner()).round())
        } else {
            Shares::ZERO
        };
        let fee =
            self.fee_calculator
                .calculate(shares, opp.entry_price, opp.category, &opp.token_id);

        let neg_risk = self
            .market_registry
            .get_market(&opp.market_id)
            .is_some_and(|market| market.neg_risk);

        ExecutionPlan {
            execution_id,
            opportunity_id: opp.opportunity_id.clone(),
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            side: opp.side,
            shares,
            limit_price: opp.entry_price,
            estimated_cost: approved_size,
            estimated_fee: fee,
            category: opp.category,
            neg_risk,
            reservation_id: reservation.id.clone(),
            detected_at: opp.detected_at,
            planned_at: Utc::now(),
        }
    }
}
