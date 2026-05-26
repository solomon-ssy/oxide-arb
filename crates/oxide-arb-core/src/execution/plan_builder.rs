use std::sync::Arc;

use chrono::Utc;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_models::domain::execution::{ExecutionPlan, ReservationHandle};
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::types::{ExecutionId, Shares, Usd};

pub struct PlanBuilder {
    fee_calculator: Arc<FeeCalculator>,
}

impl PlanBuilder {
    pub const fn new(fee_calculator: Arc<FeeCalculator>) -> Self {
        Self { fee_calculator }
    }

    pub fn build(
        &self,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
    ) -> ExecutionPlan {
        let shares = if opp.entry_price.inner() > rust_decimal::Decimal::ZERO {
            Shares::new((approved_size.inner() / opp.entry_price.inner()).round())
        } else {
            Shares::ZERO
        };
        let fee =
            self.fee_calculator
                .calculate(shares, opp.entry_price, opp.category, &opp.token_id);

        ExecutionPlan {
            execution_id: ExecutionId::generate(),
            opportunity_id: opp.opportunity_id.clone(),
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            side: opp.side,
            shares,
            limit_price: opp.entry_price,
            estimated_cost: approved_size,
            estimated_fee: fee,
            neg_risk: false,
            reservation_id: reservation.id.clone(),
            detected_at: opp.detected_at,
            planned_at: Utc::now(),
        }
    }
}
