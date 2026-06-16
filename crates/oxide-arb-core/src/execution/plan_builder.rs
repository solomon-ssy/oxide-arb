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

const CLOB_SHARE_DECIMAL_PLACES: u32 = 2;

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

    #[must_use]
    pub fn market_registry(&self) -> &MarketRegistry {
        &self.market_registry
    }

    pub fn build(
        &self,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
        execution_id: ExecutionId,
    ) -> ExecutionPlan {
        let neg_risk = self
            .market_registry
            .neg_risk(&opp.market_id)
            .unwrap_or(false);
        Self::build_plan_inner(
            self,
            opp,
            approved_size,
            reservation,
            execution_id,
            neg_risk,
        )
    }

    fn build_plan_inner(
        &self,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
        execution_id: ExecutionId,
        neg_risk: bool,
    ) -> ExecutionPlan {
        let shares = if opp.entry_price.inner() > rust_decimal::Decimal::ZERO {
            let raw = approved_size.inner() / opp.entry_price.inner();
            let lot_scale = rust_decimal::Decimal::new(10_i64.pow(CLOB_SHARE_DECIMAL_PLACES), 0);
            Shares::new((raw * lot_scale).floor() / lot_scale)
        } else {
            Shares::ZERO
        };
        let fee =
            self.fee_calculator
                .calculate(shares, opp.entry_price, opp.category, &opp.token_id);

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
