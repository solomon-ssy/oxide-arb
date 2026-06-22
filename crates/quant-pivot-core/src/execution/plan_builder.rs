use crate::pipeline::market_registry::MarketRegistry;
use chrono::Utc;
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{
        execution::{ExecutionPlan, ReservationHandle},
        fee::FeeQuoteInput,
        opportunity::Opportunity,
    },
    enums::{common::ExecutionMode, fee::FeeLiquidityRole},
    types::{ExecutionId, Shares, Usd},
};
use rust_decimal::Decimal;
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

    /// Shares implied by Kelly sizing at the opportunity entry price.
    #[must_use]
    pub fn shares_for_size(opp: &Opportunity, approved_size: Usd) -> Shares {
        if opp.entry_price.inner() <= Decimal::ZERO {
            return Shares::ZERO;
        }
        let raw = approved_size.inner() / opp.entry_price.inner();
        let lot_scale = Decimal::new(10_i64.pow(CLOB_SHARE_DECIMAL_PLACES), 0);
        Shares::new((raw * lot_scale).floor() / lot_scale)
    }

    /// Fee quote for a sized intent without building a full execution plan.
    pub fn preview_fee(
        &self,
        mode: ExecutionMode,
        opp: &Opportunity,
        approved_size: Usd,
    ) -> Result<Usd, OxideError> {
        let shares = Self::shares_for_size(opp, approved_size);
        let input = FeeQuoteInput {
            market_id: opp.market_id.clone(),
            token_id: opp.token_id.clone(),
            category: opp.category,
            side: opp.side,
            liquidity_role: FeeLiquidityRole::Taker,
            shares,
            price: opp.entry_price,
            allow_category_fallback: mode != ExecutionMode::Live,
        };
        self.fee_calculator
            .quote_for_mode(mode, input)
            .map(|quote| quote.fee_usd)
            .map_err(OxideError::from)
    }

    pub fn build(
        &self,
        mode: ExecutionMode,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
        execution_id: ExecutionId,
    ) -> Result<ExecutionPlan, OxideError> {
        let neg_risk = self
            .market_registry
            .neg_risk(&opp.market_id)
            .unwrap_or(false);
        Self::build_plan_inner(
            self,
            mode,
            opp,
            approved_size,
            reservation,
            execution_id,
            neg_risk,
        )
    }

    fn build_plan_inner(
        &self,
        mode: ExecutionMode,
        opp: &Opportunity,
        approved_size: Usd,
        reservation: &ReservationHandle,
        execution_id: ExecutionId,
        neg_risk: bool,
    ) -> Result<ExecutionPlan, OxideError> {
        let shares = Self::shares_for_size(opp, approved_size);
        let fee = self.preview_fee(mode, opp, approved_size)?;
        Ok(ExecutionPlan {
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
        })
    }
}
