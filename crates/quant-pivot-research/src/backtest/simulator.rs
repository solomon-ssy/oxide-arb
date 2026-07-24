//! Resolution settlement of an already executed historical BUY.
//!
//! Entry price, depth, fee rounding, and cash-budget feasibility are resolved
//! upstream by the shared venue book walk. This module only applies the exact
//! token payout to that frozen executable fill; it never reconstructs a gross
//! return from a reference price.

use quant_pivot_models::types::PayoutRatio;

use crate::execution_semantics::{
    BookWalkFill, FeeError, ResolutionBuyEconomics, ResolutionBuySettlement,
};

/// Settle one executable cash-budget BUY against the binary market truth.
pub fn settle_executed_buy(
    fill: &BookWalkFill,
    token_payout_ratio: PayoutRatio,
) -> Result<ResolutionBuySettlement, FeeError> {
    ResolutionBuyEconomics::from_fill(fill).map(|economics| economics.settle(token_payout_ratio))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::market::{book::BookLevel, fee::BuilderFeeAttribution},
        enums::quant::FillRequirement,
        types::{Bps, ContentHash, PayoutRatio, Price, Shares, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::settle_executed_buy;
    use crate::execution_semantics::{LiquidityRole, PitFeeSchedule, walk_buy_cash_budget};

    fn schedule() -> PitFeeSchedule {
        let at = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        PitFeeSchedule {
            schedule_hash: ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash"),
            effective_at: at,
            available_at: at,
            platform_rate: dec!(0.05),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        }
    }

    #[test]
    fn settlement_uses_walk_cash_outlay_and_fee() {
        let fees = schedule();
        let asks = [BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.5)),
            Shares::new(dec!(1000)),
        )];
        let fill = walk_buy_cash_budget(
            &asks,
            Usd::new(dec!(25)),
            Price::new(dec!(0.5)),
            FillRequirement::AllOrNothing,
            &fees,
            LiquidityRole::Taker,
            fees.effective_at,
        )
        .expect("walk");
        let won = settle_executed_buy(&fill, PayoutRatio::ONE).expect("settlement");
        let lost = settle_executed_buy(&fill, PayoutRatio::ZERO).expect("settlement");
        let split =
            settle_executed_buy(&fill, PayoutRatio::try_new(dec!(0.5)).expect("half payout"))
                .expect("settlement");

        assert_eq!(won.economics.entry_fee, fill.expected_fee);
        assert_eq!(
            lost.realized_pnl_usd,
            Usd::new(-lost.economics.cash_outlay.inner())
        );
        assert!(won.realized_return_bps < Bps::new(dec!(10_000)));
        assert_eq!(
            split.payout_usd,
            Usd::new(split.economics.filled_shares.inner() * dec!(0.5))
        );
        assert!(!won.economics.all_in_price.inner().is_zero());
    }
}
