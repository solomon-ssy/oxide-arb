//! Polymarket V2 platform-fee formula.
//!
//! `fee = shares * fee_rate * (price * (1 - price)) ^ exponent`
//!
//! The venue rounds the resulting pUSD charge to five decimal places. Invalid
//! fixed-point input is rejected instead of being coerced to zero.

use quant_pivot_error::fee::FeeQuoteError;
use quant_pivot_models::types::{Price, Shares, Usd};
use rust_decimal::{Decimal, MathematicalOps, RoundingStrategy};

pub fn calculate_fee(
    shares: Shares,
    price: Price,
    fee_rate: Decimal,
    exponent: Decimal,
) -> Result<Usd, FeeQuoteError> {
    let shares = shares.inner();
    let price = price.inner();
    if shares < Decimal::ZERO {
        return Err(invalid("shares must be non-negative"));
    }
    if price < Decimal::ZERO || price > Decimal::ONE {
        return Err(invalid("price must be within [0, 1]"));
    }
    if fee_rate < Decimal::ZERO || fee_rate > Decimal::ONE {
        return Err(invalid("fee_rate must be within [0, 1]"));
    }
    if exponent < Decimal::ZERO {
        return Err(invalid("exponent must be non-negative"));
    }
    if shares == Decimal::ZERO
        || price == Decimal::ZERO
        || price == Decimal::ONE
        || fee_rate == Decimal::ZERO
    {
        return Ok(Usd::ZERO);
    }

    let curve = (price * (Decimal::ONE - price)).powd(exponent);
    let fee = (shares * fee_rate * curve)
        .round_dp_with_strategy(5, RoundingStrategy::MidpointAwayFromZero);
    if fee < Decimal::new(1, 5) {
        Ok(Usd::ZERO)
    } else {
        Ok(Usd::new(fee))
    }
}

fn invalid(detail: &'static str) -> FeeQuoteError {
    FeeQuoteError::InvalidCalculation {
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn fee_at_price_zero_is_zero() {
        let fee = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(Decimal::ZERO),
            dec!(0.02),
            dec!(1.5),
        )
        .expect("valid fee input");
        assert_eq!(fee, Usd::ZERO);
    }

    #[test]
    fn invalid_rate_fails_closed() {
        let result = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(dec!(0.5)),
            dec!(1.01),
            Decimal::ONE,
        );
        assert!(matches!(
            result,
            Err(FeeQuoteError::InvalidCalculation { .. })
        ));
    }

    #[test]
    fn exponent_zero_is_a_valid_flat_fee_curve() {
        let fee = calculate_fee(
            Shares::new(dec!(10)),
            Price::new(dec!(0.4)),
            dec!(0.02),
            Decimal::ZERO,
        )
        .expect("valid exponent-zero fee input");
        assert_eq!(fee, Usd::new(dec!(0.2)));
    }
}
