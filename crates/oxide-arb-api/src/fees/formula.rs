//! Polymarket fee formula implementation.
//!
//! fee = shares × price × feeRate × (price × (1 - price))^exponent
//!
//! - Precision: 4 decimal places
//! - Values < 0.0001 round to 0

use oxide_arb_models::types::{Price, Shares, Usd};
use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use rust_decimal_macros::dec;

/// Calculate Polymarket trading fee.
///
/// Returns `Usd::ZERO` when fees are effectively negligible (< 0.0001).
pub fn calculate_fee(shares: Shares, price: Price, fee_rate: Decimal, exponent: Decimal) -> Usd {
    let p = price.inner();

    if p == Decimal::ZERO || p == Decimal::ONE {
        return Usd::ZERO;
    }

    let p_complement = Decimal::ONE - p;
    let volatility_factor = (p * p_complement).powd(exponent);
    let raw_fee = shares.inner() * p * fee_rate * volatility_factor;

    let rounded = raw_fee.round_dp(4);
    if rounded < dec!(0.0001) {
        Usd::ZERO
    } else {
        Usd::new(rounded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_at_price_zero_is_zero() {
        let fee = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(Decimal::ZERO),
            dec!(0.02),
            dec!(1.5),
        );
        assert_eq!(fee, Usd::ZERO);
    }

    #[test]
    fn fee_at_price_one_is_zero() {
        let fee = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(Decimal::ONE),
            dec!(0.02),
            dec!(1.5),
        );
        assert_eq!(fee, Usd::ZERO);
    }

    #[test]
    fn fee_at_midpoint_is_maximum() {
        let fee_mid = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(dec!(0.5)),
            dec!(0.02),
            dec!(1.0),
        );
        let fee_side = calculate_fee(
            Shares::new(dec!(100)),
            Price::new(dec!(0.9)),
            dec!(0.02),
            dec!(1.0),
        );
        assert!(fee_mid > fee_side);
    }
}
