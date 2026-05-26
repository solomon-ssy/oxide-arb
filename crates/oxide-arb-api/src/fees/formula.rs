//! Polymarket fee formula — fixed-point hot path with endgame LUT.
//!
//! fee = shares × feeRate × (price × (1 - price))^exponent

use num_traits::ToPrimitive;
use oxide_arb_models::types::{MICRO_SCALE, MicroPrice, MicroShares, MicroUsd, Price, Shares, Usd};
use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use rust_decimal_macros::dec;
use std::sync::LazyLock;

/// Precomputed volatility factor `(p × (1−p))^exp` for endgame cents 95–99.
static ENDGAME_VOL_MICRO_LUT: LazyLock<[[i64; 5]; 2]> = LazyLock::new(|| {
    let mut lut = [[0_i64; 5]; 2];
    for (i, cent) in (95..=99).enumerate() {
        let p = Decimal::from(cent) / dec!(100);
        let pc = p * (Decimal::ONE - p);
        lut[0][i] = (pc * Decimal::from(MICRO_SCALE))
            .trunc()
            .to_i64()
            .unwrap_or(0);
        lut[1][i] = (pc.powd(dec!(1.5)) * Decimal::from(MICRO_SCALE))
            .trunc()
            .to_i64()
            .unwrap_or(0);
    }
    lut
});

#[inline]
fn exp_index(exponent: Decimal) -> Option<usize> {
    if exponent == dec!(1) {
        Some(0)
    } else if exponent == dec!(1.5) {
        Some(1)
    } else {
        None
    }
}

#[inline]
fn volatility_factor_micro(price: MicroPrice, exponent: Decimal) -> i64 {
    if let Some(exp_idx) = exp_index(exponent) {
        let cents = (price.to_decimal() * dec!(100)).trunc();
        if let Some(cent) = cents.to_u8()
            && (95..=99).contains(&cent)
        {
            return ENDGAME_VOL_MICRO_LUT[exp_idx][usize::from(cent - 95)];
        }
    }
    let p = price.to_decimal();
    let p_complement = Decimal::ONE - p;
    let vol = (p * p_complement).powd(exponent);
    (vol * Decimal::from(MICRO_SCALE))
        .trunc()
        .to_i64()
        .unwrap_or(0)
}

#[inline]
fn calculate_fee_micro(
    shares: MicroShares,
    price: MicroPrice,
    fee_rate_micro: i64,
    exponent: Decimal,
) -> MicroUsd {
    if price.micro() <= 0 || price.micro() >= MICRO_SCALE || shares.micro() <= 0 {
        return MicroUsd::ZERO;
    }
    let vol = volatility_factor_micro(price, exponent);
    let raw = i128::from(shares.micro()) * i128::from(fee_rate_micro) * i128::from(vol)
        / i128::from(MICRO_SCALE)
        / i128::from(MICRO_SCALE);
    MicroUsd::from_micro(ToPrimitive::to_i64(&raw).unwrap_or(i64::MAX))
}

/// Calculate Polymarket trading fee (production path).
#[inline]
pub fn calculate_fee(shares: Shares, price: Price, fee_rate: Decimal, exponent: Decimal) -> Usd {
    let p = price.inner();
    if p == Decimal::ZERO || p == Decimal::ONE {
        return Usd::ZERO;
    }

    let shares_m = MicroShares::try_from_decimal(shares.inner()).unwrap_or(MicroShares::ZERO);
    let price_m = MicroPrice::try_from_decimal(p).unwrap_or(MicroPrice::ZERO);
    let fee_rate_micro = (fee_rate * Decimal::from(MICRO_SCALE))
        .trunc()
        .to_i64()
        .unwrap_or(0);
    let fee = calculate_fee_micro(shares_m, price_m, fee_rate_micro, exponent);
    let rounded = fee.to_decimal().round_dp(4);
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
    fn endgame_lut_covers_95_to_99() {
        for cent in 95..=99 {
            let p = Decimal::from(cent) / dec!(100);
            let price_m = MicroPrice::try_from_decimal(p).unwrap();
            for exp in [dec!(1), dec!(1.5)] {
                let idx = usize::from(cent - 95u8);
                let lut = ENDGAME_VOL_MICRO_LUT[usize::from(exp != dec!(1))][idx];
                let computed = volatility_factor_micro(price_m, exp);
                assert_eq!(lut, computed);
            }
        }
    }

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
}
