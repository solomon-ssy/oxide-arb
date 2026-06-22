//! Reference fee implementation aligned with Polymarket SDK `utilities` tests.
//!
//! Used as the golden oracle in unit tests — not called on the hot path.
//! Golden tests assert [`super::formula::calculate_fee`] matches this oracle
//! within official 5-decimal-place rounding.

use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;

/// Platform fee per official formula (exponent may be > 1 in legacy markets).
///
/// `fee = shares × rate × (price × (1 − price))^exponent` (SDK-aligned).
#[must_use]
pub fn platform_fee_usd(
    shares: Decimal,
    price: Decimal,
    fee_rate: Decimal,
    exponent: Decimal,
) -> Decimal {
    if price == Decimal::ZERO || price == Decimal::ONE {
        return Decimal::ZERO;
    }

    let p_complement = Decimal::ONE - price;
    let volatility_factor = (price * p_complement).powd(exponent);
    let raw = shares * fee_rate * volatility_factor;
    round_fee(raw)
}

/// Production Polymarket fee (exponent = 1 for all current categories).
#[must_use]
pub fn production_fee_usd(shares: Decimal, price: Decimal, fee_rate: Decimal) -> Decimal {
    platform_fee_usd(shares, price, fee_rate, Decimal::ONE)
}

#[must_use]
pub fn round_fee(raw: Decimal) -> Decimal {
    let rounded = raw.round_dp(5);
    if rounded < dec!(0.00001) {
        Decimal::ZERO
    } else {
        rounded
    }
}
