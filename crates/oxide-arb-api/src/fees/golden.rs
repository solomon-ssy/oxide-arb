//! Golden fee vectors — must match Polymarket SDK `utilities` test suite.

use super::{
    formula::calculate_fee,
    reference::{platform_fee_usd, production_fee_usd, round_fee},
};
use oxide_arb_models::types::{Price, Shares};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const TOL: Decimal = dec!(0.0001);

fn close(actual: Decimal, expected: Decimal) {
    let diff = (actual - expected).abs();
    assert!(
        diff <= TOL,
        "fee mismatch: got {actual}, expected {expected}, diff {diff}"
    );
}

#[test]
fn formula_matches_reference_production_tiers() {
    // SDK tests pass `amount_usd` (notional); shares = amount_usd / price.
    let cases = [
        (dec!(100), dec!(0.5), dec!(0.03), dec!(1.5)),
        (dec!(100), dec!(0.3), dec!(0.03), dec!(2.1)),
        (dec!(100), dec!(0.7), dec!(0.03), dec!(0.9)),
        (dec!(100), dec!(0.5), dec!(0.04), dec!(2.0)),
        (dec!(100), dec!(0.5), dec!(0.05), dec!(2.5)),
        (dec!(100), dec!(0.5), dec!(0.072), dec!(3.6)),
    ];

    for (amount_usd, price, rate, expected) in cases {
        let shares = amount_usd / price;
        let ours =
            calculate_fee(Shares::new(shares), Price::new(price), rate, Decimal::ONE).inner();
        let reference = production_fee_usd(shares, price, rate);
        close(ours, reference);
        close(ours, expected);
    }
}

#[test]
fn formula_matches_sdk_legacy_exponent_two() {
    let amount_usd = dec!(100) * dec!(0.5);
    let price = dec!(0.5);
    let shares = amount_usd / price;
    let rate = dec!(0.25);
    let exponent = dec!(2);

    let ours = calculate_fee(Shares::new(shares), Price::new(price), rate, exponent).inner();
    let reference = platform_fee_usd(shares, price, rate, exponent);
    close(ours, reference);
    close(ours, dec!(1.5625));
}

#[test]
fn sub_minimum_fee_rounds_to_zero() {
    let fee = calculate_fee(
        Shares::new(dec!(1)),
        Price::new(dec!(0.5)),
        dec!(0.000001),
        dec!(1),
    );
    assert_eq!(fee, oxide_arb_models::types::Usd::ZERO);
    assert_eq!(round_fee(dec!(0.000005)), Decimal::ZERO);
}

#[test]
fn peak_fee_at_midpoint_exceeds_wings() {
    let mid = calculate_fee(
        Shares::new(dec!(100)),
        Price::new(dec!(0.5)),
        dec!(0.05),
        dec!(1),
    );
    let wing = calculate_fee(
        Shares::new(dec!(100)),
        Price::new(dec!(0.9)),
        dec!(0.05),
        dec!(1),
    );
    assert!(mid > wing);
}
