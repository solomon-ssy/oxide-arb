//! Outcome resolver: realized return / `PnL` of a candidate against the settled
//! market truth.
//!
//! Binary markets settle a token at `1` (won) or `0` (lost). For a position
//! entered at `entry ∈ (0, 1]`, the realized return fraction is
//! `(payoff - entry) / entry`, where `payoff` is the chosen side's settled value.

use quant_pivot_models::{enums::quant::OutcomeSide, types::Price};
use rust_decimal::Decimal;

/// Basis-point denominator (`1.0` fraction = 10 000 bps).
const BPS_PER_UNIT: i64 = 10_000;

/// The settled payoff (`0` or `1`) of the chosen outcome given the YES outcome.
const fn payoff(outcome_side: OutcomeSide, settled_yes: bool) -> Decimal {
    match outcome_side {
        OutcomeSide::Yes => {
            if settled_yes {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        }
        OutcomeSide::No => {
            if settled_yes {
                Decimal::ZERO
            } else {
                Decimal::ONE
            }
        }
    }
}

/// Realized return in basis points for a candidate entered at `entry_price`.
///
/// Returns `0` for a non-positive entry price.
#[must_use]
pub fn realized_return_bps(
    outcome_side: OutcomeSide,
    entry_price: Price,
    settled_yes: bool,
) -> Decimal {
    let entry = entry_price.inner();
    if entry <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (payoff(outcome_side, settled_yes) - entry) / entry * Decimal::from(BPS_PER_UNIT)
}

/// Realized `PnL` (USD) for `allocated` capital entered at `entry_price`.
#[must_use]
pub fn realized_pnl_usd(
    allocated_usd: Decimal,
    outcome_side: OutcomeSide,
    entry_price: Price,
    settled_yes: bool,
) -> Decimal {
    let entry = entry_price.inner();
    if entry <= Decimal::ZERO || allocated_usd <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    allocated_usd * (payoff(outcome_side, settled_yes) - entry) / entry
}
