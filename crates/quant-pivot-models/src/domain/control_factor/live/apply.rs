//! Pure consumption math for live control-factor effects.
//!
//! This is the single implementation of the §8.3 consumption algorithms. The
//! scorer (algorithm), execution validation / risk / sizer (core, risk), and the
//! shadow evaluator all call these functions so baseline and shadow decisions
//! can never drift apart. Every function is total and side-effect free; callers
//! pair the numeric output with an [`AppliedControlFactor`] for audit.

use super::applied::AppliedControlFactor;
use crate::{
    enums::control_factor::ControlFactorType,
    types::{ControlFactorId, FactorPublicationId, Usd},
};
use rust_decimal::Decimal;

/// Clamp a value into the closed unit interval `[0, 1]`.
#[must_use]
#[inline]
pub fn clamp_unit(value: Decimal) -> Decimal {
    value.clamp(Decimal::ZERO, Decimal::ONE)
}

/// Bucket-risk haircut applied to the base resolution probability (`[0, 1]`).
#[must_use]
#[inline]
pub fn effective_resolution_prob(base: Decimal, haircut: Decimal) -> Decimal {
    clamp_unit(base * clamp_unit(haircut))
}

/// Recompute expected net profit under an effective resolution probability.
///
/// `payout_if_correct` is the raw payout assuming the predicted outcome wins,
/// i.e. `net_profit + total_cost + total_fees` for an endgame buy.
#[must_use]
#[inline]
pub fn expected_net_profit(
    payout_if_correct: Usd,
    total_cost: Usd,
    total_fees: Usd,
    effective_p: Decimal,
) -> Usd {
    Usd::new(payout_if_correct.inner() * effective_p - total_cost.inner() - total_fees.inner())
}

/// Effective minimum-edge threshold after a conservative bucket addon (bps).
#[must_use]
#[inline]
pub fn effective_min_edge_bps(base_min_edge_bps: Decimal, addon: Decimal) -> Decimal {
    base_min_edge_bps + addon.max(Decimal::ZERO)
}

/// Effective fill probability after an execution-quality multiplier (`[0, 1]`).
#[must_use]
#[inline]
pub fn effective_fill_probability(base: Decimal, multiplier: Decimal) -> Decimal {
    clamp_unit(base * clamp_unit(multiplier))
}

/// Effective slippage limit after a conservative addon (tightening).
///
/// May become negative, which the caller must treat as an outright rejection
/// (no admissible slippage remains for this opportunity).
#[must_use]
#[inline]
pub fn effective_slippage_limit_bps(base_limit_bps: Decimal, addon: Decimal) -> Decimal {
    base_limit_bps - addon.max(Decimal::ZERO)
}

/// Conservative size cap `base_size * multiplier`, never negative.
#[must_use]
#[inline]
pub fn size_cap(base_size: Usd, multiplier: Decimal) -> Usd {
    Usd::new((base_size.inner() * clamp_unit(multiplier)).max(Decimal::ZERO))
}

/// Build an audit trace for a bucket-risk resolution-probability haircut.
#[must_use]
pub fn bucket_resolution_trace(
    factor_id: ControlFactorId,
    publication_id: FactorPublicationId,
    base_prob: Decimal,
    effective_prob: Decimal,
) -> AppliedControlFactor {
    AppliedControlFactor::new(
        factor_id,
        ControlFactorType::BucketRisk,
        publication_id,
        base_prob,
        effective_prob,
        "bucket resolution haircut",
    )
}

/// Build an audit trace for an execution-quality fill-probability discount.
#[must_use]
pub fn execution_quality_fill_trace(
    factor_id: ControlFactorId,
    publication_id: FactorPublicationId,
    base_fill: Decimal,
    effective_fill: Decimal,
) -> AppliedControlFactor {
    AppliedControlFactor::new(
        factor_id,
        ControlFactorType::ExecutionQuality,
        publication_id,
        base_fill,
        effective_fill,
        "execution quality fill probability discount",
    )
}

/// Build an audit trace for a sizer size cap.
#[must_use]
pub fn size_cap_trace(
    factor_id: ControlFactorId,
    factor_type: ControlFactorType,
    publication_id: FactorPublicationId,
    base_size: Usd,
    capped_size: Usd,
    reason: impl Into<String>,
) -> AppliedControlFactor {
    AppliedControlFactor::new(
        factor_id,
        factor_type,
        publication_id,
        base_size.inner(),
        capped_size.inner(),
        reason,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn haircut_tightens_probability() {
        assert_eq!(
            effective_resolution_prob(dec!(0.96), dec!(0.9)),
            dec!(0.864)
        );
        // Out-of-range haircut is clamped.
        assert_eq!(effective_resolution_prob(dec!(0.96), dec!(1.5)), dec!(0.96));
    }

    #[test]
    fn expected_net_profit_recomputes_from_payout() {
        // payout 100, cost 90, fees 1, p 0.9 -> 100*0.9 - 90 - 1 = -1
        let enp = expected_net_profit(
            Usd::new(dec!(100)),
            Usd::new(dec!(90)),
            Usd::new(dec!(1)),
            dec!(0.9),
        );
        assert_eq!(enp.inner(), dec!(-1.0));
    }

    #[test]
    fn slippage_limit_can_go_negative() {
        assert_eq!(effective_slippage_limit_bps(dec!(20), dec!(25)), dec!(-5));
    }

    #[test]
    fn size_cap_is_conservative() {
        assert_eq!(size_cap(Usd::new(dec!(100)), dec!(0.5)).inner(), dec!(50.0));
        assert_eq!(size_cap(Usd::new(dec!(100)), dec!(-0.5)).inner(), dec!(0));
    }
}
