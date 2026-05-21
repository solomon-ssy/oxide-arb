//! Non-linear urgency multiplier for time-to-settlement.
//!
//! Markets closer to their settlement deadline deserve higher priority
//! because missing the window has infinite opportunity cost. The urgency
//! factor uses a smoothstep curve to avoid harsh discontinuities.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Stateless urgency factor calculator.
pub struct UrgencyFactor;

impl UrgencyFactor {
    /// Compute urgency multiplier ∈ [1.0, 3.0] using smoothstep interpolation.
    ///
    /// - `hours_remaining` = 0  → urgency = 3.0 (maximum)
    /// - `hours_remaining` >= `window_hours` → urgency = 1.0 (neutral)
    ///
    /// Smoothstep: `t² × (3 − 2t)` where `t = progress ∈ [0, 1]`.
    #[must_use]
    pub fn compute(hours_remaining: Decimal, window_hours: Decimal) -> Decimal {
        if window_hours.is_zero() {
            return Decimal::ONE;
        }

        let ratio = hours_remaining / window_hours;
        let progress = (Decimal::ONE - ratio).max(Decimal::ZERO).min(Decimal::ONE);

        let t_sq = progress * progress;
        let smoothstep = t_sq * (dec!(3) - dec!(2) * progress);

        Decimal::ONE + smoothstep * dec!(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn at_deadline_maximum_urgency() {
        let u = UrgencyFactor::compute(Decimal::ZERO, dec!(24));
        assert_eq!(u, dec!(3));
    }

    #[test]
    fn at_window_boundary_neutral() {
        let u = UrgencyFactor::compute(dec!(24), dec!(24));
        assert_eq!(u, Decimal::ONE);
    }

    #[test]
    fn beyond_window_still_neutral() {
        let u = UrgencyFactor::compute(dec!(48), dec!(24));
        assert_eq!(u, Decimal::ONE);
    }

    #[test]
    fn midpoint_between_one_and_three() {
        let u = UrgencyFactor::compute(dec!(12), dec!(24));
        assert!(u > Decimal::ONE);
        assert!(u < dec!(3));
    }

    #[test]
    fn zero_window_returns_one() {
        let u = UrgencyFactor::compute(dec!(5), Decimal::ZERO);
        assert_eq!(u, Decimal::ONE);
    }

    #[test]
    fn monotonically_increasing_as_deadline_approaches() {
        let u_far = UrgencyFactor::compute(dec!(20), dec!(24));
        let u_mid = UrgencyFactor::compute(dec!(12), dec!(24));
        let u_close = UrgencyFactor::compute(dec!(4), dec!(24));
        let u_imminent = UrgencyFactor::compute(dec!(1), dec!(24));

        assert!(u_far < u_mid);
        assert!(u_mid < u_close);
        assert!(u_close < u_imminent);
    }
}
