//! Non-linear urgency multiplier for time-to-settlement.
//!
//! Markets closer to their settlement deadline deserve higher priority
//! because missing the window has infinite opportunity cost. The urgency
//! factor uses a smoothstep curve to avoid harsh discontinuities.

use oxide_arb_models::types::{MICRO_SCALE, MicroProb};

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
    #[inline]
    pub fn compute(hours_remaining: i64, window_hours: u64) -> MicroProb {
        if window_hours == 0 {
            return MicroProb::ONE;
        }

        let h_win = i128::from(window_hours);
        let h_rem = i128::from(hours_remaining.max(0)).min(h_win);
        // progress ∈ [0, MICRO_SCALE]: 0 at window boundary, MICRO_SCALE at deadline.
        let progress =
            i64::try_from((h_win - h_rem) * i128::from(MICRO_SCALE) / h_win).unwrap_or(MICRO_SCALE);
        let t = progress.clamp(0, MICRO_SCALE);
        let t_sq = i128::from(t) * i128::from(t);
        let inner = i128::from(3 * MICRO_SCALE) - 2 * i128::from(t);
        let smoothstep = t_sq * inner / i128::from(MICRO_SCALE) / i128::from(MICRO_SCALE);
        let urgency_micro = i128::from(MICRO_SCALE) + 2 * smoothstep;
        let urgency = i64::try_from(urgency_micro).unwrap_or(MICRO_SCALE);
        MicroProb::from_factor_micro(urgency)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn at_deadline_maximum_urgency() {
        let u = UrgencyFactor::compute(0, 24);
        assert_eq!(u.to_decimal(), dec!(3));
    }

    #[test]
    fn at_window_boundary_neutral() {
        let u = UrgencyFactor::compute(24, 24);
        assert_eq!(u.to_decimal(), dec!(1));
    }

    #[test]
    fn beyond_window_still_neutral() {
        let u = UrgencyFactor::compute(48, 24);
        assert_eq!(u.to_decimal(), dec!(1));
    }

    #[test]
    fn midpoint_between_one_and_three() {
        let u = UrgencyFactor::compute(12, 24);
        assert!(u.to_decimal() > dec!(1));
        assert!(u.to_decimal() < dec!(3));
    }

    #[test]
    fn zero_window_returns_one() {
        let u = UrgencyFactor::compute(5, 0);
        assert_eq!(u.to_decimal(), dec!(1));
    }

    #[test]
    fn monotonically_increasing_as_deadline_approaches() {
        let u_far = UrgencyFactor::compute(20, 24);
        let u_mid = UrgencyFactor::compute(12, 24);
        let u_close = UrgencyFactor::compute(4, 24);
        let u_imminent = UrgencyFactor::compute(1, 24);

        assert!(u_far.micro() < u_mid.micro());
        assert!(u_mid.micro() < u_close.micro());
        assert!(u_close.micro() < u_imminent.micro());
    }
}
