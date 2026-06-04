use oxide_arb_error::control::{StatsError, StatsResult};
use oxide_arb_models::domain::control_factor::ConfidenceInterval;
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RatioEstimate {
    pub lower: Decimal,
    pub point: Decimal,
    pub upper: Decimal,
}

impl RatioEstimate {
    #[must_use]
    pub fn to_confidence_interval(&self) -> ConfidenceInterval {
        ConfidenceInterval {
            lower: self.lower,
            point_estimate: self.point,
            upper: self.upper,
            confidence_level: Decimal::new(95, 2),
        }
    }
}

/// Neutral bounds for rule-based or sample-insufficient factors (not a Wilson/materialized estimate).
#[must_use]
pub fn unestimated_confidence_interval() -> ConfidenceInterval {
    ConfidenceInterval {
        lower: Decimal::ZERO,
        point_estimate: Decimal::ONE,
        upper: Decimal::ONE,
        confidence_level: Decimal::new(95, 2),
    }
}

pub fn conservative_ratio(numerator: Decimal, denominator: Decimal) -> StatsResult<Decimal> {
    if denominator <= Decimal::ZERO {
        return Err(StatsError::ZeroDenominator);
    }
    Ok(clamp_unit(numerator / denominator))
}

pub fn conservative_addon(observed: Decimal, baseline: Decimal) -> Decimal {
    (observed - baseline).max(Decimal::ZERO)
}

pub fn clamp_unit(value: Decimal) -> Decimal {
    value.clamp(Decimal::ZERO, Decimal::ONE)
}

pub fn percentile(values: &[Decimal], percentile_bps: u32) -> StatsResult<Decimal> {
    if values.is_empty() {
        return Err(StatsError::EmptySample);
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let len = sorted.len();
    let max_index = len.saturating_sub(1);
    let rank = (u128::from(max_index as u64) * u128::from(percentile_bps)).div_ceil(10_000);
    let index = usize::try_from(rank).unwrap_or(max_index).min(max_index);
    Ok(sorted[index])
}

pub fn observed_rate_lower_bound(successes: u64, samples: u64) -> StatsResult<RatioEstimate> {
    if samples == 0 {
        return Err(StatsError::EmptySample);
    }
    let point = Decimal::from(successes) / Decimal::from(samples);
    let uncertainty_bps = if samples >= 400 {
        Decimal::new(500, 4)
    } else if samples >= 100 {
        Decimal::new(1_000, 4)
    } else if samples >= 25 {
        Decimal::new(2_000, 4)
    } else {
        Decimal::new(4_000, 4)
    };
    let lower = clamp_unit(point - uncertainty_bps);
    let upper = clamp_unit(point + uncertainty_bps);
    Ok(RatioEstimate {
        lower,
        point: clamp_unit(point),
        upper,
    })
}

pub fn dominance_bps(largest_group_count: u64, total_count: u64) -> StatsResult<u32> {
    if total_count == 0 {
        return Err(StatsError::EmptySample);
    }
    let bps = largest_group_count.saturating_mul(10_000) / total_count;
    Ok(u32::try_from(bps).unwrap_or(u32::MAX))
}

pub const fn parent_bucket_required(child_samples: u64, min_samples: u64) -> bool {
    child_samples < min_samples
}

#[cfg(test)]
mod tests {
    use super::{StatsError, conservative_addon, conservative_ratio, dominance_bps, percentile};
    use rust_decimal_macros::dec;

    #[test]
    fn ratio_rejects_zero_denominator() {
        assert_eq!(
            conservative_ratio(dec!(1), dec!(0)),
            Err(StatsError::ZeroDenominator)
        );
    }

    #[test]
    fn percentile_uses_conservative_upper_rank() {
        assert_eq!(
            percentile(&[dec!(1), dec!(3), dec!(2), dec!(4)], 7_500),
            Ok(dec!(4))
        );
    }

    #[test]
    fn addon_never_goes_negative() {
        assert_eq!(conservative_addon(dec!(10), dec!(15)), dec!(0));
    }

    #[test]
    fn dominance_is_bps() {
        assert_eq!(dominance_bps(3, 4), Ok(7_500));
    }
}
