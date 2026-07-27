//! Fitted normalization statistics and their deterministic reductions.
//!
//! Every step that crosses into `f64` (only the standard-deviation `sqrt`) is
//! **quantized to a fixed decimal scale** before it can build a
//! [`Probability`](quant_pivot_models::types::Probability), so a factor value is
//! bit-identical across hardware. All other arithmetic is exact `Decimal`.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::enums::factor::FactorIndeterminateReason;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

/// Decimal places every `f64`-derived statistic is quantized to. Twelve places
/// sits well inside `f64`'s ~15 digits of precision, so rounding is stable
/// across platforms (mirrors `features::stats::STAT_SCALE`).
pub(super) const NORM_SCALE: u32 = 12;

/// Frozen statistics a [`CrossSectionalNormalizer`](super::CrossSectionalNormalizer)
/// fits over a distribution and then applies pointwise.
///
/// Because it is fit once and applied per value, the same normalizer serves both
/// the online cross-section (fit + apply on today's column) and the historical
/// quantile path (fit on history, apply on today) — the training/serving parity
/// seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizationStats {
    /// Winsorize bounds plus the mean/std of the winsorized distribution.
    WinsorizedZScore {
        /// Lower winsorize bound (`winsor_p` quantile).
        lower: Decimal,
        /// Upper winsorize bound (`1 - winsor_p` quantile).
        upper: Decimal,
        /// Mean of the winsorized distribution.
        mean: Decimal,
        /// Population standard deviation of the winsorized distribution (`> 0`).
        std: Decimal,
        /// Sigma clamp bound applied to standardized scores.
        clamp_sigma: Decimal,
    },
    /// The ascending distribution used for average-rank lookup.
    Rank {
        /// Present values, sorted ascending.
        sorted: Vec<Decimal>,
    },
    /// Fixed semantic bounds for per-market min/max scaling.
    MinMax {
        /// Lower bound mapped to 0.
        lo: Decimal,
        /// Upper bound mapped to 1.
        hi: Decimal,
    },
    /// The distribution carried no dispersion — every present value is
    /// indeterminate for the recorded reason.
    Degenerate {
        /// Why the distribution could not be normalized.
        reason: FactorIndeterminateReason,
    },
}

/// The linearly interpolated type-7 quantile of an ascending slice at
/// percentile `p ∈ [0, 1]`.
///
/// Exact `Decimal` index arithmetic (no float cast), so the winsorize bounds are
/// deterministic. Empty input, an out-of-range percentile, or an index
/// conversion failure is a typed factor-computation error.
pub(super) fn quantile_value(sorted: &[Decimal], p: Decimal) -> QuantResult<Decimal> {
    let n = sorted.len();
    if n == 0 {
        return Err(ResearchError::FactorComputation {
            detail: "cannot fit a quantile from an empty distribution".to_owned(),
        }
        .into());
    }
    if p < Decimal::ZERO || p > Decimal::ONE {
        return Err(ResearchError::FactorComputation {
            detail: format!("quantile percentile must be in [0, 1], got {p}"),
        }
        .into());
    }
    let position = p * Decimal::from(n - 1);
    let lower_decimal = position.floor();
    let upper_decimal = position.ceil();
    let lower = lower_decimal
        .to_usize()
        .ok_or_else(|| ResearchError::FactorComputation {
            detail: format!("quantile index {lower_decimal} is not representable as usize"),
        })?
        .min(n - 1);
    let upper = upper_decimal
        .to_usize()
        .ok_or_else(|| ResearchError::FactorComputation {
            detail: format!("quantile index {upper_decimal} is not representable as usize"),
        })?
        .min(n - 1);
    let fraction = position - lower_decimal;
    Ok(sorted[lower] + ((sorted[upper] - sorted[lower]) * fraction))
}

/// Piecewise-linear percentile rank against a sorted reference distribution.
///
/// Exact observations use their average rank across ties. Unseen observations
/// interpolate between the average ranks of the adjacent distinct values. The
/// caller owns the ascending-order invariant; fitting and artifact validation
/// establish it once before this hot-path lookup.
pub(in crate::factors) fn interpolated_percentile(
    sorted: &[Decimal],
    raw: Decimal,
) -> QuantResult<Decimal> {
    if sorted.len() < 2 {
        return Err(ResearchError::FactorComputation {
            detail: "rank interpolation requires at least two observations".to_owned(),
        }
        .into());
    }
    let span = Decimal::from(sorted.len() - 1);
    if raw < sorted[0] {
        return Ok(Decimal::ZERO);
    }
    if raw > sorted[sorted.len() - 1] {
        return Ok(Decimal::ONE);
    }

    let first_equal = sorted.partition_point(|value| *value < raw);
    let after_equal = sorted.partition_point(|value| *value <= raw);
    let rank = if first_equal < after_equal {
        Decimal::from(first_equal + after_equal - 1) / Decimal::from(2_u8)
    } else {
        let lower_value = sorted[first_equal - 1];
        let upper_value = sorted[first_equal];
        let lower_rank = average_rank(sorted, lower_value);
        let upper_rank = average_rank(sorted, upper_value);
        let fraction = (raw - lower_value) / (upper_value - lower_value);
        lower_rank + ((upper_rank - lower_rank) * fraction)
    };
    Ok((rank / span).clamp(Decimal::ZERO, Decimal::ONE))
}

fn average_rank(sorted: &[Decimal], value: Decimal) -> Decimal {
    let first = sorted.partition_point(|entry| *entry < value);
    let after = sorted.partition_point(|entry| *entry <= value);
    Decimal::from(first + after - 1) / Decimal::from(2_u8)
}

/// The arithmetic mean of a non-empty slice (exact `Decimal`).
#[must_use]
pub(super) fn mean(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let sum: Decimal = values.iter().copied().sum();
    Some(sum / Decimal::from(values.len()))
}

/// The population standard deviation of a non-empty slice about `mean`.
///
/// The variance is exact `Decimal`; only the `sqrt` crosses into `f64`, and its
/// result is quantized to [`NORM_SCALE`]. Returns `Ok(None)` only when the slice
/// is empty; conversion and non-finite failures are typed errors.
pub(super) fn population_std(values: &[Decimal], mean: Decimal) -> QuantResult<Option<Decimal>> {
    if values.is_empty() {
        return Ok(None);
    }
    let count = Decimal::from(values.len());
    let variance = values
        .iter()
        .map(|value| {
            let deviation = *value - mean;
            deviation * deviation
        })
        .sum::<Decimal>()
        / count;
    let variance_f64 = variance
        .to_f64()
        .ok_or_else(|| ResearchError::FactorComputation {
            detail: format!("normalization variance {variance} is not representable as f64"),
        })?;
    let std = variance_f64.sqrt();
    if !std.is_finite() {
        return Err(ResearchError::FactorComputation {
            detail: format!("normalization standard deviation is non-finite: {std}"),
        }
        .into());
    }
    let decimal = Decimal::from_f64(std).ok_or_else(|| ResearchError::FactorComputation {
        detail: format!("normalization standard deviation {std} is not representable as Decimal"),
    })?;
    Ok(Some(decimal.round_dp(NORM_SCALE)))
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{interpolated_percentile, quantile_value};

    #[test]
    fn quantile_uses_linear_interpolation() {
        let sorted = [dec!(0), dec!(10), dec!(20), dec!(30)];
        assert_eq!(
            quantile_value(&sorted, dec!(0.25)).expect("lower quartile"),
            dec!(7.5)
        );
        assert_eq!(
            quantile_value(&sorted, dec!(0.5)).expect("median"),
            dec!(15)
        );
    }

    #[test]
    fn percentile_interpolates_ties() {
        let sorted = [dec!(1), dec!(2), dec!(2), dec!(4)];
        assert_eq!(
            interpolated_percentile(&sorted, dec!(2)).expect("exact tie"),
            dec!(0.5)
        );
        assert_eq!(
            interpolated_percentile(&sorted, dec!(3)).expect("unseen midpoint"),
            dec!(0.75)
        );
        assert_eq!(
            interpolated_percentile(&sorted, dec!(1.5)).expect("lower midpoint"),
            dec!(0.25)
        );
    }

    #[test]
    fn endpoint_ties_average_rank() {
        assert_eq!(
            interpolated_percentile(&[dec!(1), dec!(1), dec!(2), dec!(3)], dec!(1))
                .expect("minimum tie"),
            dec!(1) / dec!(6)
        );
        assert_eq!(
            interpolated_percentile(&[dec!(1), dec!(2), dec!(3), dec!(3)], dec!(3))
                .expect("maximum tie"),
            dec!(5) / dec!(6)
        );
    }
}
