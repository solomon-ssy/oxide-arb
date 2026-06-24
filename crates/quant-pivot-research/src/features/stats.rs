//! Deterministic statistical reductions for windowed time-series features.
//!
//! Ratios and returns are exact `Decimal` arithmetic — cross-platform stable by
//! construction. The one reduction that needs a transcendental (realized
//! volatility's standard deviation) crosses into `f64` via `ndarray`, then is
//! **quantized to a fixed decimal scale** before it can enter a `FeatureVector`,
//! so `feature_hash` stays bit-identical across hardware (no unrounded `f64`
//! value ever reaches the vector or its canonical digest).

use ndarray::Array1;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

/// Decimal places every `f64`-derived statistic is quantized to before it can
/// enter a feature vector. Twelve places sits well inside `f64`'s ~15 decimal
/// digits of precision, so the rounding is stable across platforms.
const STAT_SCALE: u32 = 12;

/// Overall simple return across the series: `(last - first) / first`.
///
/// `None` for fewer than two points or a zero base. Exact `Decimal` arithmetic.
pub fn simple_return(series: &[Decimal]) -> Option<Decimal> {
    if series.len() < 2 {
        return None;
    }
    let first = *series.first()?;
    let last = *series.last()?;
    if first.is_zero() {
        return None;
    }
    Some((last - first) / first)
}

/// Distance of the last value from the window mean: `(mean - last) / mean`.
///
/// `None` for fewer than two points or a zero mean. Exact `Decimal` arithmetic.
pub fn mean_reversion(series: &[Decimal]) -> Option<Decimal> {
    if series.len() < 2 {
        return None;
    }
    let count = Decimal::from(series.len());
    let sum: Decimal = series.iter().copied().sum();
    let mean = sum / count;
    if mean.is_zero() {
        return None;
    }
    let last = *series.last()?;
    Some((mean - last) / mean)
}

/// Realized volatility: the population standard deviation of consecutive simple
/// returns.
///
/// Requires at least three points (two returns). Computed in `f64` via
/// `ndarray`, then quantized to [`STAT_SCALE`] so the result is deterministic.
/// `None` when any base is zero or the result is non-finite.
pub fn realized_volatility(series: &[Decimal]) -> Option<Decimal> {
    if series.len() < 3 {
        return None;
    }
    let mut returns = Vec::with_capacity(series.len() - 1);
    for pair in series.windows(2) {
        let prev = pair[0].to_f64()?;
        let next = pair[1].to_f64()?;
        if prev == 0.0 {
            return None;
        }
        returns.push((next - prev) / prev);
    }
    // ddof = 0 ⇒ population standard deviation (matches the realized-vol of the
    // observed window, not an inferential estimate).
    let std = Array1::from_vec(returns).std(0.0);
    if !std.is_finite() {
        return None;
    }
    Decimal::from_f64(std).map(|value| value.round_dp(STAT_SCALE))
}

#[cfg(test)]
mod tests {
    use super::{mean_reversion, realized_volatility, simple_return};
    use rust_decimal::Decimal;

    #[test]
    fn simple_return_is_exact() {
        let series = [Decimal::from(100), Decimal::from(110)];
        assert_eq!(simple_return(&series), Some(Decimal::new(1, 1)));
    }

    #[test]
    fn simple_return_needs_two_points_and_nonzero_base() {
        assert_eq!(simple_return(&[Decimal::from(100)]), None);
        assert_eq!(simple_return(&[Decimal::ZERO, Decimal::from(5)]), None);
    }

    #[test]
    fn realized_volatility_is_quantized_and_deterministic() {
        let series = [
            Decimal::from(100),
            Decimal::from(101),
            Decimal::from(100),
            Decimal::from(102),
        ];
        let first = realized_volatility(&series).expect("vol");
        let second = realized_volatility(&series).expect("vol");
        assert_eq!(first, second, "must be deterministic");
        assert!(first.scale() <= 12, "must be quantized to <= 12 dp");
        assert!(first > Decimal::ZERO);
    }

    #[test]
    fn realized_volatility_needs_three_points() {
        assert_eq!(
            realized_volatility(&[Decimal::from(1), Decimal::from(2)]),
            None
        );
    }

    #[test]
    fn mean_reversion_centers_on_window_mean() {
        let series = [Decimal::from(10), Decimal::from(20), Decimal::from(30)];
        // mean = 20, last = 30 ⇒ (20 - 30) / 20 = -0.5
        assert_eq!(mean_reversion(&series), Some(Decimal::new(-5, 1)));
    }
}
