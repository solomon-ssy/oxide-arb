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

/// Rate of change between a base and a more recent value: `(recent - base) / base`.
///
/// The momentum feature layer uses this over a lag-skipped window (base at
/// `t - W`, recent at `t - lag`) so momentum is **not** the endpoint-to-endpoint
/// simple return. `None` for a zero base. Exact `Decimal` arithmetic (quantized).
pub fn rate_of_change(base: Decimal, recent: Decimal) -> Option<Decimal> {
    if base.is_zero() {
        return None;
    }
    Some(((recent - base) / base).round_dp(STAT_SCALE))
}

/// Volatility-adjusted return: `return / realized_vol` (a Sharpe-like momentum).
///
/// `None` for a non-positive volatility. Exact `Decimal` arithmetic (quantized).
pub fn vol_adjusted(simple_return_value: Decimal, realized_vol: Decimal) -> Option<Decimal> {
    if realized_vol <= Decimal::ZERO {
        return None;
    }
    Some((simple_return_value / realized_vol).round_dp(STAT_SCALE))
}

/// The exponential moving average series of `series` with the given span (in
/// points). `alpha = 2 / (span + 1)`; seeded with the first observation.
///
/// Fully exact `Decimal` (fixed-point, deterministic across platforms), quantized
/// each step. `None` for an empty series or a zero span.
#[must_use]
pub fn ema_series(series: &[Decimal], span_points: u64) -> Option<Vec<Decimal>> {
    if series.is_empty() || span_points == 0 {
        return None;
    }
    let alpha = (Decimal::from(2) / Decimal::from(span_points + 1)).round_dp(STAT_SCALE);
    let one_minus_alpha = Decimal::ONE - alpha;
    let mut out = Vec::with_capacity(series.len());
    let mut previous = series[0];
    out.push(previous);
    for value in &series[1..] {
        previous = (alpha * value + one_minus_alpha * previous).round_dp(STAT_SCALE);
        out.push(previous);
    }
    Some(out)
}

/// The last value of the EMA series (the smoothed current level).
#[must_use]
pub fn ema_last(series: &[Decimal], span_points: u64) -> Option<Decimal> {
    ema_series(series, span_points).and_then(|ema| ema.last().copied())
}

/// The normalized instantaneous EMA slope: `(ema_last - ema_prev) / ema_last`.
///
/// This is the *current smoothed velocity* of the price — distinct from the
/// window's total return (`simple_return`), which is total displacement. `None`
/// for fewer than two EMA points or a zero level.
#[must_use]
pub fn ema_slope(series: &[Decimal], span_points: u64) -> Option<Decimal> {
    let ema = ema_series(series, span_points)?;
    if ema.len() < 2 {
        return None;
    }
    let last = ema[ema.len() - 1];
    let previous = ema[ema.len() - 2];
    if last.is_zero() {
        return None;
    }
    Some(((last - previous) / last).round_dp(STAT_SCALE))
}

/// Volatility-normalized MACD: `((EMA_fast - EMA_slow) / EMA_slow) / realized_vol`.
///
/// A vol-normalized trend-crossover estimator (Baz-style). `None` when either
/// EMA is undefined, the slow EMA is zero, or realized volatility is non-positive.
#[must_use]
pub fn macd_normalized(series: &[Decimal], fast_span: u64, slow_span: u64) -> Option<Decimal> {
    let fast = ema_last(series, fast_span)?;
    let slow = ema_last(series, slow_span)?;
    if slow.is_zero() {
        return None;
    }
    let macd_line = (fast - slow) / slow;
    let vol = realized_volatility(series)?;
    if vol <= Decimal::ZERO {
        return None;
    }
    Some((macd_line / vol).round_dp(STAT_SCALE))
}

#[cfg(test)]
mod tests {
    use super::{
        ema_last, ema_slope, macd_normalized, mean_reversion, rate_of_change, realized_volatility,
        simple_return, vol_adjusted,
    };
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

    #[test]
    fn momentum_roc_not_equal_simple_return() {
        // A price that runs up then reverses: the lag-skipped ROC (base → the
        // pre-reversal level) is positive, while the full-window simple return is
        // negative. Momentum is provably NOT a return clone (audit #2).
        let full = [
            Decimal::from(100),
            Decimal::from(110),
            Decimal::from(120),
            Decimal::from(95),
        ];
        // ROC over [t-W, t-lag]: base = 100, at-lag ≈ 120 (skip the final drop).
        let roc = rate_of_change(full[0], full[2]).expect("roc");
        let full_return = simple_return(&full).expect("return");
        assert!(
            roc > Decimal::ZERO,
            "pre-reversal momentum is positive: {roc}"
        );
        assert!(
            full_return < Decimal::ZERO,
            "full return is negative: {full_return}"
        );
        assert_ne!(
            roc, full_return,
            "momentum ROC must differ from simple return"
        );
    }

    #[test]
    fn ema_last_smooths_toward_recent() {
        let series = [Decimal::from(100), Decimal::from(100), Decimal::from(130)];
        let ema = ema_last(&series, 2).expect("ema");
        // The EMA of a step-up lands strictly between the old and new level.
        assert!(
            ema > Decimal::from(100) && ema < Decimal::from(130),
            "{ema}"
        );
    }

    #[test]
    fn ema_slope_is_signed_by_recent_move() {
        let up = [Decimal::from(100), Decimal::from(101), Decimal::from(105)];
        let down = [Decimal::from(105), Decimal::from(101), Decimal::from(100)];
        assert!(ema_slope(&up, 2).expect("slope") > Decimal::ZERO);
        assert!(ema_slope(&down, 2).expect("slope") < Decimal::ZERO);
    }

    #[test]
    fn vol_adjusted_is_return_over_vol() {
        assert_eq!(
            vol_adjusted(Decimal::new(6, 2), Decimal::new(2, 2)),
            Some(Decimal::from(3))
        );
        assert_eq!(vol_adjusted(Decimal::new(6, 2), Decimal::ZERO), None);
    }

    #[test]
    fn macd_normalized_defined_for_trending_series() {
        let series: Vec<Decimal> = (0..30).map(|i| Decimal::from(100 + i)).collect();
        let macd = macd_normalized(&series, 5, 15).expect("macd");
        // A steady uptrend keeps the fast EMA above the slow EMA → positive MACD.
        assert!(macd > Decimal::ZERO, "{macd}");
    }
}
