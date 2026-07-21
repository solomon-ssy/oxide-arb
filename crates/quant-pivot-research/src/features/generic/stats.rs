//! Deterministic statistical reductions for windowed time-series features.
//!
//! Ratios and returns are exact `Decimal` arithmetic — cross-platform stable by
//! construction. The one reduction that needs a transcendental (realized
//! volatility's standard deviation) crosses into `f64` via `ndarray`, then is
//! **quantized to a fixed decimal scale** before it can enter a `FeatureVector`,
//! so `feature_hash` stays bit-identical across hardware (no unrounded `f64`
//! value ever reaches the vector or its canonical digest).

use std::f64::consts::LN_2;

use ndarray::Array1;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
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

/// Time-decayed EMA over irregularly spaced `(epoch_ms, value)` samples.
///
/// The smoothing horizon is a true **duration**, not a point count: a past
/// observation's weight halves every `halflife_secs` of elapsed time
/// (`decay_i = 2^(-Δt_i / halflife)`, i.e. `alpha_i = 1 - exp(-Δt_i / τ)` with
/// `τ = halflife / ln 2`), so `ema_i = (1 - decay_i)·value_i + decay_i·ema_{i-1}`.
/// This is the irregular-interval generalization of a fixed-span EMA and the
/// correct estimator for sparse, unevenly sampled prediction-market books — a
/// fixed point-count span would smooth over wildly different time horizons on a
/// dense vs. a sparse book.
///
/// `samples` must be ascending by time. A non-monotonic timestamp is a typed
/// determinism failure; it is never treated as a simultaneous observation. The
/// transcendental (`exp`) crosses into `f64`, then every output is quantized to
/// [`STAT_SCALE`] so the series is deterministic across platforms. `None` for an
/// empty series or a zero half-life. The first sample seeds the series exactly.
///
/// # Errors
///
/// Returns a determinism error for non-monotonic timestamps, overflowing time
/// differences, non-finite arithmetic, or an unrepresentable numeric conversion.
pub fn ema_time_decayed(
    samples: &[(i64, Decimal)],
    halflife_secs: u64,
) -> QuantResult<Option<Vec<Decimal>>> {
    if samples.is_empty() || halflife_secs == 0 {
        return Ok(None);
    }
    let halflife = strict_f64(Decimal::from(halflife_secs), "EMA half-life")?;
    let mut out = Vec::with_capacity(samples.len());
    let (seed_ms, seed_value) = samples[0];
    out.push(seed_value.round_dp(STAT_SCALE));
    let mut previous = strict_f64(seed_value, "EMA seed")?;
    let mut previous_ms = seed_ms;
    for &(ms, value) in &samples[1..] {
        let value_f = strict_f64(value, "EMA observation")?;
        let delta_ms = ms.checked_sub(previous_ms).ok_or_else(|| {
            invalid_ema(format!(
                "EMA timestamp difference overflow: previous={previous_ms}, current={ms}"
            ))
        })?;
        if delta_ms < 0 {
            return Err(invalid_ema(format!(
                "EMA samples are not ascending: previous={previous_ms}, current={ms}"
            )));
        }
        let delta_secs = strict_f64(Decimal::from(delta_ms), "EMA elapsed milliseconds")? / 1000.0;
        let decay = (-delta_secs * LN_2 / halflife).exp();
        let ema = decay.mul_add(previous, (1.0 - decay) * value_f);
        if !decay.is_finite() || !ema.is_finite() {
            return Err(invalid_ema("EMA arithmetic produced a non-finite value"));
        }
        let quantized = Decimal::from_f64(ema)
            .map(|value| value.round_dp(STAT_SCALE))
            .ok_or_else(|| invalid_ema("EMA result cannot be represented as Decimal"))?;
        out.push(quantized);
        previous = ema;
        previous_ms = ms;
    }
    Ok(Some(out))
}

/// The normalized instantaneous EMA slope: `(ema_last - ema_prev) / ema_last`
/// over the time-decayed EMA (see [`ema_time_decayed`]).
///
/// This is the *current smoothed velocity* of the price — distinct from the
/// window's total return (`simple_return`), which is total displacement. `None`
/// for fewer than two EMA points or a zero level.
///
/// # Errors
///
/// Propagates [`ema_time_decayed`] timestamp and numeric failures.
pub fn ema_slope_time(
    samples: &[(i64, Decimal)],
    halflife_secs: u64,
) -> QuantResult<Option<Decimal>> {
    let Some(ema) = ema_time_decayed(samples, halflife_secs)? else {
        return Ok(None);
    };
    if ema.len() < 2 {
        return Ok(None);
    }
    let last = ema[ema.len() - 1];
    let previous = ema[ema.len() - 2];
    if last.is_zero() {
        return Ok(None);
    }
    Ok(Some(((last - previous) / last).round_dp(STAT_SCALE)))
}

/// Volatility-normalized MACD: `((EMA_fast - EMA_slow) / EMA_slow) / realized_vol`.
///
/// A vol-normalized trend-crossover estimator (Baz-style) built on the
/// time-decayed EMA legs (half-lives in seconds), so its smoothing horizon is a
/// duration independent of sampling density. `None` when either EMA is
/// undefined, the slow EMA is zero, or realized volatility is non-positive.
///
/// # Errors
///
/// Propagates [`ema_time_decayed`] timestamp and numeric failures.
pub fn macd_time(
    samples: &[(i64, Decimal)],
    fast_halflife_secs: u64,
    slow_halflife_secs: u64,
) -> QuantResult<Option<Decimal>> {
    let Some(fast) =
        ema_time_decayed(samples, fast_halflife_secs)?.and_then(|ema| ema.last().copied())
    else {
        return Ok(None);
    };
    let Some(slow) =
        ema_time_decayed(samples, slow_halflife_secs)?.and_then(|ema| ema.last().copied())
    else {
        return Ok(None);
    };
    if slow.is_zero() {
        return Ok(None);
    }
    let macd_line = (fast - slow) / slow;
    let values: Vec<Decimal> = samples.iter().map(|&(_, value)| value).collect();
    let Some(vol) = realized_volatility(&values) else {
        return Ok(None);
    };
    if vol <= Decimal::ZERO {
        return Ok(None);
    }
    Ok(Some((macd_line / vol).round_dp(STAT_SCALE)))
}

fn strict_f64(value: Decimal, field: &'static str) -> QuantResult<f64> {
    value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| invalid_ema(format!("{field} is not representable as a finite f64")))
}

fn invalid_ema(detail: impl Into<String>) -> QuantError {
    ResearchError::Determinism {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        ema_slope_time, ema_time_decayed, macd_time, mean_reversion, rate_of_change,
        realized_volatility, simple_return, vol_adjusted,
    };

    /// Build an ascending `(epoch_ms, value)` series on a fixed cadence.
    fn timed(values: &[i64], cadence_secs: i64) -> Vec<(i64, Decimal)> {
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    i64::try_from(index).unwrap() * cadence_secs * 1_000,
                    Decimal::from(*value),
                )
            })
            .collect()
    }

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
        let series = timed(&[100, 100, 130], 60);
        let ema = ema_time_decayed(&series, 120)
            .expect("valid EMA input")
            .and_then(|ema| ema.last().copied())
            .expect("ema");
        // The EMA of a step-up lands strictly between the old and new level.
        assert!(
            ema > Decimal::from(100) && ema < Decimal::from(130),
            "{ema}"
        );
    }

    #[test]
    fn ema_slope_is_signed_by_recent_move() {
        let up = timed(&[100, 101, 105], 60);
        let down = timed(&[105, 101, 100], 60);
        assert!(
            ema_slope_time(&up, 120)
                .expect("valid EMA input")
                .expect("slope")
                > Decimal::ZERO
        );
        assert!(
            ema_slope_time(&down, 120)
                .expect("valid EMA input")
                .expect("slope")
                < Decimal::ZERO
        );
    }

    #[test]
    fn ema_horizon_is_time_native_not_point_count() {
        // The same price path sampled at two different cadences (dense vs.
        // sparse) must produce the same time-decayed EMA at matched elapsed
        // times — a fixed point-count span could not. Here both series span the
        // identical wall-clock duration with proportional decay.
        let dense: Vec<(i64, Decimal)> = (0..=8)
            .map(|i| (i * 30_000, Decimal::from(100 + i)))
            .collect();
        let sparse = [
            (0_i64, Decimal::from(100)),
            (120_000, Decimal::from(104)),
            (240_000, Decimal::from(108)),
        ];
        // Both end at t=240s having risen 100→108; a duration-native EMA on the
        // sparse series lands in the same neighborhood as the dense one, not at
        // a point-count-dependent lag.
        let dense_last = ema_time_decayed(&dense, 120)
            .expect("valid dense EMA input")
            .and_then(|ema| ema.last().copied())
            .expect("dense ema");
        let sparse_last = ema_time_decayed(&sparse, 120)
            .expect("valid sparse EMA input")
            .and_then(|ema| ema.last().copied())
            .expect("sparse ema");
        let gap = (dense_last - sparse_last).abs();
        assert!(gap < Decimal::from(2), "time-native EMAs agree: {gap}");
    }

    #[test]
    fn ema_time_decayed_is_deterministic() {
        let series = timed(&[100, 102, 101, 105, 103], 45);
        let first = ema_time_decayed(&series, 90)
            .expect("valid EMA input")
            .expect("ema");
        let second = ema_time_decayed(&series, 90)
            .expect("valid EMA input")
            .expect("ema");
        assert_eq!(first, second, "must be deterministic");
    }

    #[test]
    fn ema_rejects_non_monotonic_timestamps_instead_of_clamping_the_gap() {
        let series = [
            (60_000_i64, Decimal::from(100)),
            (30_000_i64, Decimal::from(101)),
        ];

        let error = ema_time_decayed(&series, 120).expect_err("descending time must fail");

        assert!(error.to_string().contains("not ascending"));
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
    fn macd_time_defined_for_trending_series() {
        let series: Vec<(i64, Decimal)> = (0..30)
            .map(|i| (i * 30_000, Decimal::from(100 + i)))
            .collect();
        let macd = macd_time(&series, 150, 450)
            .expect("valid MACD input")
            .expect("macd");
        // A steady uptrend keeps the fast EMA above the slow EMA → positive MACD.
        assert!(macd > Decimal::ZERO, "{macd}");
    }
}
