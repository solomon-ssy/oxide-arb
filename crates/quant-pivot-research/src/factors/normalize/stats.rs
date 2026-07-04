//! Fitted normalization statistics and their deterministic reductions.
//!
//! Every step that crosses into `f64` (only the standard-deviation `sqrt`) is
//! **quantized to a fixed decimal scale** before it can build a
//! [`Probability`](quant_pivot_models::types::Probability), so a factor value is
//! bit-identical across hardware. All other arithmetic is exact `Decimal`.

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

/// The nearest-rank quantile of an ascending slice at percentile `p ∈ [0, 1]`.
///
/// Exact `Decimal` index arithmetic (no float cast), so the winsorize bounds are
/// deterministic. Returns `Decimal::ZERO` for an empty slice (never called that
/// way — the fit guards `len >= 2`).
#[must_use]
pub(super) fn quantile_value(sorted: &[Decimal], p: Decimal) -> Decimal {
    let n = sorted.len();
    if n == 0 {
        return Decimal::ZERO;
    }
    let span = Decimal::from(n - 1);
    let index = (p * span).round().to_usize().unwrap_or(0).min(n - 1);
    sorted[index]
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
/// result is quantized to [`NORM_SCALE`]. Returns `None` when the slice is empty
/// or the `sqrt` is non-finite.
#[must_use]
pub(super) fn population_std(values: &[Decimal], mean: Decimal) -> Option<Decimal> {
    if values.is_empty() {
        return None;
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
    let std = variance.to_f64().map(f64::sqrt)?;
    if !std.is_finite() {
        return None;
    }
    Decimal::from_f64(std).map(|value| value.round_dp(NORM_SCALE))
}
