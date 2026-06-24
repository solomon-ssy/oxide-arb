//! Normalization of raw factor values into `[0, 1]` scores.
//!
//! Two regimes share one entry point ([`normalize_column`]):
//!
//! - **Per-market** ([`NormalizationSpec::MinMax`] / [`NormalizationSpec::Logistic`]):
//!   each value normalizes independently against fixed spec parameters.
//! - **Cross-sectional** ([`NormalizationSpec::ZScore`] / [`NormalizationSpec::Rank`]):
//!   the column of same-`as_of` values is normalized against its own
//!   distribution; this is only meaningful through the batch engine.
//!
//! Every step that crosses into `f64` (logistic `exp`, z-score `sqrt`) is
//! **quantized to a fixed decimal scale** before building a [`Probability`], so
//! the resulting factor value is bit-identical across hardware. Clamping into
//! the unit interval is **never silent**: each clamp records a
//! [`NormalizationClampAudit`].

use quant_pivot_models::types::Probability;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::factors::value::{NormalizationClampAudit, NormalizationSpec};

/// Decimal places every `f64`-derived normalization is quantized to. Twelve
/// places sits well inside `f64`'s ~15 digits of precision, so the rounding is
/// stable across platforms (mirrors `features::stats::STAT_SCALE`).
const NORM_SCALE: u32 = 12;

/// The neutral midpoint used for degenerate distributions (zero variance, a
/// single-element rank cohort, or a malformed spec range).
fn neutral() -> Decimal {
    Decimal::new(5, 1)
}

/// Convert a (small) cohort count to `f64` without a lossy `as` cast.
fn count_to_f64(count: usize) -> f64 {
    u32::try_from(count).map_or(f64::MAX, f64::from)
}

/// A normalized score plus any clamp that was applied to produce it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Normalized {
    /// The normalized score in `[0, 1]`.
    pub score: Probability,
    /// Recorded clamp, when the input fell outside the normalization domain.
    pub clamp: Option<NormalizationClampAudit>,
}

/// Clamp a decimal into `[0, 1]` and build a [`Probability`], recording the
/// clamp when one was applied.
#[must_use]
pub fn to_probability_clamped(value: Decimal, method: &str) -> Normalized {
    let clamped = value.clamp(Decimal::ZERO, Decimal::ONE);
    let clamp = (value != clamped).then(|| NormalizationClampAudit {
        method: method.to_owned(),
        raw: value,
        clamped_to: clamped,
    });
    Normalized {
        score: Probability::new(clamped),
        clamp,
    }
}

/// Normalize a column of optional raw values under `spec`.
///
/// The returned vector is index-aligned with `raws`: a `None` input yields a
/// `None` output (a missing factor is never normalized). Per-market specs
/// normalize each present value independently; cross-sectional specs normalize
/// against the column's own present values.
#[must_use]
pub fn normalize_column(
    spec: &NormalizationSpec,
    raws: &[Option<Decimal>],
) -> Vec<Option<Normalized>> {
    match spec {
        NormalizationSpec::MinMax { lo, hi } => raws
            .iter()
            .map(|raw| raw.map(|value| min_max(value, *lo, *hi)))
            .collect(),
        NormalizationSpec::Logistic { k, x0 } => raws
            .iter()
            .map(|raw| raw.map(|value| logistic(value, *k, *x0)))
            .collect(),
        NormalizationSpec::ZScore { clamp_sigma } => z_score_column(raws, *clamp_sigma),
        NormalizationSpec::Rank => rank_column(raws),
    }
}

/// Linear min/max scaling into `[0, 1]`, clamping (and recording) out-of-range
/// values. A degenerate range (`hi <= lo`) maps to the neutral midpoint.
fn min_max(value: Decimal, lo: Decimal, hi: Decimal) -> Normalized {
    if hi <= lo {
        return Normalized {
            score: Probability::new(neutral()),
            clamp: None,
        };
    }
    let clamped = value.clamp(lo, hi);
    let score = ((clamped - lo) / (hi - lo)).round_dp(NORM_SCALE);
    let clamp = (value != clamped).then(|| NormalizationClampAudit {
        method: "min_max".to_owned(),
        raw: value,
        clamped_to: clamped,
    });
    Normalized {
        score: Probability::new(score),
        clamp,
    }
}

/// Logistic squashing `1 / (1 + e^(-k(x - x0)))`, quantized and clamped into
/// `[0, 1]` for safety (the function is intrinsically in `(0, 1)`).
fn logistic(value: Decimal, k: Decimal, x0: Decimal) -> Normalized {
    let x = (value - x0).to_f64().unwrap_or(0.0);
    let steepness = k.to_f64().unwrap_or(0.0);
    let squashed = 1.0 / (1.0 + (-(steepness * x)).exp());
    let quantized = Decimal::from_f64(squashed)
        .unwrap_or_else(neutral)
        .round_dp(NORM_SCALE);
    to_probability_clamped(quantized, "logistic")
}

/// Cross-sectional z-score: `(z_clamped + sigma) / (2 * sigma)`, mapping the
/// clamped standard score into `[0, 1]`. Zero variance (or a non-positive sigma)
/// maps every present value to the neutral midpoint.
fn z_score_column(raws: &[Option<Decimal>], clamp_sigma: Decimal) -> Vec<Option<Normalized>> {
    let present: Vec<f64> = raws
        .iter()
        .filter_map(|raw| raw.and_then(|value| value.to_f64()))
        .collect();
    let count = present.len();
    if count == 0 {
        return raws.iter().map(|_| None).collect();
    }
    let len = count_to_f64(count);
    let mean = present.iter().sum::<f64>() / len;
    let variance = present
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / len;
    let std = variance.sqrt();
    let sigma = clamp_sigma.to_f64().unwrap_or(0.0);

    raws.iter()
        .map(|raw| {
            raw.map(|value| {
                if std == 0.0 || sigma <= 0.0 {
                    return Normalized {
                        score: Probability::new(neutral()),
                        clamp: None,
                    };
                }
                let standardized = (value.to_f64().unwrap_or(mean) - mean) / std;
                let bounded = standardized.clamp(-sigma, sigma);
                let mapped = (bounded + sigma) / (2.0 * sigma);
                let score = Decimal::from_f64(mapped)
                    .unwrap_or_else(neutral)
                    .round_dp(NORM_SCALE);
                let mut normalized = to_probability_clamped(score, "z_score");
                if standardized.abs() > sigma {
                    normalized.clamp = Some(NormalizationClampAudit {
                        method: "z_score".to_owned(),
                        raw: Decimal::from_f64(standardized)
                            .unwrap_or_default()
                            .round_dp(NORM_SCALE),
                        clamped_to: Decimal::from_f64(bounded)
                            .unwrap_or_default()
                            .round_dp(NORM_SCALE),
                    });
                }
                normalized
            })
        })
        .collect()
}

/// Cross-sectional rank in `[0, 1]` using average ranks for ties. A single
/// present value maps to the neutral midpoint (rank is undefined for one point).
fn rank_column(raws: &[Option<Decimal>]) -> Vec<Option<Normalized>> {
    let mut scores: Vec<Option<Normalized>> = vec![None; raws.len()];
    let mut present: Vec<(usize, Decimal)> = raws
        .iter()
        .enumerate()
        .filter_map(|(index, raw)| raw.map(|value| (index, value)))
        .collect();
    let count = present.len();
    if count == 0 {
        return scores;
    }
    if count == 1 {
        scores[present[0].0] = Some(Normalized {
            score: Probability::new(neutral()),
            clamp: None,
        });
        return scores;
    }
    present.sort_by_key(|entry| entry.1);
    // Exact `Decimal` rank arithmetic (no `f64`): average position / (n - 1).
    let span = Decimal::from(count - 1);
    let two = Decimal::from(2);
    let mut start = 0;
    while start < present.len() {
        let mut end = start;
        while end + 1 < present.len() && present[end + 1].1 == present[start].1 {
            end += 1;
        }
        let normalized = (Decimal::from(start + end) / two / span).round_dp(NORM_SCALE);
        for entry in &present[start..=end] {
            scores[entry.0] = Some(Normalized {
                score: Probability::new(normalized),
                clamp: None,
            });
        }
        start = end + 1;
    }
    scores
}
