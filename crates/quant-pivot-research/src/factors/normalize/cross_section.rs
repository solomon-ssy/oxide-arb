//! The [`CrossSectionalNormalizer`] contract and its three concrete methods:
//! winsorized z-score, average rank, and semantic min/max.
//!
//! A normalizer `fit`s frozen [`NormalizationStats`] over a distribution and
//! `apply`s them pointwise. Distributional parameters (`winsor_p`,
//! `clamp_sigma`, min/max bounds) are supplied at construction time from runtime
//! config — there are **no hardcoded normalization constants** in this module.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    enums::factor::{FactorIndeterminateReason, FactorNormalization, NormalizationSource},
    runtime_config::{DecimalValue, FactorNormalizationConfig},
    types::Probability,
};
use rust_decimal::Decimal;

use crate::factors::normalize::{
    outcome::{NormalizationClampAudit, NormalizedFactor, RawFactorColumn},
    stats::{NORM_SCALE, NormalizationStats, mean, population_std, quantile_value},
};

/// Fits a normalization distribution and applies it pointwise.
///
/// `fit` and `apply` are separated so the same normalizer can serve both the
/// online cross-section and the historical-quantile fallback (fit on history,
/// apply on today) — the training/serving parity seam.
pub trait CrossSectionalNormalizer: Send + Sync {
    /// The normalization method this normalizer implements.
    fn method(&self) -> FactorNormalization;

    /// Whether this method needs the full cross-section (and so is subject to
    /// the small-cross-section policy). `MinMax` is per-market.
    fn is_cross_sectional(&self) -> bool;

    /// Fit frozen statistics over a raw column's present values.
    fn fit(&self, column: &RawFactorColumn) -> QuantResult<NormalizationStats>;

    /// Apply frozen statistics to each market's raw value, tagging the source.
    fn apply(
        &self,
        column: &RawFactorColumn,
        stats: &NormalizationStats,
        source: NormalizationSource,
    ) -> Vec<NormalizedFactor>;
}

/// Cross-sectional winsorize (to `[p, 1-p]`) then z-score, `±σ` clamped.
pub struct WinsorizedZScoreNormalizer {
    /// Winsorize percentile in `(0, 0.5)`.
    pub winsor_p: Decimal,
    /// Sigma clamp bound, `> 0`.
    pub clamp_sigma: Decimal,
}

impl CrossSectionalNormalizer for WinsorizedZScoreNormalizer {
    fn method(&self) -> FactorNormalization {
        FactorNormalization::WinsorizedZScore
    }

    fn is_cross_sectional(&self) -> bool {
        true
    }

    fn fit(&self, column: &RawFactorColumn) -> QuantResult<NormalizationStats> {
        let mut present = column.present();
        if present.len() < 2 {
            return Ok(NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance,
            });
        }
        present.sort();
        let lower = quantile_value(&present, self.winsor_p)?;
        let upper = quantile_value(&present, Decimal::ONE - self.winsor_p)?;
        let winsorized: Vec<Decimal> = present
            .iter()
            .copied()
            .map(|value| value.clamp(lower, upper))
            .collect();
        let Some(mean_value) = mean(&winsorized) else {
            return Ok(NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance,
            });
        };
        Ok(match population_std(&winsorized, mean_value)? {
            Some(std) if std > Decimal::ZERO => NormalizationStats::WinsorizedZScore {
                lower,
                upper,
                mean: mean_value,
                std,
                clamp_sigma: self.clamp_sigma,
            },
            _ => NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance,
            },
        })
    }

    fn apply(
        &self,
        column: &RawFactorColumn,
        stats: &NormalizationStats,
        source: NormalizationSource,
    ) -> Vec<NormalizedFactor> {
        let (lower, upper, mean_value, std, clamp_sigma) = match stats {
            NormalizationStats::WinsorizedZScore {
                lower,
                upper,
                mean,
                std,
                clamp_sigma,
            } => (*lower, *upper, *mean, *std, *clamp_sigma),
            NormalizationStats::Degenerate { reason } => {
                return indeterminate_present(column, *reason);
            }
            NormalizationStats::Rank { .. } | NormalizationStats::MinMax { .. } => {
                return indeterminate_present(column, FactorIndeterminateReason::ZeroVariance);
            }
        };
        column
            .values
            .iter()
            .map(|value| {
                let Some(raw) = *value else {
                    return NormalizedFactor::MissingInput;
                };
                let winsorized = raw.clamp(lower, upper);
                let standardized = (winsorized - mean_value) / std;
                let bounded = standardized.clamp(-clamp_sigma, clamp_sigma);
                let span = clamp_sigma * Decimal::from(2);
                let mapped = (bounded + clamp_sigma) / span;
                let clamp = if standardized.abs() > clamp_sigma {
                    Some(NormalizationClampAudit {
                        method: "winsorized_zscore".to_owned(),
                        raw: standardized.round_dp(NORM_SCALE),
                        clamped_to: bounded.round_dp(NORM_SCALE),
                    })
                } else if winsorized != raw {
                    Some(NormalizationClampAudit {
                        method: "winsorize".to_owned(),
                        raw,
                        clamped_to: winsorized,
                    })
                } else {
                    None
                };
                NormalizedFactor::Scored {
                    score: unit_probability(mapped),
                    source,
                    clamp,
                }
            })
            .collect()
    }
}

/// Cross-sectional average rank mapped to `[0, 1]` (distribution-free).
pub struct RankNormalizer;

impl CrossSectionalNormalizer for RankNormalizer {
    fn method(&self) -> FactorNormalization {
        FactorNormalization::Rank
    }

    fn is_cross_sectional(&self) -> bool {
        true
    }

    fn fit(&self, column: &RawFactorColumn) -> QuantResult<NormalizationStats> {
        let mut sorted = column.present();
        if sorted.len() < 2 {
            return Ok(NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance,
            });
        }
        sorted.sort();
        if sorted.first() == sorted.last() {
            return Ok(NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance,
            });
        }
        Ok(NormalizationStats::Rank { sorted })
    }

    fn apply(
        &self,
        column: &RawFactorColumn,
        stats: &NormalizationStats,
        source: NormalizationSource,
    ) -> Vec<NormalizedFactor> {
        let sorted = match stats {
            NormalizationStats::Rank { sorted } => sorted,
            NormalizationStats::Degenerate { reason } => {
                return indeterminate_present(column, *reason);
            }
            NormalizationStats::WinsorizedZScore { .. } | NormalizationStats::MinMax { .. } => {
                return indeterminate_present(column, FactorIndeterminateReason::ZeroVariance);
            }
        };
        let span = Decimal::from(sorted.len() - 1);
        let two = Decimal::from(2);
        column
            .values
            .iter()
            .map(|value| {
                let Some(raw) = *value else {
                    return NormalizedFactor::MissingInput;
                };
                let first = sorted.iter().position(|entry| *entry == raw);
                let last = sorted.iter().rposition(|entry| *entry == raw);
                match (first, last) {
                    (Some(first), Some(last)) => {
                        let average = Decimal::from(first + last) / two;
                        let score = (average / span).round_dp(NORM_SCALE);
                        NormalizedFactor::Scored {
                            score: unit_probability(score),
                            source,
                            clamp: None,
                        }
                    }
                    // A present-but-unseen value only happens on the historical
                    // path (today's value absent from the historical distribution);
                    // rank it by interpolation against the sorted history.
                    _ => NormalizedFactor::Scored {
                        score: unit_probability(interpolated_rank(sorted, raw, span)),
                        source,
                        clamp: None,
                    },
                }
            })
            .collect()
    }
}

/// Per-market min/max scaling into `[0, 1]` against a fixed semantic domain.
pub struct MinMaxNormalizer {
    /// Lower bound mapped to 0.
    pub lo: Decimal,
    /// Upper bound mapped to 1.
    pub hi: Decimal,
}

impl CrossSectionalNormalizer for MinMaxNormalizer {
    fn method(&self) -> FactorNormalization {
        FactorNormalization::MinMax
    }

    fn is_cross_sectional(&self) -> bool {
        false
    }

    fn fit(&self, _column: &RawFactorColumn) -> QuantResult<NormalizationStats> {
        Ok(NormalizationStats::MinMax {
            lo: self.lo,
            hi: self.hi,
        })
    }

    fn apply(
        &self,
        column: &RawFactorColumn,
        stats: &NormalizationStats,
        source: NormalizationSource,
    ) -> Vec<NormalizedFactor> {
        let (lo, hi) = match stats {
            NormalizationStats::MinMax { lo, hi } => (*lo, *hi),
            NormalizationStats::Degenerate { reason } => {
                return indeterminate_present(column, *reason);
            }
            NormalizationStats::WinsorizedZScore { .. } | NormalizationStats::Rank { .. } => {
                return indeterminate_present(column, FactorIndeterminateReason::ZeroVariance);
            }
        };
        column
            .values
            .iter()
            .map(|value| {
                let Some(raw) = *value else {
                    return NormalizedFactor::MissingInput;
                };
                let clamped = raw.clamp(lo, hi);
                let score = ((clamped - lo) / (hi - lo)).round_dp(NORM_SCALE);
                let clamp = (clamped != raw).then(|| NormalizationClampAudit {
                    method: "min_max".to_owned(),
                    raw,
                    clamped_to: clamped,
                });
                NormalizedFactor::Scored {
                    score: unit_probability(score),
                    source,
                    clamp,
                }
            })
            .collect()
    }
}

/// Resolve a factor's normalizer from its default method and runtime config.
///
/// The per-factor override wins; parameters fall back to the section defaults.
/// Fails closed on unparseable parameters or a `MinMax` factor without bounds.
///
/// # Errors
///
/// Returns [`QuantError::config`] on an unparseable parameter or a `MinMax`
/// method missing `min`/`max` bounds.
pub fn resolve_normalizer(
    factor_name: &str,
    default_method: FactorNormalization,
    config: &FactorNormalizationConfig,
) -> QuantResult<Box<dyn CrossSectionalNormalizer>> {
    let over = config.per_factor.get(factor_name);
    let method = over.map_or(default_method, |spec| spec.method);
    match method {
        FactorNormalization::WinsorizedZScore => {
            let winsor_p = resolve_param(
                over.and_then(|spec| spec.winsor_p.as_ref()),
                &config.default_winsor_p,
            );
            let clamp_sigma = resolve_param(
                over.and_then(|spec| spec.clamp_sigma.as_ref()),
                &config.default_clamp_sigma,
            );
            Ok(Box::new(WinsorizedZScoreNormalizer {
                winsor_p,
                clamp_sigma,
            }))
        }
        FactorNormalization::Rank => Ok(Box::new(RankNormalizer)),
        FactorNormalization::MinMax => {
            let (Some(min), Some(max)) = (
                over.and_then(|spec| spec.min.as_ref()),
                over.and_then(|spec| spec.max.as_ref()),
            ) else {
                return Err(QuantError::config(format!(
                    "factor `{factor_name}` uses MinMax normalization but has no min/max bounds in factors.normalization.per_factor"
                )));
            };
            let lo = min.value;
            let hi = max.value;
            if hi <= lo {
                return Err(QuantError::config(format!(
                    "factor `{factor_name}` MinMax bounds invalid: max {hi} must exceed min {lo}"
                )));
            }
            Ok(Box::new(MinMaxNormalizer { lo, hi }))
        }
    }
}

/// Every present value → indeterminate for `reason`; missing values stay missing.
pub(in crate::factors) fn indeterminate_present(
    column: &RawFactorColumn,
    reason: FactorIndeterminateReason,
) -> Vec<NormalizedFactor> {
    column
        .values
        .iter()
        .map(|value| {
            if value.is_some() {
                NormalizedFactor::Indeterminate { reason }
            } else {
                NormalizedFactor::MissingInput
            }
        })
        .collect()
}

/// Interpolated rank of `raw` against a sorted ascending distribution (used only
/// on the historical path, where today's value may not appear in history).
fn interpolated_rank(sorted: &[Decimal], raw: Decimal, span: Decimal) -> Decimal {
    let below = sorted.iter().filter(|entry| **entry < raw).count();
    (Decimal::from(below) / span)
        .round_dp(NORM_SCALE)
        .clamp(Decimal::ZERO, Decimal::ONE)
}

/// Clamp into `[0, 1]` and build a [`Probability`] at the normalization scale.
fn unit_probability(value: Decimal) -> Probability {
    Probability::new(
        value
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(NORM_SCALE),
    )
}

fn resolve_param(over: Option<&DecimalValue>, default: &DecimalValue) -> Decimal {
    over.unwrap_or(default).value
}

#[cfg(test)]
mod tests {
    use super::{
        CrossSectionalNormalizer, MinMaxNormalizer, RankNormalizer, WinsorizedZScoreNormalizer,
    };
    use quant_pivot_models::enums::factor::{FactorIndeterminateReason, NormalizationSource};
    use rust_decimal::Decimal;

    use crate::factors::{
        normalize::{NormalizationStats, NormalizedFactor, RawFactorColumn},
        value::FactorName,
    };

    fn column(values: &[Option<i64>]) -> RawFactorColumn {
        RawFactorColumn {
            factor: FactorName::from_static("test"),
            values: values.iter().map(|v| v.map(Decimal::from)).collect(),
        }
    }

    fn score(outcome: &NormalizedFactor) -> Decimal {
        match outcome {
            NormalizedFactor::Scored { score, .. } => score.inner(),
            other => panic!("expected a scored outcome, got {other:?}"),
        }
    }

    #[test]
    fn winsorize_caps_at_configured_percentile() {
        // With a 20% winsorize the far outlier (1000) is capped to the top
        // non-outlier level (4) before standardizing, so it scores identically to
        // that legitimate maximum and records a winsorize clamp — the raw tail
        // never dominates the cross-section.
        let normalizer = WinsorizedZScoreNormalizer {
            winsor_p: Decimal::new(2, 1),
            clamp_sigma: Decimal::from(3),
        };
        let raw = column(&[Some(1), Some(2), Some(3), Some(4), Some(1_000)]);
        let stats = normalizer.fit(&raw).expect("fit normalization");
        assert!(matches!(stats, NormalizationStats::WinsorizedZScore { .. }));
        let out = normalizer.apply(&raw, &stats, NormalizationSource::CrossSection);
        assert_eq!(
            score(&out[4]),
            score(&out[3]),
            "the winsorized outlier scores the same as the capped maximum"
        );
        assert!(score(&out[4]) <= Decimal::ONE);
        match &out[4] {
            NormalizedFactor::Scored { clamp, .. } => {
                assert!(
                    clamp.is_some(),
                    "the capped outlier records a winsorize clamp"
                );
            }
            other => panic!("expected scored, got {other:?}"),
        }
    }

    #[test]
    fn zero_variance_column_is_degenerate() {
        let normalizer = WinsorizedZScoreNormalizer {
            winsor_p: Decimal::new(1, 2),
            clamp_sigma: Decimal::from(3),
        };
        let raw = column(&[Some(5), Some(5), Some(5), Some(5)]);
        let stats = normalizer.fit(&raw).expect("fit normalization");
        assert!(matches!(
            stats,
            NormalizationStats::Degenerate {
                reason: FactorIndeterminateReason::ZeroVariance
            }
        ));
        let out = normalizer.apply(&raw, &stats, NormalizationSource::CrossSection);
        assert!(out.iter().all(|value| matches!(
            value,
            NormalizedFactor::Indeterminate {
                reason: FactorIndeterminateReason::ZeroVariance
            }
        )));
    }

    #[test]
    fn rank_orders_into_unit_interval() {
        let normalizer = RankNormalizer;
        let raw = column(&[Some(10), Some(30), Some(20), None]);
        let stats = normalizer.fit(&raw).expect("fit normalization");
        let out = normalizer.apply(&raw, &stats, NormalizationSource::CrossSection);
        assert_eq!(score(&out[0]), Decimal::ZERO, "smallest → 0");
        assert_eq!(score(&out[1]), Decimal::ONE, "largest → 1");
        assert_eq!(score(&out[2]), Decimal::new(5, 1), "middle → 0.5");
        assert!(matches!(out[3], NormalizedFactor::MissingInput));
    }

    #[test]
    fn min_max_clamps_and_audits() {
        let normalizer = MinMaxNormalizer {
            lo: Decimal::ZERO,
            hi: Decimal::ONE,
        };
        let raw = RawFactorColumn {
            factor: FactorName::from_static("dq"),
            values: vec![Some(Decimal::new(5, 1)), Some(Decimal::from(5))],
        };
        let stats = normalizer.fit(&raw).expect("fit normalization");
        let out = normalizer.apply(&raw, &stats, NormalizationSource::PerMarket);
        assert_eq!(score(&out[0]), Decimal::new(5, 1));
        match &out[1] {
            NormalizedFactor::Scored { score, clamp, .. } => {
                assert_eq!(score.inner(), Decimal::ONE, "5 clamps to the [0,1] top");
                assert!(
                    clamp.is_some(),
                    "an out-of-domain value records a clamp audit"
                );
            }
            other => panic!("expected scored, got {other:?}"),
        }
    }
}
