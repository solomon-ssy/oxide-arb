//! Confidence fusion: dynamic-weight blend of calibrator posterior and
//! real-time convergence confidence.
//!
//! All arithmetic is pure `rust_decimal::Decimal` — no `f64` on the hot path.

use oxide_arb_models::config::CalibrationConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Fuses calibrator posterior probability with real-time confidence.
///
/// Uses dynamic weight `w(n) = n / (n + n₀)` where `n` is the sample count
/// and `n₀` is [`CalibrationConfig::fusion_prior_strength`].
///
/// When `n` is small, real-time confidence dominates. As `n` grows, the
/// calibrator's posterior takes over.
pub struct ConfidenceFusion {
    prior_strength: Decimal,
    p_floor: Decimal,
    p_ceiling: Decimal,
}

impl ConfidenceFusion {
    /// Create from calibration configuration.
    #[must_use]
    pub fn new(config: &CalibrationConfig) -> Self {
        Self {
            prior_strength: Decimal::from(config.fusion_prior_strength),
            p_floor: config.fused_p_floor,
            p_ceiling: config.fused_p_ceiling,
        }
    }

    /// Fuse calibrator posterior with real-time confidence.
    ///
    /// `fused = w × p_calibrator + (1−w) × p_realtime`, clamped to `[floor, ceiling]`.
    #[must_use]
    #[inline]
    pub fn fuse(&self, p_calibrator: Decimal, p_realtime: Decimal, sample_count: u32) -> Decimal {
        let n = Decimal::from(sample_count);
        let w = n / (n + self.prior_strength);
        let raw = w * p_calibrator + (Decimal::ONE - w) * p_realtime;
        raw.max(self.p_floor).min(self.p_ceiling)
    }
}

/// Compute real-time confidence from price proximity and convergence duration.
///
/// Both factors are combined with 70% price weight and 30% duration weight.
/// Output is clamped to `[0.50, 0.995]`.
///
/// Duration confidence uses a piecewise-linear approximation of log-saturating
/// behaviour to avoid `f64` — all arithmetic stays in `Decimal`.
#[must_use]
#[inline]
pub fn compute_realtime_confidence(
    entry_price: Decimal,
    convergence_secs: u64,
    high_threshold: Decimal,
) -> Decimal {
    let range = Decimal::ONE - high_threshold;
    let price_conf = if range.is_zero() {
        dec!(0.99)
    } else {
        let excess = (entry_price - high_threshold).max(Decimal::ZERO);
        dec!(0.80) + excess / range * dec!(0.19)
    };

    let duration_conf = duration_confidence_factor(convergence_secs);

    let raw = dec!(0.7) * price_conf + dec!(0.3) * duration_conf;
    raw.max(dec!(0.50)).min(dec!(0.995))
}

/// Piecewise-linear approximation of log-saturating duration confidence.
///
/// Maps convergence seconds → `[0.0, 1.0]`:
/// - 0–300s   → 0.0–0.2
/// - 300–3600 → 0.2–0.6
/// - 3600–21600 → 0.6–0.85
/// - 21600–86400 → 0.85–1.0
/// - 86400+ → 1.0
#[inline]
fn duration_confidence_factor(secs: u64) -> Decimal {
    match secs {
        0..=300 => Decimal::from(secs) / dec!(300) * dec!(0.2),
        301..=3600 => dec!(0.2) + Decimal::from(secs - 300) / dec!(3300) * dec!(0.4),
        3601..=21600 => dec!(0.6) + Decimal::from(secs - 3600) / dec!(18000) * dec!(0.25),
        21601..=86400 => dec!(0.85) + Decimal::from(secs - 21600) / dec!(64800) * dec!(0.15),
        _ => Decimal::ONE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_fusion() -> ConfidenceFusion {
        ConfidenceFusion {
            prior_strength: dec!(20),
            p_floor: dec!(0.80),
            p_ceiling: dec!(0.995),
        }
    }

    #[test]
    fn n_zero_uses_only_realtime() {
        let fusion = default_fusion();
        let result = fusion.fuse(dec!(0.90), dec!(0.85), 0);
        assert_eq!(result, dec!(0.85));
    }

    #[test]
    fn n_equals_n0_is_50_50() {
        let fusion = default_fusion();
        let result = fusion.fuse(dec!(0.90), dec!(0.80), 20);
        let expected = dec!(0.5) * dec!(0.90) + dec!(0.5) * dec!(0.80);
        assert_eq!(result, expected);
    }

    #[test]
    fn large_n_converges_to_calibrator() {
        let fusion = default_fusion();
        let result = fusion.fuse(dec!(0.95), dec!(0.80), 10000);
        assert!(result > dec!(0.94));
        assert!(result < dec!(0.96));
    }

    #[test]
    fn output_clamped_to_floor() {
        let fusion = ConfidenceFusion {
            prior_strength: dec!(20),
            p_floor: dec!(0.80),
            p_ceiling: dec!(0.995),
        };
        let result = fusion.fuse(dec!(0.50), dec!(0.50), 0);
        assert_eq!(result, dec!(0.80));
    }

    #[test]
    fn output_clamped_to_ceiling() {
        let fusion = default_fusion();
        let result = fusion.fuse(dec!(1.0), dec!(1.0), 1000);
        assert_eq!(result, dec!(0.995));
    }

    #[test]
    fn realtime_confidence_at_threshold() {
        let conf = compute_realtime_confidence(dec!(0.95), 600, dec!(0.95));
        assert!(conf >= dec!(0.50));
        assert!(conf <= dec!(0.995));
    }

    #[test]
    fn realtime_confidence_at_0_99() {
        let conf = compute_realtime_confidence(dec!(0.99), 600, dec!(0.95));
        // price_conf = 0.80 + 0.8*0.19 = 0.952, dur ≈ 0.236
        // raw = 0.7*0.952 + 0.3*0.236 ≈ 0.737
        assert!(conf > dec!(0.70));
        assert!(conf < dec!(0.80));
    }

    #[test]
    fn duration_factor_boundary_values() {
        assert_eq!(duration_confidence_factor(0), Decimal::ZERO);
        assert_eq!(duration_confidence_factor(300), dec!(0.2));
        assert!(duration_confidence_factor(3600) >= dec!(0.59));
        assert!(duration_confidence_factor(3600) <= dec!(0.61));
        assert!(duration_confidence_factor(86400) >= dec!(0.99));
        assert_eq!(duration_confidence_factor(100_000), Decimal::ONE);
    }
}
