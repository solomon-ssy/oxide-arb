//! Confidence fusion: dynamic-weight blend of calibrator posterior and
//! real-time convergence confidence.
//!
//! All hot-path arithmetic uses fixed-point [`MicroProb`] / [`MicroPrice`].

use num_traits::ToPrimitive;
use oxide_arb_models::{
    config::CalibrationConfig,
    types::{MICRO_SCALE, MicroPrice, MicroProb},
};

/// Fuses calibrator posterior probability with real-time confidence.
///
/// Uses dynamic weight `w(n) = n / (n + n₀)` where `n` is the sample count
/// and `n₀` is [`CalibrationConfig::fusion_prior_strength`].
///
/// When `n` is small, real-time confidence dominates. As `n` grows, the
/// calibrator's posterior takes over.
pub struct ConfidenceFusion {
    prior_strength: u32,
    p_floor: MicroProb,
    p_ceiling: MicroProb,
}

impl ConfidenceFusion {
    /// Create from calibration configuration.
    #[must_use]
    pub fn new(config: &CalibrationConfig) -> Self {
        Self {
            prior_strength: config.fusion_prior_strength,
            p_floor: MicroProb::try_from_decimal(config.fused_p_floor).unwrap_or(MicroProb::ZERO),
            p_ceiling: MicroProb::try_from_decimal(config.fused_p_ceiling)
                .unwrap_or(MicroProb::ONE),
        }
    }

    /// Fuse calibrator posterior with real-time confidence.
    ///
    /// `fused = w × p_calibrator + (1−w) × p_realtime`, clamped to `[floor, ceiling]`.
    #[must_use]
    #[inline]
    pub fn fuse(
        &self,
        p_calibrator: MicroProb,
        p_realtime: MicroProb,
        sample_count: u32,
    ) -> MicroProb {
        let denom = sample_count.saturating_add(self.prior_strength);
        let blended = if denom == 0 {
            p_realtime
        } else {
            p_realtime.blend(p_calibrator, sample_count, denom)
        };
        blended.clamp_prob(self.p_floor, self.p_ceiling)
    }
}

/// Compute real-time confidence from price proximity and convergence duration.
///
/// Both factors are combined with 70% price weight and 30% duration weight.
/// Output is clamped to `[0.50, 0.995]`.
#[must_use]
#[inline]
pub fn compute_realtime_confidence(
    entry_price: MicroPrice,
    convergence_secs: u64,
    high_threshold: MicroPrice,
) -> MicroProb {
    const PRICE_WEIGHT: i64 = 700_000;
    const DURATION_WEIGHT: i64 = 300_000;

    let range = MicroPrice::ONE
        .micro()
        .saturating_sub(high_threshold.micro());
    let price_conf = if range <= 0 {
        MicroProb::from_micro(990_000)
    } else {
        let excess = entry_price.micro().saturating_sub(high_threshold.micro());
        let scaled = i128::from(excess) * 190_000 / i128::from(range);
        MicroProb::from_micro(800_000 + ToPrimitive::to_i64(&scaled).unwrap_or(0))
    };

    let duration_conf = duration_confidence_factor(convergence_secs);

    let raw = (i128::from(price_conf.micro()) * i128::from(PRICE_WEIGHT)
        + i128::from(duration_conf.micro()) * i128::from(DURATION_WEIGHT))
        / i128::from(MICRO_SCALE);
    MicroProb::from_micro(ToPrimitive::to_i64(&raw).unwrap_or(0)).clamp_prob(
        MicroProb::from_micro(500_000),
        MicroProb::from_micro(995_000),
    )
}

/// Piecewise-linear approximation of log-saturating duration confidence.
#[inline]
fn duration_confidence_factor(secs: u64) -> MicroProb {
    let micro = match secs {
        0..=300 => i128::from(secs) * 200_000 / 300,
        301..=3600 => 200_000 + i128::from(secs - 300) * 400_000 / 3300,
        3601..=21600 => 600_000 + i128::from(secs - 3600) * 250_000 / 18000,
        21601..=86400 => 850_000 + i128::from(secs - 21600) * 150_000 / 64800,
        _ => i128::from(MICRO_SCALE),
    };
    MicroProb::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(MICRO_SCALE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::types::MicroPrice;
    use rust_decimal_macros::dec;
    fn default_fusion() -> ConfidenceFusion {
        ConfidenceFusion {
            prior_strength: 20,
            p_floor: MicroProb::from_micro(800_000),
            p_ceiling: MicroProb::from_micro(995_000),
        }
    }

    #[test]
    fn n_zero_uses_only_realtime() {
        let fusion = default_fusion();
        let result = fusion.fuse(
            MicroProb::try_from_decimal(dec!(0.90)).unwrap(),
            MicroProb::try_from_decimal(dec!(0.85)).unwrap(),
            0,
        );
        assert_eq!(result.to_decimal(), dec!(0.85));
    }

    #[test]
    fn n_equals_n0_is_50_50() {
        let fusion = default_fusion();
        let result = fusion.fuse(
            MicroProb::try_from_decimal(dec!(0.90)).unwrap(),
            MicroProb::try_from_decimal(dec!(0.80)).unwrap(),
            20,
        );
        let expected_dec = dec!(0.5) * dec!(0.90) + dec!(0.5) * dec!(0.80);
        assert_eq!(result.to_decimal(), expected_dec);
    }

    #[test]
    fn large_n_converges_to_calibrator() {
        let fusion = default_fusion();
        let result = fusion.fuse(
            MicroProb::try_from_decimal(dec!(0.95)).unwrap(),
            MicroProb::try_from_decimal(dec!(0.80)).unwrap(),
            10000,
        );
        assert!(result.to_decimal() > dec!(0.94));
        assert!(result.to_decimal() < dec!(0.96));
    }

    #[test]
    fn output_clamped_to_floor() {
        let fusion = ConfidenceFusion {
            prior_strength: 20,
            p_floor: MicroProb::from_micro(800_000),
            p_ceiling: MicroProb::from_micro(995_000),
        };
        let result = fusion.fuse(
            MicroProb::try_from_decimal(dec!(0.50)).unwrap(),
            MicroProb::try_from_decimal(dec!(0.50)).unwrap(),
            0,
        );
        assert_eq!(result.to_decimal(), dec!(0.80));
    }

    #[test]
    fn output_clamped_to_ceiling() {
        let fusion = default_fusion();
        let result = fusion.fuse(
            MicroProb::try_from_decimal(dec!(1.0)).unwrap(),
            MicroProb::try_from_decimal(dec!(1.0)).unwrap(),
            1000,
        );
        assert_eq!(result.to_decimal(), dec!(0.995));
    }

    #[test]
    fn realtime_confidence_at_threshold() {
        let conf = compute_realtime_confidence(
            MicroPrice::try_from_decimal(dec!(0.95)).unwrap(),
            600,
            MicroPrice::try_from_decimal(dec!(0.95)).unwrap(),
        );
        assert!(conf.to_decimal() >= dec!(0.50));
        assert!(conf.to_decimal() <= dec!(0.995));
    }

    #[test]
    fn realtime_confidence_at_0_99() {
        let conf = compute_realtime_confidence(
            MicroPrice::try_from_decimal(dec!(0.99)).unwrap(),
            600,
            MicroPrice::try_from_decimal(dec!(0.95)).unwrap(),
        );
        assert!(conf.to_decimal() > dec!(0.70));
        assert!(conf.to_decimal() < dec!(0.80));
    }

    #[test]
    fn duration_factor_boundary_values() {
        assert_eq!(duration_confidence_factor(0).to_decimal(), dec!(0));
        assert_eq!(duration_confidence_factor(300).to_decimal(), dec!(0.2));
        assert!(duration_confidence_factor(3600).to_decimal() >= dec!(0.59));
        assert!(duration_confidence_factor(3600).to_decimal() <= dec!(0.61));
        assert!(duration_confidence_factor(86400).to_decimal() >= dec!(0.99));
        assert_eq!(duration_confidence_factor(100_000).to_decimal(), dec!(1));
    }
}
