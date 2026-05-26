//! Endgame-specific fill probability estimation.
//!
//! Estimates the probability that a FOK order will fill at the desired
//! price. Simpler than multi-leg arbitrage because endgame always places
//! a single order.

use oxide_arb_models::{config::FillProbabilityConfig, enums::common::StalenessLevel};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Fill probability estimator configured by [`FillProbabilityConfig`].
pub struct FillProbabilityEstimator {
    base_fill_prob: Decimal,
    depth_penalty_threshold_pct: Decimal,
    depth_penalty_per_pct: Decimal,
    staleness_penalty_per_level: Decimal,
    resolution_proximity_bonus: Decimal,
}

impl FillProbabilityEstimator {
    /// Create from configuration.
    #[must_use]
    pub const fn new(config: &FillProbabilityConfig) -> Self {
        Self {
            base_fill_prob: config.base_fill_prob,
            depth_penalty_threshold_pct: config.depth_penalty_threshold_pct,
            depth_penalty_per_pct: config.depth_penalty_per_pct,
            staleness_penalty_per_level: config.staleness_penalty_per_level,
            resolution_proximity_bonus: config.resolution_proximity_bonus,
        }
    }

    /// Estimate fill probability for an endgame FOK order.
    ///
    /// Three factors modify the base probability:
    /// 1. **Depth penalty**: linear above `depth_penalty_threshold_pct`.
    /// 2. **Staleness penalty**: per-`StalenessLevel`-step.
    /// 3. **Resolution proximity bonus**: within 6 hours of settlement.
    ///
    /// Output is clamped to `[0.10, 0.99]`.
    #[must_use]
    #[inline]
    pub fn estimate(
        &self,
        depth_used_pct: Decimal,
        staleness: StalenessLevel,
        hours_to_settlement: i64,
    ) -> Decimal {
        let mut p = self.base_fill_prob;

        let excess_depth = (depth_used_pct - self.depth_penalty_threshold_pct).max(Decimal::ZERO);
        p -= excess_depth * self.depth_penalty_per_pct;

        let staleness_steps = match staleness {
            StalenessLevel::Fresh => Decimal::ZERO,
            StalenessLevel::Acceptable => Decimal::ONE,
            StalenessLevel::Stale => Decimal::from(2),
            StalenessLevel::Expired => Decimal::from(3),
        };
        p -= staleness_steps * self.staleness_penalty_per_level;

        if (0..=6).contains(&hours_to_settlement) {
            let fraction = Decimal::ONE - Decimal::from(hours_to_settlement) / Decimal::from(6);
            p += self.resolution_proximity_bonus * fraction;
        }

        p.max(dec!(0.10)).min(dec!(0.99))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> FillProbabilityConfig {
        FillProbabilityConfig::default()
    }

    #[test]
    fn fresh_low_depth_near_base() {
        let est = FillProbabilityEstimator::new(&default_config());
        let p = est.estimate(dec!(5), StalenessLevel::Fresh, 24);
        assert_eq!(p, dec!(0.90));
    }

    #[test]
    fn depth_penalty_applied() {
        let est = FillProbabilityEstimator::new(&default_config());
        let low = est.estimate(dec!(10), StalenessLevel::Fresh, 24);
        let high = est.estimate(dec!(30), StalenessLevel::Fresh, 24);
        assert!(low > high);
    }

    #[test]
    fn staleness_monotonically_decreasing() {
        let est = FillProbabilityEstimator::new(&default_config());
        let fresh = est.estimate(dec!(15), StalenessLevel::Fresh, 24);
        let acceptable = est.estimate(dec!(15), StalenessLevel::Acceptable, 24);
        let stale = est.estimate(dec!(15), StalenessLevel::Stale, 24);
        let expired = est.estimate(dec!(15), StalenessLevel::Expired, 24);

        assert!(fresh > acceptable);
        assert!(acceptable > stale);
        assert!(stale > expired);
    }

    #[test]
    fn resolution_proximity_bonus() {
        let est = FillProbabilityEstimator::new(&default_config());
        let far = est.estimate(dec!(15), StalenessLevel::Fresh, 24);
        let close = est.estimate(dec!(15), StalenessLevel::Fresh, 2);
        assert!(close > far);
    }

    #[test]
    fn output_clamped_low() {
        let cfg = FillProbabilityConfig {
            base_fill_prob: dec!(0.05),
            ..default_config()
        };
        let est = FillProbabilityEstimator::new(&cfg);
        let p = est.estimate(dec!(50), StalenessLevel::Expired, 24);
        assert_eq!(p, dec!(0.10));
    }

    #[test]
    fn output_clamped_high() {
        let cfg = FillProbabilityConfig {
            base_fill_prob: dec!(0.99),
            resolution_proximity_bonus: dec!(0.10),
            ..default_config()
        };
        let est = FillProbabilityEstimator::new(&cfg);
        let p = est.estimate(dec!(0), StalenessLevel::Fresh, 0);
        assert_eq!(p, dec!(0.99));
    }
}
