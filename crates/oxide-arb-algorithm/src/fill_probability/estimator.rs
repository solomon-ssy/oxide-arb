//! Endgame-specific fill probability estimation.
//!
//! Estimates the probability that a FOK order will fill at the desired
//! price. Simpler than multi-leg arbitrage because endgame always places
//! a single order.

use num_traits::ToPrimitive;
use oxide_arb_models::{
    config::FillProbabilityConfig,
    enums::common::StalenessLevel,
    types::{MICRO_SCALE, MicroPct, MicroProb},
};

const FILL_FLOOR: MicroProb = MicroProb::from_micro(100_000);
const FILL_CEILING: MicroProb = MicroProb::from_micro(990_000);

/// Fill probability estimator configured by [`FillProbabilityConfig`].
pub struct FillProbabilityEstimator {
    base_fill_prob: MicroProb,
    depth_penalty_threshold_pct: MicroPct,
    depth_penalty_per_pct: MicroProb,
    staleness_penalty_per_level: MicroProb,
    resolution_proximity_bonus: MicroProb,
}

impl FillProbabilityEstimator {
    /// Create from configuration.
    #[must_use]
    pub fn new(config: &FillProbabilityConfig) -> Self {
        Self {
            base_fill_prob: MicroProb::try_from_decimal(config.base_fill_prob)
                .unwrap_or(MicroProb::ZERO),
            depth_penalty_threshold_pct: MicroPct::try_from_pct_decimal(
                config.depth_penalty_threshold_pct,
            )
            .unwrap_or(MicroPct::ZERO),
            depth_penalty_per_pct: MicroProb::try_from_decimal(config.depth_penalty_per_pct)
                .unwrap_or(MicroProb::ZERO),
            staleness_penalty_per_level: MicroProb::try_from_decimal(
                config.staleness_penalty_per_level,
            )
            .unwrap_or(MicroProb::ZERO),
            resolution_proximity_bonus: MicroProb::try_from_decimal(
                config.resolution_proximity_bonus,
            )
            .unwrap_or(MicroProb::ZERO),
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
        depth_used_pct: MicroPct,
        staleness: StalenessLevel,
        hours_to_settlement: i64,
    ) -> MicroProb {
        let mut p_micro = self.base_fill_prob.micro();

        let excess = depth_used_pct
            .micro()
            .saturating_sub(self.depth_penalty_threshold_pct.micro());
        if excess > 0 {
            let penalty = i128::from(excess) * i128::from(self.depth_penalty_per_pct.micro()) * 100
                / i128::from(MICRO_SCALE);
            p_micro = p_micro.saturating_sub(ToPrimitive::to_i64(&penalty).unwrap_or(i64::MAX));
        }

        let staleness_steps = match staleness {
            StalenessLevel::Fresh => 0,
            StalenessLevel::Acceptable => 1,
            StalenessLevel::Stale => 2,
            StalenessLevel::Expired => 3,
        };
        p_micro =
            p_micro.saturating_sub(staleness_steps * self.staleness_penalty_per_level.micro());

        if (0..=6).contains(&hours_to_settlement) {
            let fraction = i128::from(MICRO_SCALE)
                - i128::from(hours_to_settlement.max(0)) * i128::from(MICRO_SCALE) / 6;
            let bonus = i128::from(self.resolution_proximity_bonus.micro()) * fraction
                / i128::from(MICRO_SCALE);
            p_micro = p_micro.saturating_add(ToPrimitive::to_i64(&bonus).unwrap_or(0));
        }

        MicroProb::from_micro(p_micro).clamp_prob(FILL_FLOOR, FILL_CEILING)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn default_config() -> FillProbabilityConfig {
        FillProbabilityConfig::default()
    }

    #[test]
    fn fresh_low_depth_near_base() {
        let est = FillProbabilityEstimator::new(&default_config());
        let p = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(5)).unwrap(),
            StalenessLevel::Fresh,
            24,
        );
        assert_eq!(p.to_decimal(), dec!(0.90));
    }

    #[test]
    fn depth_penalty_applied() {
        let est = FillProbabilityEstimator::new(&default_config());
        let low = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(10)).unwrap(),
            StalenessLevel::Fresh,
            24,
        );
        let high = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(30)).unwrap(),
            StalenessLevel::Fresh,
            24,
        );
        assert!(low.micro() > high.micro());
    }

    #[test]
    fn staleness_monotonically_decreasing() {
        let est = FillProbabilityEstimator::new(&default_config());
        let fresh = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Fresh,
            24,
        );
        let acceptable = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Acceptable,
            24,
        );
        let stale = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Stale,
            24,
        );
        let expired = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Expired,
            24,
        );

        assert!(fresh.micro() >= acceptable.micro());
        assert!(acceptable.micro() >= stale.micro());
        assert!(stale.micro() >= expired.micro());
    }

    #[test]
    fn resolution_proximity_bonus() {
        let est = FillProbabilityEstimator::new(&default_config());
        let far = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Fresh,
            24,
        );
        let close = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(15)).unwrap(),
            StalenessLevel::Fresh,
            2,
        );
        assert!(close.micro() > far.micro());
    }

    #[test]
    fn output_clamped_low() {
        let cfg = FillProbabilityConfig {
            base_fill_prob: dec!(0.05),
            ..default_config()
        };
        let est = FillProbabilityEstimator::new(&cfg);
        let p = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(50)).unwrap(),
            StalenessLevel::Expired,
            24,
        );
        assert_eq!(p.to_decimal(), dec!(0.10));
    }

    #[test]
    fn output_clamped_high() {
        let cfg = FillProbabilityConfig {
            base_fill_prob: dec!(0.99),
            resolution_proximity_bonus: dec!(0.10),
            ..default_config()
        };
        let est = FillProbabilityEstimator::new(&cfg);
        let p = est.estimate(
            MicroPct::try_from_pct_decimal(dec!(0)).unwrap(),
            StalenessLevel::Fresh,
            0,
        );
        assert_eq!(p.to_decimal(), dec!(0.99));
    }
}
