//! Property-based tests for algorithm invariants.

use oxide_arb_algorithm::{
    endgame::confidence::{ConfidenceFusion, compute_realtime_confidence},
    fill_probability::FillProbabilityEstimator,
    urgency::UrgencyFactor,
    walker::OrderbookWalker,
};
use oxide_arb_models::{
    config::{CalibrationConfig, FillProbabilityConfig},
    domain::BookLevel,
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::StalenessLevel,
    },
    types::{MicroPct, MicroPrice, MicroProb, MicroUsd, Price, Shares},
};
use proptest::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ── PriceZone classification ─────────────────────────────────────────

proptest! {
    #[test]
    fn price_zone_always_valid(price_f in 0.95_f64..1.0_f64) {
        let price = Price::new(Decimal::try_from(price_f).unwrap());
        let zone = PriceZone::from_price(price);
        let p = price.inner();
        match zone {
            PriceZone::Z95 => assert!(p < dec!(0.96)),
            PriceZone::Z96 => { assert!(p >= dec!(0.96)); assert!(p < dec!(0.97)); }
            PriceZone::Z97 => { assert!(p >= dec!(0.97)); assert!(p < dec!(0.98)); }
            PriceZone::Z98 => { assert!(p >= dec!(0.98)); assert!(p < dec!(0.99)); }
            PriceZone::Z99 => assert!(p >= dec!(0.99)),
        }
    }
}

// ── DurationBucket classification ────────────────────────────────────

proptest! {
    #[test]
    fn duration_bucket_always_valid(secs in 0_u64..200_000) {
        let bucket = DurationBucket::from_secs(secs);
        match bucket {
            DurationBucket::Short    => assert!(secs < 3600),
            DurationBucket::Medium   => { assert!(secs >= 3600); assert!(secs < 21600); }
            DurationBucket::Long     => { assert!(secs >= 21600); assert!(secs < 86400); }
            DurationBucket::VeryLong => assert!(secs >= 86400),
        }
    }
}

// ── ConfidenceFusion bounds ──────────────────────────────────────────

proptest! {
    #[test]
    fn fused_p_always_in_bounds(
        p_cal in 0.5_f64..1.0,
        p_rt in 0.5_f64..1.0,
        n in 0_u32..1000,
    ) {
        let config = CalibrationConfig::default();
        let fusion = ConfidenceFusion::new(&config);
        let result = fusion.fuse(
            MicroProb::try_from_decimal(Decimal::try_from(p_cal).unwrap()).unwrap(),
            MicroProb::try_from_decimal(Decimal::try_from(p_rt).unwrap()).unwrap(),
            n,
        );
        assert!(result.to_decimal() >= config.fused_p_floor);
        assert!(result.to_decimal() <= config.fused_p_ceiling);
    }
}

// ── FillProbability staleness monotonicity ────────────────────────────

proptest! {
    #[test]
    fn fill_prob_decreases_with_staleness(
        depth_pct in 0.0_f64..50.0,
        hours in 0_i64..48,
    ) {
        let estimator = FillProbabilityEstimator::new(&FillProbabilityConfig::default());
        let d = MicroPct::try_from_pct_decimal(Decimal::try_from(depth_pct).unwrap()).unwrap();

        let fresh = estimator.estimate(d, StalenessLevel::Fresh, hours);
        let acceptable = estimator.estimate(d, StalenessLevel::Acceptable, hours);
        let stale = estimator.estimate(d, StalenessLevel::Stale, hours);
        let expired = estimator.estimate(d, StalenessLevel::Expired, hours);

        assert!(fresh.micro() >= acceptable.micro());
        assert!(acceptable.micro() >= stale.micro());
        assert!(stale.micro() >= expired.micro());
    }
}

// ── Urgency monotonicity ─────────────────────────────────────────────

proptest! {
    #[test]
    fn urgency_monotonically_increasing(
        hours_a in 0_i64..24,
        hours_b in 0_i64..24,
    ) {
        let ua = UrgencyFactor::compute(hours_a, 24);
        let ub = UrgencyFactor::compute(hours_b, 24);

        if hours_a < hours_b {
            assert!(ua.micro() >= ub.micro());
        } else if hours_a > hours_b {
            assert!(ub.micro() >= ua.micro());
        }
    }
}

// ── Walker cost never exceeds budget ─────────────────────────────────

proptest! {
    #[test]
    fn walk_cost_never_exceeds_budget(
        budget in 1.0_f64..1000.0,
        price in 0.95_f64..0.99,
        size in 10.0_f64..10000.0,
    ) {
        let budget_d = Decimal::try_from(budget).unwrap();
        let asks = vec![BookLevel::from_decimal_unchecked(
            Price::new(Decimal::try_from(price).unwrap()),
            Shares::new(Decimal::try_from(size).unwrap()),
        )];
        let depth = oxide_arb_models::domain::book::total_depth_usd(&asks);
        let budget = MicroUsd::try_from_decimal(budget_d).unwrap();
        let floor = MicroPrice::try_from_decimal(dec!(0.95)).unwrap();

        if let Some(walk) =
            OrderbookWalker::walk_asks_by_cost(&asks, budget, floor, depth)
        {
            assert!(walk.total_cost.to_decimal() <= budget_d + dec!(0.01));
        }
    }
}

// ── Realtime confidence always in [0.50, 0.995] ─────────────────────

proptest! {
    #[test]
    fn realtime_confidence_in_bounds(
        price in 0.95_f64..1.0,
        secs in 0_u64..200_000,
    ) {
        let result = compute_realtime_confidence(
            MicroPrice::try_from_decimal(Decimal::try_from(price).unwrap()).unwrap(),
            secs,
            MicroPrice::try_from_decimal(dec!(0.95)).unwrap(),
        );
        assert!(result.to_decimal() >= dec!(0.50));
        assert!(result.to_decimal() <= dec!(0.995));
    }
}
