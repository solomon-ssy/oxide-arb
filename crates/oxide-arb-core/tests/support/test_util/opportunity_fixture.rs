//! Shared [`Opportunity`] fixture for validator and execution tests.

use chrono::Utc;
use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
use oxide_arb_models::enums::opportunity::PayoutModel;
use oxide_arb_models::types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd};
use rust_decimal_macros::dec;

#[must_use]
pub fn sample_opportunity() -> Opportunity {
    Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xtest_market"),
        event_id: EventId::new("test_event"),
        token_id: TokenId::new("yes-token"),
        side: Side::Buy,
        payout_model: PayoutModel::DirectionalSettlement {
            projected_payout_if_correct: Usd::new(dec!(100)),
            expected_payout: Usd::new(dec!(95)),
            predicted_side: Side::Buy,
        },
        shares: Shares::new(dec!(100)),
        entry_price: Price::new(dec!(0.92)),
        total_cost: Usd::new(dec!(20)),
        total_fees: Usd::new(dec!(0.40)),
        net_profit: Usd::new(dec!(5)),
        expected_net_profit: Usd::new(dec!(4.5)),
        edge_bps: Bps::new(dec!(300)),
        resolution_adjust: dec!(0.95),
        depth_used_pct: dec!(10),
        staleness: StalenessLevel::Fresh,
        category: MarketCategory::Politics,
        meta: EndgameMeta {
            predicted_yes: true,
            confidence: dec!(0.95),
            convergence_duration_secs: 600,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
            settlement_deadline: None,
        },
        calibration: CalibrationSnapshot {
            bucket_key: BucketKey {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
            },
            posterior_mean: dec!(0.93),
            sample_size: 50,
            alpha_prior: dec!(2.0),
            beta_prior: dec!(1.0),
            fallback_tier: 1,
            fused_probability: dec!(0.99),
        },
        detected_at: Utc::now(),
    }
}
