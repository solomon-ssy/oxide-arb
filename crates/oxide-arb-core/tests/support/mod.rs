//! Shared helpers for core integration tests.

use std::sync::Arc;

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::config::{KellyConfig, RiskConfig};
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
use oxide_arb_models::enums::opportunity::PayoutModel;
use oxide_arb_models::types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd};
use oxide_arb_risk::traits::RiskMetrics;
use rust_decimal_macros::dec;

pub struct TestRiskMetrics;

impl RiskMetrics for TestRiskMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::new(dec!(100))
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<oxide_arb_models::domain::position::PositionInfo> {
        Vec::new()
    }
    fn cached_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }
    fn open_directional_count(&self, _: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
        0
    }
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
}

pub fn test_risk_config() -> RiskConfig {
    RiskConfig {
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        max_daily_loss_usd: dec!(75),
        max_weekly_loss_usd: dec!(120),
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 3,
        bankroll_usd: dec!(5000),
        kelly: KellyConfig {
            min_edge_bps: dec!(50),
            ..KellyConfig::default()
        },
        ..RiskConfig::default()
    }
}

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

pub fn sample_scored() -> ScoredOpportunity {
    let yes = TokenId::new("yes-token");
    let no = TokenId::new("no-token");

    ScoredOpportunity {
        opportunity: Arc::new(sample_opportunity()),
        score: dec!(0.8),
        token_yes: yes,
        token_no: no,
        book_yes_version: 1,
        book_no_version: 1,
        fill_probability: dec!(0.99),
        urgency_factor: dec!(1),
        category_weight: dec!(1),
        staleness_discount: dec!(1),
    }
}

pub fn seed_book_store(
    store: &oxide_arb_core::pipeline::book_store::BookStore,
    scored: &ScoredOpportunity,
) {
    let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.07)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.08)),
        Shares::new(dec!(1000)),
    )];
    store.apply_snapshot(&scored.token_yes, vec![], yes_asks, now_ms);
    store.apply_snapshot(&scored.token_no, no_bids, no_asks, now_ms);
}
