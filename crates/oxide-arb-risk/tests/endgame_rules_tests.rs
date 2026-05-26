//! Endgame-specific risk check tests.
//!
//! Validates directional concentration limits and daily directional budget
//! via the pipeline check structs.

use chrono::Utc;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::types::Usd;
use oxide_arb_risk::context::{BlacklistGate, CircuitBreakerGate, ManualHaltGate, RiskContext};
use oxide_arb_risk::pipeline::RiskCheck;
use oxide_arb_risk::pipeline::checks::{
    DailyDirectionalBudgetCheck, DirectionalConcentrationCheck,
};
use oxide_arb_risk::types::{DrawdownAction, RiskCheckId, StateVersion};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn make_context(
    open_directional_same_side: usize,
    daily_directional_trades_same_side: u32,
) -> RiskContext {
    use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
    use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
    use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
    use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
    use oxide_arb_models::enums::opportunity::PayoutModel;
    use oxide_arb_models::types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId};

    let opp = Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xtest"),
        event_id: EventId::new("evt_test"),
        token_id: TokenId::new("12345"),
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
            fused_probability: dec!(0.95),
        },
        detected_at: Utc::now(),
    };

    RiskContext {
        state_version: StateVersion::ZERO,
        opportunity: Arc::new(opp),
        probability: ProbabilityInput {
            calibrated_win_prob: dec!(0.95),
            fill_prob: dec!(0.90),
            calibration_confidence: dec!(0.85),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        },
        market_exposure_before: Usd::ZERO,
        total_exposure_before: Usd::new(dec!(100)),
        total_potential_loss: Usd::ZERO,
        active_reservation_count: 0,
        reserved_usd: Usd::ZERO,
        open_position_count: 1,
        cached_balance: Usd::new(dec!(5000)),
        ws_disconnect_secs: 0,
        open_directional_count_same_side: open_directional_same_side,
        daily_directional_trades_same_side,
        consecutive_market_misses: 0,
        hourly_loss: Usd::ZERO,
        daily_loss: Usd::ZERO,
        daily_budget_remaining: Usd::new(dec!(50)),
        weekly_loss: Usd::ZERO,
        daily_pnl: Usd::ZERO,
        circuit_breaker: CircuitBreakerGate {
            allows_trading: true,
            is_probe: false,
        },
        manual_halt: ManualHaltGate::Clear,
        blacklist: BlacklistGate::Clear,
        token_blacklisted: false,
        api_error_count: 0,
        api_request_count: 0,
        drawdown_factor: Decimal::ONE,
        drawdown_action: DrawdownAction::Normal,
        snapshot_at: Utc::now(),
    }
}

#[test]
fn directional_concentration_blocks_when_at_max() {
    let config = RiskConfig {
        max_concurrent_directional: 3,
        ..RiskConfig::default()
    };
    let check = DirectionalConcentrationCheck::new(&config);
    let ctx = make_context(3, 0);

    let result = check.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::DirectionalConcentration);
}

#[test]
fn directional_concentration_allows_when_below_max() {
    let config = RiskConfig {
        max_concurrent_directional: 3,
        ..RiskConfig::default()
    };
    let check = DirectionalConcentrationCheck::new(&config);
    let ctx = make_context(2, 0);

    let result = check.evaluate(&ctx);
    assert!(result.passed);
}

#[test]
fn daily_directional_budget_blocks_at_limit() {
    let config = RiskConfig {
        daily_directional_budget: 10,
        ..RiskConfig::default()
    };
    let check = DailyDirectionalBudgetCheck::new(&config);
    let ctx = make_context(0, 10);

    let result = check.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::DailyDirectionalBudget);
}

#[test]
fn daily_directional_budget_allows_below_limit() {
    let config = RiskConfig {
        daily_directional_budget: 10,
        ..RiskConfig::default()
    };
    let check = DailyDirectionalBudgetCheck::new(&config);
    let ctx = make_context(0, 9);

    let result = check.evaluate(&ctx);
    assert!(result.passed);
}
