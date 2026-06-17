//! Endgame-specific risk check tests.

use chrono::Utc;
use oxide_arb_models::{
    domain::{
        TradeIntegritySnapshot,
        calibration::{BucketKey, CalibrationSnapshot},
        opportunity::{EndgameMeta, Opportunity},
        risk::ProbabilityInput,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, Side, StalenessLevel},
        opportunity::PayoutModel,
    },
    runtime_config::RiskConfig,
    types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd},
};
use oxide_arb_risk::{
    context::{PreTradeContext, SettlementGateInput},
    pipeline::{
        RiskCheck,
        checks::{DailyDirectionalBudgetCheck, DirectionalConcentrationCheck},
    },
    snapshot::{DailyAccountingSnapshot, RiskSnapshot},
    traits::RiskMetricsSnapshot,
    types::RiskCheckId,
};
use rust_decimal_macros::dec;

fn with_context(
    open_directional_same_side: usize,
    daily_directional_trades_same_side: u32,
    f: impl FnOnce(&PreTradeContext<'_>),
) {
    let opp = Opportunity {
        opportunity_id: OpportunityId::from_v7(),
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

    let snap = RiskSnapshot {
        daily: DailyAccountingSnapshot {
            daily_budget_remaining: Usd::new(dec!(50)),
            ..RiskSnapshot::zeroed().daily
        },
        ..RiskSnapshot::zeroed()
    };

    let metrics = RiskMetricsSnapshot {
        total_exposure: Usd::new(dec!(100)),
        open_position_count: 1,
        cash_balance: Usd::new(dec!(5000)),
        equity: Usd::new(dec!(5000)),
        is_authoritative: true,
        is_stale: false,
        metrics_age_secs: 0,
        open_directional_count_buy: open_directional_same_side,
        daily_directional_trades_buy: daily_directional_trades_same_side,
        ..RiskMetricsSnapshot::zeroed()
    };

    let ctx = PreTradeContext {
        opportunity: &opp,
        probability: ProbabilityInput {
            calibrated_win_prob: dec!(0.95),
            fill_prob: dec!(0.90),
            calibration_confidence: dec!(0.85),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        },
        snap: &snap,
        metrics,
        factor_context: None,
        settlement_gate: SettlementGateInput::default(),
        integrity: &TradeIntegritySnapshot::zero(Utc::now()),
        now: Utc::now(),
        sized_intent: None,
    };

    f(&ctx);
}

#[test]
fn directional_concentration_blocks_when_at_max() {
    let config = RiskConfig {
        max_concurrent_directional: 3,
        ..RiskConfig::default()
    };
    let check = DirectionalConcentrationCheck::new(&config);
    with_context(3, 0, |ctx| {
        let result = check.evaluate(ctx);
        assert!(!result.passed);
        assert_eq!(result.check_id, RiskCheckId::DirectionalConcentration);
    });
}

#[test]
fn directional_concentration_allows_when_below_max() {
    let config = RiskConfig {
        max_concurrent_directional: 3,
        ..RiskConfig::default()
    };
    let check = DirectionalConcentrationCheck::new(&config);
    with_context(2, 0, |ctx| {
        assert!(check.evaluate(ctx).passed);
    });
}

#[test]
fn daily_directional_budget_blocks_at_limit() {
    let config = RiskConfig {
        daily_directional_budget: 10,
        ..RiskConfig::default()
    };
    let check = DailyDirectionalBudgetCheck::new(&config);
    with_context(0, 10, |ctx| {
        let result = check.evaluate(ctx);
        assert!(!result.passed);
        assert_eq!(result.check_id, RiskCheckId::DailyDirectionalBudget);
    });
}

#[test]
fn daily_directional_budget_allows_below_limit() {
    let config = RiskConfig {
        daily_directional_budget: 10,
        ..RiskConfig::default()
    };
    let check = DailyDirectionalBudgetCheck::new(&config);
    with_context(0, 9, |ctx| {
        assert!(check.evaluate(ctx).passed);
    });
}
