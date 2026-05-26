//! Risk engine config tuned for integration tests.

use oxide_arb_models::config::{KellyConfig, RiskConfig};
use rust_decimal_macros::dec;

#[must_use]
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
