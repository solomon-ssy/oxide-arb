//! Runtime configuration key enum.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

/// Strongly-typed keys for the `runtime_config` table.
///
/// Each variant maps to a known configuration key. The `as_str()` method
/// returns the canonical string used as the primary key in `PostgreSQL`.
/// Adding a new runtime-tunable parameter means adding a variant here,
/// ensuring compile-time coverage in any `match` expression.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigKey {
    // --- Risk parameters ---
    #[sea_orm(string_value = "max_portfolio_exposure_usd")]
    MaxPortfolioExposureUsd,
    #[sea_orm(string_value = "max_single_position_usd")]
    MaxSinglePositionUsd,
    #[sea_orm(string_value = "max_daily_loss_usd")]
    MaxDailyLossUsd,
    #[sea_orm(string_value = "circuit_breaker_threshold")]
    CircuitBreakerThreshold,
    // --- Detection parameters ---
    #[sea_orm(string_value = "min_profit_threshold_usd")]
    MinProfitThresholdUsd,
    #[sea_orm(string_value = "endgame_hours_before_close")]
    EndgameHoursBeforeClose,
    #[sea_orm(string_value = "convergence_threshold")]
    ConvergenceThreshold,
    // --- Execution parameters ---
    #[sea_orm(string_value = "max_slippage_bps")]
    MaxSlippageBps,
    #[sea_orm(string_value = "order_timeout_secs")]
    OrderTimeoutSecs,
    #[sea_orm(string_value = "cooldown_after_trade_secs")]
    CooldownAfterTradeSecs,
    // --- Sizing parameters ---
    #[sea_orm(string_value = "kelly_fraction")]
    KellyFraction,
    #[sea_orm(string_value = "max_position_fraction_of_book")]
    MaxPositionFractionOfBook,
    // --- General ---
    #[sea_orm(string_value = "maintenance_mode")]
    MaintenanceMode,
    #[sea_orm(string_value = "dry_run_mode")]
    DryRunMode,
}

impl RuntimeConfigKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MaxPortfolioExposureUsd => "max_portfolio_exposure_usd",
            Self::MaxSinglePositionUsd => "max_single_position_usd",
            Self::MaxDailyLossUsd => "max_daily_loss_usd",
            Self::CircuitBreakerThreshold => "circuit_breaker_threshold",
            Self::MinProfitThresholdUsd => "min_profit_threshold_usd",
            Self::EndgameHoursBeforeClose => "endgame_hours_before_close",
            Self::ConvergenceThreshold => "convergence_threshold",
            Self::MaxSlippageBps => "max_slippage_bps",
            Self::OrderTimeoutSecs => "order_timeout_secs",
            Self::CooldownAfterTradeSecs => "cooldown_after_trade_secs",
            Self::KellyFraction => "kelly_fraction",
            Self::MaxPositionFractionOfBook => "max_position_fraction_of_book",
            Self::MaintenanceMode => "maintenance_mode",
            Self::DryRunMode => "dry_run_mode",
        }
    }
}

impl FromStr for RuntimeConfigKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "max_portfolio_exposure_usd" => Ok(Self::MaxPortfolioExposureUsd),
            "max_single_position_usd" => Ok(Self::MaxSinglePositionUsd),
            "max_daily_loss_usd" => Ok(Self::MaxDailyLossUsd),
            "circuit_breaker_threshold" => Ok(Self::CircuitBreakerThreshold),
            "min_profit_threshold_usd" => Ok(Self::MinProfitThresholdUsd),
            "endgame_hours_before_close" => Ok(Self::EndgameHoursBeforeClose),
            "convergence_threshold" => Ok(Self::ConvergenceThreshold),
            "max_slippage_bps" => Ok(Self::MaxSlippageBps),
            "order_timeout_secs" => Ok(Self::OrderTimeoutSecs),
            "cooldown_after_trade_secs" => Ok(Self::CooldownAfterTradeSecs),
            "kelly_fraction" => Ok(Self::KellyFraction),
            "max_position_fraction_of_book" => Ok(Self::MaxPositionFractionOfBook),
            "maintenance_mode" => Ok(Self::MaintenanceMode),
            "dry_run_mode" => Ok(Self::DryRunMode),
            other => Err(format!("unknown runtime config key: {other}")),
        }
    }
}

impl Display for RuntimeConfigKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
