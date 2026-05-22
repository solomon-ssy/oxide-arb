//! `runtime_config` table entity (key-value store for hot-reloadable params).

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "runtime_config")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub key: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub value: serde_json::Value,
    #[sea_orm(column_type = "Text", nullable)]
    pub description: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

/// Strongly-typed keys for the `runtime_config` table.
///
/// Each variant maps to a known configuration key. The `as_str()` method
/// returns the canonical string used as the primary key in `PostgreSQL`.
/// Adding a new runtime-tunable parameter means adding a variant here,
/// ensuring compile-time coverage in any `match` expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::EnumIter)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigKey {
    // --- Risk parameters ---
    MaxPortfolioExposureUsd,
    MaxSinglePositionUsd,
    MaxDailyLossUsd,
    CircuitBreakerThreshold,
    // --- Detection parameters ---
    MinProfitThresholdUsd,
    EndgameHoursBeforeClose,
    ConvergenceThreshold,
    // --- Execution parameters ---
    MaxSlippageBps,
    OrderTimeoutSecs,
    CooldownAfterTradeSecs,
    // --- Sizing parameters ---
    KellyFraction,
    MaxPositionFractionOfBook,
    // --- General ---
    MaintenanceMode,
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

impl std::str::FromStr for RuntimeConfigKey {
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

impl std::fmt::Display for RuntimeConfigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
