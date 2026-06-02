//! Runtime configuration key enum.

use std::str::FromStr;

active_string_enum! {
    /// Strongly-typed keys for the `runtime_config` table.
    ///
    /// Each variant maps to a known configuration key. The `as_str()` method
    /// returns the canonical string used as the primary key in `PostgreSQL`.
    /// Adding a new runtime-tunable parameter means adding a variant here,
    /// ensuring compile-time coverage in any `match` expression.
    pub enum RuntimeConfigKey {
        // --- Risk parameters ---
        MaxPortfolioExposureUsd => "max_portfolio_exposure_usd",
        MaxSinglePositionUsd => "max_single_position_usd",
        MaxDailyLossUsd => "max_daily_loss_usd",
        CircuitBreakerThreshold => "circuit_breaker_threshold",
        // --- Detection parameters ---
        MinProfitThresholdUsd => "min_profit_threshold_usd",
        EndgameHoursBeforeClose => "endgame_hours_before_close",
        ConvergenceThreshold => "convergence_threshold",
        // --- Execution parameters ---
        MaxSlippageBps => "max_slippage_bps",
        OrderTimeoutSecs => "order_timeout_secs",
        CooldownAfterTradeSecs => "cooldown_after_trade_secs",
        // --- Sizing parameters ---
        KellyFraction => "kelly_fraction",
        MaxPositionFractionOfBook => "max_position_fraction_of_book",
        // --- General ---
        MaintenanceMode => "maintenance_mode",
        DryRunMode => "dry_run_mode",
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
