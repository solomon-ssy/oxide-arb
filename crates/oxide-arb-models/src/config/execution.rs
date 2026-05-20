//! Trade execution pipeline configuration.
//!
//! Endgame is a single-order strategy (FOK buy/sell held to settlement).
//! No multi-leg orchestration, no hedging.

use crate::enums::common::ExecutionMode;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    /// Minimum profit margin (USD) beyond fees before execution proceeds.
    #[serde(default = "default_min_profit_margin")]
    pub min_profit_margin_usd: Decimal,
    #[serde(default)]
    pub timeout: TradeTimeoutConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::default(),
            min_profit_margin_usd: default_min_profit_margin(),
            timeout: TradeTimeoutConfig::default(),
        }
    }
}

const fn default_min_profit_margin() -> Decimal {
    dec!(0.50)
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct TradeTimeoutConfig {
    /// Max time (ms) for validation (fresh book + risk + fees).
    #[serde(default = "default_max_validation_ms")]
    pub max_validation_time_ms: u64,
    /// Max price slippage between detection and validation (bps).
    #[serde(default = "default_max_slippage_bps")]
    pub max_validation_slippage_bps: Decimal,
    /// Hard-kill timeout (ms) for execution dispatch.
    #[serde(default = "default_dispatcher_timeout")]
    pub dispatcher_timeout_ms: u64,
    /// Total time budget (s) to confirm trade reached terminal state.
    #[serde(default = "default_confirm_timeout")]
    pub trade_confirm_timeout_secs: u64,
    /// Interval (s) between confirmation polls.
    #[serde(default = "default_confirm_poll")]
    pub trade_confirm_poll_interval_secs: u64,
}

impl Default for TradeTimeoutConfig {
    fn default() -> Self {
        Self {
            max_validation_time_ms: default_max_validation_ms(),
            max_validation_slippage_bps: default_max_slippage_bps(),
            dispatcher_timeout_ms: default_dispatcher_timeout(),
            trade_confirm_timeout_secs: default_confirm_timeout(),
            trade_confirm_poll_interval_secs: default_confirm_poll(),
        }
    }
}

const fn default_max_validation_ms() -> u64 {
    500
}
const fn default_max_slippage_bps() -> Decimal {
    dec!(50)
}
const fn default_dispatcher_timeout() -> u64 {
    30_000
}
const fn default_confirm_timeout() -> u64 {
    60
}
const fn default_confirm_poll() -> u64 {
    2
}
