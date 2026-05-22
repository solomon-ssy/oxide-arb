//! Trade execution pipeline configuration.
//!
//! Endgame is a single-order strategy (FOK buy/sell held to settlement).
//! No multi-leg orchestration, no hedging.

use crate::enums::common::ExecutionMode;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    #[serde(default)]
    pub timeout: TradeTimeoutConfig,
    #[serde(default)]
    pub tiered: TieredExecutionConfig,
}

/// FOK + GTD tiered execution strategy configuration.
///
/// Endgame execution tries tiers in order: FOK → short GTD → long GTD.
/// Each tier may adjust price by `price_tolerance_ticks` tick increments.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct TieredExecutionConfig {
    /// FOK order timeout (ms) — how long to wait for immediate fill.
    #[serde(default = "default_fok_timeout")]
    pub fok_timeout_ms: u64,
    /// Short GTD expiry (secs) — used when FOK fails due to minor slippage.
    #[serde(default = "default_gtd_short_expiry")]
    pub gtd_short_expiry_secs: u64,
    /// Long GTD expiry (secs) — used for larger orders needing fill time.
    #[serde(default = "default_gtd_long_expiry")]
    pub gtd_long_expiry_secs: u64,
    /// Max retries within a single tier before falling through.
    #[serde(default = "default_max_retries_per_tier")]
    pub max_retries_per_tier: u32,
    /// Price tolerance in ticks added per tier (cumulative).
    #[serde(default = "default_price_tolerance_ticks")]
    pub price_tolerance_ticks: i32,
}

impl Default for TieredExecutionConfig {
    fn default() -> Self {
        Self {
            fok_timeout_ms: default_fok_timeout(),
            gtd_short_expiry_secs: default_gtd_short_expiry(),
            gtd_long_expiry_secs: default_gtd_long_expiry(),
            max_retries_per_tier: default_max_retries_per_tier(),
            price_tolerance_ticks: default_price_tolerance_ticks(),
        }
    }
}

const fn default_fok_timeout() -> u64 {
    5_000
}
const fn default_gtd_short_expiry() -> u64 {
    30
}
const fn default_gtd_long_expiry() -> u64 {
    300
}
const fn default_max_retries_per_tier() -> u32 {
    1
}
const fn default_price_tolerance_ticks() -> i32 {
    2
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
