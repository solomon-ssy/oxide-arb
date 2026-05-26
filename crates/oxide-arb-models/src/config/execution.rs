//! Trade execution pipeline configuration.
//!
//! Endgame is a single-order strategy (FOK buy/sell held to settlement).
//! No multi-leg orchestration, no hedging.

use crate::enums::common::ExecutionMode;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    #[serde(default)]
    pub timeout: TradeTimeoutConfig,
    #[serde(default)]
    pub tiered: TieredExecutionConfig,
    #[serde(default)]
    pub funnel: FunnelConfig,
    #[serde(default)]
    pub coalescer: CoalescerConfig,
    #[serde(default)]
    pub endgame_latency: EndgameLatencyConfig,
    #[serde(default)]
    pub book_apply: BookApplyConfig,
}

/// Endgame-specific latency tuning (SLO-1 fast lane + SLO-3 coalesce).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndgameLatencyConfig {
    /// Scores at or above this bypass funnel sweep delay (immediate shard dispatch).
    #[serde(default = "default_dispatch_immediate_threshold")]
    pub dispatch_immediate_threshold: Decimal,
    /// Funnel tick interval for low-score sweep only (ms).
    #[serde(default = "default_funnel_sweep_interval_ms")]
    pub funnel_sweep_interval_ms: u64,
    /// Max ms from last book apply to order emit (SLO-2).
    #[serde(default = "default_max_book_to_order_ms")]
    pub max_book_to_order_ms: u64,
}

impl Default for EndgameLatencyConfig {
    fn default() -> Self {
        Self {
            dispatch_immediate_threshold: default_dispatch_immediate_threshold(),
            funnel_sweep_interval_ms: default_funnel_sweep_interval_ms(),
            max_book_to_order_ms: default_max_book_to_order_ms(),
        }
    }
}

const fn default_dispatch_immediate_threshold() -> Decimal {
    dec!(0.5)
}
const fn default_funnel_sweep_interval_ms() -> u64 {
    75
}
const fn default_max_book_to_order_ms() -> u64 {
    5
}

/// Sharded book-apply workers (500-market single host).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookApplyConfig {
    #[serde(default = "default_book_shard_count")]
    pub shard_count: usize,
    #[serde(default = "default_book_channel_capacity")]
    pub channel_capacity: usize,
}

impl Default for BookApplyConfig {
    fn default() -> Self {
        Self {
            shard_count: default_book_shard_count(),
            channel_capacity: default_book_channel_capacity(),
        }
    }
}

const fn default_book_shard_count() -> usize {
    4
}
const fn default_book_channel_capacity() -> usize {
    2048
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

/// Funnel (rate-limited opportunity dispatch) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunnelConfig {
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
    #[serde(default = "default_min_dispatch_interval_ms")]
    pub min_dispatch_interval_ms: u64,
}

impl Default for FunnelConfig {
    fn default() -> Self {
        Self {
            max_queue_size: default_max_queue_size(),
            min_dispatch_interval_ms: default_min_dispatch_interval_ms(),
        }
    }
}

const fn default_max_queue_size() -> usize {
    50
}
const fn default_min_dispatch_interval_ms() -> u64 {
    75
}

/// Coalescer (multi-token → single-market scan dedup) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoalescerConfig {
    #[serde(default = "default_coalesce_window_ms")]
    pub coalesce_window_ms: u64,
}

impl Default for CoalescerConfig {
    fn default() -> Self {
        Self {
            coalesce_window_ms: default_coalesce_window_ms(),
        }
    }
}

const fn default_coalesce_window_ms() -> u64 {
    60
}
