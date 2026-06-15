//! Trade-execution runtime configuration (`execution` section).
//!
//! Endgame is a single-order strategy (FOK buy held to settlement): no
//! multi-leg orchestration, no hedging. Structural parameters (shard counts,
//! channel capacities) are **deploy** configuration and live in
//! `config::ExecutionDeployConfig` — only operational tunables are here.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Execution-path operational tunables (hot-reloadable).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionRuntimeConfig {
    /// Validation / dispatch / confirmation time budgets.
    pub timeout: TradeTimeoutConfig,
    /// Rate-limited low-priority dispatch queue parameters.
    pub funnel: FunnelConfig,
    /// Multi-token → single-market scan dedup parameters.
    pub coalescer: CoalescerConfig,
    /// Endgame latency SLO tuning (fast lane + freshness budget).
    pub endgame_latency: EndgameLatencyConfig,
}

// ── Timeouts ─────────────────────────────────────────────────────────────────

/// Validation / dispatch / confirmation time budgets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TradeTimeoutConfig {
    /// Max price slippage between detection and validation (bps). Exceeding
    /// this rejects the trade. Default: `50`.
    #[schemars(with = "String", extend("x-format" = "decimal", "x-money-critical" = true))]
    pub max_validation_slippage_bps: Decimal,
    /// Hard-kill timeout (ms) for execution dispatch (FOK order round trip).
    /// Default: `30000`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub dispatcher_timeout_ms: u64,
    /// Total time budget (s) to confirm a trade reached a terminal state.
    /// Read per relay poll — takes effect on the next cycle. Default: `60`.
    #[schemars(extend("x-format" = "integer"))]
    pub trade_confirm_timeout_secs: u64,
    /// Interval (s) between confirmation polls. Read per relay poll — takes
    /// effect on the next cycle. Default: `2`.
    #[schemars(extend("x-format" = "integer"))]
    pub trade_confirm_poll_interval_secs: u64,
}

impl Default for TradeTimeoutConfig {
    fn default() -> Self {
        Self {
            max_validation_slippage_bps: default_max_slippage_bps(),
            dispatcher_timeout_ms: default_dispatcher_timeout(),
            trade_confirm_timeout_secs: default_confirm_timeout(),
            trade_confirm_poll_interval_secs: default_confirm_poll(),
        }
    }
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

// ── Funnel ───────────────────────────────────────────────────────────────────

/// Funnel (rate-limited opportunity dispatch) configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FunnelConfig {
    /// Bounded priority-queue capacity; overflow evicts the lowest score.
    /// Default: `50`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_queue_size: usize,
    /// Sweep interval (ms) between low-priority dispatches (high-score
    /// opportunities bypass via the fast lane). Default: `75`.
    #[schemars(extend("x-format" = "duration_ms"))]
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

// ── Coalescer ────────────────────────────────────────────────────────────────

/// Coalescer (multi-token → single-market scan dedup) configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CoalescerConfig {
    /// Max wait (ms) for the second token leg before flushing a market scan.
    /// Lower = lower latency, more duplicate scans. Default: `40`.
    #[schemars(extend("x-format" = "duration_ms"))]
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
    40
}

// ── Endgame latency ──────────────────────────────────────────────────────────

/// Endgame-specific latency tuning (SLO-1 fast lane + SLO-2 freshness).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndgameLatencyConfig {
    /// Scores at or above this bypass the funnel sweep delay (immediate shard
    /// dispatch). Default: `0.5`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub dispatch_immediate_threshold: Decimal,
    /// Max ms from last book apply to order emit (SLO-2); older books fail
    /// validation. Default: `5`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub max_book_to_order_ms: u64,
}

impl Default for EndgameLatencyConfig {
    fn default() -> Self {
        Self {
            dispatch_immediate_threshold: default_dispatch_immediate_threshold(),
            max_book_to_order_ms: default_max_book_to_order_ms(),
        }
    }
}

const fn default_dispatch_immediate_threshold() -> Decimal {
    dec!(0.5)
}
const fn default_max_book_to_order_ms() -> u64 {
    5
}
