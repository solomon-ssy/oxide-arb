//! Risk guard-rail configuration.
//!
//! Single-strategy (endgame) risk model. No separate arb/directional
//! budgets — all limits apply to the one strategy we run.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RiskConfig {
    // ── Per-opportunity static filters ───────────────────────────────
    #[serde(default = "default_min_profit")]
    pub min_profit_usd: Decimal,

    // ── Rolling counters + adaptive cooldown ─────────────────────────
    #[serde(default = "default_max_misses")]
    pub max_consecutive_misses: u32,
    #[serde(default = "default_max_hourly_loss")]
    pub max_hourly_loss_usd: Decimal,
    #[serde(default = "default_base_cooldown")]
    pub base_cooldown_secs: u64,
    #[serde(default = "default_cooldown_mult")]
    pub cooldown_multiplier: Decimal,
    #[serde(default = "default_max_cooldown")]
    pub max_cooldown_secs: u64,

    // ── Daily / weekly loss caps ─────────────────────────────────────
    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss_usd: Decimal,
    #[serde(default = "default_max_single_loss")]
    pub max_single_loss_usd: Decimal,
    #[serde(default = "default_max_weekly_loss")]
    pub max_weekly_loss_usd: Decimal,
    /// Independent daily budget (USD). Execution stops when exhausted.
    #[serde(default = "default_daily_budget")]
    pub daily_budget_usd: Decimal,

    // ── Connectivity + balance health ────────────────────────────────
    #[serde(default = "default_ws_disconnect_threshold")]
    pub ws_disconnect_threshold_secs: u64,
    #[serde(default = "default_min_balance")]
    pub min_balance_usd: Decimal,

    // ── Blacklist ────────────────────────────────────────────────────
    #[serde(default = "default_blacklist_count")]
    pub market_miss_blacklist_count: u32,
    #[serde(default = "default_blacklist_duration")]
    pub market_miss_blacklist_duration_secs: u64,
    #[serde(default)]
    pub permanent_blacklist_markets: Vec<String>,
    #[serde(default)]
    pub permanent_blacklist_tokens: Vec<String>,

    // ── Exposure limits ──────────────────────────────────────────────
    #[serde(default = "default_max_total_exposure")]
    pub max_total_exposure_usd: Decimal,
    #[serde(default = "default_reserve_balance")]
    pub reserve_balance_usd: Decimal,
    #[serde(default = "default_max_open_positions")]
    pub max_open_positions: usize,
    #[serde(default = "default_max_market_exposure")]
    pub max_single_market_exposure_usd: Decimal,
    /// Maximum USD for a single bet.
    #[serde(default = "default_max_single_bet")]
    pub max_single_bet_usd: Decimal,
    /// Stop-loss as percentage of entry cost. Auto-close if exceeded.
    #[serde(default = "default_stop_loss_pct")]
    pub stop_loss_pct: Decimal,

    // ── Circuit breaker ──────────────────────────────────────────────
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            min_profit_usd: default_min_profit(),
            max_consecutive_misses: default_max_misses(),
            max_hourly_loss_usd: default_max_hourly_loss(),
            base_cooldown_secs: default_base_cooldown(),
            cooldown_multiplier: default_cooldown_mult(),
            max_cooldown_secs: default_max_cooldown(),
            max_daily_loss_usd: default_max_daily_loss(),
            max_single_loss_usd: default_max_single_loss(),
            max_weekly_loss_usd: default_max_weekly_loss(),
            daily_budget_usd: default_daily_budget(),
            ws_disconnect_threshold_secs: default_ws_disconnect_threshold(),
            min_balance_usd: default_min_balance(),
            market_miss_blacklist_count: default_blacklist_count(),
            market_miss_blacklist_duration_secs: default_blacklist_duration(),
            permanent_blacklist_markets: Vec::new(),
            permanent_blacklist_tokens: Vec::new(),
            max_total_exposure_usd: default_max_total_exposure(),
            reserve_balance_usd: default_reserve_balance(),
            max_open_positions: default_max_open_positions(),
            max_single_market_exposure_usd: default_max_market_exposure(),
            max_single_bet_usd: default_max_single_bet(),
            stop_loss_pct: default_stop_loss_pct(),
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

const fn default_min_profit() -> Decimal {
    dec!(0.50)
}
const fn default_max_misses() -> u32 {
    3
}
const fn default_max_hourly_loss() -> Decimal {
    dec!(30)
}
const fn default_base_cooldown() -> u64 {
    900
}
const fn default_cooldown_mult() -> Decimal {
    dec!(2.0)
}
const fn default_max_cooldown() -> u64 {
    7200
}
const fn default_max_daily_loss() -> Decimal {
    dec!(75)
}
const fn default_max_single_loss() -> Decimal {
    dec!(30)
}
const fn default_max_weekly_loss() -> Decimal {
    dec!(120)
}
const fn default_daily_budget() -> Decimal {
    dec!(50)
}
const fn default_ws_disconnect_threshold() -> u64 {
    30
}
const fn default_min_balance() -> Decimal {
    dec!(50)
}
const fn default_blacklist_count() -> u32 {
    3
}
const fn default_blacklist_duration() -> u64 {
    3600
}
const fn default_max_total_exposure() -> Decimal {
    dec!(5000)
}
const fn default_reserve_balance() -> Decimal {
    dec!(1000)
}
const fn default_max_open_positions() -> usize {
    3
}
const fn default_max_market_exposure() -> Decimal {
    dec!(500)
}
const fn default_max_single_bet() -> Decimal {
    dec!(25)
}
const fn default_stop_loss_pct() -> Decimal {
    dec!(30)
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_cb_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cb_consecutive")]
    pub consecutive_failure_threshold: u32,
    #[serde(default = "default_cb_open_wait")]
    pub open_wait_secs: u64,
    #[serde(default = "default_cb_max_wait")]
    pub max_open_wait_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: default_cb_enabled(),
            consecutive_failure_threshold: default_cb_consecutive(),
            open_wait_secs: default_cb_open_wait(),
            max_open_wait_secs: default_cb_max_wait(),
        }
    }
}

const fn default_cb_enabled() -> bool {
    true
}
const fn default_cb_consecutive() -> u32 {
    5
}
const fn default_cb_open_wait() -> u64 {
    60
}
const fn default_cb_max_wait() -> u64 {
    300
}
