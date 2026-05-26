//! Risk guard-rail configuration.
//!
//! Single-strategy (endgame) risk model. All limits, position sizing,
//! and endgame-specific parameters live in [`RiskConfig`].
//! `PositionSizingConfig` has been absorbed here per ADR-001 §4.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RiskConfig {
    // ── Per-opportunity static filters ───────────────────────────────
    /// Minimum order-book depth (USD) required before execution.
    #[serde(default = "default_min_depth_usd")]
    pub min_depth_usd: Decimal,
    /// Maximum fraction of visible book depth a single order may consume (%).
    #[serde(default = "default_max_depth_usage_pct")]
    pub max_depth_usage_pct: Decimal,

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

    // ── Exposure as percentage of balance ─────────────────────────────
    /// Maximum portfolio exposure as a percentage of available balance.
    #[serde(default = "default_max_total_exposure_pct")]
    pub max_total_exposure_pct: Decimal,

    // ── Reconciliation ───────────────────────────────────────────────
    /// Interval (secs) between ledger reconciliation runs.
    #[serde(default = "default_reconciliation_interval")]
    pub reconciliation_interval_secs: u64,
    /// Maximum acceptable drift (USD) before triggering an alert.
    #[serde(default = "default_reconciliation_tolerance")]
    pub reconciliation_tolerance_usd: Decimal,

    // ── Circuit breaker ──────────────────────────────────────────────
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,

    // ── Position Sizing (absorbed from PositionSizingConfig) ─────────
    /// Quarter-Kelly fraction multiplier (f*/4).
    #[serde(default = "default_kelly_fraction")]
    pub kelly_fraction: Decimal,
    /// Total bankroll available for Kelly computation (USD).
    #[serde(default = "default_bankroll")]
    pub bankroll_usd: Decimal,
    /// Minimum trade size (below this, skip the opportunity).
    #[serde(default = "default_min_trade")]
    pub min_trade_usd: Decimal,
    #[serde(default)]
    pub kelly: KellyConfig,
    #[serde(default)]
    pub drawdown: DrawdownConfig,

    // ── Fee spend caps ─────────────────────────────────────────────────
    /// Maximum daily fee spend (USD). Exceeding triggers L3 Daily halt.
    #[serde(default = "default_max_daily_fee_spend")]
    pub max_daily_fee_spend_usd: Decimal,
    /// Maximum hourly fee spend (USD). Exceeding triggers L2 Session trip.
    #[serde(default = "default_max_hourly_fee_spend")]
    pub max_hourly_fee_spend_usd: Decimal,

    // ── API health ────────────────────────────────────────────────────
    /// API error rate threshold (0..1). Exceeding triggers L2 Session breaker.
    #[serde(default = "default_api_error_rate_threshold")]
    pub api_error_rate_threshold: Decimal,
    /// Number of consecutive heartbeat failures before triggering L4 System halt.
    #[serde(default = "default_heartbeat_max_failures")]
    pub heartbeat_max_failures: u32,

    // ── Potential loss escalation ──────────────────────────────────────
    /// Maximum age (secs) of an active potential-loss entry before
    /// escalation triggers an L4 System halt.
    #[serde(default = "default_potential_loss_escalation")]
    pub potential_loss_escalation_secs: u64,

    // ── Endgame-specific rules ───────────────────────────────────────
    /// Max concurrent positions in the same directional side.
    #[serde(default = "default_max_concurrent_directional")]
    pub max_concurrent_directional: usize,
    /// Daily budget of directional trades per side.
    #[serde(default = "default_daily_directional_budget")]
    pub daily_directional_budget: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            min_depth_usd: default_min_depth_usd(),
            max_depth_usage_pct: default_max_depth_usage_pct(),
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
            max_total_exposure_pct: default_max_total_exposure_pct(),
            reconciliation_interval_secs: default_reconciliation_interval(),
            reconciliation_tolerance_usd: default_reconciliation_tolerance(),
            circuit_breaker: CircuitBreakerConfig::default(),
            kelly_fraction: default_kelly_fraction(),
            bankroll_usd: default_bankroll(),
            min_trade_usd: default_min_trade(),
            kelly: KellyConfig::default(),
            drawdown: DrawdownConfig::default(),
            max_daily_fee_spend_usd: default_max_daily_fee_spend(),
            max_hourly_fee_spend_usd: default_max_hourly_fee_spend(),
            api_error_rate_threshold: default_api_error_rate_threshold(),
            heartbeat_max_failures: default_heartbeat_max_failures(),
            potential_loss_escalation_secs: default_potential_loss_escalation(),
            max_concurrent_directional: default_max_concurrent_directional(),
            daily_directional_budget: default_daily_directional_budget(),
        }
    }
}

const fn default_min_depth_usd() -> Decimal {
    dec!(200)
}
const fn default_max_depth_usage_pct() -> Decimal {
    dec!(30)
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
    dec!(100)
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
const fn default_max_total_exposure_pct() -> Decimal {
    dec!(80)
}
const fn default_reconciliation_interval() -> u64 {
    300
}
const fn default_reconciliation_tolerance() -> Decimal {
    dec!(1.0)
}
const fn default_kelly_fraction() -> Decimal {
    dec!(0.25)
}
const fn default_bankroll() -> Decimal {
    dec!(1000)
}
const fn default_min_trade() -> Decimal {
    dec!(1)
}
const fn default_max_daily_fee_spend() -> Decimal {
    dec!(20)
}
const fn default_max_hourly_fee_spend() -> Decimal {
    dec!(5)
}
const fn default_api_error_rate_threshold() -> Decimal {
    dec!(0.10)
}
const fn default_heartbeat_max_failures() -> u32 {
    3
}
const fn default_potential_loss_escalation() -> u64 {
    3600
}
const fn default_max_concurrent_directional() -> usize {
    3
}
const fn default_daily_directional_budget() -> u32 {
    10
}

/// Kelly criterion sub-configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct KellyConfig {
    /// Maximum Kelly fraction before capping.
    #[serde(default = "default_kelly_max")]
    pub max_kelly: Decimal,
    /// Minimum edge (bps) below which Kelly returns zero.
    #[serde(default = "default_kelly_min_edge")]
    pub min_edge_bps: Decimal,
    /// Minimum calibration confidence (0..1) below which Kelly returns zero.
    #[serde(default = "default_kelly_min_confidence")]
    pub min_probability_confidence: Decimal,
    /// Minimum historical sample count required for calibration to be trusted.
    #[serde(default = "default_kelly_min_samples")]
    pub min_calibration_samples: u32,
    /// Maximum staleness (secs) of calibration model before Kelly returns zero.
    #[serde(default = "default_kelly_max_staleness")]
    pub max_probability_staleness_secs: u64,
}

impl Default for KellyConfig {
    fn default() -> Self {
        Self {
            max_kelly: default_kelly_max(),
            min_edge_bps: default_kelly_min_edge(),
            min_probability_confidence: default_kelly_min_confidence(),
            min_calibration_samples: default_kelly_min_samples(),
            max_probability_staleness_secs: default_kelly_max_staleness(),
        }
    }
}

const fn default_kelly_max() -> Decimal {
    dec!(0.25)
}
const fn default_kelly_min_edge() -> Decimal {
    dec!(200)
}
const fn default_kelly_min_confidence() -> Decimal {
    dec!(0.3)
}
const fn default_kelly_min_samples() -> u32 {
    10
}
const fn default_kelly_max_staleness() -> u64 {
    7200
}

/// Drawdown protection sub-configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct DrawdownConfig {
    /// Maximum drawdown (%) before reducing position sizes.
    #[serde(default = "default_max_dd")]
    pub max_drawdown_pct: Decimal,
    /// Size reduction factor when drawdown limit is hit.
    #[serde(default = "default_dd_reduction")]
    pub drawdown_reduction_factor: Decimal,
}

impl Default for DrawdownConfig {
    fn default() -> Self {
        Self {
            max_drawdown_pct: default_max_dd(),
            drawdown_reduction_factor: default_dd_reduction(),
        }
    }
}

const fn default_max_dd() -> Decimal {
    dec!(10)
}
const fn default_dd_reduction() -> Decimal {
    dec!(0.5)
}

/// 4-level circuit breaker configuration.
///
/// Each level has an independent cooldown duration. The FSM transitions:
/// Closed → Open (tripped) → `HalfOpen` (cooldown expired) → Recovered (probes pass) → Closed.
///
/// | Level | Trigger | Default Cooldown |
/// |-------|---------|------------------|
/// | L1 (Trade) | Per-opportunity static filter failure | 60s |
/// | L2 (Session) | Rolling window breach (misses, hourly loss) | 15min |
/// | L3 (Daily) | Daily/weekly cap breach | 1h |
/// | L4 (System) | Connectivity/balance emergency | 2h |
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CircuitBreakerConfig {
    /// Level 1 (Trade): per-opportunity static filter failure cooldown.
    #[serde(default = "default_cb_l1_cooldown")]
    pub l1_cooldown_secs: u64,
    /// Level 2 (Session): rolling window breach cooldown.
    #[serde(default = "default_cb_l2_cooldown")]
    pub l2_cooldown_secs: u64,
    /// Level 3 (Daily): daily/weekly cap breach cooldown.
    #[serde(default = "default_cb_l3_cooldown")]
    pub l3_cooldown_secs: u64,
    /// Level 4 (System): connectivity/balance emergency cooldown.
    #[serde(default = "default_cb_l4_cooldown")]
    pub l4_cooldown_secs: u64,
    /// Number of successful probe trades required in `HalfOpen` before Recovered.
    #[serde(default = "default_cb_half_open_probes")]
    pub half_open_probes: u32,
    /// Observation period (secs) in Recovered state before returning to Closed.
    #[serde(default = "default_cb_recovery_observation")]
    pub recovery_observation_secs: u64,
    /// Maximum cooldown duration (secs) for L2 exponential back-off.
    #[serde(default = "default_cb_max_cooldown")]
    pub max_cooldown_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            l1_cooldown_secs: default_cb_l1_cooldown(),
            l2_cooldown_secs: default_cb_l2_cooldown(),
            l3_cooldown_secs: default_cb_l3_cooldown(),
            l4_cooldown_secs: default_cb_l4_cooldown(),
            half_open_probes: default_cb_half_open_probes(),
            recovery_observation_secs: default_cb_recovery_observation(),
            max_cooldown_secs: default_cb_max_cooldown(),
        }
    }
}

// ── Exposure Reservation Config ──────────────────────────────────────────────

/// Configuration for the exposure reservation system.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ExposureReservationConfig {
    /// Maximum total exposure across all active reservations (USD cents).
    #[serde(default = "default_max_total_exposure_cents")]
    pub max_total_exposure_cents: u64,
    /// Maximum exposure per market (USD cents).
    #[serde(default = "default_max_per_market_cents")]
    pub max_per_market_cents: u64,
    /// Default TTL for reservations in seconds (auto-expire if not confirmed/released).
    #[serde(default = "default_reservation_ttl_secs")]
    pub default_ttl_secs: u64,
    /// GC interval in seconds for cleaning expired reservations.
    #[serde(default = "default_reservation_gc_interval_secs")]
    pub gc_interval_secs: u64,
}

impl Default for ExposureReservationConfig {
    fn default() -> Self {
        Self {
            max_total_exposure_cents: default_max_total_exposure_cents(),
            max_per_market_cents: default_max_per_market_cents(),
            default_ttl_secs: default_reservation_ttl_secs(),
            gc_interval_secs: default_reservation_gc_interval_secs(),
        }
    }
}

const fn default_max_total_exposure_cents() -> u64 {
    5_000_000 // $50,000
}
const fn default_max_per_market_cents() -> u64 {
    1_000_000 // $10,000
}
const fn default_reservation_ttl_secs() -> u64 {
    300
}
const fn default_reservation_gc_interval_secs() -> u64 {
    30
}

// ── Circuit Breaker Config defaults ─────────────────────────────────────────

const fn default_cb_l1_cooldown() -> u64 {
    60
}
const fn default_cb_l2_cooldown() -> u64 {
    900
}
const fn default_cb_l3_cooldown() -> u64 {
    3600
}
const fn default_cb_l4_cooldown() -> u64 {
    7200
}
const fn default_cb_half_open_probes() -> u32 {
    2
}
const fn default_cb_recovery_observation() -> u64 {
    300
}
const fn default_cb_max_cooldown() -> u64 {
    14400
}
