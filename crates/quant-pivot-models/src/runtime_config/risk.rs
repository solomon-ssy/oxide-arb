//! Risk guard-rail runtime configuration (`risk` section).
//!
//! Single-strategy (endgame) risk model. All limits, position sizing, and
//! endgame-specific parameters live in [`RiskConfig`] (`PositionSizingConfig`
//! was absorbed here per ADR-001 §4). Every field is hot-reloadable through
//! the versioned runtime-config activation path; tightening exposure limits
//! below the currently reserved amounts is rejected by the activation
//! preflight (fail-closed).

use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Risk limits, accounting caps, circuit breaker, and position sizing.
///
/// The whole section is money-critical: every field bounds money at risk, so
/// the schema marks the container and the UI requires confirmation for all of
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
#[schemars(extend("x-money-critical" = true))]
pub struct RiskConfig {
    // ── Per-opportunity static filters ───────────────────────────────
    /// Minimum order-book depth (USD) required before execution. Default: `200`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub min_depth_usd: Decimal,
    /// Maximum fraction of visible book depth a single order may consume (%).
    /// Default: `30`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_depth_usage_pct: Decimal,

    // ── Rolling counters + adaptive cooldown ─────────────────────────
    /// Consecutive misses before the session breaker trips. Default: `3`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_consecutive_misses: u32,
    /// Rolling hourly loss cap (USD); breach trips the L2 breaker. Default: `30`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_hourly_loss_usd: Decimal,
    /// Rolling hourly fee-spend cap (USD); breach trips the L2 breaker.
    /// Default: `10`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_hourly_fee_spend_usd: Decimal,
    /// Base adaptive cooldown after repeated misses (seconds). Default: `900`.
    #[schemars(extend("x-format" = "integer"))]
    pub base_cooldown_secs: u64,
    /// Exponential multiplier applied per consecutive cooldown. Default: `2.0`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub cooldown_multiplier: Decimal,
    /// Hard ceiling for the adaptive cooldown (seconds). Default: `7200`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_cooldown_secs: u64,

    // ── Daily / weekly loss caps ─────────────────────────────────────
    /// Daily realized-loss cap (USD); breach halts at L3. Default: `75`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_daily_loss_usd: Decimal,
    /// Daily fee-spend cap (USD); breach halts at L3. Default: `25`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_daily_fee_spend_usd: Decimal,
    /// Single-trade loss cap (USD); breach halts at L3. Default: `30`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_single_loss_usd: Decimal,
    /// Weekly realized-loss cap (USD); breach halts at L4. Default: `120`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_weekly_loss_usd: Decimal,
    /// Independent daily spend budget (USD). Execution stops when exhausted.
    /// Default: `50`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub daily_budget_usd: Decimal,

    // ── Connectivity + balance health ────────────────────────────────
    /// WS disconnect duration (seconds) before trading is gated. Default: `30`.
    #[schemars(extend("x-format" = "integer"))]
    pub ws_disconnect_threshold_secs: u64,
    /// Minimum CLOB collateral balance (USD); below this trading is gated.
    /// Default: `50`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub min_balance_usd: Decimal,

    // ── Blacklist ────────────────────────────────────────────────────
    /// Consecutive misses on one market before auto-blacklisting. Default: `3`.
    #[schemars(extend("x-format" = "integer"))]
    pub market_miss_blacklist_count: u32,
    /// Auto-blacklist TTL (seconds). Default: `3600`.
    #[schemars(extend("x-format" = "integer"))]
    pub market_miss_blacklist_duration_secs: u64,
    /// Permanently blacklisted market condition IDs. Reload merges with — and
    /// never removes — entries added at runtime via the blacklist API.
    /// Default: empty.
    #[schemars(extend(
        "items" = {
            "type": "string",
            "pattern": "^0x[0-9a-fA-F]{64}$"
        }
    ))]
    pub permanent_blacklist_markets: Vec<String>,
    /// Permanently blacklisted CLOB token IDs. Same merge semantics as
    /// `permanent_blacklist_markets`. Default: empty.
    #[schemars(extend(
        "items" = {
            "type": "string",
            "pattern": "^[0-9]+$"
        }
    ))]
    pub permanent_blacklist_tokens: Vec<String>,

    // ── Exposure limits ──────────────────────────────────────────────
    /// Maximum total exposure across all reservations (USD). Preflight rejects
    /// activation when set below the currently reserved total. Default: `5000`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_total_exposure_usd: Decimal,
    /// Balance reserve (USD) excluded from the Kelly bankroll. Default: `100`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub reserve_balance_usd: Decimal,
    /// Maximum concurrently open positions. Default: `3`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_open_positions: usize,
    /// Maximum exposure per market (USD). Preflight rejects activation when set
    /// below any in-flight market exposure. Default: `500`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_single_market_exposure_usd: Decimal,
    /// Maximum USD for a single bet. Default: `25`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_single_bet_usd: Decimal,
    /// Default TTL (seconds) for in-flight capital reservations. Default: `300`.
    #[schemars(extend("x-format" = "integer"))]
    pub reservation_ttl_secs: u64,
    /// Interval (seconds) for cleaning expired in-flight reservations.
    /// Default: `30`.
    #[schemars(extend("x-format" = "integer"))]
    pub reservation_gc_interval_secs: u64,

    // ── Exposure as percentage of balance ────────────────────────────
    /// Maximum portfolio exposure as a percentage of available balance.
    /// Default: `80`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_total_exposure_pct: Decimal,

    // ── Reconciliation ───────────────────────────────────────────────
    /// Interval (seconds) between CLOB balance + open-position metrics
    /// refreshes. Default: `5`.
    #[schemars(extend("x-format" = "integer"))]
    pub metrics_refresh_interval_secs: u64,
    /// Maximum age (seconds) of the risk metrics snapshot allowed on the Live
    /// hot path. Must be >= `metrics_refresh_interval_secs`. Default: `15`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_metrics_staleness_secs: u64,
    /// Interval (seconds) between ledger reconciliation runs. Default: `300`.
    #[schemars(extend("x-format" = "integer"))]
    pub reconciliation_interval_secs: u64,
    /// Maximum acceptable balance drift (USD) before alerting. Default: `1.0`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub reconciliation_tolerance_usd: Decimal,

    // ── Circuit breaker ──────────────────────────────────────────────
    /// 4-level circuit breaker cooldowns and recovery policy.
    pub circuit_breaker: CircuitBreakerConfig,

    // ── Position sizing (absorbed from PositionSizingConfig) ─────────
    /// Quarter-Kelly fraction multiplier (`f*/4`). Default: `0.25`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub kelly_fraction: Decimal,
    /// Total bankroll available for Kelly computation (USD). Also seeds the
    /// simulated balance in `DryRun`/`Paper`. Default: `1000`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub bankroll_usd: Decimal,
    /// Minimum trade size (USD); sized below this the opportunity is skipped.
    /// Default: `1`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub min_trade_usd: Decimal,
    /// Kelly criterion guards.
    pub kelly: KellyConfig,
    /// Drawdown protection.
    pub drawdown: DrawdownConfig,

    // ── API health ───────────────────────────────────────────────────
    /// API error rate threshold (0..1). Exceeding trips the L2 Session breaker.
    /// Default: `0.10`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub api_error_rate_threshold: Decimal,
    /// Consecutive heartbeat failures before an L4 System halt. Default: `3`.
    #[schemars(extend("x-format" = "integer"))]
    pub heartbeat_max_failures: u32,

    // ── Potential loss escalation ────────────────────────────────────
    /// Maximum age (seconds) of an active potential-loss entry before
    /// escalation triggers an L4 System halt. Default: `3600`.
    #[schemars(extend("x-format" = "integer"))]
    pub potential_loss_escalation_secs: u64,

    // ── Endgame-specific rules ───────────────────────────────────────
    /// Max concurrent positions on the same directional side. Default: `3`.
    #[schemars(extend("x-format" = "integer"))]
    pub max_concurrent_directional: usize,
    /// Daily budget of directional trades per side. Default: `10`.
    #[schemars(extend("x-format" = "integer"))]
    pub daily_directional_budget: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            min_depth_usd: default_min_depth_usd(),
            max_depth_usage_pct: default_max_depth_usage_pct(),
            max_consecutive_misses: default_max_misses(),
            max_hourly_loss_usd: default_max_hourly_loss(),
            max_hourly_fee_spend_usd: default_max_hourly_fee_spend(),
            base_cooldown_secs: default_base_cooldown(),
            cooldown_multiplier: default_cooldown_mult(),
            max_cooldown_secs: default_max_cooldown(),
            max_daily_loss_usd: default_max_daily_loss(),
            max_daily_fee_spend_usd: default_max_daily_fee_spend(),
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
            reservation_ttl_secs: default_reservation_ttl_secs(),
            reservation_gc_interval_secs: default_reservation_gc_interval_secs(),
            max_total_exposure_pct: default_max_total_exposure_pct(),
            metrics_refresh_interval_secs: default_metrics_refresh_interval(),
            max_metrics_staleness_secs: default_max_metrics_staleness(),
            reconciliation_interval_secs: default_reconciliation_interval(),
            reconciliation_tolerance_usd: default_reconciliation_tolerance(),
            circuit_breaker: CircuitBreakerConfig::default(),
            kelly_fraction: default_kelly_fraction(),
            bankroll_usd: default_bankroll(),
            min_trade_usd: default_min_trade(),
            kelly: KellyConfig::default(),
            drawdown: DrawdownConfig::default(),
            api_error_rate_threshold: default_api_error_rate_threshold(),
            heartbeat_max_failures: default_heartbeat_max_failures(),
            potential_loss_escalation_secs: default_potential_loss_escalation(),
            max_concurrent_directional: default_max_concurrent_directional(),
            daily_directional_budget: default_daily_directional_budget(),
        }
    }
}

impl RiskConfig {
    /// Derive the exposure-reservation limits from the risk limits so risk
    /// gates and reservation accounting share one authority.
    #[must_use]
    pub fn exposure_reservation_config(&self) -> ExposureReservationConfig {
        ExposureReservationConfig {
            max_total_exposure_cents: usd_decimal_to_cents(self.max_total_exposure_usd),
            max_per_market_cents: usd_decimal_to_cents(self.max_single_market_exposure_usd),
            default_ttl_secs: self.reservation_ttl_secs,
            gc_interval_secs: self.reservation_gc_interval_secs,
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
const fn default_max_hourly_fee_spend() -> Decimal {
    dec!(10)
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
const fn default_max_daily_fee_spend() -> Decimal {
    dec!(25)
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
const fn default_metrics_refresh_interval() -> u64 {
    5
}
const fn default_max_metrics_staleness() -> u64 {
    default_metrics_refresh_interval() * 3
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct KellyConfig {
    /// Maximum Kelly fraction before capping. Default: `0.25`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_kelly: Decimal,
    /// Minimum edge (bps) below which Kelly returns zero. Default: `200`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub min_edge_bps: Decimal,
    /// Minimum calibration confidence (0..1) below which Kelly returns zero.
    /// Default: `0.3`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub min_probability_confidence: Decimal,
    /// Minimum historical sample count for calibration to be trusted.
    /// Default: `10`.
    #[schemars(extend("x-format" = "integer"))]
    pub min_calibration_samples: u32,
    /// Maximum staleness (seconds) of the calibration model before Kelly
    /// returns zero. Default: `7200`.
    #[schemars(extend("x-format" = "integer"))]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DrawdownConfig {
    /// Maximum drawdown (%) before position sizes are reduced. Default: `10`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub max_drawdown_pct: Decimal,
    /// Size reduction factor applied when the drawdown limit is hit.
    /// Default: `0.5`.
    #[schemars(with = "String", extend("x-format" = "decimal"))]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    /// L1 (Trade): per-opportunity static filter failure cooldown (seconds).
    /// Default: `60`.
    #[schemars(extend("x-format" = "integer"))]
    pub l1_cooldown_secs: u64,
    /// L2 (Session): rolling window breach cooldown (seconds). Default: `900`.
    #[schemars(extend("x-format" = "integer"))]
    pub l2_cooldown_secs: u64,
    /// L3 (Daily): daily/weekly cap breach cooldown (seconds). Default: `3600`.
    #[schemars(extend("x-format" = "integer"))]
    pub l3_cooldown_secs: u64,
    /// L4 (System): connectivity/balance emergency cooldown (seconds).
    /// Default: `7200`.
    #[schemars(extend("x-format" = "integer"))]
    pub l4_cooldown_secs: u64,
    /// Successful probe trades required in `HalfOpen` before Recovered.
    /// Default: `2`.
    #[schemars(extend("x-format" = "integer"))]
    pub half_open_probes: u32,
    /// Observation period (seconds) in Recovered before returning to Closed.
    /// Default: `300`.
    #[schemars(extend("x-format" = "integer"))]
    pub recovery_observation_secs: u64,
    /// Maximum cooldown duration (seconds) for L2 exponential back-off.
    /// Default: `14400`.
    #[schemars(extend("x-format" = "integer"))]
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
    14_400
}

// ── Exposure Reservation Config ──────────────────────────────────────────────

/// Derived configuration for the exposure reservation system.
///
/// Production code must construct this from [`RiskConfig`] via
/// [`RiskConfig::exposure_reservation_config`] so risk gates and reservation
/// limits share one authority. Direct construction is kept for focused tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExposureReservationConfig {
    /// Maximum total exposure across all active reservations (USD cents).
    pub max_total_exposure_cents: u64,
    /// Maximum exposure per market (USD cents).
    pub max_per_market_cents: u64,
    /// Default TTL for reservations in seconds (auto-expire if not
    /// confirmed/released).
    pub default_ttl_secs: u64,
    /// GC interval in seconds for cleaning expired reservations.
    pub gc_interval_secs: u64,
}

impl Default for ExposureReservationConfig {
    fn default() -> Self {
        RiskConfig::default().exposure_reservation_config()
    }
}

const fn default_reservation_ttl_secs() -> u64 {
    300
}
const fn default_reservation_gc_interval_secs() -> u64 {
    30
}

fn usd_decimal_to_cents(value: Decimal) -> u64 {
    (value * dec!(100)).ceil().to_u64().unwrap_or(u64::MAX)
}
