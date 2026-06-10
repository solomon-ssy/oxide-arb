//! Opportunity-detection runtime configuration (`detection` section).
//!
//! Endgame-only by design (ADR-001): there is no `enabled` switch — trading is
//! stopped through the governed execution mode or the circuit breaker, never by
//! silently disabling detection. All fields are hot-reloadable through the
//! versioned runtime-config activation path.

use crate::enums::common::MarketCategory;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Detection pipeline tunables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DetectionConfig {
    /// Authoritative minimum net profit (USD) for detection, validation, and
    /// risk (single source per ADR-001 — never duplicated under `execution`
    /// or `risk`). Opportunities below this expected net profit are dropped.
    /// Default: `0.50`.
    #[schemars(with = "String", extend("x-money-critical" = true))]
    pub min_profit_threshold_usd: Decimal,
    /// Endgame convergence detection parameters.
    pub endgame: EndgameDetectionConfig,
    /// Resolution-calibration pipeline parameters.
    pub calibration: CalibrationConfig,
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            min_profit_threshold_usd: default_min_profit_threshold_usd(),
            endgame: EndgameDetectionConfig::default(),
            calibration: CalibrationConfig::default(),
        }
    }
}

const fn default_min_profit_threshold_usd() -> Decimal {
    dec!(0.50)
}

// ── Endgame Detection ────────────────────────────────────────────────────────

/// Endgame convergence detection configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndgameDetectionConfig {
    /// Only markets settling within this many hours are scanned. Larger windows
    /// admit slower-converging markets but tie up capital longer. Default: `24`.
    pub settlement_window_hours: u64,
    /// Best-ask price at or above this value marks a market as converged
    /// (YES or NO side). Money-critical: lowering it admits less-certain
    /// markets into the endgame funnel. Default: `0.95`.
    #[schemars(with = "String", extend("x-money-critical" = true))]
    pub high_threshold: Decimal,
    /// A market must hold convergence for at least this long before an
    /// opportunity may be emitted. Guards against transient spikes.
    /// Default: `300` (5 minutes).
    pub min_convergence_duration_secs: u64,
    /// Minimum profit per share (`1 - entry VWAP`) to act. Below this the
    /// edge cannot cover fees + slippage. Default: `0.005`.
    #[schemars(with = "String", extend("x-money-critical" = true))]
    pub min_profit_per_share: Decimal,
    /// Maximum USD walked into the order book per opportunity. Caps single-shot
    /// sizing before risk sizing applies. Default: `500`.
    #[schemars(with = "String", extend("x-money-critical" = true))]
    pub max_investment_usd: Decimal,
    /// Fill-probability estimation parameters.
    pub fill_probability: FillProbabilityConfig,
    /// Opportunity scoring parameters.
    pub scorer: ScorerConfig,
    /// Per-market emission cooldown (anti-flood) parameters.
    pub emission_cooldown: EmissionCooldownConfig,
    /// Convergence tracker cache parameters.
    pub convergence_tracker: ConvergenceTrackerConfig,
}

impl Default for EndgameDetectionConfig {
    fn default() -> Self {
        Self {
            settlement_window_hours: default_settlement_window_hours(),
            high_threshold: default_high_threshold(),
            min_convergence_duration_secs: default_min_convergence_secs(),
            min_profit_per_share: default_min_profit_per_share(),
            max_investment_usd: default_max_investment_usd(),
            fill_probability: FillProbabilityConfig::default(),
            scorer: ScorerConfig::default(),
            emission_cooldown: EmissionCooldownConfig::default(),
            convergence_tracker: ConvergenceTrackerConfig::default(),
        }
    }
}

const fn default_settlement_window_hours() -> u64 {
    24
}
const fn default_high_threshold() -> Decimal {
    dec!(0.95)
}
const fn default_min_convergence_secs() -> u64 {
    300
}
const fn default_min_profit_per_share() -> Decimal {
    dec!(0.005)
}
const fn default_max_investment_usd() -> Decimal {
    dec!(500)
}

// ── Calibration ──────────────────────────────────────────────────────────────

/// Calibration data pipeline configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CalibrationConfig {
    /// Minimum sample size before a bucket's resolution rate is trusted.
    /// Below this threshold the fallback chain is activated. Default: `30`.
    pub min_sample_size: u32,
    /// How often (seconds) the background updater reconciles calibration data
    /// from the DB and oracles. Default: `3600`.
    pub refresh_interval_secs: u64,
    /// Prior strength `n₀` for the dynamic fusion weight `w(n) = n / (n + n₀)`.
    /// Higher values give more weight to the calibrator (slower adaptation to
    /// real-time signals). Default: `20`.
    pub fusion_prior_strength: u32,
    /// Floor for the fused probability output. Default: `0.80`.
    #[schemars(with = "String")]
    pub fused_p_floor: Decimal,
    /// Ceiling for the fused probability output. Default: `0.995`.
    #[schemars(with = "String")]
    pub fused_p_ceiling: Decimal,
    /// Bootstrap alpha prior (before `MoM` estimation is available).
    /// Default: `2.0`.
    #[schemars(with = "String")]
    pub bootstrap_alpha: Decimal,
    /// Bootstrap beta prior. Default: `0.2`.
    #[schemars(with = "String")]
    pub bootstrap_beta: Decimal,
}

impl Default for CalibrationConfig {
    fn default() -> Self {
        Self {
            min_sample_size: default_min_sample_size(),
            refresh_interval_secs: default_refresh_interval_secs(),
            fusion_prior_strength: default_fusion_prior_strength(),
            fused_p_floor: default_fused_p_floor(),
            fused_p_ceiling: default_fused_p_ceiling(),
            bootstrap_alpha: default_bootstrap_alpha(),
            bootstrap_beta: default_bootstrap_beta(),
        }
    }
}

const fn default_min_sample_size() -> u32 {
    30
}
const fn default_refresh_interval_secs() -> u64 {
    3600
}
const fn default_fusion_prior_strength() -> u32 {
    20
}
const fn default_fused_p_floor() -> Decimal {
    dec!(0.80)
}
const fn default_fused_p_ceiling() -> Decimal {
    dec!(0.995)
}
const fn default_bootstrap_alpha() -> Decimal {
    dec!(2.0)
}
const fn default_bootstrap_beta() -> Decimal {
    dec!(0.2)
}

// ── Fill Probability ─────────────────────────────────────────────────────────

/// Endgame-specific fill probability estimation parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FillProbabilityConfig {
    /// Base fill probability for a single FOK order with fresh data.
    /// Default: `0.90`.
    #[schemars(with = "String")]
    pub base_fill_prob: Decimal,
    /// Depth usage (%) above which fill probability drops. Default: `20`.
    #[schemars(with = "String")]
    pub depth_penalty_threshold_pct: Decimal,
    /// Per-percentage-point penalty above the threshold. Default: `0.02`.
    #[schemars(with = "String")]
    pub depth_penalty_per_pct: Decimal,
    /// Per-`StalenessLevel`-step penalty. Default: `0.05`.
    #[schemars(with = "String")]
    pub staleness_penalty_per_level: Decimal,
    /// Bonus for near-resolution markets (within 6 hours). Default: `0.05`.
    #[schemars(with = "String")]
    pub resolution_proximity_bonus: Decimal,
}

impl Default for FillProbabilityConfig {
    fn default() -> Self {
        Self {
            base_fill_prob: default_base_fill_prob(),
            depth_penalty_threshold_pct: default_depth_penalty_threshold(),
            depth_penalty_per_pct: default_depth_penalty_per_pct(),
            staleness_penalty_per_level: default_staleness_penalty(),
            resolution_proximity_bonus: default_resolution_bonus(),
        }
    }
}

const fn default_base_fill_prob() -> Decimal {
    dec!(0.90)
}
const fn default_depth_penalty_threshold() -> Decimal {
    dec!(20)
}
const fn default_depth_penalty_per_pct() -> Decimal {
    dec!(0.02)
}
const fn default_staleness_penalty() -> Decimal {
    dec!(0.05)
}
const fn default_resolution_bonus() -> Decimal {
    dec!(0.05)
}

// ── Scorer ───────────────────────────────────────────────────────────────────

/// Endgame opportunity scorer configuration.
///
/// Wire format is decimal everywhere (symmetric serialize/deserialize so the
/// stored runtime-config JSON round-trips exactly); the algorithm layer
/// converts to fixed-point `Micro*` values at construction/reload time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ScorerConfig {
    /// Minimum composite score (0..1) to emit an opportunity. Default: `0.10`.
    #[schemars(with = "String")]
    pub min_score: Decimal,
    /// Maximum depth usage (%) the detector may accept. Default: `50`.
    #[schemars(with = "String")]
    pub max_depth_usage_pct: Decimal,
    /// Per-category weight multipliers for scoring (lower fee categories are
    /// weighted higher). Categories absent from the map default to `1.0` at
    /// conversion time.
    #[schemars(with = "HashMap<String, String>")]
    pub category_weights: HashMap<MarketCategory, Decimal>,
}

impl Default for ScorerConfig {
    fn default() -> Self {
        Self {
            min_score: default_min_score(),
            max_depth_usage_pct: default_max_depth_usage(),
            category_weights: default_category_weights(),
        }
    }
}

const fn default_min_score() -> Decimal {
    dec!(0.10)
}
const fn default_max_depth_usage() -> Decimal {
    dec!(50)
}

/// Default category weights derived from fee rates: lower fees → higher weight.
#[must_use]
pub fn default_category_weights() -> HashMap<MarketCategory, Decimal> {
    HashMap::from([
        (MarketCategory::Geopolitics, dec!(1.5)),
        (MarketCategory::Sports, dec!(1.2)),
        (MarketCategory::Politics, dec!(1.0)),
        (MarketCategory::Finance, dec!(1.0)),
        (MarketCategory::Tech, dec!(1.0)),
        (MarketCategory::Culture, dec!(0.8)),
        (MarketCategory::Weather, dec!(0.8)),
        (MarketCategory::Economics, dec!(0.8)),
        (MarketCategory::Crypto, dec!(0.8)),
        (MarketCategory::Other, dec!(0.8)),
    ])
}

// ── Emission Cooldown ────────────────────────────────────────────────────────

/// Emission cooldown configuration preventing duplicate opportunity signals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EmissionCooldownConfig {
    /// Base cooldown duration in seconds. Default: `30`.
    pub base_cooldown_secs: u64,
    /// Maximum exponential backoff multiplier for consecutive emissions.
    /// Default: `16.0`.
    #[schemars(with = "String")]
    pub max_multiplier: Decimal,
    /// Maximum cache capacity (number of tracked markets). Caution: changing
    /// this at runtime rebuilds the cache, clearing all in-flight cooldown
    /// state. Default: `4096`.
    pub max_capacity: u64,
}

impl Default for EmissionCooldownConfig {
    fn default() -> Self {
        Self {
            base_cooldown_secs: default_base_cooldown_secs(),
            max_multiplier: default_cooldown_max_multiplier(),
            max_capacity: default_cooldown_capacity(),
        }
    }
}

const fn default_base_cooldown_secs() -> u64 {
    30
}
const fn default_cooldown_max_multiplier() -> Decimal {
    dec!(16.0)
}
const fn default_cooldown_capacity() -> u64 {
    4096
}

// ── Convergence Tracker ──────────────────────────────────────────────────────

/// Convergence tracker cache configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ConvergenceTrackerConfig {
    /// Max idle time before a market's convergence state is evicted (seconds).
    /// Default: `7200`.
    pub max_idle_secs: u64,
    /// Maximum number of tracked markets. Caution: capacity changes only apply
    /// to detectors constructed after activation (the live tracker keeps its
    /// capacity to preserve accumulated convergence durations). Default: `10000`.
    pub max_capacity: u64,
}

impl Default for ConvergenceTrackerConfig {
    fn default() -> Self {
        Self {
            max_idle_secs: default_tracker_max_idle(),
            max_capacity: default_tracker_capacity(),
        }
    }
}

const fn default_tracker_max_idle() -> u64 {
    7200
}
const fn default_tracker_capacity() -> u64 {
    10_000
}
