//! Opportunity detection configuration.

use crate::enums::common::MarketCategory;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::collections::HashMap;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct DetectionConfig {
    #[serde(default = "default_scan_interval_secs")]
    pub fallback_scan_interval_secs: u64,
    /// Authoritative minimum net profit (USD) for detection, validation, and risk.
    /// Single source per ADR-001 — do not duplicate under `[execution]` or `[risk]`.
    #[serde(default = "default_min_profit_threshold_usd")]
    pub min_profit_threshold_usd: Decimal,
    #[serde(default = "default_warmup_secs")]
    pub detection_warmup_secs: u64,
    #[serde(default = "default_coalesce_ms")]
    pub dedup_coalesce_window_ms: u64,
    #[serde(default = "default_scan_concurrency")]
    pub fallback_scan_concurrency: usize,
    #[serde(default)]
    pub endgame: EndgameDetectionConfig,
    #[serde(default)]
    pub calibration: CalibrationConfig,
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            fallback_scan_interval_secs: default_scan_interval_secs(),
            min_profit_threshold_usd: default_min_profit_threshold_usd(),
            detection_warmup_secs: default_warmup_secs(),
            dedup_coalesce_window_ms: default_coalesce_ms(),
            fallback_scan_concurrency: default_scan_concurrency(),
            endgame: EndgameDetectionConfig::default(),
            calibration: CalibrationConfig::default(),
        }
    }
}

const fn default_scan_interval_secs() -> u64 {
    5
}
const fn default_min_profit_threshold_usd() -> Decimal {
    dec!(0.50)
}
const fn default_warmup_secs() -> u64 {
    90
}
const fn default_coalesce_ms() -> u64 {
    300
}
const fn default_scan_concurrency() -> usize {
    32
}

// ── Endgame Detection ────────────────────────────────────────────────────────

/// Endgame convergence detection configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct EndgameDetectionConfig {
    #[serde(default = "default_endgame_enabled")]
    pub enabled: bool,
    #[serde(default = "default_settlement_window_hours")]
    pub settlement_window_hours: u64,
    #[serde(default = "default_high_threshold")]
    pub high_threshold: Decimal,
    #[serde(default = "default_low_threshold")]
    pub low_threshold: Decimal,
    #[serde(default = "default_min_convergence_secs")]
    pub min_convergence_duration_secs: u64,
    #[serde(default = "default_min_profit_per_share")]
    pub min_profit_per_share: Decimal,
    #[serde(default = "default_max_investment_usd")]
    pub max_investment_usd: Decimal,
    #[serde(default = "default_max_convergence_age_secs")]
    pub max_convergence_age_secs: u64,
    #[serde(default)]
    pub fill_probability: FillProbabilityConfig,
    #[serde(default)]
    pub scorer: ScorerConfig,
    #[serde(default)]
    pub emission_cooldown: EmissionCooldownConfig,
    #[serde(default)]
    pub convergence_tracker: ConvergenceTrackerConfig,
}

impl Default for EndgameDetectionConfig {
    fn default() -> Self {
        Self {
            enabled: default_endgame_enabled(),
            settlement_window_hours: default_settlement_window_hours(),
            high_threshold: default_high_threshold(),
            low_threshold: default_low_threshold(),
            min_convergence_duration_secs: default_min_convergence_secs(),
            min_profit_per_share: default_min_profit_per_share(),
            max_investment_usd: default_max_investment_usd(),
            max_convergence_age_secs: default_max_convergence_age_secs(),
            fill_probability: FillProbabilityConfig::default(),
            scorer: ScorerConfig::default(),
            emission_cooldown: EmissionCooldownConfig::default(),
            convergence_tracker: ConvergenceTrackerConfig::default(),
        }
    }
}

const fn default_endgame_enabled() -> bool {
    true
}
const fn default_settlement_window_hours() -> u64 {
    24
}
const fn default_high_threshold() -> Decimal {
    dec!(0.95)
}
const fn default_low_threshold() -> Decimal {
    dec!(0.05)
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
const fn default_max_convergence_age_secs() -> u64 {
    7200
}

// ── Calibration ──────────────────────────────────────────────────────────────

/// Calibration data pipeline configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CalibrationConfig {
    /// Minimum sample size before a bucket's resolution rate is trusted.
    /// Below this threshold the fallback chain is activated.
    #[serde(default = "default_min_sample_size")]
    pub min_sample_size: u32,
    /// How often (seconds) to refresh calibration data from the DB.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
    /// Prior strength parameter `n₀` for the dynamic fusion weight
    /// `w(n) = n / (n + n₀)`. Higher values give more weight to the
    /// calibrator (slower adaptation to real-time signals).
    #[serde(default = "default_fusion_prior_strength")]
    pub fusion_prior_strength: u32,
    /// Floor for fused probability output.
    #[serde(default = "default_fused_p_floor")]
    pub fused_p_floor: Decimal,
    /// Ceiling for fused probability output.
    #[serde(default = "default_fused_p_ceiling")]
    pub fused_p_ceiling: Decimal,
    /// Bootstrap alpha prior (before `MoM` estimation is available).
    #[serde(default = "default_bootstrap_alpha")]
    pub bootstrap_alpha: Decimal,
    /// Bootstrap beta prior.
    #[serde(default = "default_bootstrap_beta")]
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
    10
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
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct FillProbabilityConfig {
    /// Base fill probability for a single FOK order with fresh data.
    #[serde(default = "default_base_fill_prob")]
    pub base_fill_prob: Decimal,
    /// Depth usage (%) above which fill probability drops.
    #[serde(default = "default_depth_penalty_threshold")]
    pub depth_penalty_threshold_pct: Decimal,
    /// Per-percentage-point penalty above the threshold.
    #[serde(default = "default_depth_penalty_per_pct")]
    pub depth_penalty_per_pct: Decimal,
    /// Per-`StalenessLevel`-step penalty.
    #[serde(default = "default_staleness_penalty")]
    pub staleness_penalty_per_level: Decimal,
    /// Bonus for near-resolution markets (within 6 hours).
    #[serde(default = "default_resolution_bonus")]
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
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ScorerConfig {
    /// Minimum composite score to emit an opportunity.
    #[serde(default = "default_min_score")]
    pub min_score: Decimal,
    /// Maximum depth usage (%) to accept.
    #[serde(default = "default_max_depth_usage")]
    pub max_depth_usage_pct: Decimal,
    /// Per-category weight multipliers for scoring.
    #[serde(default = "default_category_weights")]
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
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct EmissionCooldownConfig {
    /// Base cooldown duration in seconds.
    #[serde(default = "default_base_cooldown_secs")]
    pub base_cooldown_secs: u64,
    /// Maximum exponential backoff multiplier for consecutive emissions.
    #[serde(default = "default_cooldown_max_multiplier")]
    pub max_multiplier: Decimal,
    /// Maximum cache capacity (number of tracked markets).
    #[serde(default = "default_cooldown_capacity")]
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
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ConvergenceTrackerConfig {
    /// Max idle time before eviction (seconds).
    #[serde(default = "default_tracker_max_idle")]
    pub max_idle_secs: u64,
    /// Maximum number of tracked markets.
    #[serde(default = "default_tracker_capacity")]
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
