//! Opportunity detection configuration.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct DetectionConfig {
    #[serde(default = "default_scan_interval_secs")]
    pub fallback_scan_interval_secs: u64,
    #[serde(default = "default_min_profit_threshold_usd")]
    pub min_profit_threshold_usd: Decimal,
    #[serde(default = "default_budget_targets_usd")]
    pub budget_targets_usd: Vec<Decimal>,
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
            budget_targets_usd: default_budget_targets_usd(),
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
fn default_budget_targets_usd() -> Vec<Decimal> {
    vec![dec!(5), dec!(10), dec!(25), dec!(50), dec!(100)]
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

/// Calibration data pipeline configuration.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CalibrationConfig {
    /// Minimum sample size before a bucket's resolution rate is trusted.
    #[serde(default = "default_min_sample_size")]
    pub min_sample_size: u32,
    /// How often (seconds) to refresh calibration data from the DB.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

impl Default for CalibrationConfig {
    fn default() -> Self {
        Self {
            min_sample_size: default_min_sample_size(),
            refresh_interval_secs: default_refresh_interval_secs(),
        }
    }
}

const fn default_min_sample_size() -> u32 {
    30
}
const fn default_refresh_interval_secs() -> u64 {
    3600
}
