//! Position sizing configuration (Quarter-Kelly + drawdown guard).
//!
//! All trading thresholds live here — not in constants.rs.
//! These are runtime-tunable via config TOML / env vars / hot-reload API.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PositionSizingConfig {
    /// Quarter-Kelly fraction multiplier (f*/4).
    #[serde(default = "default_kelly_fraction")]
    pub kelly_fraction: Decimal,
    /// Total bankroll available for Kelly computation (USD).
    #[serde(default = "default_bankroll")]
    pub bankroll_usd: Decimal,
    /// Minimum trade size (below this, skip the opportunity).
    #[serde(default = "default_min_trade")]
    pub min_trade_usd: Decimal,
    /// Maximum single trade size cap.
    #[serde(default = "default_max_trade")]
    pub max_single_trade_usd: Decimal,
    #[serde(default)]
    pub kelly: KellyConfig,
    #[serde(default)]
    pub drawdown: DrawdownConfig,
}

impl Default for PositionSizingConfig {
    fn default() -> Self {
        Self {
            kelly_fraction: default_kelly_fraction(),
            bankroll_usd: default_bankroll(),
            min_trade_usd: default_min_trade(),
            max_single_trade_usd: default_max_trade(),
            kelly: KellyConfig::default(),
            drawdown: DrawdownConfig::default(),
        }
    }
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
const fn default_max_trade() -> Decimal {
    dec!(250)
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct KellyConfig {
    /// Maximum Kelly fraction before capping.
    #[serde(default = "default_kelly_max")]
    pub max_kelly: Decimal,
    /// Minimum edge (bps) below which Kelly returns zero.
    #[serde(default = "default_kelly_min_edge")]
    pub min_edge_bps: Decimal,
}

impl Default for KellyConfig {
    fn default() -> Self {
        Self {
            max_kelly: default_kelly_max(),
            min_edge_bps: default_kelly_min_edge(),
        }
    }
}

const fn default_kelly_max() -> Decimal {
    dec!(0.25)
}
const fn default_kelly_min_edge() -> Decimal {
    dec!(200)
}

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
