//! Canonical static [`FactorName`] constants — single source of truth for
//! generic factor identifiers.

use super::FactorName;

/// Generic liquidity factors.
pub const LIQUIDITY_DEPTH: FactorName = FactorName::from_static("liquidity_depth");
/// Spread-efficiency factor (inverse spread).
pub const SPREAD_EFFICIENCY: FactorName = FactorName::from_static("spread_efficiency");

/// Generic microstructure factors.
pub const BOOK_IMBALANCE: FactorName = FactorName::from_static("book_imbalance");

/// Generic momentum / mean-reversion factors.
pub const MOMENTUM: FactorName = FactorName::from_static("momentum");
/// Mean-reversion factor.
pub const MEAN_REVERSION: FactorName = FactorName::from_static("mean_reversion");

/// Volatility-regime factor.
pub const VOLATILITY_REGIME: FactorName = FactorName::from_static("volatility_regime");

/// Market-activity factor.
pub const MARKET_ACTIVITY: FactorName = FactorName::from_static("market_activity");

/// Resolution-timing factor.
pub const TIME_TO_RESOLUTION: FactorName = FactorName::from_static("time_to_resolution");

/// Aggregate data-quality factor.
pub const DATA_QUALITY: FactorName = FactorName::from_static("data_quality");
