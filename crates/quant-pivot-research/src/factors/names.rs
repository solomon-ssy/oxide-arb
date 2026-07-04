//! Canonical static [`FactorName`] constants — single source of truth for
//! generic factor identifiers.

use super::FactorName;

/// Generic liquidity factors.
pub const LIQUIDITY_DEPTH: FactorName = FactorName::from_static("liquidity_depth");
/// Spread-efficiency factor (inverse spread).
pub const SPREAD_EFFICIENCY: FactorName = FactorName::from_static("spread_efficiency");

/// Generic microstructure factors.
pub const BOOK_IMBALANCE: FactorName = FactorName::from_static("book_imbalance");

/// Generic momentum factors — each a **distinct** estimator, not a return clone.
///
/// Lag-skipped rate of change (12-1 style).
pub const MOMENTUM_ROC: FactorName = FactorName::from_static("momentum_roc");
/// Smoothed EMA-slope momentum (current trend velocity).
pub const MOMENTUM_EMA_SLOPE: FactorName = FactorName::from_static("momentum_ema_slope");
/// Volatility-adjusted (Sharpe-like) momentum.
pub const MOMENTUM_VOL_ADJUSTED: FactorName = FactorName::from_static("momentum_vol_adjusted");
/// Volatility-normalized MACD (trend crossover) momentum.
pub const MOMENTUM_MACD: FactorName = FactorName::from_static("momentum_macd");

/// Mean-reversion (reversal) factor — independent of the momentum family.
pub const MEAN_REVERSION: FactorName = FactorName::from_static("mean_reversion");

/// Volatility-regime factor.
pub const VOLATILITY_REGIME: FactorName = FactorName::from_static("volatility_regime");

/// Market-activity factor.
pub const MARKET_ACTIVITY: FactorName = FactorName::from_static("market_activity");

/// Resolution-timing factor.
pub const TIME_TO_RESOLUTION: FactorName = FactorName::from_static("time_to_resolution");

/// Aggregate data-quality factor.
pub const DATA_QUALITY: FactorName = FactorName::from_static("data_quality");

/// Signed unrealized `PnL` as a fraction of cost basis (`(mark − avg) / avg`).
///
/// Position-state pseudo-factors consumed only by the Sell-side hold-vs-exit
/// scorer (Phase 06.1). Computed per open lot from the ledger + live mark, not
/// by the market [`FactorEngine`](crate::factors::FactorEngine); they let the
/// exit scorer weigh the lot's own state alongside market factors.
pub const POSITION_UNREALIZED_PNL: FactorName =
    FactorName::from_static("position_unrealized_pnl_pct");
/// Fraction of the model horizon the lot has been held (`[0, 1]`).
pub const POSITION_TIME_IN_TRADE: FactorName = FactorName::from_static("position_time_in_trade");
/// Drawdown of the current mark from the lot's peak mark (`[0, 1]`).
pub const POSITION_PEAK_DRAWDOWN: FactorName = FactorName::from_static("position_peak_drawdown");
