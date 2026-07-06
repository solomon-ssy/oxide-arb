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

/// Structural (prediction-market-aware) factors (Phase 11.2.1).
///
/// Shock-gated reversal (conditional; orthogonal to the linear `mean_reversion`).
pub const STRUCT_REVERSAL_AFTER_SHOCK: FactorName =
    FactorName::from_static("struct.reversal_after_shock");
/// Resolution-proximity × price-extremity interaction (orthogonal to the linear
/// `time_to_resolution`).
pub const STRUCT_RESOLUTION_PROXIMITY_REGIME: FactorName =
    FactorName::from_static("struct.resolution_proximity_regime");
/// Book-churn intensity (delta-to-update ratio over the maker window).
///
/// This is a **book-derived** liquidity-turnover proxy — NOT true maker
/// participant concentration (Gini / top-1% share), which requires trade-tape
/// (maker/taker address + fill size) the platform does not yet ingest. The
/// honest concentration factor is designed in
/// [`11.2.1.1-trade-tape-participant-concentration.md`] and will supersede this
/// proxy; the name reflects exactly what the current facts can compute.
pub const STRUCT_BOOK_CHURN_INTENSITY: FactorName =
    FactorName::from_static("struct.book_churn_intensity");
/// Neg-risk full-leg YES-ask-sum drift (`Σ ask − 1`); neg-risk markets only.
pub const STRUCT_NEGRISK_LEG_SUM_DRIFT: FactorName =
    FactorName::from_static("struct.negrisk_leg_sum_drift");
/// Neg-risk conversion edge (basket vs NO-favorite); neg-risk markets only.
pub const STRUCT_NEGRISK_CONVERT_EDGE: FactorName =
    FactorName::from_static("struct.negrisk_convert_edge");
/// Favorite-longshot bias correction (consumes the fitted bias table).
pub const STRUCT_FAVORITE_LONGSHOT: FactorName =
    FactorName::from_static("struct.favorite_longshot");

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
