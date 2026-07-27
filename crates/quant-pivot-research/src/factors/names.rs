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

/// Structural (prediction-market-aware) factors.
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
/// This is a **book-derived** liquidity-turnover proxy, not maker participant
/// concentration. The separate `struct.participant_concentration` factor
/// computes Gini, CR1, and HHI from trade-tape participant facts.
pub const STRUCT_BOOK_CHURN_INTENSITY: FactorName =
    FactorName::from_static("struct.book_churn_intensity");
/// Trade-tape participant concentration (neutral structural regime signal).
pub const STRUCT_PARTICIPANT_CONCENTRATION: FactorName =
    FactorName::from_static("struct.participant_concentration");
/// Neg-risk full-leg YES-ask-sum drift (`Σ ask − 1`); neg-risk markets only.
pub const STRUCT_NEGRISK_LEG_SUM_DRIFT: FactorName =
    FactorName::from_static("struct.negrisk_leg_sum_drift");

/// Crypto domain plane (category-routed, never config-selected):
/// strike distance scaled by settlement urgency.
pub const DOMAIN_CRYPTO_STRIKE_PRESSURE: FactorName =
    FactorName::from_static("domain_crypto_strike_pressure");
/// Underlying momentum per unit realized vol (the crypto subject's regime).
pub const DOMAIN_CRYPTO_BETA_REGIME: FactorName =
    FactorName::from_static("domain_crypto_beta_regime");
/// Calibrated GEFS member probability of the linked temperature band.
pub const DOMAIN_WEATHER_ENSEMBLE_BIN_PROBABILITY: FactorName =
    FactorName::from_static("domain.weather.ensemble_bin_probability");
/// Dispersion of calibrated GEFS daily member extrema.
pub const DOMAIN_WEATHER_ENSEMBLE_SPREAD: FactorName =
    FactorName::from_static("domain.weather.ensemble_spread");
/// Whole-degree distance of the observed max/min from the decisive band bound.
pub const DOMAIN_WEATHER_OBSERVED_EXTREME_HEADROOM: FactorName =
    FactorName::from_static("domain.weather.observed_extreme_headroom");
/// Empirical AviationWeather-vs-GHCNh daily-extreme proxy basis risk.
pub const DOMAIN_WEATHER_NOAA_RESOLUTION_BASIS_RISK: FactorName =
    FactorName::from_static("domain.weather.noaa_resolution_basis_risk");
/// Neg-risk conversion edge (basket vs NO-favorite); neg-risk markets only.
pub const STRUCT_NEGRISK_CONVERT_EDGE: FactorName =
    FactorName::from_static("struct.negrisk_convert_edge");
/// Favorite-longshot bias correction (consumes the fitted bias table).
pub const STRUCT_FAVORITE_LONGSHOT: FactorName =
    FactorName::from_static("struct.favorite_longshot");

/// Sell-only position state computed per lot from ledger state and its live
/// mark, outside the market [`FactorEngine`](crate::factors::FactorEngine).
///
/// Positive unrealized `PnL` pressure, saturated at the governed move scale.
pub const POSITION_TAKE_PROFIT_PRESSURE: FactorName =
    FactorName::from_static("position_take_profit_pressure");
/// Negative unrealized `PnL` magnitude supporting a stop-loss exit.
pub const POSITION_STOP_LOSS_PRESSURE: FactorName =
    FactorName::from_static("position_stop_loss_pressure");
/// Fraction of the model horizon the lot has been held (`[0, 1]`).
pub const POSITION_TIME_IN_TRADE: FactorName = FactorName::from_static("position_time_in_trade");
/// Drawdown of the current mark from the lot's peak mark (`[0, 1]`).
pub const POSITION_PEAK_DRAWDOWN: FactorName = FactorName::from_static("position_peak_drawdown");
