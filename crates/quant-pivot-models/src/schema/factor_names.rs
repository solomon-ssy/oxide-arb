//! Canonical generic scoring factor identifiers for the runtime-config UI.
//!
//! Mirrors the compile-time registry in `quant-pivot-research::factors::names`
//! (market factors only — position-state pseudo-factors are Sell-scorer local).

/// Generic market factor wire keys shown in the factor-weights editor.
pub const GENERIC_SCORING_FACTOR_NAMES: &[&str] = &[
    "liquidity_depth",
    "spread_efficiency",
    "book_imbalance",
    "momentum",
    "mean_reversion",
    "volatility_regime",
    "market_activity",
    "time_to_resolution",
    "data_quality",
];
