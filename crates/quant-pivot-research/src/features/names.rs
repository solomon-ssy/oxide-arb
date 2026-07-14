//! Canonical static [`FeatureName`] constants — single source of truth for
//! built-in feature identifiers.
//!
//! Parameterized features (windowed time series, depth levels) are constructed
//! via [`FeatureName::ts_return`] and siblings in [`super::value`] so the naming
//! formula is never duplicated.

use super::FeatureName;

/// Price-book features from a resolved L2 order book.
pub mod book {
    use super::FeatureName;

    /// Top-of-book best bid price.
    pub const BEST_BID: FeatureName = FeatureName::from_static("book.best_bid");
    /// Top-of-book best ask price.
    pub const BEST_ASK: FeatureName = FeatureName::from_static("book.best_ask");
    /// Top-of-book best ask for the market's secondary (NO) outcome token.
    pub const SECONDARY_BEST_ASK: FeatureName = FeatureName::from_static("book.secondary_best_ask");
    /// Mid price `(bid + ask) / 2`.
    pub const MID: FeatureName = FeatureName::from_static("book.mid");
    /// Top-of-book spread in basis points.
    pub const SPREAD_BPS: FeatureName = FeatureName::from_static("book.spread_bps");
    /// Depth imbalance between bid and ask sides.
    pub const DEPTH_IMBALANCE: FeatureName = FeatureName::from_static("book.depth_imbalance");
    /// Order-book slope proxy.
    pub const SLOPE: FeatureName = FeatureName::from_static("book.slope");
    /// Visible liquidity in USD at the top of book.
    pub const VISIBLE_LIQUIDITY_USD: FeatureName =
        FeatureName::from_static("book.visible_liquidity_usd");
    /// Book age in milliseconds at decision time.
    pub const AGE_MS: FeatureName = FeatureName::from_static("book.age_ms");
    /// Whether the book is crossed (ask < bid).
    pub const CROSSED: FeatureName = FeatureName::from_static("book.crossed");
    /// Whether the book is empty.
    pub const EMPTY: FeatureName = FeatureName::from_static("book.empty");
}

/// Gamma market / event metadata features.
pub mod market {
    use super::FeatureName;

    /// Market category slug.
    pub const CATEGORY: FeatureName = FeatureName::from_static("market.category");
    /// Seconds until market resolution.
    pub const TIME_TO_RESOLUTION_SECS: FeatureName =
        FeatureName::from_static("market.time_to_resolution_secs");
    /// Age of the parent event in seconds.
    pub const EVENT_AGE_SECS: FeatureName = FeatureName::from_static("market.event_age_secs");
    /// Negative-risk flag from Gamma metadata.
    pub const NEG_RISK: FeatureName = FeatureName::from_static("market.neg_risk");
    /// Whether the market is active.
    pub const IS_ACTIVE: FeatureName = FeatureName::from_static("market.is_active");
}

/// Time-series features derived from microstructure windows.
pub mod ts {
    use super::FeatureName;

    /// Price-reversal signal over the configured lookback.
    pub const PRICE_REVERSAL: FeatureName = FeatureName::from_static("ts.price_reversal");
    /// Volatility-normalized MACD (trend-crossover) momentum.
    pub const MACD_NORM: FeatureName = FeatureName::from_static("ts.macd_norm");
}

/// Microstructure features from `ClickHouse` tick windows.
pub mod micro {
    use super::FeatureName;

    /// Quote update rate (per second).
    pub const QUOTE_UPDATE_RATE: FeatureName = FeatureName::from_static("micro.quote_update_rate");
    /// Book churn ratio.
    pub const BOOK_CHURN: FeatureName = FeatureName::from_static("micro.book_churn");
    /// Queue depletion ratio.
    pub const QUEUE_DEPLETION: FeatureName = FeatureName::from_static("micro.queue_depletion");
    /// Sudden liquidity withdrawal ratio.
    pub const SUDDEN_LIQUIDITY_WITHDRAWAL: FeatureName =
        FeatureName::from_static("micro.sudden_liquidity_withdrawal");
    /// Adverse-selection proxy.
    pub const ADVERSE_SELECTION_PROXY: FeatureName =
        FeatureName::from_static("micro.adverse_selection_proxy");
    /// Stale-quote frequency in `[0, 1]`.
    pub const STALE_QUOTE_FREQUENCY: FeatureName =
        FeatureName::from_static("micro.stale_quote_frequency");
}

/// Structural (prediction-market-aware) features — platform-computable from
/// existing facts, no external data source (Phase 11.2.1).
pub mod structural {
    use super::FeatureName;

    /// Signed return over the shock window (drives reversal direction).
    pub const SHORT_RETURN: FeatureName = FeatureName::from_static("struct.short_return");
    /// Shock ratio `|short_return| / realized_vol_short` (gates reversal).
    pub const SHOCK_RATIO: FeatureName = FeatureName::from_static("struct.shock_ratio");
    /// Signed price extremity `mid − 0.5` (interacts with time-to-resolution).
    ///
    /// Signed (not `|mid − 0.5|`) so the resolution-proximity factor can push a
    /// favorite (mid > 0.5) toward YES and a longshot (mid < 0.5) toward NO,
    /// consistent with the favorite-longshot bias.
    pub const PRICE_EXTREMITY: FeatureName = FeatureName::from_static("struct.price_extremity");
    /// Book-churn intensity (delta-to-update ratio over the maker window).
    ///
    /// A book-derived liquidity-turnover proxy, NOT true maker concentration
    /// (which needs trade-tape); see `11.2.1.1`.
    pub const BOOK_CHURN_INTENSITY: FeatureName =
        FeatureName::from_static("struct.book_churn_intensity");
    /// Count of trade-tape participant rows in the configured PIT window.
    pub const TRADE_TAPE_COUNT: FeatureName = FeatureName::from_static("struct.trade_tape_count");
    /// Total trade-tape notional USD in the configured PIT window.
    pub const TRADE_TAPE_NOTIONAL_USD: FeatureName =
        FeatureName::from_static("struct.trade_tape_notional_usd");
    /// Participant-address coverage ratio in the trade-tape window.
    pub const PARTICIPANT_COVERAGE_RATIO: FeatureName =
        FeatureName::from_static("struct.participant_coverage_ratio");
    /// Distinct fill-participant addresses in the trade-tape window.
    pub const PARTICIPANT_COUNT: FeatureName = FeatureName::from_static("struct.participant_count");
    /// Notional-weighted Gini concentration over fill-side participants.
    pub const PARTICIPANT_GINI: FeatureName = FeatureName::from_static("struct.participant_gini");
    /// Notional-weighted Herfindahl-Hirschman concentration over participants.
    pub const PARTICIPANT_HHI: FeatureName = FeatureName::from_static("struct.participant_hhi");
    /// Largest single-participant notional share (CR1).
    pub const PARTICIPANT_CR1_SHARE: FeatureName =
        FeatureName::from_static("struct.participant_cr1_share");
    /// Maker role Gini when role coverage is sufficient.
    pub const MAKER_GINI: FeatureName = FeatureName::from_static("struct.maker_gini");
    /// Taker role Gini when role coverage is sufficient.
    pub const TAKER_GINI: FeatureName = FeatureName::from_static("struct.taker_gini");
    /// Sum of best-ask across all neg-risk YES legs (drift = sum − 1).
    pub const NEGRISK_LEG_ASK_SUM: FeatureName =
        FeatureName::from_static("struct.negrisk_leg_ask_sum");
    /// Sum of best-bid across all neg-risk YES legs (bid-side drift; the
    /// ask/bid gap is the drift-confidence corroborant — tight legs ⇒ reliable).
    pub const NEGRISK_LEG_BID_SUM: FeatureName =
        FeatureName::from_static("struct.negrisk_leg_bid_sum");
    /// Count of resolved neg-risk YES legs at `as_of`.
    pub const NEGRISK_LEG_COUNT: FeatureName = FeatureName::from_static("struct.negrisk_leg_count");
    /// Neg-risk conversion edge (buy YES basket of all-but-favorite vs NO-favorite).
    pub const NEGRISK_CONVERT_EDGE: FeatureName =
        FeatureName::from_static("struct.negrisk_convert_edge");
}

/// Crypto external-vertical (domain-slice) features — computed from frozen
/// market linkages + PIT domain observations (Phase 11.2.2).
pub mod domain_crypto {
    use super::FeatureName;

    /// Signed relative distance from the underlying to the strike.
    ///
    /// Oriented so positive favors YES: `(close − strike)/strike` under `Above`,
    /// `(strike − close)/strike` under `Below`, and `(close − ref)/ref` for up/down
    /// markets (`UpVsReference`).
    pub const DISTANCE_TO_STRIKE: FeatureName =
        FeatureName::from_static("domain.crypto.distance_to_strike");
    /// Underlying close-to-close momentum over the configured window.
    pub const UNDERLYING_MOMENTUM: FeatureName =
        FeatureName::from_static("domain.crypto.underlying_momentum");
    /// Underlying realized volatility (stdev of log returns) over the window.
    pub const UNDERLYING_REALIZED_VOL: FeatureName =
        FeatureName::from_static("domain.crypto.underlying_realized_vol");
    /// Seconds from `as_of` until the subject's settlement observation.
    pub const TIME_TO_OBSERVATION: FeatureName =
        FeatureName::from_static("domain.crypto.time_to_observation");
    /// Basis (bps) between the feature source (Binance close) and the
    /// settlement oracle quote (Chainlink) — risk / cross-check signal.
    pub const BASIS_VS_RESOLUTION_SOURCE: FeatureName =
        FeatureName::from_static("domain.crypto.basis_vs_resolution_source");
}

/// Airport daily-high Weather features. Values are emitted only from typed
/// AviationWeather/GHCNh/GEFS facts bound by the market linkage.
pub mod domain_weather {
    use super::FeatureName;

    pub const ENSEMBLE_BIN_PROBABILITY: FeatureName =
        FeatureName::from_static("domain.weather.ensemble_bin_probability");
    pub const ENSEMBLE_SPREAD: FeatureName =
        FeatureName::from_static("domain.weather.ensemble_spread");
    pub const OBSERVED_HIGH_HEADROOM: FeatureName =
        FeatureName::from_static("domain.weather.observed_high_headroom");
    pub const NOAA_RESOLUTION_BASIS_RISK: FeatureName =
        FeatureName::from_static("domain.weather.noaa_resolution_basis_risk");
}
