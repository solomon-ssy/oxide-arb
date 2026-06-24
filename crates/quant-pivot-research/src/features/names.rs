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
    /// Number of outcome tokens.
    pub const OUTCOME_COUNT: FeatureName = FeatureName::from_static("market.outcome_count");
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

/// Vertical (domain) features — one representative per [`DomainFamily`].
pub mod domain {
    use super::FeatureName;

    /// Sports vertical pre-match move proxy.
    pub const SPORTS_PRE_MATCH_MOVE: FeatureName =
        FeatureName::from_static("domain.sports.pre_match_move");
    /// Politics vertical poll momentum proxy.
    pub const POLITICS_POLL_MOMENTUM: FeatureName =
        FeatureName::from_static("domain.politics.poll_momentum");
    /// Crypto vertical underlying beta proxy.
    pub const CRYPTO_UNDERLYING_BETA: FeatureName =
        FeatureName::from_static("domain.crypto.underlying_beta");
    /// Weather vertical forecast revision proxy.
    pub const WEATHER_FORECAST_REVISION: FeatureName =
        FeatureName::from_static("domain.weather.forecast_revision");
    /// Geopolitics vertical news-shock decay proxy.
    pub const GEOPOLITICS_NEWS_SHOCK_DECAY: FeatureName =
        FeatureName::from_static("domain.geopolitics.news_shock_decay");
}
