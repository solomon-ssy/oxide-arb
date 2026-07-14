//! External-vertical (domain) vocabulary newtypes (Phase 11.2.2).
//!
//! Covers [`ResolverVersion`], [`CryptoAsset`], [`CryptoQuote`], [`BinanceSymbol`],
//! [`ChainlinkFeedKey`], station identifiers, temperature values, and the
//! canonical [`DomainInstrumentKey`] constructors.
//!
//! Assets are deliberately **not** a hard-coded enum: adding a listed asset is
//! a resolver-ruleset data change (alias table + symbol + feed bindings) plus a
//! [`ResolverVersion`] bump — never a type change. Every string newtype here is
//! validated on construction so a bare string can never masquerade as a ticker,
//! venue symbol, or feed key.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
    sync::Arc,
};

use rust_decimal::{Decimal, RoundingStrategy};
use schemars::JsonSchema;
use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{
    enums::domain::KlineInterval,
    types::{DomainInstrumentKey, DomainSourceId},
};

/// A malformed domain vocabulary value (ticker / symbol / feed key / version).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainVocabularyError {
    /// The ticker/symbol/key failed structural validation.
    #[error("invalid {kind} `{value}`: {detail}")]
    Invalid {
        /// Which vocabulary type rejected the value.
        kind: &'static str,
        /// The offending input.
        value: String,
        /// Why it was rejected.
        detail: &'static str,
    },
    /// A resolver version outside the valid `>= 1` range.
    #[error("invalid resolver version {value}: must be >= 1")]
    InvalidResolverVersion {
        /// The offending version.
        value: i32,
    },
}

// ── ResolverVersion ─────────────────────────────────────────────────────────

/// Monotonic version of the frozen linkage resolver ruleset.
///
/// Every deterministic resolver output records the ruleset version that
/// produced it, so historical linkages replay bit-identically even after the
/// alias/symbol/feed tables grow. Bumped whenever the ruleset data changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ResolverVersion(i32);

impl ResolverVersion {
    /// The first valid resolver-ruleset version.
    pub const FIRST: Self = Self(1);

    /// Wrap a raw version without validation (compile-time constants only).
    #[must_use]
    #[inline]
    pub const fn new(version: i32) -> Self {
        Self(version)
    }

    /// Validate (`>= 1`) and wrap a raw version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainVocabularyError::InvalidResolverVersion`] when `version < 1`.
    pub const fn try_new(version: i32) -> Result<Self, DomainVocabularyError> {
        if version >= 1 {
            Ok(Self(version))
        } else {
            Err(DomainVocabularyError::InvalidResolverVersion { value: version })
        }
    }

    /// The raw integer version.
    #[must_use]
    #[inline]
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl Display for ResolverVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for ResolverVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = i32::deserialize(deserializer)?;
        Self::try_new(raw).map_err(serde::de::Error::custom)
    }
}

impl IntoActiveValue<Self> for ResolverVersion {
    #[inline]
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

impl From<ResolverVersion> for Value {
    #[inline]
    fn from(v: ResolverVersion) -> Self {
        Self::Int(Some(v.get()))
    }
}

impl From<&ResolverVersion> for Value {
    #[inline]
    fn from(v: &ResolverVersion) -> Self {
        Self::Int(Some(v.get()))
    }
}

impl TryGetable for ResolverVersion {
    fn try_get_by<I: ColIdx>(res: &sea_orm::QueryResult, index: I) -> Result<Self, TryGetError> {
        let raw: i32 = <i32 as TryGetable>::try_get_by(res, index)?;
        Self::try_new(raw).map_err(|e| TryGetError::DbErr(sea_orm::DbErr::Type(e.to_string())))
    }
}

impl ValueType for ResolverVersion {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::Int(Some(raw)) => Self::try_new(raw).map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(ResolverVersion).to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::Int
    }

    fn column_type() -> ColumnType {
        ColumnType::Integer
    }
}

impl Nullable for ResolverVersion {
    fn null() -> Value {
        Value::Int(None)
    }
}

// ── Validated uppercase token newtypes ──────────────────────────────────────

/// Validate an uppercase `A-Z0-9` token (optionally allowing `-`), bounded in
/// length, starting with a letter.
fn validate_token(
    kind: &'static str,
    value: &str,
    max_len: usize,
    allow_dash: bool,
) -> Result<(), DomainVocabularyError> {
    let invalid = |detail: &'static str| DomainVocabularyError::Invalid {
        kind,
        value: value.to_owned(),
        detail,
    };
    if value.is_empty() || value.len() > max_len {
        return Err(invalid("length out of range"));
    }
    if !value.as_bytes()[0].is_ascii_uppercase() {
        return Err(invalid("must start with an uppercase letter"));
    }
    let ok = value
        .bytes()
        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || (allow_dash && b == b'-'));
    if ok {
        Ok(())
    } else {
        Err(invalid("must be uppercase A-Z / 0-9"))
    }
}

macro_rules! validated_token {
    (
        $(#[$meta:meta])*
        $name:ident, kind = $kind:literal, max_len = $max_len:expr, allow_dash = $allow_dash:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema)]
        #[schemars(transparent)]
        pub struct $name(#[schemars(with = "String")] Arc<str>);

        impl $name {
            /// Validate and wrap a raw token.
            ///
            /// # Errors
            ///
            /// Returns [`DomainVocabularyError::Invalid`] when the value fails
            /// structural validation.
            pub fn parse(value: impl AsRef<str>) -> Result<Self, DomainVocabularyError> {
                let value = value.as_ref();
                validate_token($kind, value, $max_len, $allow_dash)?;
                Ok(Self(Arc::from(value)))
            }

            /// The validated token string.
            #[must_use]
            #[inline]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = DomainVocabularyError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(deserializer)?;
                Self::parse(raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

validated_token! {
    /// A crypto base-asset ticker (e.g. `BTC`, `ETH`, `DOGE`).
    ///
    /// Not an enum: the asset set is resolver-ruleset data, so listing a
    /// new asset never requires a type change (see module docs).
    CryptoAsset, kind = "crypto asset", max_len = 10, allow_dash = false
}

validated_token! {
    /// A four-letter ICAO airport/weather station identifier (e.g. `KJFK`).
    IcaoStation, kind = "ICAO station", max_len = 4, allow_dash = false
}

/// Temperature unit exposed by a Polymarket weather contract.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
}

/// Temperature stored canonically in degrees Celsius.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct TemperatureCelsius(#[schemars(with = "String")] Decimal);

impl TemperatureCelsius {
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }

    /// Convert to the market unit without applying the contract's whole-degree proxy.
    #[must_use]
    pub fn in_unit(self, unit: TemperatureUnit) -> Decimal {
        match unit {
            TemperatureUnit::Celsius => self.0,
            TemperatureUnit::Fahrenheit => {
                self.0 * Decimal::new(9, 0) / Decimal::new(5, 0) + Decimal::new(32, 0)
            }
        }
    }

    /// Apply the frozen whole-degree proxy used by weather event predicates.
    #[must_use]
    pub fn whole_degrees(self, unit: TemperatureUnit) -> Decimal {
        self.in_unit(unit)
            .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
    }
}

/// Inclusive integer-temperature outcome band in the market's displayed unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TemperatureBand {
    #[schemars(with = "Option<String>")]
    pub lower_inclusive: Option<Decimal>,
    #[schemars(with = "Option<String>")]
    pub upper_inclusive: Option<Decimal>,
}

impl TemperatureBand {
    /// A valid market band has at least one bound and, when both are present,
    /// the lower bound does not exceed the upper bound.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match (self.lower_inclusive, self.upper_inclusive) {
            (None, None) => false,
            (Some(lower), Some(upper)) => lower <= upper,
            (Some(_), None) | (None, Some(_)) => true,
        }
    }

    #[must_use]
    pub fn contains(&self, value: Decimal) -> bool {
        self.is_valid()
            && self.lower_inclusive.is_none_or(|lower| value >= lower)
            && self.upper_inclusive.is_none_or(|upper| value <= upper)
    }
}

validated_token! {
    /// A crypto quote currency (e.g. `USD`, `USDT`).
    CryptoQuote, kind = "crypto quote", max_len = 10, allow_dash = false
}

validated_token! {
    /// A Binance spot symbol (e.g. `BTCUSDT`).
    BinanceSymbol, kind = "binance symbol", max_len = 20, allow_dash = false
}

validated_token! {
    /// A Chainlink feed key as configured in deploy config (e.g. `BTC-USD`).
    ChainlinkFeedKey, kind = "chainlink feed key", max_len = 20, allow_dash = true
}

// ── DomainInstrumentKey canonical constructors ──────────────────────────────

impl DomainInstrumentKey {
    /// Canonical key for a Binance kline series: `BINANCE:{symbol}:{interval}`.
    #[must_use]
    pub fn binance_kline(symbol: &BinanceSymbol, interval: KlineInterval) -> Self {
        Self::new(format!("BINANCE:{symbol}:{}", interval.as_str()))
    }

    /// Canonical key for a Binance aggregate-trade stream.
    #[must_use]
    pub fn binance_agg_trade(symbol: &BinanceSymbol) -> Self {
        Self::new(format!("BINANCE_AGG_TRADE:{symbol}"))
    }

    /// Canonical key for a Chainlink Data Streams feed.
    #[must_use]
    pub fn chainlink_data_streams(feed: &ChainlinkFeedKey) -> Self {
        Self::new(format!("CHAINLINK_DATA_STREAMS:{feed}"))
    }

    /// Canonical key for an airport observation stream.
    #[must_use]
    pub fn aviation_weather(station: &IcaoStation) -> Self {
        Self::new(format!("AVIATION_WEATHER:{station}"))
    }

    /// Canonical key for an airport's `GHCNh` history.
    #[must_use]
    pub fn ghcnh(station: &IcaoStation) -> Self {
        Self::new(format!("GHCNH:{station}"))
    }

    /// Canonical key for an airport-bound GEFS forecast series.
    #[must_use]
    pub fn gefs(station: &IcaoStation) -> Self {
        Self::new(format!("GEFS:{station}"))
    }

    /// Independent resumable cursor for historical 00Z GEFS calibration runs.
    #[must_use]
    pub fn gefs_backfill(station: &IcaoStation) -> Self {
        Self::new(format!("GEFS_BACKFILL:{station}"))
    }

    /// The source this key belongs to, derived from the canonical prefix.
    #[must_use]
    pub fn source_id(&self) -> Option<DomainSourceId> {
        match self.as_str().split_once(':')?.0 {
            "BINANCE" => Some(DomainSourceId::binance()),
            "BINANCE_AGG_TRADE" => Some(DomainSourceId::binance_agg_trade()),
            "CHAINLINK_DATA_STREAMS" => Some(DomainSourceId::chainlink_data_streams()),
            "AVIATION_WEATHER" => Some(DomainSourceId::aviation_weather()),
            "GHCNH" => Some(DomainSourceId::ghcnh()),
            "GEFS" | "GEFS_BACKFILL" => Some(DomainSourceId::gefs()),
            _ => None,
        }
    }

    /// Decode a canonical aggregate-trade instrument without accepting aliases.
    #[must_use]
    pub fn as_binance_agg_trade_symbol(&self) -> Option<BinanceSymbol> {
        BinanceSymbol::parse(self.as_str().strip_prefix("BINANCE_AGG_TRADE:")?).ok()
    }

    /// Decode a canonical Data Streams instrument without accepting aliases.
    #[must_use]
    pub fn as_chainlink_feed(&self) -> Option<ChainlinkFeedKey> {
        ChainlinkFeedKey::parse(self.as_str().strip_prefix("CHAINLINK_DATA_STREAMS:")?).ok()
    }

    /// Decode a canonical `AviationWeather` instrument without accepting aliases.
    #[must_use]
    pub fn as_aviation_weather_station(&self) -> Option<IcaoStation> {
        IcaoStation::parse(self.as_str().strip_prefix("AVIATION_WEATHER:")?).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BinanceSymbol, ChainlinkFeedKey, CryptoAsset, DomainInstrumentKey, KlineInterval,
        ResolverVersion,
    };
    use crate::types::DomainSourceId;

    #[test]
    fn crypto_asset_validates_shape() {
        assert!(CryptoAsset::parse("BTC").is_ok());
        assert!(CryptoAsset::parse("btc").is_err());
        assert!(CryptoAsset::parse("").is_err());
        assert!(CryptoAsset::parse("1INCH").is_err(), "must start alpha");
        assert!(CryptoAsset::parse("TOO-LONG-TICKER").is_err());
    }

    #[test]
    fn instrument_keys_are_canonical() {
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        assert_eq!(key.as_str(), "BINANCE:BTCUSDT:1m");
        assert_eq!(key.source_id(), Some(DomainSourceId::binance()));

        let feed = DomainInstrumentKey::chainlink_data_streams(
            &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
        );
        assert_eq!(feed.as_str(), "CHAINLINK_DATA_STREAMS:BTC-USD");
        assert_eq!(
            feed.source_id(),
            Some(DomainSourceId::chainlink_data_streams())
        );
    }

    #[test]
    fn resolver_version_rejects_non_positive() {
        assert!(ResolverVersion::try_new(0).is_err());
        assert_eq!(ResolverVersion::FIRST.get(), 1);
        assert!(serde_json::from_str::<ResolverVersion>("0").is_err());
        assert_eq!(
            serde_json::from_str::<ResolverVersion>("3").expect("valid"),
            ResolverVersion::new(3)
        );
    }
}
