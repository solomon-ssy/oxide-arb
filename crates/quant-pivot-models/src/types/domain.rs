//! External-vertical (domain) vocabulary newtypes (Phase 11.2.2).
//!
//! Covers [`ResolverVersion`], [`CryptoAsset`], [`CryptoQuote`], [`BinanceSymbol`],
//! [`ChainlinkFeedKey`], and the canonical [`DomainInstrumentKey`] constructors.
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

    /// Canonical key for a Chainlink aggregator feed: `CHAINLINK:{feed}`.
    #[must_use]
    pub fn chainlink_feed(feed: &ChainlinkFeedKey) -> Self {
        Self::new(format!("CHAINLINK:{feed}"))
    }

    /// The source this key belongs to, derived from the canonical prefix.
    #[must_use]
    pub fn source_id(&self) -> Option<DomainSourceId> {
        match self.as_str().split_once(':')?.0 {
            "BINANCE" => Some(DomainSourceId::binance()),
            "CHAINLINK" => Some(DomainSourceId::chainlink()),
            _ => None,
        }
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

        let feed =
            DomainInstrumentKey::chainlink_feed(&ChainlinkFeedKey::parse("BTC-USD").expect("feed"));
        assert_eq!(feed.as_str(), "CHAINLINK:BTC-USD");
        assert_eq!(feed.source_id(), Some(DomainSourceId::chainlink()));
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
