//! `ClickHouse` storage boundary types.
//!
//! `ClickHouse` `Decimal*` values are serialized by the Rust client as scaled
//! signed integers. These wrappers keep that storage detail out of domain code.

use chrono::{Datelike, NaiveDate};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, Visitor},
};
use std::fmt::{self, Display, Formatter};

use crate::types::{Bps, Price, Probability, SchemaVersion, Shares, Usd};

const PRICE_SCALE: u32 = 8;
const MONEY_SCALE: u32 = 18;
const BPS_SCALE: u32 = 4;
const UNIX_EPOCH_FROM_CE_DAYS: i32 = 719_163;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChPrice(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChProbability(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChFactor(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChBps(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChUsd(i128);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChShares(i128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChDecimal64(i64);

/// Signed days since 1970-01-01 for historical calendar dates.
///
/// `ClickHouse` `Date` starts in 1970 and the Rust client's `Date32` serializer
/// starts in 1900, while GISTEMP begins in 1880. The explicit `Int32` storage
/// contract is lossless across chrono's supported `NaiveDate` range and keeps
/// calendar conversion at this typed boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChEpochDay(i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChSchemaVersion(pub u32);

impl ChSchemaVersion {
    /// The first valid `ClickHouse` fact-row schema version.
    pub const FIRST: Self = Self(1);

    /// Fallible conversion from a domain [`SchemaVersion`], rejecting non-positive
    /// values and any `i32` that does not fit in `u32`.
    pub fn try_from_schema_version(value: SchemaVersion) -> Result<Self, CanonicalDigestError> {
        let raw = value.get();
        if raw < 1 {
            return Err(CanonicalDigestError::InvalidSchemaVersion { value: raw });
        }
        u32::try_from(raw)
            .map(Self)
            .map_err(|_| CanonicalDigestError::InvalidSchemaVersion { value: raw })
    }
}

impl ChEpochDay {
    #[must_use]
    pub fn to_naive_date(self) -> Option<NaiveDate> {
        self.0
            .checked_add(UNIX_EPOCH_FROM_CE_DAYS)
            .and_then(NaiveDate::from_num_days_from_ce_opt)
    }
}

impl From<NaiveDate> for ChEpochDay {
    fn from(value: NaiveDate) -> Self {
        Self(value.num_days_from_ce() - UNIX_EPOCH_FROM_CE_DAYS)
    }
}

impl From<SchemaVersion> for ChSchemaVersion {
    /// Infallible conversion for validated schema versions at the `ClickHouse` write boundary.
    ///
    /// Prefer [`Self::try_from_schema_version`] for untrusted input.
    fn from(value: SchemaVersion) -> Self {
        Self::try_from_schema_version(value).unwrap_or(Self::FIRST)
    }
}

impl ChPrice {
    #[must_use]
    pub fn to_price(self) -> Price {
        Price::new(decimal_from_i64(self.0, PRICE_SCALE))
    }
}

impl From<Price> for ChPrice {
    fn from(value: Price) -> Self {
        Self(decimal_to_i64(value.inner(), PRICE_SCALE))
    }
}

impl ChProbability {
    #[must_use]
    pub fn to_probability(self) -> Probability {
        Probability::new(decimal_from_i64(self.0, PRICE_SCALE))
    }
}

impl From<Decimal> for ChProbability {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i64(value, PRICE_SCALE))
    }
}

impl From<Probability> for ChProbability {
    fn from(value: Probability) -> Self {
        Self::from(value.inner())
    }
}

impl ChFactor {
    #[must_use]
    pub fn to_decimal(self) -> Decimal {
        decimal_from_i64(self.0, PRICE_SCALE)
    }
}

impl From<Decimal> for ChFactor {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i64(value, PRICE_SCALE))
    }
}

impl ChBps {
    #[must_use]
    pub fn to_bps(self) -> Bps {
        Bps::new(decimal_from_i64(self.0, BPS_SCALE))
    }
}

impl From<Bps> for ChBps {
    fn from(value: Bps) -> Self {
        Self(decimal_to_i64(value.inner(), BPS_SCALE))
    }
}

impl From<Decimal> for ChBps {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i64(value, BPS_SCALE))
    }
}

impl ChUsd {
    #[must_use]
    pub fn to_usd(self) -> Usd {
        Usd::new(decimal_from_i128(self.0, MONEY_SCALE))
    }
}

impl From<Usd> for ChUsd {
    fn from(value: Usd) -> Self {
        Self(decimal_to_i128(value.inner(), MONEY_SCALE))
    }
}

impl From<Decimal> for ChUsd {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i128(value, MONEY_SCALE))
    }
}

impl ChShares {
    #[must_use]
    pub fn to_shares(self) -> Shares {
        Shares::new(decimal_from_i128(self.0, MONEY_SCALE))
    }
}

impl From<Shares> for ChShares {
    fn from(value: Shares) -> Self {
        Self(decimal_to_i128(value.inner(), MONEY_SCALE))
    }
}

impl From<Decimal> for ChShares {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i128(value, MONEY_SCALE))
    }
}

fn serialize_scaled_i128<S: Serializer>(value: i128, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        serializer.serialize_str(&value.to_string())
    } else {
        serializer.serialize_i128(value)
    }
}

struct ScaledI128Visitor;

impl Visitor<'_> for ScaledI128Visitor {
    type Value = i128;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("an exact signed 128-bit integer encoded as a decimal string")
    }

    fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
        value.parse().map_err(E::custom)
    }

    fn visit_i64<E: DeError>(self, value: i64) -> Result<Self::Value, E> {
        Ok(i128::from(value))
    }

    fn visit_u64<E: DeError>(self, value: u64) -> Result<Self::Value, E> {
        Ok(i128::from(value))
    }

    fn visit_i128<E: DeError>(self, value: i128) -> Result<Self::Value, E> {
        Ok(value)
    }

    fn visit_u128<E: DeError>(self, value: u128) -> Result<Self::Value, E> {
        i128::try_from(value).map_err(E::custom)
    }
}

fn deserialize_scaled_i128<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
    if !deserializer.is_human_readable() {
        return i128::deserialize(deserializer);
    }

    deserializer.deserialize_any(ScaledI128Visitor)
}

impl Serialize for ChUsd {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_scaled_i128(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ChUsd {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_scaled_i128(deserializer).map(Self)
    }
}

impl Serialize for ChShares {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_scaled_i128(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ChShares {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_scaled_i128(deserializer).map(Self)
    }
}

impl ChDecimal64 {
    #[must_use]
    pub fn to_decimal(self) -> Decimal {
        decimal_from_i64(self.0, PRICE_SCALE)
    }
}

impl From<Decimal> for ChDecimal64 {
    fn from(value: Decimal) -> Self {
        Self(decimal_to_i64(value, PRICE_SCALE))
    }
}

impl Display for ChPrice {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_price())
    }
}

impl Display for ChUsd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_usd())
    }
}

impl Display for ChShares {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_shares())
    }
}

fn decimal_to_i64(mut value: Decimal, scale: u32) -> i64 {
    value.rescale(scale);
    i64::try_from(value.mantissa()).unwrap_or_else(|_| {
        if value.is_sign_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn decimal_to_i128(mut value: Decimal, scale: u32) -> i128 {
    value.rescale(scale);
    value.mantissa()
}

fn decimal_from_i64(value: i64, scale: u32) -> Decimal {
    decimal_from_i128(i128::from(value), scale)
}

/// Trailing zeros are stripped so projections serialize the canonical form
/// (`"250"`, not `"250.0000"`); the numeric value is unchanged.
fn decimal_from_i128(value: i128, scale: u32) -> Decimal {
    Decimal::from_i128_with_scale(value, scale).normalize()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::{ChEpochDay, ChPrice, ChSchemaVersion, ChShares, ChUsd};
    use crate::types::{Price, SchemaVersion, Shares, Usd};
    use quant_pivot_error::hashing::CanonicalDigestError;
    use rust_decimal_macros::dec;

    #[test]
    fn schema_version_roundtrips_positive_values() {
        let version = SchemaVersion::new(3);
        assert_eq!(ChSchemaVersion::from(version), ChSchemaVersion(3));
        assert_eq!(
            ChSchemaVersion::try_from_schema_version(version).expect("valid"),
            ChSchemaVersion(3)
        );
    }

    #[test]
    fn schema_version_rejects_non_positive() {
        let err =
            ChSchemaVersion::try_from_schema_version(SchemaVersion::new(0)).expect_err("zero");
        assert_eq!(err, CanonicalDigestError::InvalidSchemaVersion { value: 0 });
    }

    #[test]
    fn schema_version_rejects_i32_min() {
        assert!(ChSchemaVersion::try_from_schema_version(SchemaVersion::new(i32::MIN)).is_err());
    }

    #[test]
    fn epoch_day_roundtrips_pre_1900_and_modern_dates() {
        for date in [
            NaiveDate::from_ymd_opt(1880, 1, 1).expect("historical date"),
            NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
            NaiveDate::from_ymd_opt(2026, 7, 18).expect("modern date"),
        ] {
            assert_eq!(ChEpochDay::from(date).to_naive_date(), Some(date));
        }
    }

    #[test]
    fn price_roundtrips_scaled_decimal64() {
        let price = Price::new(dec!(0.12345678));
        assert_eq!(ChPrice::from(price).to_price(), price);
    }

    #[test]
    fn usd_roundtrips_scaled_decimal128() {
        let usd = Usd::new(dec!(123.456789123456789123));
        assert_eq!(ChUsd::from(usd).to_usd(), usd);
    }

    #[test]
    fn shares_roundtrips_scaled_decimal128() {
        let shares = Shares::new(dec!(1000.000000000000000001));
        assert_eq!(ChShares::from(shares).to_shares(), shares);
    }

    #[test]
    fn decimal128_json_uses_lossless_strings() {
        let usd = ChUsd::from(Usd::new(dec!(123.456789123456789123)));
        let shares = ChShares::from(Shares::new(dec!(1000.000000000000000001)));
        let encoded = serde_json::to_value((usd, shares)).expect("serialize exact decimals");
        assert_eq!(
            encoded,
            serde_json::json!(["123456789123456789123", "1000000000000000000001"])
        );
        let decoded: (ChUsd, ChShares) =
            serde_json::from_value(encoded).expect("deserialize exact decimals");
        assert_eq!(decoded, (usd, shares));
    }
}
