//! `ClickHouse` storage boundary types.
//!
//! `ClickHouse` `Decimal*` values are serialized by the Rust client as scaled
//! signed integers. These wrappers keep that storage detail out of domain code.

use crate::types::{Bps, Price, Probability, SchemaVersion, Shares, Usd};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

const PRICE_SCALE: u32 = 8;
const MONEY_SCALE: u32 = 18;
const BPS_SCALE: u32 = 4;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChUsd(i128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChShares(i128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChDecimal64(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChSchemaVersion(pub u32);

impl ChSchemaVersion {
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

impl From<SchemaVersion> for ChSchemaVersion {
    /// Infallible conversion for validated schema versions at the `ClickHouse` write boundary.
    ///
    /// Prefer [`Self::try_from_schema_version`] for untrusted input.
    fn from(value: SchemaVersion) -> Self {
        Self::try_from_schema_version(value).unwrap_or(Self(1))
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
    use super::{ChPrice, ChSchemaVersion, ChShares, ChUsd};
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
}
