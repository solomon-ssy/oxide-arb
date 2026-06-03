//! `ClickHouse` storage boundary types.
//!
//! `ClickHouse` `Decimal*` values are serialized by the Rust client as scaled
//! signed integers. These wrappers keep that storage detail out of domain code.

use crate::types::{Bps, Price, Probability, Shares, Usd};
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
    Decimal::from_i128_with_scale(i128::from(value), scale)
}

fn decimal_from_i128(value: i128, scale: u32) -> Decimal {
    Decimal::from_i128_with_scale(value, scale)
}

#[cfg(test)]
mod tests {
    use super::{ChPrice, ChShares, ChUsd};
    use crate::types::{Price, Shares, Usd};
    use rust_decimal_macros::dec;

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
