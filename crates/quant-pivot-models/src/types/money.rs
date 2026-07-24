//! Decimal-based monetary newtypes preventing accidental mixing.
//!
//! # Design rationale
//!
//! Every monetary quantity is a distinct newtype wrapping `rust_decimal::Decimal`.
//! Cross-type arithmetic is restricted to semantically valid operations:
//!
//! - `Shares × Price → Usd`
//! - `Usd / Price → Shares`
//! - Scalar multiplication (`Usd * Decimal`, etc.)
//!
//! `f64` is **never** used for money. The `From<f64>` trait is intentionally
//! **not** implemented.
//!
//! # `SeaORM` persistence
//!
//! All monetary newtypes bind as `Value::Decimal` and persist into native
//! Postgres `NUMERIC(precision, scale)` columns. The mapping is lossless:
//! Postgres `NUMERIC` covers the full range of `rust_decimal::Decimal`, so a
//! round-trip never truncates. Each newtype declares its DDL precision via the
//! `PRECISION` associated constant, consumed by the schema column builders.

use std::{
    fmt::{self, Display, Formatter, Result as FmtResult},
    iter::Sum,
    ops::{Add, AddAssign, Div, Mul, Neg, Sub, SubAssign},
};

use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue, ColIdx, DbErr, DeriveValueType, IntoActiveValue, QueryResult, TryGetError,
    TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, de::Error as SerdeDeError};
use thiserror::Error;

macro_rules! decimal_newtype {
    ($(#[$meta:meta])* $name:ident, precision = ($precision:expr, $scale:expr)) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            DeriveValueType,
        )]
        #[serde(transparent)]
        pub struct $name(Decimal);

        impl $name {
            pub const ZERO: Self = Self(Decimal::ZERO);
            pub const ONE: Self = Self(Decimal::ONE);

            /// Postgres `NUMERIC(precision, scale)` declaration for this type.
            ///
            /// Consumed by the schema column builders so every persisted column
            /// of this newtype uses one canonical precision.
            pub const PRECISION: (u32, u32) = ($precision, $scale);

            #[must_use]
            #[inline]
            pub const fn new(value: Decimal) -> Self { Self(value) }

            #[must_use]
            #[inline]
            pub const fn inner(self) -> Decimal { self.0 }

            #[must_use]
            #[inline]
            pub const fn is_zero(&self) -> bool { self.0.is_zero() }

            #[must_use]
            #[inline]
            pub fn is_positive(&self) -> bool { self.0 > Decimal::ZERO }

            #[must_use]
            #[inline]
            pub fn is_negative(&self) -> bool { self.0 < Decimal::ZERO }

            #[must_use]
            #[inline]
            pub fn abs(self) -> Self { Self(self.0.abs()) }

            #[must_use]
            #[inline]
            pub fn min(self, other: Self) -> Self { Self(self.0.min(other.0)) }

            #[must_use]
            #[inline]
            pub fn max(self, other: Self) -> Self { Self(self.0.max(other.0)) }

            #[must_use]
            #[inline]
            pub fn round_dp(self, dp: u32) -> Self { Self(self.0.round_dp(dp)) }

            #[must_use]
            #[inline]
            pub fn floor(self) -> Self { Self(self.0.floor()) }

            #[must_use]
            #[inline]
            pub fn ceil(self) -> Self { Self(self.0.ceil()) }
        }

        impl Default for $name {
            fn default() -> Self { Self::ZERO }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<Decimal> for $name {
            fn from(d: Decimal) -> Self { Self(d) }
        }

        impl From<$name> for Decimal {
            fn from(v: $name) -> Decimal { v.0 }
        }

        // ── Same-type arithmetic ────────────────────────────────────

        impl Add for $name {
            type Output = Self;
            #[inline]
            fn add(self, rhs: Self) -> Self { Self(self.0 + rhs.0) }
        }

        impl AddAssign for $name {
            #[inline]
            fn add_assign(&mut self, rhs: Self) { self.0 += rhs.0; }
        }

        impl Sub for $name {
            type Output = Self;
            #[inline]
            fn sub(self, rhs: Self) -> Self { Self(self.0 - rhs.0) }
        }

        impl SubAssign for $name {
            #[inline]
            fn sub_assign(&mut self, rhs: Self) { self.0 -= rhs.0; }
        }

        impl Neg for $name {
            type Output = Self;
            #[inline]
            fn neg(self) -> Self { Self(-self.0) }
        }

        impl Sum for $name {
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                iter.fold(Self::ZERO, Add::add)
            }
        }
    };
}

decimal_newtype!(
    /// USD-denominated monetary amount (`USDC.e` on Polygon).
    Usd,
    precision = (28, 8)
);

decimal_newtype!(
    /// Price per share in a prediction market. Range \[0, 1\].
    Price,
    precision = (20, 18)
);

decimal_newtype!(
    /// Number of shares (condition tokens).
    Shares,
    precision = (38, 18)
);

decimal_newtype!(
    /// Basis points (1 bps = 0.01%).
    Bps,
    precision = (10, 4)
);

decimal_newtype!(
    /// Statistical probability, confidence, or model weight stored losslessly.
    Probability,
    precision = (20, 18)
);

/// Redemption value of one resolved outcome token, in collateral units.
///
/// Unlike the historical unconstrained [`Probability`] and [`Price`] wrappers,
/// this type validates every untrusted boundary. A corrupt database value or
/// wire payload outside the closed interval `0..=1` is rejected rather than
/// becoming a training label. Split resolutions such as `0.5` are preserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PayoutRatio(Decimal);

impl PayoutRatio {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    /// Canonical `PostgreSQL` `NUMERIC` precision for payout ratios.
    pub const PRECISION: (u32, u32) = (20, 18);

    /// Validate and construct a payout ratio.
    pub fn try_new(value: Decimal) -> Result<Self, PayoutRatioError> {
        if (Decimal::ZERO..=Decimal::ONE).contains(&value) {
            let normalized = value.normalize();
            let scale = normalized.scale();
            if scale <= Self::PRECISION.1 {
                return Ok(Self(normalized));
            }
            return Err(PayoutRatioError::UnsupportedScale {
                value,
                scale,
                maximum_scale: Self::PRECISION.1,
            });
        }
        Err(PayoutRatioError::OutOfRange { value })
    }

    #[must_use]
    pub const fn inner(self) -> Decimal {
        self.0
    }

    /// Complementary payout for the other token in a binary condition.
    #[must_use]
    pub fn complement(self) -> Self {
        Self(Decimal::ONE - self.0)
    }
}

impl Display for PayoutRatio {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for PayoutRatio {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = <Decimal as Deserialize>::deserialize(deserializer)?;
        Self::try_new(value).map_err(SerdeDeError::custom)
    }
}

impl TryFrom<Decimal> for PayoutRatio {
    type Error = PayoutRatioError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<PayoutRatio> for Decimal {
    fn from(value: PayoutRatio) -> Self {
        value.inner()
    }
}

impl From<PayoutRatio> for Value {
    fn from(value: PayoutRatio) -> Self {
        Self::Decimal(Some(value.inner()))
    }
}

impl From<&PayoutRatio> for Value {
    fn from(value: &PayoutRatio) -> Self {
        Self::Decimal(Some(value.inner()))
    }
}

impl TryGetable for PayoutRatio {
    fn try_get_by<I: ColIdx>(result: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let value = <Decimal as TryGetable>::try_get_by(result, index)?;
        Self::try_new(value).map_err(|error| TryGetError::DbErr(DbErr::Type(error.to_string())))
    }
}

impl ValueType for PayoutRatio {
    fn try_from(value: Value) -> Result<Self, ValueTypeErr> {
        match value {
            Value::Decimal(Some(value)) => Self::try_new(value).map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(PayoutRatio).to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::Decimal
    }

    fn column_type() -> ColumnType {
        ColumnType::Decimal(Some(Self::PRECISION))
    }
}

impl Nullable for PayoutRatio {
    fn null() -> Value {
        Value::Decimal(None)
    }
}

impl IntoActiveValue<Self> for PayoutRatio {
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

/// Validation failure for a payout ratio read from an external boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PayoutRatioError {
    #[error("payout ratio must be within the closed interval [0, 1], got {value}")]
    OutOfRange { value: Decimal },
    #[error(
        "payout ratio {value} has scale {scale}, exceeding the canonical maximum {maximum_scale}"
    )]
    UnsupportedScale {
        value: Decimal,
        scale: u32,
        maximum_scale: u32,
    },
}

// ── Cross-type arithmetic ───────────────────────────────────────────────

impl Mul<Price> for Shares {
    type Output = Usd;
    #[inline]
    fn mul(self, rhs: Price) -> Usd {
        Usd::new(self.inner() * rhs.inner())
    }
}

impl Mul<Shares> for Price {
    type Output = Usd;
    #[inline]
    fn mul(self, rhs: Shares) -> Usd {
        Usd::new(self.inner() * rhs.inner())
    }
}

impl Div<Price> for Usd {
    type Output = Shares;
    #[inline]
    fn div(self, rhs: Price) -> Shares {
        Shares::new(self.inner() / rhs.inner())
    }
}

// ── Scalar arithmetic ───────────────────────────────────────────────────

impl Mul<Decimal> for Usd {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl Div<Decimal> for Usd {
    type Output = Self;
    fn div(self, rhs: Decimal) -> Self {
        Self::new(self.inner() / rhs)
    }
}

impl Mul<Decimal> for Shares {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl Div<Decimal> for Shares {
    type Output = Self;
    fn div(self, rhs: Decimal) -> Self {
        Self::new(self.inner() / rhs)
    }
}

impl Mul<Decimal> for Price {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl Mul<Decimal> for Bps {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

// ── Bps utility ─────────────────────────────────────────────────────────

impl Bps {
    /// Convert basis points to a decimal fraction (e.g. 200 bps → 0.02).
    #[must_use]
    #[inline]
    pub fn to_fraction(self) -> Decimal {
        self.inner() / Decimal::from(10_000)
    }

    /// Relative spread in basis points: `numerator / denominator × 10_000`.
    ///
    /// Returns `None` when `denominator` is zero.
    #[must_use]
    pub fn relative(numerator: Decimal, denominator: Decimal) -> Option<Self> {
        if denominator.is_zero() {
            return None;
        }
        Some(Self::new(numerator / denominator * Decimal::from(10_000)))
    }

    /// Compute basis-point spread: `(actual - expected) / expected × 10000`.
    ///
    /// Returns `None` when `expected` is zero (division undefined).
    #[must_use]
    pub fn spread(actual: Price, expected: Price) -> Option<Self> {
        if expected.is_zero() {
            return None;
        }
        let diff = actual.inner() - expected.inner();
        Some(Self::new(diff / expected.inner() * Decimal::from(10_000)))
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use sea_orm::sea_query::{Value, ValueType};

    use super::*;

    #[test]
    fn payout_ratio_accepts_closed_interval_and_split_payout() {
        for value in [dec!(0), dec!(0.5), dec!(1)] {
            let ratio = PayoutRatio::try_new(value).expect("valid payout ratio");
            assert_eq!(ratio.inner(), value);
        }
    }

    #[test]
    fn payout_ratio_rejects_values_outside_closed_interval() {
        for value in [dec!(-0.000000000000000001), dec!(1.000000000000000001)] {
            assert_eq!(
                PayoutRatio::try_new(value),
                Err(PayoutRatioError::OutOfRange { value })
            );
        }
    }

    #[test]
    fn payout_ratio_normalizes_and_rejects_unpersistable_scale() {
        assert_eq!(
            PayoutRatio::try_new(dec!(0.5000000000000000000))
                .expect("canonical half")
                .inner(),
            dec!(0.5)
        );
        let value = dec!(0.1234567890123456789);
        assert_eq!(
            PayoutRatio::try_new(value),
            Err(PayoutRatioError::UnsupportedScale {
                value,
                scale: 19,
                maximum_scale: 18,
            })
        );
    }

    #[test]
    fn payout_ratio_serde_is_validated_decimal_string() {
        let split = PayoutRatio::try_new(dec!(0.5)).expect("valid split payout");
        assert_eq!(
            serde_json::to_string(&split).expect("serialize payout ratio"),
            r#""0.5""#
        );
        assert_eq!(
            serde_json::from_str::<PayoutRatio>(r#""0.5""#).expect("deserialize payout ratio"),
            split
        );
        assert!(serde_json::from_str::<PayoutRatio>(r#""-0.1""#).is_err());
        assert!(serde_json::from_str::<PayoutRatio>(r#""1.1""#).is_err());
    }

    #[test]
    fn payout_ratio_seaorm_roundtrip_and_read_validation() {
        let split = PayoutRatio::try_new(dec!(0.5)).expect("valid split payout");
        let value: Value = split.into();
        assert_eq!(
            <PayoutRatio as ValueType>::try_from(value).expect("roundtrip payout ratio"),
            split
        );
        assert!(<PayoutRatio as ValueType>::try_from(Value::Decimal(Some(dec!(-0.1)))).is_err());
        assert!(<PayoutRatio as ValueType>::try_from(Value::Decimal(Some(dec!(1.1)))).is_err());
        assert!(<PayoutRatio as ValueType>::try_from(Value::Decimal(None)).is_err());
    }

    #[test]
    fn shares_times_price_equals_usd() {
        let shares = Shares::new(dec!(100));
        let price = Price::new(dec!(0.65));
        let usd: Usd = shares * price;
        assert_eq!(usd.inner(), dec!(65.00));
    }

    #[test]
    fn usd_div_price_equals_shares() {
        let usd = Usd::new(dec!(100));
        let price = Price::new(dec!(0.50));
        let shares: Shares = usd / price;
        assert_eq!(shares.inner(), dec!(200));
    }

    #[test]
    fn commutative_mul() {
        let shares = Shares::new(dec!(50));
        let price = Price::new(dec!(0.80));
        assert_eq!(shares * price, price * shares);
    }

    #[test]
    fn bps_spread_calculation() {
        let actual = Price::new(dec!(0.92));
        let expected = Price::new(dec!(0.90));
        let bps = Bps::spread(actual, expected).unwrap();
        assert_eq!(bps.round_dp(2).inner(), dec!(222.22));
    }

    #[test]
    fn bps_spread_zero_expected_returns_none() {
        assert!(Bps::spread(Price::new(dec!(0.5)), Price::ZERO).is_none());
    }

    #[test]
    fn bps_to_fraction() {
        let bps = Bps::new(dec!(200));
        assert_eq!(bps.to_fraction(), dec!(0.02));
    }

    #[test]
    fn sum_iterator() {
        let values = vec![Usd::new(dec!(10)), Usd::new(dec!(20)), Usd::new(dec!(30))];
        let total: Usd = values.into_iter().sum();
        assert_eq!(total.inner(), dec!(60));
    }

    #[test]
    fn serde_roundtrip() {
        let price = Price::new(dec!(0.123456789));
        let json = serde_json::to_string(&price).unwrap();
        let back: Price = serde_json::from_str(&json).unwrap();
        assert_eq!(price, back);
    }

    #[test]
    fn no_f64_precision_loss() {
        let a = Usd::new(dec!(0.1));
        let b = Usd::new(dec!(0.2));
        assert_eq!((a + b).inner(), dec!(0.3));
    }

    #[test]
    fn neg_works() {
        let usd = Usd::new(dec!(42));
        assert_eq!((-usd).inner(), dec!(-42));
    }

    #[test]
    fn scalar_mul_and_div() {
        let usd = Usd::new(dec!(100));
        assert_eq!((usd * dec!(0.25)).inner(), dec!(25));
        assert_eq!((usd / dec!(4)).inner(), dec!(25));
    }
}
