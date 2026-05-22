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
//! All monetary newtypes bind as `Value::String(Decimal::to_string)` and
//! parse back in `TryGetable`. Columns are declared as `TEXT` in the DDL
//! layer so the round-trip is lossless. Binding as `Value::Decimal` risks
//! silent truncation to `REAL` / `f64` precision on some backends.

use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! decimal_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Decimal);

        impl $name {
            pub const ZERO: Self = Self(Decimal::ZERO);
            pub const ONE: Self = Self(Decimal::ONE);

            #[must_use]
            pub const fn new(value: Decimal) -> Self { Self(value) }

            #[must_use]
            pub const fn inner(self) -> Decimal { self.0 }

            #[must_use]
            pub const fn is_zero(&self) -> bool { self.0.is_zero() }

            #[must_use]
            pub fn is_positive(&self) -> bool { self.0 > Decimal::ZERO }

            #[must_use]
            pub fn is_negative(&self) -> bool { self.0 < Decimal::ZERO }

            #[must_use]
            pub fn abs(self) -> Self { Self(self.0.abs()) }

            #[must_use]
            pub fn min(self, other: Self) -> Self { Self(self.0.min(other.0)) }

            #[must_use]
            pub fn max(self, other: Self) -> Self { Self(self.0.max(other.0)) }

            #[must_use]
            pub fn round_dp(self, dp: u32) -> Self { Self(self.0.round_dp(dp)) }

            #[must_use]
            pub fn floor(self) -> Self { Self(self.0.floor()) }

            #[must_use]
            pub fn ceil(self) -> Self { Self(self.0.ceil()) }
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

        impl std::ops::Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self { Self(self.0 + rhs.0) }
        }

        impl std::ops::AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) { self.0 += rhs.0; }
        }

        impl std::ops::Sub for $name {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self { Self(self.0 - rhs.0) }
        }

        impl std::ops::SubAssign for $name {
            fn sub_assign(&mut self, rhs: Self) { self.0 -= rhs.0; }
        }

        impl std::ops::Neg for $name {
            type Output = Self;
            fn neg(self) -> Self { Self(-self.0) }
        }

        impl std::iter::Sum for $name {
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                iter.fold(Self::ZERO, std::ops::Add::add)
            }
        }

        // ── SeaORM bindings (TEXT column, lossless round-trip) ──────

        impl From<$name> for Value {
            #[inline]
            fn from(v: $name) -> Self {
                Value::String(Some(Box::new(v.0.to_string())))
            }
        }

        impl From<&$name> for Value {
            #[inline]
            fn from(v: &$name) -> Self {
                Value::String(Some(Box::new(v.0.to_string())))
            }
        }

        impl TryGetable for $name {
            fn try_get_by<I: ColIdx>(
                res: &sea_orm::QueryResult,
                index: I,
            ) -> Result<Self, TryGetError> {
                let raw: String =
                    <String as TryGetable>::try_get_by(res, index).map_err(|e| match e {
                        TryGetError::DbErr(sea_orm::DbErr::Type(ref msg))
                            if msg.contains("null value") =>
                        {
                            TryGetError::Null(format!("{index:?}"))
                        }
                        other => other,
                    })?;
                let inner: Decimal = raw.parse().map_err(|e| {
                    TryGetError::DbErr(sea_orm::DbErr::Type(format!(
                        "failed to parse {} from '{raw}': {e}",
                        stringify!($name)
                    )))
                })?;
                Ok(Self(inner))
            }
        }

        impl ValueType for $name {
            fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
                match v {
                    Value::String(Some(s)) => s.parse::<Decimal>().map(Self).map_err(|_| ValueTypeErr),
                    _ => Err(ValueTypeErr),
                }
            }

            fn type_name() -> String { stringify!($name).to_owned() }
            fn array_type() -> ArrayType { ArrayType::String }
            fn column_type() -> ColumnType { ColumnType::Text }
        }

        impl Nullable for $name {
            fn null() -> Value { Value::String(None) }
        }

        impl IntoActiveValue<$name> for $name {
            #[inline]
            fn into_active_value(self) -> ActiveValue<$name> { ActiveValue::Set(self) }
        }
    };
}

decimal_newtype!(
    /// USD-denominated monetary amount (`USDC.e` on Polygon).
    Usd
);

decimal_newtype!(
    /// Price per share in a prediction market. Range \[0, 1\].
    Price
);

decimal_newtype!(
    /// Number of shares (condition tokens).
    Shares
);

decimal_newtype!(
    /// Basis points (1 bps = 0.01%).
    Bps
);

decimal_newtype!(
    /// Statistical probability, confidence, or model weight stored losslessly.
    Probability
);

// ── Cross-type arithmetic ───────────────────────────────────────────────

impl std::ops::Mul<Price> for Shares {
    type Output = Usd;
    fn mul(self, rhs: Price) -> Usd {
        Usd::new(self.inner() * rhs.inner())
    }
}

impl std::ops::Mul<Shares> for Price {
    type Output = Usd;
    fn mul(self, rhs: Shares) -> Usd {
        Usd::new(self.inner() * rhs.inner())
    }
}

impl std::ops::Div<Price> for Usd {
    type Output = Shares;
    fn div(self, rhs: Price) -> Shares {
        Shares::new(self.inner() / rhs.inner())
    }
}

// ── Scalar arithmetic ───────────────────────────────────────────────────

impl std::ops::Mul<Decimal> for Usd {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl std::ops::Div<Decimal> for Usd {
    type Output = Self;
    fn div(self, rhs: Decimal) -> Self {
        Self::new(self.inner() / rhs)
    }
}

impl std::ops::Mul<Decimal> for Shares {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl std::ops::Div<Decimal> for Shares {
    type Output = Self;
    fn div(self, rhs: Decimal) -> Self {
        Self::new(self.inner() / rhs)
    }
}

impl std::ops::Mul<Decimal> for Price {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

impl std::ops::Mul<Decimal> for Bps {
    type Output = Self;
    fn mul(self, rhs: Decimal) -> Self {
        Self::new(self.inner() * rhs)
    }
}

// ── Bps utility ─────────────────────────────────────────────────────────

impl Bps {
    /// Convert basis points to a decimal fraction (e.g. 200 bps → 0.02).
    #[must_use]
    pub fn to_fraction(self) -> Decimal {
        self.inner() / Decimal::from(10_000)
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
    use super::*;
    use rust_decimal_macros::dec;

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
