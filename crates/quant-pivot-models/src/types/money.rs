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
    fmt,
    iter::Sum,
    ops::{Add, AddAssign, Div, Mul, Neg, Sub, SubAssign},
};

use rust_decimal::Decimal;
use sea_orm::DeriveValueType;
use serde::{Deserialize, Serialize};

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

    use super::*;

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
