//! Fixed-point monetary types for hot paths (6 decimal places).
//!
//! Interior arithmetic uses `i64` micro-units. Convert to [`Decimal`](rust_decimal::Decimal)
//! only at serde, persistence, and API boundaries.

use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    error::Error,
    fmt,
    ops::{Add, AddAssign, Div, Mul, Sub, SubAssign},
};

/// Scale factor: 1 unit = 1 / `MICRO_SCALE`.
pub const MICRO_SCALE: i64 = 1_000_000;

/// Conversion error when a decimal value is out of fixed-point range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MicroConversionError;

impl fmt::Display for MicroConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("decimal value out of fixed-point range")
    }
}

impl Error for MicroConversionError {}

macro_rules! micro_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default
        )]
        #[serde(transparent)]
        pub struct $name(i64);

        impl $name {
            pub const ZERO: Self = Self(0);

            #[must_use]
            #[inline]
            pub const fn from_micro(v: i64) -> Self {
                Self(v)
            }

            #[must_use]
            #[inline]
            pub const fn micro(self) -> i64 {
                self.0
            }

            #[must_use]
            #[inline]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }

            #[must_use]
            #[inline]
            pub const fn is_positive(self) -> bool {
                self.0 > 0
            }

            pub fn try_from_decimal(d: Decimal) -> Result<Self, MicroConversionError> {
                let scaled = d * Decimal::from(MICRO_SCALE);
                if scaled.is_sign_negative() {
                    return Err(MicroConversionError);
                }
                scaled
                    .trunc()
                    .to_i64()
                    .filter(|v| *v >= 0)
                    .map(Self)
                    .ok_or(MicroConversionError)
            }

            /// Normalized so callers serialize the canonical form
            /// (`"1.5"`, not `"1.50"`).
            #[must_use]
            pub fn to_decimal(self) -> Decimal {
                (Decimal::from(self.0) / Decimal::from(MICRO_SCALE)).normalize()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.to_decimal())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.to_decimal())
            }
        }

        impl Add for $name {
            type Output = Self;
            #[inline]
            fn add(self, rhs: Self) -> Self {
                Self(self.0.saturating_add(rhs.0))
            }
        }

        impl AddAssign for $name {
            #[inline]
            fn add_assign(&mut self, rhs: Self) {
                self.0 = self.0.saturating_add(rhs.0);
            }
        }

        impl Sub for $name {
            type Output = Self;
            #[inline]
            fn sub(self, rhs: Self) -> Self {
                Self(self.0.saturating_sub(rhs.0))
            }
        }

        impl SubAssign for $name {
            #[inline]
            fn sub_assign(&mut self, rhs: Self) {
                self.0 = self.0.saturating_sub(rhs.0);
            }
        }
    };
}

micro_newtype!(
    /// Price per share in micro-USD (e.g. 0.97 → `970_000`).
    MicroPrice
);
micro_newtype!(
    /// Share quantity in micro-units.
    MicroShares
);
micro_newtype!(
    /// USD amount in micro-units.
    MicroUsd
);

impl MicroPrice {
    pub const ONE: Self = Self(MICRO_SCALE);

    /// `shares × price` → USD micro (both operands in micro-units).
    #[must_use]
    #[inline]
    pub fn mul_shares(self, shares: MicroShares) -> MicroUsd {
        let micro = i128::from(self.0) * i128::from(shares.0) / i128::from(MICRO_SCALE);
        MicroUsd::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }

    /// Affordable shares for a USD budget at this price (rounded down).
    #[must_use]
    #[inline]
    pub fn affordable_shares(self, budget: MicroUsd) -> MicroShares {
        if self.0 <= 0 {
            return MicroShares::ZERO;
        }
        let micro = i128::from(budget.0) * i128::from(MICRO_SCALE) / i128::from(self.0);
        MicroShares::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }

    /// VWAP from cost and shares with nearest-micro rounding.
    #[must_use]
    #[inline]
    pub fn vwap_from_cost(total_cost: MicroUsd, total_shares: MicroShares) -> Self {
        if total_shares.0 <= 0 {
            return Self::ZERO;
        }
        let num = i128::from(total_cost.0) * i128::from(MICRO_SCALE);
        let den = i128::from(total_shares.0);
        let micro = (num + den / 2) / den;
        Self::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }
}

impl MicroShares {
    /// Cost to buy these shares at `price`.
    #[must_use]
    #[inline]
    pub fn cost_at(self, price: MicroPrice) -> MicroUsd {
        price.mul_shares(self)
    }
}

impl MicroUsd {
    /// Percentage of `total` represented by `self` (0–100 scale, 2 decimal places in micro).
    #[must_use]
    #[inline]
    pub fn percent_of(self, total: Self) -> i64 {
        if total.0 <= 0 {
            return 0;
        }
        let pct = i128::from(self.0) * 100 / i128::from(total.0);
        ToPrimitive::to_i64(&pct).unwrap_or(0)
    }

    /// Basis points of `total` represented by `self` (`10_000` = 100%).
    #[must_use]
    #[inline]
    pub fn ratio_bps(self, total: Self) -> i32 {
        if total.0 <= 0 {
            return 0;
        }
        let bps = (i128::from(self.0) * 10_000) / i128::from(total.0);
        ToPrimitive::to_i32(&bps).unwrap_or(0)
    }

    /// VWAP = `total_cost` / `total_shares`.
    #[must_use]
    #[inline]
    pub fn vwap_price(self, shares: MicroShares) -> MicroPrice {
        if shares.0 <= 0 {
            return MicroPrice::ZERO;
        }
        let micro = i128::from(self.0) * i128::from(MICRO_SCALE) / i128::from(shares.0);
        MicroPrice::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }
}

impl Div<MicroShares> for MicroUsd {
    type Output = MicroPrice;
    #[inline]
    fn div(self, rhs: MicroShares) -> MicroPrice {
        self.vwap_price(rhs)
    }
}

impl Mul<MicroPrice> for MicroShares {
    type Output = MicroUsd;
    #[inline]
    fn mul(self, rhs: MicroPrice) -> MicroUsd {
        rhs.mul_shares(self)
    }
}

/// Probability in micro-units (`0..=1_000_000`).
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default, Debug,
)]
#[serde(transparent)]
pub struct MicroProb(i64);

impl MicroProb {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(MICRO_SCALE);

    #[must_use]
    #[inline]
    pub const fn from_micro(v: i64) -> Self {
        Self(if v < 0 {
            0
        } else if v > MICRO_SCALE {
            MICRO_SCALE
        } else {
            v
        })
    }

    /// Multiplier factor without upper clamp (e.g. urgency ∈ [1.0, 3.0], category weight 1.5).
    #[must_use]
    #[inline]
    pub const fn from_factor_micro(v: i64) -> Self {
        Self(if v < 0 { 0 } else { v })
    }

    #[must_use]
    #[inline]
    pub const fn micro(self) -> i64 {
        self.0
    }

    pub fn try_from_decimal(d: Decimal) -> Result<Self, MicroConversionError> {
        MicroPrice::try_from_decimal(d).map(|p| Self(p.micro()))
    }

    /// Normalized so callers serialize the canonical form.
    #[must_use]
    pub fn to_decimal(self) -> Decimal {
        (Decimal::from(self.0) / Decimal::from(MICRO_SCALE)).normalize()
    }

    /// `self × other / MICRO_SCALE` with saturation.
    #[must_use]
    #[inline]
    pub fn saturating_mul(self, other: Self) -> Self {
        let v = i128::from(self.0) * i128::from(other.0) / i128::from(MICRO_SCALE);
        Self::from_factor_micro(ToPrimitive::to_i64(&v).unwrap_or(i64::MAX))
    }

    /// `self × (1 − w) + other × w` where `w = weight_num / weight_den`.
    #[must_use]
    #[inline]
    pub fn blend(self, other: Self, weight_num: u32, weight_den: u32) -> Self {
        if weight_den == 0 {
            return self;
        }
        let w = i128::from(weight_num) * i128::from(MICRO_SCALE) / i128::from(weight_den);
        let one_minus_w = i128::from(MICRO_SCALE) - w;
        let v =
            (i128::from(self.0) * one_minus_w + i128::from(other.0) * w) / i128::from(MICRO_SCALE);
        Self::from_micro(ToPrimitive::to_i64(&v).unwrap_or(i64::MAX))
    }

    #[must_use]
    #[inline]
    pub fn clamp_prob(self, floor: Self, ceiling: Self) -> Self {
        Self::from_micro(self.0.clamp(floor.micro(), ceiling.micro()))
    }
}

micro_newtype!(
    /// Percentage 0..=100% stored as fraction micro-units (50% → `500_000`).
    MicroPct
);

impl MicroPct {
    /// Parse a 0–100 percentage (e.g. `50` → 50%).
    pub fn try_from_pct_decimal(d: Decimal) -> Result<Self, MicroConversionError> {
        let fraction = d / Decimal::from(100);
        MicroPrice::try_from_decimal(fraction).map(|p| Self(p.micro()))
    }

    #[must_use]
    pub fn to_pct_decimal(self) -> Decimal {
        Decimal::from(self.0) * Decimal::from(100) / Decimal::from(MICRO_SCALE)
    }
}

micro_newtype!(
    /// Dimensionless composite score for ranking (profit × prob products).
    MicroScore
);

impl MicroScore {
    /// `profit × prob / MICRO_SCALE`.
    #[must_use]
    #[inline]
    pub fn from_profit_prob(profit: MicroUsd, prob: MicroProb) -> Self {
        let micro = i128::from(profit.micro()) * i128::from(prob.micro()) / i128::from(MICRO_SCALE);
        Self::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }

    /// Multiply score by an additional factor (`/ MICRO_SCALE`).
    #[must_use]
    #[inline]
    pub fn scale_by_factor(self, factor: MicroProb) -> Self {
        let micro = i128::from(self.micro()) * i128::from(factor.micro()) / i128::from(MICRO_SCALE);
        Self::from_micro(ToPrimitive::to_i64(&micro).unwrap_or(i64::MAX))
    }

    /// Descending sort order (higher score first).
    #[must_use]
    #[inline]
    pub fn cmp_desc(self, other: Self) -> Ordering {
        other.cmp(&self)
    }
}

/// Basis points as plain integer (100 bps = 1%).
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default, Debug,
)]
#[serde(transparent)]
pub struct MicroBps(i32);

impl MicroBps {
    pub const ZERO: Self = Self(0);

    #[must_use]
    #[inline]
    pub const fn from_bps(v: i32) -> Self {
        Self(v)
    }

    #[must_use]
    #[inline]
    pub const fn bps(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn price_shares_to_usd() {
        let price = MicroPrice::try_from_decimal(dec!(0.97)).unwrap();
        let shares = MicroShares::try_from_decimal(dec!(100)).unwrap();
        let cost = price.mul_shares(shares);
        assert_eq!(cost.to_decimal(), dec!(97));
    }

    #[test]
    fn affordable_shares_rounds_down() {
        let price = MicroPrice::try_from_decimal(dec!(0.97)).unwrap();
        let budget = MicroUsd::try_from_decimal(dec!(50)).unwrap();
        let shares = price.affordable_shares(budget);
        assert!(shares.to_decimal() <= dec!(51.55));
    }
}
