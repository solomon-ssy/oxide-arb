//! Endgame calibration bucketing enums.

use crate::types::Price;
use oxide_arb_macros::IntoActiveValue;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

/// Price zone classification for endgame calibration bucketing.
///
/// Finer zones near 1.0 because small price differences at the extreme
/// have outsized impact on expected return. All zones assume the price
/// has already been normalised to the "winning side" perspective
/// (i.e. the price of the token we intend to buy).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum PriceZone {
    /// Price ∈ [0.95, 0.96) — weakest convergence signal.
    #[sea_orm(string_value = "z95")]
    Z95,
    /// Price ∈ [0.96, 0.97).
    #[sea_orm(string_value = "z96")]
    Z96,
    /// Price ∈ [0.97, 0.98).
    #[sea_orm(string_value = "z97")]
    Z97,
    /// Price ∈ [0.98, 0.99).
    #[sea_orm(string_value = "z98")]
    Z98,
    /// Price ∈ [0.99, 1.00] — strongest convergence signal.
    #[sea_orm(string_value = "z99")]
    Z99,
}

/// All price zone variants in ascending order.
static ALL_PRICE_ZONES: [PriceZone; 5] = [
    PriceZone::Z95,
    PriceZone::Z96,
    PriceZone::Z97,
    PriceZone::Z98,
    PriceZone::Z99,
];

impl PriceZone {
    /// Classify a winning-side token price into the appropriate zone.
    ///
    /// Prices below 0.95 still map to [`PriceZone::Z95`] as the most
    /// conservative bucket (should rarely happen in normal endgame detection
    /// since the convergence threshold is typically >= 0.95).
    #[must_use]
    pub fn from_price(price: Price) -> Self {
        let p = price.inner();
        if p >= dec!(0.99) {
            Self::Z99
        } else if p >= dec!(0.98) {
            Self::Z98
        } else if p >= dec!(0.97) {
            Self::Z97
        } else if p >= dec!(0.96) {
            Self::Z96
        } else {
            Self::Z95
        }
    }

    /// Midpoint of the zone, used for `MoM` prior estimation.
    #[must_use]
    pub const fn midpoint(&self) -> Decimal {
        match self {
            Self::Z95 => dec!(0.955),
            Self::Z96 => dec!(0.965),
            Self::Z97 => dec!(0.975),
            Self::Z98 => dec!(0.985),
            Self::Z99 => dec!(0.995),
        }
    }

    /// All zone variants as a static slice (useful for `MoM` iteration).
    #[must_use]
    pub fn all() -> &'static [Self] {
        &ALL_PRICE_ZONES
    }
}

impl Display for PriceZone {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Z95 => f.write_str("z95"),
            Self::Z96 => f.write_str("z96"),
            Self::Z97 => f.write_str("z97"),
            Self::Z98 => f.write_str("z98"),
            Self::Z99 => f.write_str("z99"),
        }
    }
}

/// Duration bucket for how long a market has been in the convergence zone.
///
/// Longer convergence durations generally correlate with higher settlement
/// accuracy, so calibration is bucketed by duration to capture this effect.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum DurationBucket {
    /// 5 minutes – 1 hour (300..3600 seconds).
    #[sea_orm(string_value = "short")]
    Short,
    /// 1 hour – 6 hours (3600..21600 seconds).
    #[sea_orm(string_value = "medium")]
    Medium,
    /// 6 hours – 24 hours (21600..86400 seconds).
    #[sea_orm(string_value = "long")]
    Long,
    /// More than 24 hours (86400+ seconds).
    #[sea_orm(string_value = "very_long")]
    VeryLong,
}

/// All duration bucket variants in ascending order.
static ALL_DURATION_BUCKETS: [DurationBucket; 4] = [
    DurationBucket::Short,
    DurationBucket::Medium,
    DurationBucket::Long,
    DurationBucket::VeryLong,
];

impl DurationBucket {
    /// Classify a convergence duration (in seconds) into the appropriate bucket.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        match secs {
            0..3600 => Self::Short,
            3600..21600 => Self::Medium,
            21600..86400 => Self::Long,
            _ => Self::VeryLong,
        }
    }

    /// All bucket variants as a static slice.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &ALL_DURATION_BUCKETS
    }
}

impl Display for DurationBucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Short => f.write_str("short"),
            Self::Medium => f.write_str("medium"),
            Self::Long => f.write_str("long"),
            Self::VeryLong => f.write_str("very_long"),
        }
    }
}
