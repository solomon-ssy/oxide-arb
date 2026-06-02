//! Endgame calibration bucketing enums.

use crate::types::Price;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

active_string_enum! {
    /// Price zone classification for endgame calibration bucketing.
    ///
    /// Finer zones near 1.0 because small price differences at the extreme
    /// have outsized impact on expected return. All zones assume the price
    /// has already been normalised to the "winning side" perspective
    /// (i.e. the price of the token we intend to buy).
    pub enum PriceZone {
        /// Price ∈ [0.95, 0.96) — weakest convergence signal.
        Z95 => "z95",
        /// Price ∈ [0.96, 0.97).
        Z96 => "z96",
        /// Price ∈ [0.97, 0.98).
        Z97 => "z97",
        /// Price ∈ [0.98, 0.99).
        Z98 => "z98",
        /// Price ∈ [0.99, 1.00] — strongest convergence signal.
        Z99 => "z99",
    }
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

active_string_enum! {
    /// Duration bucket for how long a market has been in the convergence zone.
    ///
    /// Longer convergence durations generally correlate with higher settlement
    /// accuracy, so calibration is bucketed by duration to capture this effect.
    pub enum DurationBucket {
        /// 5 minutes – 1 hour (300..3600 seconds).
        Short => "short",
        /// 1 hour – 6 hours (3600..21600 seconds).
        Medium => "medium",
        /// 6 hours – 24 hours (21600..86400 seconds).
        Long => "long",
        /// More than 24 hours (86400+ seconds).
        VeryLong => "very_long",
    }
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
