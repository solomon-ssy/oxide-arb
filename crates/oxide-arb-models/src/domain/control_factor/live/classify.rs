//! Live hot-path classification of execution-quality dimensions.
//!
//! These thresholds are the canonical bucketization shared between live
//! consumption and (eventually) materialization-side factor builders, so that a
//! factor materialized for `(Tight, Deep, Fresh)` is matched by the same live
//! book conditions. Latency is not classified live — the live key uses
//! [`LatencyBucket::Unknown`] and the index relaxes latency on lookup.

use crate::{
    domain::{
        book::BookSnapshot,
        control_factor::{
            BookAgeBucket, DepthBucket, ExecutionQualityDimensions, LatencyBucket, SpreadBucket,
        },
    },
    enums::{
        calibration::PriceZone,
        common::{MarketCategory, StalenessLevel},
    },
    types::Price,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Spread (in price units) below which a book is `Tight`.
const SPREAD_TIGHT_MAX: Decimal = dec!(0.01);
/// Spread below which a book is `Normal`.
const SPREAD_NORMAL_MAX: Decimal = dec!(0.03);
/// Spread below which a book is `Wide`; at or above is `VeryWide`.
const SPREAD_WIDE_MAX: Decimal = dec!(0.06);

/// Ask-side notional (USD) at or above which depth is `Deep`.
const DEPTH_DEEP_MIN_USD: Decimal = dec!(1000);
/// Ask-side notional (USD) at or above which depth is `Normal`; below is `Thin`.
const DEPTH_NORMAL_MIN_USD: Decimal = dec!(100);

/// Book age (ms) below which the book is `Fresh`.
const BOOK_AGE_FRESH_MAX_MS: u64 = 500;
/// Book age (ms) below which the book is `Recent`.
const BOOK_AGE_RECENT_MAX_MS: u64 = 2_000;
/// Book age (ms) below which the book is `Stale`; at or above is `VeryStale`.
const BOOK_AGE_STALE_MAX_MS: u64 = 5_000;

/// Classify the bid/ask spread of the traded token.
#[must_use]
pub fn spread_bucket(best_bid: Option<Price>, best_ask: Option<Price>) -> SpreadBucket {
    let Some((bid, ask)) = best_bid.zip(best_ask) else {
        return SpreadBucket::VeryWide;
    };
    let spread = (ask.inner() - bid.inner()).max(Decimal::ZERO);
    if spread < SPREAD_TIGHT_MAX {
        SpreadBucket::Tight
    } else if spread < SPREAD_NORMAL_MAX {
        SpreadBucket::Normal
    } else if spread < SPREAD_WIDE_MAX {
        SpreadBucket::Wide
    } else {
        SpreadBucket::VeryWide
    }
}

/// Classify ask-side liquidity from its total notional depth (USD).
#[must_use]
pub fn depth_bucket(ask_depth_usd: Decimal) -> DepthBucket {
    if ask_depth_usd >= DEPTH_DEEP_MIN_USD {
        DepthBucket::Deep
    } else if ask_depth_usd >= DEPTH_NORMAL_MIN_USD {
        DepthBucket::Normal
    } else {
        DepthBucket::Thin
    }
}

/// Classify book freshness from its age in milliseconds.
#[must_use]
pub const fn book_age_bucket(age_ms: u64) -> BookAgeBucket {
    if age_ms < BOOK_AGE_FRESH_MAX_MS {
        BookAgeBucket::Fresh
    } else if age_ms < BOOK_AGE_RECENT_MAX_MS {
        BookAgeBucket::Recent
    } else if age_ms < BOOK_AGE_STALE_MAX_MS {
        BookAgeBucket::Stale
    } else {
        BookAgeBucket::VeryStale
    }
}

/// Build the execution-quality lookup key for the traded token's book.
///
/// `book` must be the snapshot of the token actually being bought (the side
/// whose asks we consume); `now_ms` is the current wall-clock for age.
#[must_use]
pub fn execution_quality_dimensions(
    category: MarketCategory,
    price_zone: PriceZone,
    staleness_level: StalenessLevel,
    book: &BookSnapshot,
    now_ms: u64,
) -> ExecutionQualityDimensions {
    ExecutionQualityDimensions {
        category,
        price_zone,
        spread_bucket: spread_bucket(book.best_bid(), book.best_ask()),
        depth_bucket: depth_bucket(book.total_ask_depth_usd.to_decimal()),
        book_age_bucket: book_age_bucket(now_ms.saturating_sub(book.timestamp_ms)),
        latency_bucket: LatencyBucket::Unknown,
        staleness_level,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_buckets_partition_range() {
        assert_eq!(
            spread_bucket(Some(Price::new(dec!(0.95))), Some(Price::new(dec!(0.955)))),
            SpreadBucket::Tight
        );
        assert_eq!(
            spread_bucket(Some(Price::new(dec!(0.90))), Some(Price::new(dec!(0.92)))),
            SpreadBucket::Normal
        );
        assert_eq!(
            spread_bucket(Some(Price::new(dec!(0.80))), Some(Price::new(dec!(0.90)))),
            SpreadBucket::VeryWide
        );
        assert_eq!(spread_bucket(None, None), SpreadBucket::VeryWide);
    }

    #[test]
    fn depth_buckets_partition_range() {
        assert_eq!(depth_bucket(dec!(5000)), DepthBucket::Deep);
        assert_eq!(depth_bucket(dec!(500)), DepthBucket::Normal);
        assert_eq!(depth_bucket(dec!(50)), DepthBucket::Thin);
    }

    #[test]
    fn book_age_buckets_partition_range() {
        assert_eq!(book_age_bucket(100), BookAgeBucket::Fresh);
        assert_eq!(book_age_bucket(1_000), BookAgeBucket::Recent);
        assert_eq!(book_age_bucket(3_000), BookAgeBucket::Stale);
        assert_eq!(book_age_bucket(10_000), BookAgeBucket::VeryStale);
    }
}
