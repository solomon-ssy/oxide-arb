//! 4-tier fallback chain for calibration bucket lookup.
//!
//! When the exact bucket lacks sufficient data, progressively broader
//! aggregations are tried until a credible estimate is found:
//!
//! 1. Exact match `(category, price_zone, duration_bucket)`
//! 2. Same category + zone, any duration
//! 3. Same zone, any category
//! 4. Global bootstrap prior

use super::types::CalibrationEntry;
use dashmap::DashMap;
use oxide_arb_models::domain::calibration::{BucketKey, DurationBucket, PriceZone};
use oxide_arb_models::enums::common::MarketCategory;
use rust_decimal::Decimal;

/// Execute a 4-tier fallback lookup against in-memory buckets.
pub fn lookup_with_fallback(
    buckets: &DashMap<BucketKey, CalibrationEntry>,
    key: &BucketKey,
    min_samples: u32,
    bootstrap_alpha: Decimal,
    bootstrap_beta: Decimal,
) -> CalibrationEntry {
    // Tier 1: exact match
    if let Some(entry) = buckets.get(key) {
        if entry.is_credible(min_samples) {
            let mut e = entry.clone();
            e.fallback_tier = 1;
            return e;
        }
    }

    // Tier 2: same category + zone, aggregate all durations
    let tier2 = aggregate(buckets, bootstrap_alpha, bootstrap_beta, |k| {
        k.category == key.category && k.price_zone == key.price_zone
    });
    if tier2.is_credible(min_samples) {
        return CalibrationEntry {
            bucket_key: key.clone(),
            fallback_tier: 2,
            ..tier2
        };
    }

    // Tier 3: same zone, any category
    let tier3 = aggregate(buckets, bootstrap_alpha, bootstrap_beta, |k| {
        k.price_zone == key.price_zone
    });
    if tier3.is_credible(min_samples) {
        return CalibrationEntry {
            bucket_key: key.clone(),
            fallback_tier: 3,
            ..tier3
        };
    }

    // Tier 4: global prior
    CalibrationEntry {
        bucket_key: key.clone(),
        total_count: 0,
        correct_count: 0,
        alpha_prior: bootstrap_alpha,
        beta_prior: bootstrap_beta,
        fallback_tier: 4,
    }
}

/// Aggregate matching entries: sum total/correct counts, average priors.
fn aggregate(
    buckets: &DashMap<BucketKey, CalibrationEntry>,
    bootstrap_alpha: Decimal,
    bootstrap_beta: Decimal,
    predicate: impl Fn(&BucketKey) -> bool,
) -> CalibrationEntry {
    let mut total = 0_u32;
    let mut correct = 0_u32;
    let mut alpha_sum = Decimal::ZERO;
    let mut beta_sum = Decimal::ZERO;
    let mut count = 0_u32;

    for entry in buckets {
        if predicate(entry.key()) {
            total = total.saturating_add(entry.total_count);
            correct = correct.saturating_add(entry.correct_count);
            alpha_sum += entry.alpha_prior;
            beta_sum += entry.beta_prior;
            count += 1;
        }
    }

    let (alpha, beta) = if count > 0 {
        (
            alpha_sum / Decimal::from(count),
            beta_sum / Decimal::from(count),
        )
    } else {
        (bootstrap_alpha, bootstrap_beta)
    };

    CalibrationEntry {
        bucket_key: BucketKey {
            category: MarketCategory::Other,
            price_zone: PriceZone::Z95,
            duration_bucket: DurationBucket::Short,
        },
        total_count: total,
        correct_count: correct,
        alpha_prior: alpha,
        beta_prior: beta,
        fallback_tier: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn key(cat: MarketCategory, zone: PriceZone, dur: DurationBucket) -> BucketKey {
        BucketKey {
            category: cat,
            price_zone: zone,
            duration_bucket: dur,
        }
    }

    fn entry(k: BucketKey, total: u32, correct: u32) -> CalibrationEntry {
        CalibrationEntry {
            bucket_key: k,
            total_count: total,
            correct_count: correct,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 0,
        }
    }

    #[test]
    fn tier1_exact_match() {
        let buckets = DashMap::new();
        let k = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Medium,
        );
        buckets.insert(k.clone(), entry(k.clone(), 20, 18));

        let result = lookup_with_fallback(&buckets, &k, 10, dec!(2), dec!(0.2));
        assert_eq!(result.fallback_tier, 1);
        assert_eq!(result.total_count, 20);
    }

    #[test]
    fn tier2_same_category_zone() {
        let buckets = DashMap::new();
        let k_short = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Short,
        );
        let k_long = key(MarketCategory::Sports, PriceZone::Z97, DurationBucket::Long);
        buckets.insert(k_short.clone(), entry(k_short, 15, 14));
        buckets.insert(k_long.clone(), entry(k_long, 10, 9));

        let query = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Medium,
        );
        let result = lookup_with_fallback(&buckets, &query, 10, dec!(2), dec!(0.2));
        assert_eq!(result.fallback_tier, 2);
        assert_eq!(result.total_count, 25);
    }

    #[test]
    fn tier3_same_zone_any_category() {
        let buckets = DashMap::new();
        let k = key(
            MarketCategory::Crypto,
            PriceZone::Z98,
            DurationBucket::Short,
        );
        buckets.insert(k.clone(), entry(k, 20, 19));

        let query = key(
            MarketCategory::Sports,
            PriceZone::Z98,
            DurationBucket::Medium,
        );
        let result = lookup_with_fallback(&buckets, &query, 10, dec!(2), dec!(0.2));
        assert_eq!(result.fallback_tier, 3);
        assert_eq!(result.total_count, 20);
    }

    #[test]
    fn tier4_global_prior() {
        let buckets: DashMap<BucketKey, CalibrationEntry> = DashMap::new();
        let query = key(MarketCategory::Sports, PriceZone::Z99, DurationBucket::Long);
        let result = lookup_with_fallback(&buckets, &query, 10, dec!(2), dec!(0.2));
        assert_eq!(result.fallback_tier, 4);
        assert_eq!(result.total_count, 0);
        assert_eq!(result.alpha_prior, dec!(2));
    }

    #[test]
    fn exact_match_insufficient_samples_falls_through() {
        let buckets = DashMap::new();
        let k = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Medium,
        );
        buckets.insert(k.clone(), entry(k.clone(), 5, 4));

        let result = lookup_with_fallback(&buckets, &k, 10, dec!(2), dec!(0.2));
        assert!(result.fallback_tier >= 2);
    }
}
