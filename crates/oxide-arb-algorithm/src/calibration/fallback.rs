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
use oxide_arb_models::{
    domain::calibration::BucketKey,
    enums::{calibration::PriceZone, common::MarketCategory},
};
use rust_decimal::Decimal;
use std::hash::Hash;

/// Pre-aggregated counts and priors for tier-2/3 fallback lookups.
#[derive(Debug, Default)]
pub struct FallbackIndexes {
    by_category_zone: DashMap<(MarketCategory, PriceZone), AggregatedCalibration>,
    by_zone: DashMap<PriceZone, AggregatedCalibration>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregatedCalibration {
    total_count: u32,
    correct_count: u32,
    alpha_prior: Decimal,
    beta_prior: Decimal,
    bucket_count: u32,
}

impl FallbackIndexes {
    /// Rebuild secondary indexes from the primary bucket map.
    pub fn rebuild(buckets: &DashMap<BucketKey, CalibrationEntry>) -> Self {
        let indexes = Self::default();
        for entry in buckets {
            indexes.add_entry(*entry.key(), entry.value());
        }
        indexes
    }

    /// Clear all secondary indexes (before a full rebuild).
    pub(crate) fn clear(&self) {
        self.by_category_zone.clear();
        self.by_zone.clear();
    }

    pub(crate) fn add_entry(&self, key: BucketKey, entry: &CalibrationEntry) {
        Self::merge_into(
            &self.by_category_zone,
            (key.category, key.price_zone),
            entry,
        );
        Self::merge_into(&self.by_zone, key.price_zone, entry);
    }

    fn merge_into<K: Eq + Hash>(
        map: &DashMap<K, AggregatedCalibration>,
        key: K,
        entry: &CalibrationEntry,
    ) {
        map.entry(key)
            .and_modify(|agg| {
                let old_n = agg.bucket_count;
                let new_n = old_n + 1;
                agg.total_count = agg.total_count.saturating_add(entry.total_count);
                agg.correct_count = agg.correct_count.saturating_add(entry.correct_count);
                agg.alpha_prior = (agg.alpha_prior * Decimal::from(old_n) + entry.alpha_prior)
                    / Decimal::from(new_n);
                agg.beta_prior = (agg.beta_prior * Decimal::from(old_n) + entry.beta_prior)
                    / Decimal::from(new_n);
                agg.bucket_count = new_n;
            })
            .or_insert(AggregatedCalibration {
                total_count: entry.total_count,
                correct_count: entry.correct_count,
                alpha_prior: entry.alpha_prior,
                beta_prior: entry.beta_prior,
                bucket_count: 1,
            });
    }

    /// Record a new bucket or replace an existing one during reload.
    pub fn upsert_bucket(&self, key: BucketKey, entry: &CalibrationEntry) {
        self.add_entry(key, entry);
    }

    /// Increment counts after a single outcome is recorded in an existing bucket.
    pub fn bump_outcome(&self, key: &BucketKey, was_correct: bool) {
        let bump = |agg: &mut AggregatedCalibration| {
            agg.total_count = agg.total_count.saturating_add(1);
            if was_correct {
                agg.correct_count = agg.correct_count.saturating_add(1);
            }
        };
        if let Some(mut agg) = self
            .by_category_zone
            .get_mut(&(key.category, key.price_zone))
        {
            bump(&mut agg);
        }
        if let Some(mut agg) = self.by_zone.get_mut(&key.price_zone) {
            bump(&mut agg);
        }
    }

    /// Register a newly created bucket in the secondary indexes.
    pub fn register_new_bucket(&self, key: BucketKey, entry: &CalibrationEntry) {
        self.add_entry(key, entry);
    }
}

impl AggregatedCalibration {
    const fn to_entry(self, bucket_key: BucketKey, fallback_tier: u8) -> CalibrationEntry {
        CalibrationEntry {
            bucket_key,
            total_count: self.total_count,
            correct_count: self.correct_count,
            alpha_prior: self.alpha_prior,
            beta_prior: self.beta_prior,
            fallback_tier,
        }
    }
}

/// Execute a 4-tier fallback lookup against in-memory buckets.
#[inline]
pub fn lookup_with_fallback(
    buckets: &DashMap<BucketKey, CalibrationEntry>,
    indexes: &FallbackIndexes,
    key: &BucketKey,
    min_samples: u32,
    bootstrap_alpha: Decimal,
    bootstrap_beta: Decimal,
) -> CalibrationEntry {
    // Tier 1: exact match (field copy, no full clone)
    if let Some(entry) = buckets.get(key) {
        if entry.is_credible(min_samples) {
            return CalibrationEntry {
                bucket_key: *key,
                total_count: entry.total_count,
                correct_count: entry.correct_count,
                alpha_prior: entry.alpha_prior,
                beta_prior: entry.beta_prior,
                fallback_tier: 1,
            };
        }
    }

    // Tier 2: same category + zone, aggregate all durations
    if let Some(agg) = indexes
        .by_category_zone
        .get(&(key.category, key.price_zone))
    {
        let tier2 = agg.to_entry(*key, 2);
        if tier2.is_credible(min_samples) {
            return tier2;
        }
    }

    // Tier 3: same zone, any category
    if let Some(agg) = indexes.by_zone.get(&key.price_zone) {
        let tier3 = agg.to_entry(*key, 3);
        if tier3.is_credible(min_samples) {
            return tier3;
        }
    }

    // Tier 4: global prior
    CalibrationEntry {
        bucket_key: *key,
        total_count: 0,
        correct_count: 0,
        alpha_prior: bootstrap_alpha,
        beta_prior: bootstrap_beta,
        fallback_tier: 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::enums::calibration::DurationBucket;
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

    fn lookup(
        buckets: &DashMap<BucketKey, CalibrationEntry>,
        query: BucketKey,
        min_samples: u32,
    ) -> CalibrationEntry {
        let indexes = FallbackIndexes::rebuild(buckets);
        lookup_with_fallback(buckets, &indexes, &query, min_samples, dec!(2), dec!(0.2))
    }

    #[test]
    fn tier1_exact_match() {
        let buckets = DashMap::new();
        let k = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Medium,
        );
        buckets.insert(k, entry(k, 20, 18));

        let result = lookup(&buckets, k, 10);
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
        buckets.insert(k_short, entry(k_short, 15, 14));
        buckets.insert(k_long, entry(k_long, 10, 9));

        let query = key(
            MarketCategory::Sports,
            PriceZone::Z97,
            DurationBucket::Medium,
        );
        let result = lookup(&buckets, query, 10);
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
        buckets.insert(k, entry(k, 20, 19));

        let query = key(
            MarketCategory::Sports,
            PriceZone::Z98,
            DurationBucket::Medium,
        );
        let result = lookup(&buckets, query, 10);
        assert_eq!(result.fallback_tier, 3);
        assert_eq!(result.total_count, 20);
    }

    #[test]
    fn tier4_global_prior() {
        let buckets: DashMap<BucketKey, CalibrationEntry> = DashMap::new();
        let query = key(MarketCategory::Sports, PriceZone::Z99, DurationBucket::Long);
        let result = lookup(&buckets, query, 10);
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
        buckets.insert(k, entry(k, 5, 4));

        let result = lookup(&buckets, k, 10);
        assert!(result.fallback_tier >= 2);
    }
}
