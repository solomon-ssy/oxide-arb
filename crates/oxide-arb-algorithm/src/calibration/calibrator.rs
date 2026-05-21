//! In-memory resolution calibrator backed by `DashMap`.
//!
//! All lookups go through the 4-tier fallback chain. Writes (outcome recording,
//! full reload) are lock-free at the per-bucket level via `DashMap`.

use super::fallback::lookup_with_fallback;
use super::types::CalibrationEntry;
use dashmap::DashMap;
use oxide_arb_models::{config::CalibrationConfig, domain::calibration::BucketKey};

/// Thread-safe in-memory calibration store.
///
/// Loaded from the database on startup and periodically refreshed by
/// [`CalibrationUpdater`]. Detection reads are concurrent and lock-free.
pub struct ResolutionCalibrator {
    buckets: DashMap<BucketKey, CalibrationEntry>,
    config: CalibrationConfig,
}

impl ResolutionCalibrator {
    /// Construct from a pre-loaded set of entries (typically from DB).
    #[must_use]
    pub fn from_entries(entries: Vec<CalibrationEntry>, config: CalibrationConfig) -> Self {
        let buckets = DashMap::with_capacity(entries.len());
        for entry in entries {
            buckets.insert(entry.bucket_key.clone(), entry);
        }
        Self { buckets, config }
    }

    /// Construct an empty calibrator (cold start / no historical data).
    #[must_use]
    pub fn empty(config: CalibrationConfig) -> Self {
        Self {
            buckets: DashMap::new(),
            config,
        }
    }

    /// Lookup with 4-tier fallback chain.
    ///
    /// Tier 1: Exact match `(category, price_zone, duration_bucket)`
    /// Tier 2: Same category + `price_zone`, aggregate all durations
    /// Tier 3: Same `price_zone`, aggregate all categories
    /// Tier 4: Global bootstrap prior `(α₀, β₀)`
    #[must_use]
    pub fn lookup(&self, key: &BucketKey) -> CalibrationEntry {
        lookup_with_fallback(
            &self.buckets,
            key,
            self.config.min_sample_size,
            self.config.bootstrap_alpha,
            self.config.bootstrap_beta,
        )
    }

    /// Record a single resolution outcome in the matching bucket.
    ///
    /// If the bucket does not exist, it is created with bootstrap priors.
    pub fn record_outcome(&self, key: &BucketKey, was_correct: bool) {
        self.buckets
            .entry(key.clone())
            .and_modify(|e| {
                e.total_count = e.total_count.saturating_add(1);
                if was_correct {
                    e.correct_count = e.correct_count.saturating_add(1);
                }
            })
            .or_insert_with(|| CalibrationEntry {
                bucket_key: key.clone(),
                total_count: 1,
                correct_count: u32::from(was_correct),
                alpha_prior: self.config.bootstrap_alpha,
                beta_prior: self.config.bootstrap_beta,
                fallback_tier: 1,
            });
    }

    /// Atomically replace all in-memory buckets (called after full DB reload).
    pub fn reload(&self, entries: Vec<CalibrationEntry>) {
        self.buckets.clear();
        for entry in entries {
            self.buckets.insert(entry.bucket_key.clone(), entry);
        }
    }

    /// Snapshot all entries (for `MoM` re-estimation or persistence).
    #[must_use]
    pub fn all_entries(&self) -> Vec<CalibrationEntry> {
        self.buckets.iter().map(|e| e.value().clone()).collect()
    }

    /// Number of distinct buckets in memory.
    #[must_use]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Direct access to the underlying map (used by `CalibrationUpdater`
    /// for `MoM` prior updates on sparse buckets).
    pub(crate) const fn buckets(&self) -> &DashMap<BucketKey, CalibrationEntry> {
        &self.buckets
    }

    /// Access the calibration config.
    #[must_use]
    pub const fn config(&self) -> &CalibrationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::calibration::{DurationBucket, PriceZone},
        enums::common::MarketCategory,
    };
    use rust_decimal_macros::dec;

    fn default_config() -> CalibrationConfig {
        CalibrationConfig {
            min_sample_size: 10,
            bootstrap_alpha: dec!(2),
            bootstrap_beta: dec!(0.2),
            ..CalibrationConfig::default()
        }
    }

    fn make_key(zone: PriceZone) -> BucketKey {
        BucketKey {
            category: MarketCategory::Geopolitics,
            price_zone: zone,
            duration_bucket: DurationBucket::Medium,
        }
    }

    #[test]
    fn empty_calibrator_returns_tier4() {
        let cal = ResolutionCalibrator::empty(default_config());
        let result = cal.lookup(&make_key(PriceZone::Z97));
        assert_eq!(result.fallback_tier, 4);
        assert_eq!(result.total_count, 0);
    }

    #[test]
    fn record_outcome_creates_bucket() {
        let cal = ResolutionCalibrator::empty(default_config());
        let key = make_key(PriceZone::Z97);

        cal.record_outcome(&key, true);
        assert_eq!(cal.bucket_count(), 1);

        cal.record_outcome(&key, false);
        let (total_count, correct_count) = {
            let entry = cal.buckets.get(&key).unwrap();
            (entry.total_count, entry.correct_count)
        };
        assert_eq!(total_count, 2);
        assert_eq!(correct_count, 1);
    }

    #[test]
    fn reload_replaces_all_buckets() {
        let cal = ResolutionCalibrator::empty(default_config());
        let key = make_key(PriceZone::Z97);
        cal.record_outcome(&key, true);
        assert_eq!(cal.bucket_count(), 1);

        cal.reload(vec![]);
        assert_eq!(cal.bucket_count(), 0);
    }

    #[test]
    fn from_entries_loads_all() {
        let entries = vec![
            CalibrationEntry {
                bucket_key: make_key(PriceZone::Z97),
                total_count: 20,
                correct_count: 18,
                alpha_prior: dec!(2),
                beta_prior: dec!(0.2),
                fallback_tier: 1,
            },
            CalibrationEntry {
                bucket_key: make_key(PriceZone::Z98),
                total_count: 15,
                correct_count: 14,
                alpha_prior: dec!(2),
                beta_prior: dec!(0.2),
                fallback_tier: 1,
            },
        ];
        let cal = ResolutionCalibrator::from_entries(entries, default_config());
        assert_eq!(cal.bucket_count(), 2);
    }
}
