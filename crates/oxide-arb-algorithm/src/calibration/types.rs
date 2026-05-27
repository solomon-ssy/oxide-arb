//! Calibration entry type used by the in-memory calibrator.

use num_traits::ToPrimitive;
use oxide_arb_models::{
    domain::calibration::{BucketKey, CalibrationSnapshot, UpsertCalibration},
    types::Probability,
};
use rust_decimal::Decimal;

/// A single calibration entry corresponding to one bucket in the `DashMap`.
///
/// Contains the observed counts and Beta prior parameters. The posterior
/// mean is computed on-the-fly rather than cached so that `record_outcome`
/// updates are immediately reflected.
#[derive(Debug, Clone)]
pub struct CalibrationEntry {
    pub bucket_key: BucketKey,
    pub total_count: u32,
    pub correct_count: u32,
    pub alpha_prior: Decimal,
    pub beta_prior: Decimal,
    /// Which fallback tier produced this entry (1=exact, 2=cat+zone, 3=zone, 4=global).
    pub fallback_tier: u8,
}

impl CalibrationEntry {
    /// Empirical Bayes posterior mean: `(α + correct) / (α + β + total)`.
    ///
    /// This is the expected resolution rate for the bucket, shrunk toward
    /// the prior when sample size is small.
    #[must_use]
    pub fn posterior_mean(&self) -> Decimal {
        let alpha = self.alpha_prior + Decimal::from(self.correct_count);
        let beta =
            self.beta_prior + Decimal::from(self.total_count.saturating_sub(self.correct_count));
        let denominator = alpha + beta;
        if denominator.is_zero() {
            return self.alpha_prior / (self.alpha_prior + self.beta_prior);
        }
        alpha / denominator
    }

    /// Number of observations in this bucket.
    #[must_use]
    pub const fn sample_count(&self) -> u32 {
        self.total_count
    }

    /// Whether the bucket has enough data for reliable estimation.
    #[must_use]
    pub const fn is_credible(&self, min_samples: u32) -> bool {
        self.total_count >= min_samples
    }

    /// Freeze the current state into a [`CalibrationSnapshot`] for embedding
    /// in an [`Opportunity`].
    #[must_use]
    pub fn to_snapshot(&self, fused_probability: Decimal) -> CalibrationSnapshot {
        CalibrationSnapshot {
            bucket_key: self.bucket_key,
            posterior_mean: self.posterior_mean(),
            sample_size: self.total_count,
            alpha_prior: self.alpha_prior,
            beta_prior: self.beta_prior,
            fallback_tier: self.fallback_tier,
            fused_probability,
        }
    }

    /// Convert this entry into an [`UpsertCalibration`] for database persistence.
    #[must_use]
    pub fn to_upsert(&self) -> UpsertCalibration {
        UpsertCalibration {
            category: self.bucket_key.category,
            price_zone: self.bucket_key.price_zone,
            duration_bucket: self.bucket_key.duration_bucket,
            total_count: ToPrimitive::to_i32(&self.total_count).unwrap_or(i32::MAX),
            correct_count: ToPrimitive::to_i32(&self.correct_count).unwrap_or(i32::MAX),
            alpha_prior: Probability::from(self.alpha_prior),
            beta_prior: Probability::from(self.beta_prior),
            posterior_mean: Some(Probability::from(self.posterior_mean())),
            updated_at: chrono::Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::calibration::BucketKey,
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
        },
    };
    use rust_decimal_macros::dec;
    fn make_key() -> BucketKey {
        BucketKey {
            category: MarketCategory::Geopolitics,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
        }
    }

    #[test]
    fn posterior_mean_with_prior_and_data() {
        let entry = CalibrationEntry {
            bucket_key: make_key(),
            total_count: 10,
            correct_count: 8,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        };
        // (2 + 8) / (2 + 0.2 + 10) = 10 / 12.2 ≈ 0.81967213...
        let pm = entry.posterior_mean();
        let expected = dec!(10) / dec!(12.2);
        assert_eq!(pm, expected);
    }

    #[test]
    fn posterior_mean_no_data_uses_prior() {
        let entry = CalibrationEntry {
            bucket_key: make_key(),
            total_count: 0,
            correct_count: 0,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 4,
        };
        let pm = entry.posterior_mean();
        let expected = dec!(2) / dec!(2.2);
        assert_eq!(pm, expected);
    }

    #[test]
    fn is_credible_boundary() {
        let mut entry = CalibrationEntry {
            bucket_key: make_key(),
            total_count: 9,
            correct_count: 7,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        };
        assert!(!entry.is_credible(10));
        entry.total_count = 10;
        assert!(entry.is_credible(10));
    }

    #[test]
    fn to_snapshot_captures_state() {
        let entry = CalibrationEntry {
            bucket_key: make_key(),
            total_count: 20,
            correct_count: 18,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        };
        let snap = entry.to_snapshot(dec!(0.92));
        assert_eq!(snap.sample_size, 20);
        assert_eq!(snap.fused_probability, dec!(0.92));
        assert_eq!(snap.fallback_tier, 1);
        assert_eq!(snap.posterior_mean, entry.posterior_mean());
    }
}
