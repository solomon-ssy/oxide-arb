//! Method of Moments (`MoM`) prior estimation for Beta distribution parameters.
//!
//! Given empirical resolution rates across calibration buckets, estimates the
//! population-level `Beta(α₀, β₀)` that generated those rates. These priors
//! are used to initialise new buckets and regularise sparse ones.

use super::types::CalibrationEntry;
use rust_decimal::Decimal;

/// Estimate Beta distribution priors `(α₀, β₀)` from observed bucket statistics
/// using Method of Moments.
///
/// # `MoM` equations
///
/// ```text
/// μ = mean(p̂ᵢ)
/// v = var(p̂ᵢ)
/// α₀ = μ × (μ(1−μ)/v − 1)
/// β₀ = (1−μ) × (μ(1−μ)/v − 1)
/// ```
///
/// Falls back to `(fallback_alpha, fallback_beta)` when:
/// - Fewer than 3 buckets have `total_count >= min_samples`.
/// - Sample variance is zero or negative.
/// - The common factor `μ(1−μ)/v − 1` is non-positive.
/// - Resulting α₀ or β₀ is non-positive.
pub fn estimate_mom_prior(
    entries: &[CalibrationEntry],
    min_samples: u32,
    fallback_alpha: Decimal,
    fallback_beta: Decimal,
) -> (Decimal, Decimal) {
    let rates: Vec<Decimal> = entries
        .iter()
        .filter(|e| e.total_count >= min_samples)
        .filter(|e| e.total_count > 0)
        .map(|e| Decimal::from(e.correct_count) / Decimal::from(e.total_count))
        .collect();

    if rates.len() < 3 {
        return (fallback_alpha, fallback_beta);
    }

    let Ok(len) = u32::try_from(rates.len()) else {
        return (fallback_alpha, fallback_beta);
    };
    let n = Decimal::from(len);
    let mu: Decimal = rates.iter().copied().sum::<Decimal>() / n;

    let variance: Decimal = rates
        .iter()
        .map(|p| {
            let diff = *p - mu;
            diff * diff
        })
        .sum::<Decimal>()
        / (n - Decimal::ONE);

    if variance.is_zero() || variance.is_sign_negative() {
        return (fallback_alpha, fallback_beta);
    }

    let mu_complement = Decimal::ONE - mu;
    let common = mu * mu_complement / variance - Decimal::ONE;

    if common <= Decimal::ZERO {
        return (fallback_alpha, fallback_beta);
    }

    let alpha = mu * common;
    let beta = mu_complement * common;

    if alpha > Decimal::ZERO && beta > Decimal::ZERO {
        (alpha, beta)
    } else {
        (fallback_alpha, fallback_beta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::calibration::{BucketKey, DurationBucket, PriceZone},
        enums::common::MarketCategory,
    };
    use rust_decimal_macros::dec;

    fn make_entry(total: u32, correct: u32) -> CalibrationEntry {
        CalibrationEntry {
            bucket_key: BucketKey {
                category: MarketCategory::Other,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
            },
            total_count: total,
            correct_count: correct,
            alpha_prior: dec!(2),
            beta_prior: dec!(0.2),
            fallback_tier: 1,
        }
    }

    #[test]
    fn fewer_than_three_qualified_returns_fallback() {
        let entries = vec![make_entry(10, 9), make_entry(10, 8)];
        let (a, b) = estimate_mom_prior(&entries, 10, dec!(2), dec!(0.2));
        assert_eq!(a, dec!(2));
        assert_eq!(b, dec!(0.2));
    }

    #[test]
    fn three_entries_with_variance_produces_valid_priors() {
        let entries = vec![make_entry(20, 18), make_entry(20, 16), make_entry(20, 19)];
        let (a, b) = estimate_mom_prior(&entries, 5, dec!(2), dec!(0.2));
        assert!(a > Decimal::ZERO);
        assert!(b > Decimal::ZERO);
    }

    #[test]
    fn zero_variance_returns_fallback() {
        let entries = vec![make_entry(10, 9), make_entry(10, 9), make_entry(10, 9)];
        let (a, b) = estimate_mom_prior(&entries, 5, dec!(2), dec!(0.2));
        assert_eq!(a, dec!(2));
        assert_eq!(b, dec!(0.2));
    }

    #[test]
    fn entries_below_min_samples_filtered_out() {
        let entries = vec![make_entry(3, 3), make_entry(3, 2), make_entry(3, 3)];
        let (a, b) = estimate_mom_prior(&entries, 10, dec!(2), dec!(0.2));
        assert_eq!(a, dec!(2));
        assert_eq!(b, dec!(0.2));
    }
}
