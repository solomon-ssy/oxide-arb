//! Staleness-based confidence discount for orderbook data.
//!
//! The data layer classifies staleness before it reaches the algorithm.
//! This module provides a pure lookup from [`StalenessLevel`] to a
//! multiplicative discount factor applied during scoring.

use oxide_arb_models::enums::common::StalenessLevel;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Stateless staleness policy providing confidence discount factors.
pub struct StalenessPolicy;

impl StalenessPolicy {
    /// Confidence discount factor ∈ [0.0, 1.0] based on data freshness.
    ///
    /// - `Fresh`      → 1.00  (full confidence)
    /// - `Acceptable` → 0.95
    /// - `Stale`      → 0.70
    /// - `Expired`    → 0.00  (do not trade)
    #[must_use]
    pub const fn confidence_discount(level: StalenessLevel) -> Decimal {
        match level {
            StalenessLevel::Fresh => Decimal::ONE,
            StalenessLevel::Acceptable => dec!(0.95),
            StalenessLevel::Stale => dec!(0.70),
            StalenessLevel::Expired => Decimal::ZERO,
        }
    }

    /// Whether this staleness level permits trading at all.
    #[must_use]
    pub const fn is_tradeable(level: StalenessLevel) -> bool {
        !matches!(level, StalenessLevel::Expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discount_monotonically_decreasing() {
        let fresh = StalenessPolicy::confidence_discount(StalenessLevel::Fresh);
        let acceptable = StalenessPolicy::confidence_discount(StalenessLevel::Acceptable);
        let stale = StalenessPolicy::confidence_discount(StalenessLevel::Stale);
        let expired = StalenessPolicy::confidence_discount(StalenessLevel::Expired);

        assert!(fresh > acceptable);
        assert!(acceptable > stale);
        assert!(stale > expired);
        assert_eq!(expired, Decimal::ZERO);
    }

    #[test]
    fn expired_not_tradeable() {
        assert!(!StalenessPolicy::is_tradeable(StalenessLevel::Expired));
        assert!(StalenessPolicy::is_tradeable(StalenessLevel::Fresh));
        assert!(StalenessPolicy::is_tradeable(StalenessLevel::Acceptable));
        assert!(StalenessPolicy::is_tradeable(StalenessLevel::Stale));
    }
}
