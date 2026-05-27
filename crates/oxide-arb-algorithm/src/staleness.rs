//! Staleness-based confidence discount for orderbook data.
//!
//! The data layer classifies staleness before it reaches the algorithm.
//! This module provides a pure lookup from [`StalenessLevel`] to a
//! multiplicative discount factor applied during scoring.

use oxide_arb_models::{
    enums::common::StalenessLevel,
    types::{MICRO_SCALE, MicroProb},
};

const DISCOUNT_FRESH: MicroProb = MicroProb::from_micro(MICRO_SCALE);
const DISCOUNT_ACCEPTABLE: MicroProb = MicroProb::from_micro(950_000);
const DISCOUNT_STALE: MicroProb = MicroProb::from_micro(700_000);
const DISCOUNT_EXPIRED: MicroProb = MicroProb::ZERO;

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
    #[inline]
    pub const fn confidence_discount(level: StalenessLevel) -> MicroProb {
        match level {
            StalenessLevel::Fresh => DISCOUNT_FRESH,
            StalenessLevel::Acceptable => DISCOUNT_ACCEPTABLE,
            StalenessLevel::Stale => DISCOUNT_STALE,
            StalenessLevel::Expired => DISCOUNT_EXPIRED,
        }
    }

    /// Whether this staleness level permits trading at all.
    #[must_use]
    #[inline]
    pub const fn is_tradeable(level: StalenessLevel) -> bool {
        matches!(level, StalenessLevel::Fresh | StalenessLevel::Acceptable)
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

        assert!(fresh.micro() > acceptable.micro());
        assert!(acceptable.micro() > stale.micro());
        assert!(stale.micro() > expired.micro());
        assert_eq!(expired, MicroProb::ZERO);
    }

    #[test]
    fn expired_not_tradeable() {
        assert!(!StalenessPolicy::is_tradeable(StalenessLevel::Expired));
        assert!(StalenessPolicy::is_tradeable(StalenessLevel::Fresh));
        assert!(StalenessPolicy::is_tradeable(StalenessLevel::Acceptable));
        assert!(!StalenessPolicy::is_tradeable(StalenessLevel::Stale));
        assert!(!StalenessPolicy::is_tradeable(StalenessLevel::Expired));
    }
}
