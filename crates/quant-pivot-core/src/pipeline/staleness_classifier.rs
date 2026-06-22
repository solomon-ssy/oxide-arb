//! Book staleness classification against the runtime staleness ladder.
//!
//! A single classifier instance is built at startup and **cloned** into every
//! consumer (scanner, validator); clones share one hot-swappable threshold
//! snapshot, so a runtime-config activation updates all consumers atomically
//! through [`StalenessClassifier::reload`].

use arc_swap::ArcSwap;
use quant_pivot_models::{enums::common::StalenessLevel, runtime_config::DataQualityConfig};
use std::sync::Arc;

/// Maps an age (in milliseconds) to a `StalenessLevel` using config thresholds.
///
/// Thresholds form a cascade: Fresh < Acceptable < Stale < Expired.
struct StalenessThresholds {
    fresh: u64,
    acceptable: u64,
    stale: u64,
    expired: u64,
}

impl StalenessThresholds {
    const fn from_config(config: &DataQualityConfig) -> Self {
        Self {
            fresh: config.max_book_age_ms / 2,
            acceptable: config.max_book_age_ms,
            stale: config.max_book_age_ms.saturating_mul(2),
            expired: config.max_book_age_ms.saturating_mul(4),
        }
    }
}

#[derive(Clone)]
pub struct StalenessClassifier {
    thresholds: Arc<ArcSwap<StalenessThresholds>>,
}

impl StalenessClassifier {
    #[must_use]
    pub fn new(config: &DataQualityConfig) -> Self {
        Self {
            thresholds: Arc::new(ArcSwap::from_pointee(StalenessThresholds::from_config(
                config,
            ))),
        }
    }

    /// Hot-reload the staleness ladder (runtime-config activation). All clones
    /// of this classifier observe the new thresholds on their next read.
    pub fn reload(&self, config: &DataQualityConfig) {
        self.thresholds
            .store(Arc::new(StalenessThresholds::from_config(config)));
    }

    #[inline]
    pub fn classify(&self, age_ms: u64) -> StalenessLevel {
        let thresholds = self.thresholds.load();
        if age_ms <= thresholds.fresh {
            StalenessLevel::Fresh
        } else if age_ms <= thresholds.acceptable {
            StalenessLevel::Acceptable
        } else if age_ms <= thresholds.stale {
            StalenessLevel::Stale
        } else {
            StalenessLevel::Expired
        }
    }

    /// Return the acceptable threshold (Fresh + Acceptable pass [`BookGate`]).
    #[inline]
    pub fn acceptable_ms(&self) -> u64 {
        self.thresholds.load().acceptable
    }

    /// Return the expired threshold (legacy alias).
    #[inline]
    pub fn expired_ms(&self) -> u64 {
        self.thresholds.load().expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> DataQualityConfig {
        DataQualityConfig {
            max_book_age_ms: 3_000,
            ..DataQualityConfig::default()
        }
    }

    #[test]
    fn classify_levels() {
        let c = StalenessClassifier::new(&test_config());
        assert_eq!(c.classify(500), StalenessLevel::Fresh);
        assert_eq!(c.classify(1000), StalenessLevel::Fresh);
        assert_eq!(c.classify(2000), StalenessLevel::Acceptable);
        assert_eq!(c.classify(3000), StalenessLevel::Acceptable);
        assert_eq!(c.classify(4000), StalenessLevel::Stale);
        assert_eq!(c.classify(5000), StalenessLevel::Stale);
        assert_eq!(c.classify(5001), StalenessLevel::Expired);
        assert_eq!(c.classify(9000), StalenessLevel::Expired);
        assert_eq!(c.classify(10001), StalenessLevel::Expired);
    }

    #[test]
    fn expired_threshold_accessor() {
        let c = StalenessClassifier::new(&test_config());
        assert_eq!(c.expired_ms(), 10000);
    }

    #[test]
    fn reload_propagates_to_clones() {
        let original = StalenessClassifier::new(&test_config());
        let clone = original.clone();
        original.reload(&DataQualityConfig {
            max_book_age_ms: 20,
            ..DataQualityConfig::default()
        });
        assert_eq!(clone.classify(15), StalenessLevel::Acceptable);
        assert_eq!(clone.acceptable_ms(), 20);
    }
}
