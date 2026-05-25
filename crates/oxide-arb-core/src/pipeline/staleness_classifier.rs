use oxide_arb_models::config::MarketDataConfig;
use oxide_arb_models::enums::common::StalenessLevel;

/// Maps an age (in milliseconds) to a `StalenessLevel` using config thresholds.
///
/// Thresholds form a cascade: Fresh < Acceptable < Stale < Expired.
struct StalenessThresholds {
    fresh: u64,
    acceptable: u64,
    stale: u64,
    expired: u64,
}

pub struct StalenessClassifier {
    thresholds: StalenessThresholds,
}

impl StalenessClassifier {
    pub const fn new(config: &MarketDataConfig) -> Self {
        Self {
            thresholds: StalenessThresholds {
                fresh: config.staleness_fresh_ms,
                acceptable: config.staleness_acceptable_ms,
                stale: config.staleness_stale_ms,
                expired: config.staleness_expired_ms,
            },
        }
    }

    pub const fn classify(&self, age_ms: u64) -> StalenessLevel {
        if age_ms <= self.thresholds.fresh {
            StalenessLevel::Fresh
        } else if age_ms <= self.thresholds.acceptable {
            StalenessLevel::Acceptable
        } else if age_ms <= self.thresholds.stale {
            StalenessLevel::Stale
        } else {
            StalenessLevel::Expired
        }
    }

    /// Return the expired threshold (used by `BookGate`).
    pub const fn expired_ms(&self) -> u64 {
        self.thresholds.expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MarketDataConfig {
        MarketDataConfig {
            staleness_fresh_ms: 1000,
            staleness_acceptable_ms: 3000,
            staleness_stale_ms: 5000,
            staleness_expired_ms: 10000,
            ..Default::default()
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
}
